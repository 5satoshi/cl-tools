#!/usr/bin/python

import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

import warnings
warnings.filterwarnings("ignore", message="Error importing Gtk module")

import graph_tool.all as gt
import matplotlib.cm

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("EdgeBetweennessSBM")

# -----------------------------
# Load edge betweenness from BigQuery
# -----------------------------
def load_edge_betweenness(tx_type='common', date=None):
    client = bigquery.Client()
    if date is None:
        date = (datetime.now(timezone.utc) - timedelta(days=1)).date()  # default yesterday
    logger.info(f"Loading '{tx_type}' edge betweenness for date: {date}")

    query = f"""
    SELECT source, destination, shortest_path_share
    FROM `lightning-fee-optimizer.version_1.edge_betweenness`
    WHERE DATE(timestamp) = '{date}' AND type = '{tx_type}'
    AND shortest_path_share != 0
    """
    df = client.query(query).to_dataframe()
    logger.info(f"Loaded {len(df)} edges from BigQuery (type='{tx_type}')")
    return df

# -----------------------------
# Load node aliases from BigQuery
# -----------------------------
def load_node_aliases():
    client = bigquery.Client()
    logger.info("Loading node aliases from BigQuery...")
    query = """
    SELECT nodeid, alias
    FROM `lightning-fee-optimizer.version_1.nodes`
    """
    try:
        df = client.query(query).to_dataframe()
        return dict(zip(df['nodeid'], df['alias'].fillna('Unknown')))
    except Exception as e:
        logger.warning(f"Could not load aliases: {e}")
        return {}

# -----------------------------
# Build directed graph
# -----------------------------
def build_directed_graph(df):
    g = gt.Graph(directed=True)
    logger.info("Building directed graph...")

    # Map node IDs to vertices
    nodes = pd.unique(df[['source', 'destination']].values.ravel())
    node_map = {node_id: g.add_vertex() for node_id in nodes}
    vertex_to_id = {v: k for k, v in node_map.items()}

    # Add edges with shortest_path_share as weight
    weight_prop = g.new_edge_property("double")
    for _, row in df.iterrows():
        src_v = node_map[row['source']]
        dst_v = node_map[row['destination']]
        e = g.add_edge(src_v, dst_v)
        weight_prop[e] = float(row['shortest_path_share'])

    g.ep['weight'] = weight_prop
    logger.info(f"Graph created with {g.num_vertices()} nodes and {g.num_edges()} edges")
    return g, node_map, vertex_to_id

def print_community_summary(state):
    from collections import Counter
    
    blocks = state.get_levels()[0].get_blocks()
    counts = Counter(int(blocks[v]) for v in state.g.vertices())

    levels = state.get_levels()

    for i, level in enumerate(levels):
        blocks = level.get_blocks()
        counts = Counter(int(blocks[v]) for v in level.g.vertices())

        print(f"\n--- Level {i} ---")
        print(f"Blocks: {level.get_B()}")

        for block_id, size in sorted(counts.items()):
            print(f"  Block {block_id}: {size} nodes")


# -----------------------------
# Run nested SBM and draw to PNG
# -----------------------------
def run_sbm_and_draw(g, vertex_to_id, node_aliases, output_file="sbm_graph.pdf"):
    logger.info("Running nested SBM inference...")
    gt.seed_rng(47)

    # Setup state arguments for weighted edges
    sargs = dict(recs=[g.ep.weight], rec_types=["real-exponential"])
    state = gt.minimize_nested_blockmodel_dl(g, state_args=sargs)
    print_community_summary(state)

    logger.info(f"Drawing graph to: {output_file}")
    
    # Calculate weighted degree (sum of edge betweenness) for each vertex to use as node size
    v_weight = g.degree_property_map("total", weight=g.ep.weight)
    
    # Identify and log the most important nodes
    node_scores = [(vertex_to_id[v], v_weight[v]) for v in g.vertices()]
    node_scores.sort(key=lambda x: x[1], reverse=True)
    logger.info("Top 10 most important nodes (by total shortest path share):")
    for i, (node_id, score) in enumerate(node_scores[:10]):
        alias = node_aliases.get(node_id, "Unknown")
        logger.info(f"  {i+1}. {node_id} ({alias}): {score:.4f}")

    state.draw(
        output=output_file,
        output_size=(2000, 2000),
        vertex_size=gt.prop_to_size(v_weight, 2, 15, power=1, log=True),
        vertex_fill_color=state.get_bs()[0], # Color nodes by their bottom-level SBM block/community
        edge_color=gt.prop_to_size(
            g.ep.weight,
            power=1,
            log=True
        ),
        ecmap=(matplotlib.cm.inferno, 0.6),
        eorder=g.ep.weight,
        edge_pen_width=gt.prop_to_size(
            g.ep.weight,
            0.5, 6,
            power=1,
            log=True
        ),
        edge_gradient=[]
    )
    logger.info("SBM graph saved successfully.")


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Nested SBM on edge_betweenness graph")
    parser.add_argument("--date", type=str, help="Date for edge betweenness (YYYY-MM-DD), default yesterday")
    parser.add_argument("--type", type=str, default="common", help="Transaction type to load (default: common)")
    parser.add_argument("--output", type=str, default="sbm_graph.pdf", help="Output file name")
    args = parser.parse_args()

    # Load data and build graph
    df_edges = load_edge_betweenness(tx_type=args.type, date=args.date)
    node_aliases = load_node_aliases()
    g, node_map, vertex_to_id = build_directed_graph(df_edges)

    # Run SBM and save PNG
    run_sbm_and_draw(g, vertex_to_id, node_aliases, output_file=args.output)
