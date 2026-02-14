#!/usr/bin/python

import logging
import random
import time
import pandas as pd
from google.cloud import bigquery
from graph_tool.all import Graph, GraphView, betweenness
from graph_tool.search import bfs_search, BFSVisitor
from graph_tool.topology import label_components
import os

# -----------------------------
# Graph-tool threads setup
# -----------------------------
# Use all available cores or limit to a reasonable number
os.environ["OMP_NUM_THREADS"] = "4"  # adjust to your CPU cores

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,  # change to DEBUG for detailed per-edge logs
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("LightningCentrality")

# -----------------------------
# Timing decorator
# -----------------------------
def log_time(func):
    def wrapper(*args, **kwargs):
        logger_func = kwargs.get("logger", logger)
        start = time.time()
        result = func(*args, **kwargs)
        stop = time.time()
        logger_func.debug(f"{func.__name__} completed in {stop - start:.2f}s")
        return result
    return wrapper

# -----------------------------
# Data loading
# -----------------------------
def load_data(logger):
    try:
        client = bigquery.Client()
        logger.info("Connected to BigQuery")

        channels = client.query("SELECT * FROM `lightning-fee-optimizer.version_1.channels`").to_dataframe()
        nodes = client.query("SELECT * FROM `lightning-fee-optimizer.version_1.nodes`").to_dataframe()
        logger.info(f"Loaded {len(channels)} channels and {len(nodes)} nodes")

        channels['htlc_maximum_msat'] = channels['htlc_maximum_msat'].astype(int)
        channels['htlc_minimum_msat'] = channels['htlc_minimum_msat'].astype(int)
        latest_update = channels['last_update'].max()

        return channels, nodes, latest_update

    except Exception:
        logger.exception("Failed to load BigQuery tables")
        raise

# -----------------------------
# Graph building + edge lookup
# -----------------------------

def largest_scc_subgraph(g, logger=None):
    comp, hist = label_components(g, directed=True)
    largest_idx = hist.argmax()
    vertices_to_keep = [int(v) for v in g.vertices() if comp[v] == largest_idx]
    vfilt = g.new_vertex_property("bool")
    vfilt.a = False
    for v in vertices_to_keep:
        vfilt[v] = True
    sub_g = GraphView(g, vfilt=vfilt)
    if logger:
        logger.info(f"Largest SCC: {sub_g.num_vertices()} nodes, {sub_g.num_edges()} edges")
    return sub_g


def build_graph(channels, nodes, logger):
    try:
        g = Graph(directed=True)
        node_map = {node_id: g.add_vertex() for node_id in nodes['nodeid']}
        vertex_to_id = {v: k for k, v in node_map.items()}

        fee_prop = g.new_edge_property("double")
        edge_lookup = {}

        for _, row in channels[channels.active].iterrows():
            src = row['source']
            dst = row['destination']
            edge_lookup[(src, dst)] = {
                'base_fee': row['base_fee_millisatoshi'],
                'ppm': row['fee_per_millionth'] / 1_000_000,
                'htlc_min': row['htlc_minimum_msat'],
                'htlc_max': row['htlc_maximum_msat']
            }
            e = g.add_edge(node_map[src], node_map[dst])
            fee_prop[e] = 0

        g.ep['fee'] = fee_prop
        logger.info(f"Graph created with {g.num_vertices()} nodes and {g.num_edges()} edges")
        return g, vertex_to_id, edge_lookup

    except Exception:
        logger.exception("Failed to build Graph-tool graph")
        raise

# -----------------------------
# Update edge fees + HTLC filter
# -----------------------------
def update_fees_and_filter(g, edge_lookup, vertex_to_id, tx_sat, tx_type):
    edge_filter = g.new_edge_property("bool")
    for e in g.edges():
        src_id = vertex_to_id[e.source()]
        dst_id = vertex_to_id[e.target()]
        edge = edge_lookup[(src_id, dst_id)]

        # Compute float fee with small random offset
        random_offset = random.uniform(0, 1)
        g.ep['fee'][e] = edge['base_fee'] + tx_sat * edge['ppm'] * 1000 + random_offset

        edge_filter[e] = (edge['htlc_max'] > tx_sat*1000) and (edge['htlc_min'] < tx_sat*1000)

        logger.debug(f"[{tx_type}] Edge {src_id}->{dst_id}, fee={g.ep['fee'][e]:.2f}, passes_HTLC={edge_filter[e]}")

    logger.info(f"[{tx_type}] Updated fees and filtered edges")
    return GraphView(g, efilt=edge_filter)

# -----------------------------
# Test subgraph
# -----------------------------

def get_neighbors_bfs(g, start_vertex=None, k_hops=2, max_vertices=None):
    """
    Return a set of vertex indices that are within k_hops of start_vertex.
    
    Parameters:
    - g: Graph or GraphView
    - start_vertex: optional starting vertex (defaults to first vertex)
    - k_hops: maximum BFS depth
    - max_vertices: optional maximum number of vertices to return
    
    Returns:
    - Set of vertex indices
    """

    if start_vertex is None:
        start_vertex = list(g.vertices())[0]

    visited = set()
    distance = {}

    start_index = int(start_vertex)
    visited.add(start_index)
    distance[start_index] = 0

    class Visitor(BFSVisitor):
        def tree_edge(self, e):
            src = int(e.source())
            tgt = int(e.target())
            distance[tgt] = distance[src] + 1
            if distance[tgt] <= k_hops:
                visited.add(tgt)

    bfs_search(g, start_vertex, Visitor())

    selected = list(visited)
    if max_vertices is not None:
        selected = selected[:max_vertices]

    return set(selected)

def get_test_subgraph(g_filtered, logger, k_hops=2, max_vertices=200):
    """
    Build a connected subgraph for TEST_MODE using BFS.
    """
    vertices_to_keep = get_neighbors_bfs(
        g_filtered, k_hops=k_hops, max_vertices=max_vertices
    )

    vfilt = g_filtered.new_vertex_property("bool")
    vfilt.a = False
    for v in vertices_to_keep:
        vfilt[g_filtered.vertex(v)] = True

    sub_g = GraphView(g_filtered, vfilt=vfilt)

    logger.info(
        f"TEST_MODE subgraph: {sub_g.num_vertices()} nodes, "
        f"{sub_g.num_edges()} edges (k={k_hops})"
    )
    return sub_g

# -----------------------------
# Betweenness
# -----------------------------

@log_time
def compute_betweenness(g_sub, tx_type, logger):
    logger.info(f"[{tx_type}] Computing betweenness (nodes + edges)")
    v_betw, e_betw = betweenness(g_sub, weight=g_sub.ep['fee'])
    logger.info(f"[{tx_type}] Betweenness computation finished")
    return v_betw, e_betw

# -----------------------------
# Node betweenness
# -----------------------------
def process_node_betweenness(g_sub, v_betw, tx_type, nodes_df, latest_update, vertex_to_id, logger):
    try:
        logger.info(f"[{tx_type}] Computing node betweenness")
        df = pd.DataFrame({
            'nodeid': [vertex_to_id[v] for v in g_sub.vertices()],
            'shortest_path_share': list(v_betw)
        })
        df['rank'] = df['shortest_path_share'].rank(method='min', ascending=False)
        df = df.join(nodes_df[['nodeid', 'alias']].set_index('nodeid'), on='nodeid')
        df["timestamp"] = latest_update
        df["type"] = tx_type
        logger.info(f"[{tx_type}] Node betweenness computed for {len(df)} nodes")
        return df
    except Exception:
        logger.exception(f"[{tx_type}] Node betweenness failed")
        return pd.DataFrame()

# -----------------------------
# Edge betweenness
# -----------------------------
def process_edge_betweenness(g_sub, e_betw, tx_type, latest_update, vertex_to_id, channels, logger):
    try:
        logger.info(f"[{tx_type}] Computing edge betweenness")
        data = [(vertex_to_id[e.source()], vertex_to_id[e.target()], e_betw[e]) for e in g_sub.edges()]
        df = pd.DataFrame(data, columns=['source', 'destination', 'shortest_path_share'])
        df = pd.merge(df, channels[channels.active], on=['source', 'destination'], how='left')
        df['rank'] = df['shortest_path_share'].rank(method='min', ascending=False)
        df["timestamp"] = latest_update
        df["type"] = tx_type
        logger.info(f"[{tx_type}] Edge betweenness computed for {len(df)} edges")
        return df
    except Exception:
        logger.exception(f"[{tx_type}] Edge betweenness failed")
        return pd.DataFrame()

# -----------------------------
# Main pipeline
# -----------------------------
def run_pipeline(TEST_MODE=True, logger=logger):
    logger.info("Starting Lightning fee centrality computation")

    channels, nodes, latest_update = load_data(logger)
    g, vertex_to_id, edge_lookup = build_graph(channels, nodes, logger)

    tx_types = [
        ("common", 80000),
        ("micro", 200),
        ("macro", 4000000)
    ]

    for tx_type, tx_sat in tx_types:
        logger.info(f"Processing tx_type={tx_type} ({tx_sat} sat)")

        try:
            g_sub = update_fees_and_filter(g, edge_lookup, vertex_to_id, tx_sat, tx_type)
            g_sub = largest_scc_subgraph(g_sub, logger)
            
            if TEST_MODE:
                g_sub = get_test_subgraph(g_sub, logger, k_hops=3, max_vertices=200)
            
            logger.info(f"[{tx_type}] Largest SCC: {g_sub.num_vertices()} nodes, {g_sub.num_edges()} edges")
            
            # Compute once
            v_betw, e_betw = compute_betweenness(g_sub, tx_type, logger)

            # Node betweenness
            nodescores = process_node_betweenness(g_sub, v_betw, tx_type, nodes, latest_update, vertex_to_id, logger)
            if not nodescores.empty and not TEST_MODE:
                nodescores.to_gbq(
                    "lightning-fee-optimizer.version_1.betweenness",
                    if_exists='append'
                )
                logger.info(f"[{tx_type}] Node betweenness written to BigQuery")

            # Edge betweenness for all tx_types, append to BigQuery
            edgescores = process_edge_betweenness(g_sub, e_betw, tx_type, latest_update, vertex_to_id, channels, logger)
            if not edgescores.empty and not TEST_MODE:
                edgescores.to_gbq(
                    "lightning-fee-optimizer.version_1.edge_betweenness",
                    if_exists='append'
                )
                logger.info(f"[{tx_type}] Edge betweenness written to BigQuery")

        except Exception:
            logger.exception(f"Failed processing tx_type={tx_type}")
            continue

    logger.info("Lightning fee centrality computation completed")

# -----------------------------
# Main guard
# -----------------------------
if __name__ == "__main__":
    run_pipeline(TEST_MODE=True)

