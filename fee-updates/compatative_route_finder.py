#!/usr/bin/python

import sys, math, os, random, logging, argparse
import graph_tool.all as gt
from graph_helper import load_or_fetch_graph, get_filtered_graph_and_node

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("CompatativeRouteFinder")

def run_compatative_route_finder(mynode, seed=42, refresh_graph=False):
    random.seed(seed)
    
    tx_sat_cent = 80000
    DG, mynode_v = get_filtered_graph_and_node(mynode, refresh_graph=refresh_graph, tx_sat_cent=tx_sat_cent)
    if mynode_v is None:
        logger.error("Node not found in the largest component of the graph.")
        return None
        
    v_id = DG.vertex_properties["id"]

    e_base_fee = DG.edge_properties["base_fee_millisatoshi"]
    e_fee_rate = DG.edge_properties["fee_per_millionth"]
    
    for e in mynode_v.out_edges():
        e_base_fee[e] = 0
        
    e_weight = DG.new_edge_property("double")
    e_epsilon = DG.new_edge_property("double")
    for e in DG.edges():
        e_epsilon[e] = random.uniform(0.0001, 0.00011)

    # 1. Compute centrality for PPM = 1
    logger.info("Computing centrality for PPM = 1...")
    for e in mynode_v.out_edges():
        e_fee_rate[e] = 1
        
    for e in DG.edges():
        a = e_base_fee[e]
        b = e_fee_rate[e] / 1000000.0
        e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + e_epsilon[e]
        
    _, e_betw_1 = gt.betweenness(DG, weight=e_weight, norm=False)

    # 2. Compute centrality for PPM = 1,000,000
    logger.info("Computing centrality for PPM = 1,000,000...")
    for e in mynode_v.out_edges():
        e_fee_rate[e] = 1000000
        
    for e in DG.edges():
        a = e_base_fee[e]
        b = e_fee_rate[e] / 1000000.0
        e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + e_epsilon[e]
        
    _, e_betw_1M = gt.betweenness(DG, weight=e_weight, norm=False)

    # 3. Build competitive graph
    logger.info("Building competitive graph...")
    e_diff = DG.new_edge_property("int")
    e_comp_filt = DG.new_edge_property("bool")
    
    for e in DG.edges():
        diff = int(round(e_betw_1M[e])) - int(round(e_betw_1[e]))
        e_diff[e] = diff
        e_comp_filt[e] = (diff > 0)
        
    # Filter edges where diff > 0
    comp_G_edges = gt.GraphView(DG, efilt=e_comp_filt)
    
    # Filter vertices to only keep those with at least one edge remaining
    v_comp_filt = comp_G_edges.new_vertex_property("bool")
    for v in comp_G_edges.vertices():
        v_comp_filt[v] = (v.in_degree() + v.out_degree()) > 0
        
    CompetitiveGraph = gt.GraphView(comp_G_edges, vfilt=v_comp_filt)
    
    # Store the competitive difference as an edge property on the final graph
    CompetitiveGraph.edge_properties["competitive_count"] = e_diff
    
    logger.info(f"Original Graph: {DG.num_vertices()} nodes, {DG.num_edges()} edges.")
    logger.info(f"Competitive Graph built: {CompetitiveGraph.num_vertices()} nodes, {CompetitiveGraph.num_edges()} edges.")
    
    # Calculate statistics
    edge_stats = []
    e_short_id = DG.edge_properties["short_channel_id"]
    for e in CompetitiveGraph.edges():
        edge_stats.append((e_diff[e], v_id[e.source()], v_id[e.target()], e_short_id[e], e_base_fee[e], e_fee_rate[e]))
        
    edge_stats.sort(reverse=True, key=lambda x: x[0])
    
    logger.info("Top 5 Most Competitive Edges:")
    for i, (count, src, tgt, scid, base_fee, fee_rate) in enumerate(edge_stats[:5]):
        direction = "Outbound" if src == mynode else "Inbound" if tgt == mynode else "Other"
        logger.info(f"  {i+1}. {scid} [{direction}] (Count: {count}) | {src[:8]}... -> {tgt[:8]}... | Base: {int(base_fee)} msat, Rate: {int(fee_rate)} ppm")
        
    node_scores = {}
    for count, src, tgt, scid, _, _ in edge_stats:
        node_scores[src] = node_scores.get(src, 0) + count
        
    if node_scores:
        most_comp_node = max(node_scores.items(), key=lambda x: x[1])
        logger.info(f"Most Competitive Node: {most_comp_node[0]} (Score: {most_comp_node[1]})")
    
    return CompetitiveGraph

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for tie-breaking epsilon")
    parser.add_argument("--refresh-graph", action="store_true", help="Fetch a new graph from the node instead of using cache")
    args = parser.parse_args()
    
    run_compatative_route_finder(args.node, args.seed, args.refresh_graph)
