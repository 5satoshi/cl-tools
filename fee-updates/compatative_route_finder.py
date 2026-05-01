#!/usr/bin/python

import sys, math, os, random, logging, argparse
import graph_tool.all as gt
from graph_helper import get_graph_from_cli

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("CompatativeRouteFinder")

def run_compatative_route_finder(mynode, seed=42):
    random.seed(seed)
    
    rpc = os.environ.get('HOME', '') + "/.lightning/bitcoin/lightning-rpc"
    G = get_graph_from_cli(rpc)
    
    tx_sat_cent = 80000
    tx_msat = tx_sat_cent * 1000
    
    e_active = G.edge_properties["active"]
    e_htlc_max = G.edge_properties["htlc_maximum_msat"]
    
    e_filt_init = G.new_edge_property("bool")
    e_filt_init.a = e_active.a & (e_htlc_max.a >= tx_msat)
    
    wDG = gt.GraphView(G, efilt=e_filt_init)
    
    comp, hist = gt.label_components(wDG)
    largest_comp = hist.argmax()
    v_filt_init = wDG.new_vertex_property("bool")
    v_filt_init.a = (comp.a == largest_comp)
    DG = gt.GraphView(wDG, vfilt=v_filt_init)
    
    v_id = DG.vertex_properties["id"]
    try:
        mynode_v = gt.find_vertex(DG, v_id, mynode)[0]
    except IndexError:
        logger.error("Node not found in the largest component of the graph.")
        return None

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
        diff = int(round(e_betw_1[e])) - int(round(e_betw_1M[e]))
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
    
    return CompetitiveGraph

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for tie-breaking epsilon")
    args = parser.parse_args()
    
    run_compatative_route_finder(args.node, args.seed)
