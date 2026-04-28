#!/usr/bin/python

import os
import math
import random
import logging
import json
import argparse
import graph_tool.all as gt
from graph_helper import get_graph_from_cli

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("EvaluateRevenue")

def evaluate_revenue(mynode, input_json):
    if not os.path.exists(input_json):
        logger.error(f"Input JSON file not found: {input_json}")
        return
        
    with open(input_json, 'r') as f:
        best_ppms = json.load(f)
        
    rpc = os.environ.get('HOME', '') + "/.lightning/bitcoin/lightning-rpc"
    G = get_graph_from_cli(rpc)
    
    e_active = G.edge_properties["active"]
    wDG = gt.GraphView(G, efilt=e_active)
    
    comp, hist = gt.label_components(wDG)
    largest_comp = hist.argmax()
    v_filt = wDG.new_vertex_property("bool")
    v_filt.a = (comp.a == largest_comp)
    DG = gt.GraphView(wDG, vfilt=v_filt)
    
    v_id = DG.vertex_properties["id"]
    
    try:
        mynode_v = gt.find_vertex(DG, v_id, mynode)[0]
    except IndexError:
        logger.error("Node not found in the largest component of the graph.")
        return
    
    e_base_fee = DG.edge_properties["base_fee_millisatoshi"]
    e_fee_rate = DG.edge_properties["fee_per_millionth"]
    e_short_id = DG.edge_properties["short_channel_id"]
    
    # Apply the PPMs from JSON to mynode's out_edges
    applied_ppms = {}
    for e in mynode_v.out_edges():
        ch_id = e_short_id[e]
        e_base_fee[e] = 0
        
        # Default to 1 if channel wasn't in the JSON for some reason
        ppm = int(best_ppms.get(ch_id, 1))
        e_fee_rate[e] = ppm
        applied_ppms[ch_id] = ppm
        
    tx_sat_cent = 80000
    e_weight = DG.new_edge_property("double")
    e_epsilon = DG.new_edge_property("double")
    for e in DG.edges():
        e_epsilon[e] = random.uniform(0.0001, 0.00011)
        
    for e in DG.edges():
        a = e_base_fee[e]
        b = e_fee_rate[e] / 1000000.0
        e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + e_epsilon[e]
        
    logger.info("Computing betweenness centrality...")
    _, e_betw = gt.betweenness(DG, weight=e_weight, norm=False)
    
    sum_cent = 0
    sum_rev = 0
    
    print("\n=== Revenue Evaluation ===")
    for e in mynode_v.out_edges():
        ch_id = e_short_id[e]
        ppm = applied_ppms[ch_id]
        cent = int(round(e_betw[e]))
        revenue = cent * ppm
        
        sum_cent += cent
        sum_rev += revenue
        
        print(f"Channel {ch_id}: PPM = {ppm} | Centrality = {cent} | Revenue = {revenue}")
        
    print("---------------------------")
    print(f"Total Centrality: {sum_cent}")
    print(f"Total Revenue:    {sum_rev}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69")
    parser.add_argument("--input-json", type=str, default="best_ppms.json", help="JSON file containing best PPMs")
    args = parser.parse_args()
    
    evaluate_revenue(args.node, args.input_json)
