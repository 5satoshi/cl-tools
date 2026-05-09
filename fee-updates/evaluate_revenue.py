#!/usr/bin/python

import os
import math
import random
import logging
import json
import argparse
import graph_tool.all as gt
from graph_helper import load_or_fetch_graph, get_filtered_graph_and_node

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("EvaluateRevenue")

def evaluate_revenue(mynode, input_json, seed=42, refresh_graph=False):
    random.seed(seed)
    
    if not os.path.exists(input_json):
        logger.error(f"Input JSON file not found: {input_json}")
        return
        
    with open(input_json, 'r') as f:
        best_ppms = json.load(f)
        
    compare_ppms = None
    if input_json != "best_ppms.json" and os.path.exists("best_ppms.json"):
        with open("best_ppms.json", 'r') as f:
            compare_ppms = json.load(f)
            
    tx_sat_cent = 80000
    DG, mynode_v = get_filtered_graph_and_node(mynode, refresh_graph=refresh_graph, tx_sat_cent=tx_sat_cent)
    if mynode_v is None:
        logger.error("Node not found in the largest component of the graph.")
        return
    
    v_id = DG.vertex_properties["id"]
    
    e_base_fee = DG.edge_properties["base_fee_millisatoshi"]
    e_fee_rate = DG.edge_properties["fee_per_millionth"]
    e_short_id = DG.edge_properties["short_channel_id"]
    
    e_weight = DG.new_edge_property("double")
    e_epsilon = DG.new_edge_property("double")
    for e in DG.edges():
        e_epsilon[e] = random.uniform(0.0001, 0.00011)

    # 1. Evaluate Baseline Policy
    if compare_ppms is not None:
        logger.info("Computing betweenness centrality for BASELINE (best_ppms.json) policy...")
        for e in mynode_v.out_edges():
            ch_id = e_short_id[e]
            e_base_fee[e] = 0
            e_fee_rate[e] = int(compare_ppms.get(ch_id, 1))
    else:
        logger.info("Computing betweenness centrality for CURRENT policy...")

    original_ppms = {}
    for e in DG.edges():
        a = e_base_fee[e]
        b = e_fee_rate[e] / 1000000.0
        e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + e_epsilon[e]
        
    _, e_betw_current = gt.betweenness(DG, weight=e_weight, norm=False)
    
    _, pred_map_curr = gt.shortest_distance(DG, source=mynode_v, weights=e_weight, pred_map=True)
    e_counts_curr = DG.new_edge_property("int")
    for v in DG.vertices():
        if v == mynode_v:
            continue
        curr = v
        while curr != mynode_v:
            p = pred_map_curr[curr]
            if p == int(curr):
                break
            v_p = DG.vertex(p)
            e_edge = DG.edge(v_p, curr)
            if e_edge:
                e_counts_curr[e_edge] += 1
            curr = v_p
            
    for e in mynode_v.out_edges():
        original_ppms[e_short_id[e]] = int(e_fee_rate[e])

    # 2. Evaluate Optimized Policy
    logger.info("Computing betweenness centrality for OPTIMIZED policy...")
    applied_ppms = {}
    for e in mynode_v.out_edges():
        ch_id = e_short_id[e]
        e_base_fee[e] = 0
        
        # Default to 1 if channel wasn't in the JSON for some reason
        ppm = int(best_ppms.get(ch_id, 1))
        e_fee_rate[e] = ppm
        applied_ppms[ch_id] = ppm
        
    for e in DG.edges():
        a = e_base_fee[e]
        b = e_fee_rate[e] / 1000000.0
        e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + e_epsilon[e]
        
    _, e_betw_opt = gt.betweenness(DG, weight=e_weight, norm=False)
    
    _, pred_map_opt = gt.shortest_distance(DG, source=mynode_v, weights=e_weight, pred_map=True)
    e_counts_opt = DG.new_edge_property("int")
    for v in DG.vertices():
        if v == mynode_v:
            continue
        curr = v
        while curr != mynode_v:
            p = pred_map_opt[curr]
            if p == int(curr):
                break
            v_p = DG.vertex(p)
            e_edge = DG.edge(v_p, curr)
            if e_edge:
                e_counts_opt[e_edge] += 1
            curr = v_p
            
    sum_cent_curr = 0
    sum_rev_curr = 0
    sum_cent_opt = 0
    sum_rev_opt = 0
    
    policy_name = "Baseline" if compare_ppms is not None else "Current"
    print(f"\n=== Revenue Evaluation ({policy_name} vs Optimized) ===")
    print(f"{'Channel':<15} | {f'{policy_name} (PPM/Cent/Self/Rev)':<30} | {'Optimized (PPM/Cent/Self/Rev)':<30} | {'Rev Diff':<10} | {'Cent Diff'}")
    print("-" * 110)
    
    for e in mynode_v.out_edges():
        ch_id = e_short_id[e]
        
        # Current stats
        curr_ppm = original_ppms[ch_id]
        curr_cent = max(0, int(round(e_betw_current[e])) - e_counts_curr[e])
        curr_rev = curr_cent * curr_ppm
        sum_cent_curr += curr_cent
        sum_rev_curr += curr_rev
        
        # Optimized stats
        opt_ppm = applied_ppms[ch_id]
        opt_cent = max(0, int(round(e_betw_opt[e])) - e_counts_opt[e])
        opt_rev = opt_cent * opt_ppm
        sum_cent_opt += opt_cent
        sum_rev_opt += opt_rev
        
        rev_diff = opt_rev - curr_rev
        diff_str = f"+{rev_diff}" if rev_diff >= 0 else f"{rev_diff}"
        
        cent_diff = opt_cent - curr_cent
        cent_diff_str = f"+{cent_diff}" if cent_diff >= 0 else f"{cent_diff}"
        
        curr_str = f"{curr_ppm} / {curr_cent} / {e_counts_curr[e]} / {curr_rev}"
        opt_str = f"{opt_ppm} / {opt_cent} / {e_counts_opt[e]} / {opt_rev}"
        
        print(f"{ch_id:<15} | {curr_str:<30} | {opt_str:<30} | {diff_str:<10} | {cent_diff_str}")
        
    print("-" * 110)
    print(f"{'Total':<15} | {'- / ' + str(sum_cent_curr) + ' / - / ' + str(sum_rev_curr):<30} | {'- / ' + str(sum_cent_opt) + ' / - / ' + str(sum_rev_opt):<30} | {(sum_rev_opt - sum_rev_curr):<+10d} | {(sum_cent_opt - sum_cent_curr):+d}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69")
    parser.add_argument("--input-json", type=str, default="best_ppms.json", help="JSON file containing best PPMs")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for tie-breaking epsilon")
    parser.add_argument("--refresh-graph", action="store_true", help="Fetch a new graph from the node instead of using cache")
    args = parser.parse_args()
    
    evaluate_revenue(args.node, args.input_json, args.seed, args.refresh_graph)
