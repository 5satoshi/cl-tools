#!/usr/bin/python

import sys, math, os, random, logging
import graph_tool.all as gt
import matplotlib.pyplot as plt
from datetime import datetime
import argparse
from tqdm import tqdm
import csv
import numpy as np
import json
from graph_helper import load_or_fetch_graph

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("RouteFinder")


def run_centrality_sweep(mynode, input_csv=None, seed=42, refresh_graph=False):
    random.seed(seed)
    
    rpc = os.environ.get('HOME', '')+"/.lightning/bitcoin/lightning-rpc"
    G = load_or_fetch_graph(rpc, refresh=refresh_graph)
    
    tx_sat_cent = 80000
    tx_msat = tx_sat_cent * 1000
    
    e_active = G.edge_properties["active"]
    e_htlc_max = G.edge_properties["htlc_maximum_msat"]
    
    e_filt = G.new_edge_property("bool")
    e_filt.a = e_active.a & (e_htlc_max.a >= tx_msat)
    
    wDG = gt.GraphView(G, efilt=e_filt)
    
    # clean for connected component
    comp, hist = gt.label_components(wDG)
    largest_comp = hist.argmax()
    v_filt = wDG.new_vertex_property("bool")
    v_filt.a = (comp.a == largest_comp)
    DG = gt.GraphView(wDG, vfilt=v_filt)
    
    v_id = DG.vertex_properties["id"]
    mynode_v = gt.find_vertex(DG, v_id, mynode)[0]
    
    e_base_fee = DG.edge_properties["base_fee_millisatoshi"]
    e_fee_rate = DG.edge_properties["fee_per_millionth"]
    e_short_id = DG.edge_properties["short_channel_id"]
    e_satoshis = DG.edge_properties["satoshis"]
    
    # Set mynode base fees to 0 to focus strictly on PPM
    for e in mynode_v.out_edges():
        e_base_fee[e] = 0
        
    results = []
    e_weight = DG.new_edge_property("double")
    e_epsilon = DG.new_edge_property("double")
    for e in DG.edges():
        e_epsilon[e] = random.uniform(0.0001, 0.00011)
        
    csv_file = "centrality_sweep_results.csv"
    with open(csv_file, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Run_Number", "Channel", "PPM", "Edge_Centrality", "Self_Paths", "Revenue_Potential"])

    run_counter = [0]
    evaluated_uniform_cache = {}

    def evaluate_ppm(current_ppm):
        if current_ppm in evaluated_uniform_cache:
            return evaluated_uniform_cache[current_ppm]
            
        run_counter[0] += 1
        logger.info(f"Evaluating uniform PPM: {current_ppm}")
        for e in mynode_v.out_edges():
            e_fee_rate[e] = current_ppm
            
        for e in DG.edges():
            a = e_base_fee[e]
            b = e_fee_rate[e] / 1000000.0
            e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + e_epsilon[e]
            
        _, e_betw_temp = gt.betweenness(DG, weight=e_weight, norm=False)
        _, pred_map_temp = gt.shortest_distance(DG, source=mynode_v, weights=e_weight, pred_map=True)
        
        e_counts_temp = DG.new_edge_property("int")
        for v in DG.vertices():
            if v == mynode_v: continue
            curr = v
            while curr != mynode_v:
                p = pred_map_temp[curr]
                if p == int(curr): break
                v_p = DG.vertex(p)
                e_edge = DG.edge(v_p, curr)
                if e_edge: e_counts_temp[e_edge] += 1
                curr = v_p
                
        sum_cent = 0
        sum_rev = 0
        active_channels = 0
        iteration_results = []
        
        for e in mynode_v.out_edges():
            ch_id = e_short_id[e]
            cent = max(0, int(round(e_betw_temp[e])) - e_counts_temp[e])
            revenue = cent * current_ppm
            sum_cent += cent
            sum_rev += revenue
            if revenue > 0:
                active_channels += 1
            row = [run_counter[0], ch_id, current_ppm, f"{cent}", f"{e_counts_temp[e]}", f"{revenue}"]
            results.append(row)
            iteration_results.append(row)
            
        with open(csv_file, mode='a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(iteration_results)
            
        logger.info(f"Uniform PPM {current_ppm} completed | Sum Centrality: {sum_cent} | Total Revenue: {sum_rev} | Active Channels: {active_channels}")
        evaluated_uniform_cache[current_ppm] = (sum_cent, sum_rev, active_channels)
        return sum_cent, sum_rev, active_channels

    import heapq
    
    best_total_revenue = -1
    best_active_channels = -1
    best_ppm = -1
    
    logger.info("Starting exponential phase to discover intervals...")
    # Evaluate starting boundary
    cent_1, rev_1, act_1 = evaluate_ppm(1)
    if (rev_1, act_1) > (best_total_revenue, best_active_channels):
        best_total_revenue = rev_1
        best_active_channels = act_1
        best_ppm = 1
        
    queue = []
    
    prev_ppm = 1
    prev_cent = cent_1
    current_ppm = 2
    max_ppm_bound = 1000000
    
    while True:
        curr_cent, curr_rev, curr_act = evaluate_ppm(current_ppm)
        
        if (curr_rev, curr_act) > (best_total_revenue, best_active_channels):
            best_total_revenue = curr_rev
            best_active_channels = curr_act
            best_ppm = current_ppm
            
        # The potential max revenue for the interval [prev_ppm, current_ppm]
        potential = prev_cent * (current_ppm - 1)
        if potential > best_total_revenue and current_ppm - prev_ppm > 1:
            # Using negative for max-heap behavior
            heapq.heappush(queue, (-potential, prev_ppm, current_ppm, prev_cent))
            
        if curr_cent == 0 or current_ppm >= max_ppm_bound:
            break
            
        prev_ppm = current_ppm
        prev_cent = curr_cent
        current_ppm *= 2
        if current_ppm > max_ppm_bound:
            current_ppm = max_ppm_bound

    logger.info("Starting branch-and-bound interval search for maximum revenue...")
    
    while queue:
        neg_potential, lower_ppm, upper_ppm, lower_cent = heapq.heappop(queue)
        potential = -neg_potential
        
        logger.info(f"Exploring interval [{lower_ppm}, {upper_ppm}] with potential max revenue {potential}")
        
        if potential <= best_total_revenue:
            logger.info(f"Terminating search: highest remaining potential ({potential}) is <= best found ({best_total_revenue})")
            break
            
        if upper_ppm - lower_ppm <= 1:
            continue
            
        mid_ppm = (lower_ppm + upper_ppm) // 2
        
        # Evaluate midpoint
        mid_cent, mid_rev, mid_act = evaluate_ppm(mid_ppm)
        
        if (mid_rev, mid_act) > (best_total_revenue, best_active_channels):
            best_total_revenue = mid_rev
            best_active_channels = mid_act
            best_ppm = mid_ppm
            
        # Left interval: [lower_ppm, mid_ppm]
        left_potential = lower_cent * (mid_ppm - 1)
        if left_potential > best_total_revenue and mid_ppm - lower_ppm > 1:
            heapq.heappush(queue, (-left_potential, lower_ppm, mid_ppm, lower_cent))
            
        # Right interval: [mid_ppm, upper_ppm]
        right_potential = mid_cent * (upper_ppm - 1)
        if right_potential > best_total_revenue and upper_ppm - mid_ppm > 1:
            heapq.heappush(queue, (-right_potential, mid_ppm, upper_ppm, mid_cent))
            
    logger.info(f"Search complete. Results completely saved to {csv_file}")
    print(f"\n=== Best overall Total Revenue of {best_total_revenue} was achieved at uniform PPM {best_ppm} ===")

    # --- Local Search for Individual Channel Optimization ---
    logger.info("Starting individual channel optimization...")
    
    evaluated_ppms_cache = {}
    
    def evaluate_custom_ppms(ppm_dict):
        cache_key = tuple(sorted((e_short_id[e], ppm_dict[e]) for e in mynode_v.out_edges()))
        if cache_key in evaluated_ppms_cache:
            return evaluated_ppms_cache[cache_key]
            
        run_counter[0] += 1
        for e in mynode_v.out_edges():
            e_fee_rate[e] = ppm_dict[e]
            
        for e in DG.edges():
            a = e_base_fee[e]
            b = e_fee_rate[e] / 1000000.0
            e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + e_epsilon[e]
            
        _, e_betw_temp = gt.betweenness(DG, weight=e_weight, norm=False)
        _, pred_map_temp = gt.shortest_distance(DG, source=mynode_v, weights=e_weight, pred_map=True)
        
        e_counts_temp = DG.new_edge_property("int")
        for v in DG.vertices():
            if v == mynode_v: continue
            curr = v
            while curr != mynode_v:
                p = pred_map_temp[curr]
                if p == int(curr): break
                v_p = DG.vertex(p)
                e_edge = DG.edge(v_p, curr)
                if e_edge: e_counts_temp[e_edge] += 1
                curr = v_p
                
        sum_rev = 0
        ch_revs = {}
        iteration_results = []
        for e in mynode_v.out_edges():
            ch_id = e_short_id[e]
            cent = max(0, int(round(e_betw_temp[e])) - e_counts_temp[e])
            revenue = cent * ppm_dict[e]
            sum_rev += revenue
            ch_revs[e] = revenue
            row = [run_counter[0], ch_id, ppm_dict[e], f"{cent}", f"{e_counts_temp[e]}", f"{revenue}"]
            iteration_results.append(row)
            
        with open(csv_file, mode='a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(iteration_results)
            
        evaluated_ppms_cache[cache_key] = (sum_rev, ch_revs)
        return sum_rev, ch_revs

    current_ppms = {e: best_ppm for e in mynode_v.out_edges()}
    curr_total_rev, channel_revenues = evaluate_custom_ppms(current_ppms)
    
    json_file = "best_ppms.json"
    improved = True
    while improved:
        improved = False
        
        # Avoid channels being stuck with 0 revenue
        zero_channels = [e for e in mynode_v.out_edges() if channel_revenues[e] == 0 and current_ppms[e] > 0]
        if zero_channels:
            orig_zero_ppms = {e: current_ppms[e] for e in zero_channels}
            active_zeros = zero_channels[:]
            
            while active_zeros:
                for e in active_zeros:
                    current_ppms[e] -= 1
                    
                temp_rev, temp_ch_rev = evaluate_custom_ppms(current_ppms)
                
                active_zeros = [e for e in active_zeros if temp_ch_rev[e] == 0 and current_ppms[e] > 0]
                
            for e in zero_channels:
                if temp_ch_rev[e] == 0:
                    current_ppms[e] = orig_zero_ppms[e]
                else:
                    improved = True
                    logger.info(f"Restored revenue on {e_short_id[e]} by reducing PPM to {current_ppms[e]}")
                    
            if improved:
                curr_total_rev, channel_revenues = evaluate_custom_ppms(current_ppms)
                continue

        sorted_edges = sorted(channel_revenues.keys(), key=lambda e: channel_revenues[e], reverse=True)
        curr_score = (curr_total_rev, sum(1 for v in channel_revenues.values() if v > 0))
        
        for e in sorted_edges:
            orig_ppm = current_ppms[e]
            
            # Try +1
            current_ppms[e] = orig_ppm + 1
            rev_plus, ch_rev_plus = evaluate_custom_ppms(current_ppms)
            plus_score = (rev_plus, sum(1 for v in ch_rev_plus.values() if v > 0))
            
            # Try -1
            rev_minus, ch_rev_minus = -1, {}
            minus_score = (-1, -1)
            if orig_ppm > 0:
                current_ppms[e] = orig_ppm - 1
                rev_minus, ch_rev_minus = evaluate_custom_ppms(current_ppms)
                minus_score = (rev_minus, sum(1 for v in ch_rev_minus.values() if v > 0))
                
            if plus_score > curr_score and plus_score >= minus_score:
                current_ppms[e] = orig_ppm + 1
                curr_total_rev, channel_revenues = rev_plus, ch_rev_plus
                curr_score = plus_score
                logger.info(f"Increased PPM on {e_short_id[e]} to {orig_ppm + 1}. New total revenue: {curr_total_rev}")
                improved = True
                break
            elif minus_score > curr_score:
                current_ppms[e] = orig_ppm - 1
                
                dropped_channels = [other_e for other_e in mynode_v.out_edges() 
                                    if other_e != e and ch_rev_minus[other_e] == 0 and channel_revenues[other_e] > 0 and current_ppms[other_e] > 0]
                
                if dropped_channels:
                    orig_dropped_ppms = {other_e: current_ppms[other_e] for other_e in dropped_channels}
                    
                    active_dropped = dropped_channels[:]
                    while active_dropped:
                        for other_e in active_dropped:
                            current_ppms[other_e] -= 1
                            
                        temp_rev, temp_ch_rev = evaluate_custom_ppms(current_ppms)
                        
                        active_dropped = [other_e for other_e in active_dropped if temp_ch_rev[other_e] == 0 and current_ppms[other_e] > 0]
                                
                    rev_combined, ch_rev_combined = temp_rev, temp_ch_rev
                    combined_score = (rev_combined, sum(1 for v in ch_rev_combined.values() if v > 0))
                    
                    if combined_score >= minus_score:
                        curr_total_rev, channel_revenues = rev_combined, ch_rev_combined
                        curr_score = combined_score
                        logger.info(f"Decreased PPM on {e_short_id[e]} to {orig_ppm - 1} AND adjusted {len(dropped_channels)} affected channels. New total revenue: {curr_total_rev}")
                    else:
                        for other_e in dropped_channels:
                            current_ppms[other_e] = orig_dropped_ppms[other_e]
                        curr_total_rev, channel_revenues = rev_minus, ch_rev_minus
                        curr_score = minus_score
                        logger.info(f"Decreased PPM on {e_short_id[e]} to {orig_ppm - 1}. New total revenue: {curr_total_rev}")
                else:
                    curr_total_rev, channel_revenues = rev_minus, ch_rev_minus
                    curr_score = minus_score
                    logger.info(f"Decreased PPM on {e_short_id[e]} to {orig_ppm - 1}. New total revenue: {curr_total_rev}")
                    
                improved = True
                break
            else:
                # No improvement, revert change
                current_ppms[e] = orig_ppm
                
        if improved:
            with open(json_file, mode='w') as f:
                json.dump({e_short_id[edge]: current_ppms[edge] for edge in mynode_v.out_edges()}, f, indent=4)
                
    best_ppms = {e_short_id[e]: current_ppms[e] for e in mynode_v.out_edges()}
            
    with open(json_file, mode='w') as f:
        json.dump(best_ppms, f, indent=4)
        
    logger.info(f"Best individual PPM settings saved to {json_file}")
    print(f"\n=== Final Optimized Total Revenue of {curr_total_rev} was achieved ===")


if __name__ == "__main__":
    # execute only if run as a script
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69")
    parser.add_argument("--input-csv", type=str, default=None, help="Previous CSV results file to continue from")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for tie-breaking epsilon")
    parser.add_argument("--refresh-graph", action="store_true", help="Fetch a new graph from the node instead of using cache")
    args = parser.parse_args()
    
    run_centrality_sweep(args.node, args.input_csv, args.seed, args.refresh_graph)






