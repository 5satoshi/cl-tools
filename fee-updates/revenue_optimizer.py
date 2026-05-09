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
from graph_helper import load_or_fetch_graph, get_filtered_graph_and_node

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("RouteFinder")


def run_centrality_sweep(mynode, input_csv=None, seed=42, refresh_graph=False, optimize_for="revenue", max_ppm=32):
    random.seed(seed)
    
    tx_sat_cent = 80000
    DG, mynode_v = get_filtered_graph_and_node(mynode, refresh_graph=refresh_graph, tx_sat_cent=tx_sat_cent)
    if mynode_v is None:
        logger.error("Node not found in the largest component of the graph.")
        return
        
    v_id = DG.vertex_properties["id"]
    
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
        writer.writerow(["Run_Number", "Channel", "PPM", "Edge_Centrality", "from_paths", "to_paths", "Revenue_Potential", "Score"])

    run_counter = [0]
    evaluated_uniform_cache = {}

    def update_graph_and_get_metrics():
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
                
        return e_betw_temp, e_counts_temp

    def evaluate_ppm(current_ppm):
        if current_ppm in evaluated_uniform_cache:
            return evaluated_uniform_cache[current_ppm]
            
        run_counter[0] += 1
        logger.info(f"Evaluating uniform PPM: {current_ppm}")
        for e in mynode_v.out_edges():
            e_fee_rate[e] = current_ppm
            
        e_betw_temp, e_counts_temp = update_graph_and_get_metrics()
                
        sum_cent = 0
        sum_rev = 0
        sum_score = 0
        active_channels = 0
        iteration_results = []
        
        for e in mynode_v.out_edges():
            ch_id = e_short_id[e]
            from_paths = e_counts_temp[e]
            cent = max(0, int(round(e_betw_temp[e])) - from_paths)
            to_paths = cent / from_paths if from_paths > 0 else 0
            
            adj_from_paths = max(0, from_paths - math.sqrt(from_paths))
            adj_to_paths = max(0, to_paths - math.sqrt(to_paths))
            adj_cent = adj_from_paths * adj_to_paths
            
            revenue = cent * current_ppm
            score = adj_cent * current_ppm
            
            sum_cent += cent
            sum_rev += revenue
            sum_score += score
            if revenue > 0:
                active_channels += 1
            row = [run_counter[0], ch_id, current_ppm, f"{cent}", f"{from_paths}", f"{to_paths:.2f}", f"{revenue}", f"{score:.2f}"]
            results.append(row)
            iteration_results.append(row)
            
        with open(csv_file, mode='a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(iteration_results)
            
        logger.info(f"Uniform PPM {current_ppm} completed | Sum Centrality: {sum_cent} | Total Revenue: {sum_rev} | Total Score: {sum_score:.2f} | Active Channels: {active_channels}")
        evaluated_uniform_cache[current_ppm] = (sum_cent, sum_rev, sum_score, active_channels)
        return sum_cent, sum_rev, sum_score, active_channels

    import heapq
    
    def get_target(cent, rev, score):
        if optimize_for == "cent": return cent
        if optimize_for == "score": return score
        return rev
        
    best_target_val = -1
    best_active_channels = -1
    best_ppm = -1
    
    logger.info(f"Starting exponential phase to discover intervals optimizing for {optimize_for}...")
    # Evaluate starting boundary
    cent_1, rev_1, score_1, act_1 = evaluate_ppm(1)
    target_1 = get_target(cent_1, rev_1, score_1)
    if (target_1, act_1) > (best_target_val, best_active_channels):
        best_target_val = target_1
        best_active_channels = act_1
        best_ppm = 1
        
    queue = []
    
    prev_ppm = 1
    prev_cent = cent_1
    current_ppm = 2
    max_ppm_bound = max_ppm
    
    while True:
        curr_cent, curr_rev, curr_score, curr_act = evaluate_ppm(current_ppm)
        curr_target = get_target(curr_cent, curr_rev, curr_score)
        
        if (curr_target, curr_act) > (best_target_val, best_active_channels):
            best_target_val = curr_target
            best_active_channels = curr_act
            best_ppm = current_ppm
            
        # The potential max for the interval [prev_ppm, current_ppm]
        if optimize_for == "cent":
            potential = prev_cent
        else:
            potential = prev_cent * (current_ppm - 1)
            
        if potential > best_target_val and current_ppm - prev_ppm > 1:
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
        
        logger.info(f"Exploring interval [{lower_ppm}, {upper_ppm}] with potential max {optimize_for} {potential}")
        
        if potential <= best_target_val:
            logger.info(f"Terminating search: highest remaining potential ({potential}) is <= best found ({best_target_val})")
            break
            
        if upper_ppm - lower_ppm <= 1:
            continue
            
        mid_ppm = (lower_ppm + upper_ppm) // 2
        
        # Evaluate midpoint
        mid_cent, mid_rev, mid_score, mid_act = evaluate_ppm(mid_ppm)
        mid_target = get_target(mid_cent, mid_rev, mid_score)
        
        if (mid_target, mid_act) > (best_target_val, best_active_channels):
            best_target_val = mid_target
            best_active_channels = mid_act
            best_ppm = mid_ppm
            
        # Left interval: [lower_ppm, mid_ppm]
        left_potential = lower_cent if optimize_for == "cent" else lower_cent * (mid_ppm - 1)
        if left_potential > best_target_val and mid_ppm - lower_ppm > 1:
            heapq.heappush(queue, (-left_potential, lower_ppm, mid_ppm, lower_cent))
            
        # Right interval: [mid_ppm, upper_ppm]
        right_potential = mid_cent if optimize_for == "cent" else mid_cent * (upper_ppm - 1)
        if right_potential > best_target_val and upper_ppm - mid_ppm > 1:
            heapq.heappush(queue, (-right_potential, mid_ppm, upper_ppm, mid_cent))
            
    logger.info(f"Search complete. Results completely saved to {csv_file}")
    print(f"\n=== Best overall {optimize_for} of {best_target_val} was achieved at uniform PPM {best_ppm} ===")

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
            
        e_betw_temp, e_counts_temp = update_graph_and_get_metrics()
                
        sum_cent = 0
        sum_rev = 0
        sum_score = 0
        ch_targets = {}
        iteration_results = []
        for e in mynode_v.out_edges():
            ch_id = e_short_id[e]
            from_paths = e_counts_temp[e]
            cent = max(0, int(round(e_betw_temp[e])) - from_paths)
            to_paths = cent / from_paths if from_paths > 0 else 0
            
            adj_from_paths = max(0, from_paths - math.sqrt(from_paths))
            adj_to_paths = max(0, to_paths - math.sqrt(to_paths))
            adj_cent = adj_from_paths * adj_to_paths
            
            revenue = cent * ppm_dict[e]
            score = adj_cent * ppm_dict[e]
            
            sum_cent += cent
            sum_rev += revenue
            sum_score += score
            
            target = cent if optimize_for == "cent" else (score if optimize_for == "score" else revenue)
            ch_targets[e] = target
            
            row = [run_counter[0], ch_id, ppm_dict[e], f"{cent}", f"{from_paths}", f"{to_paths:.2f}", f"{revenue}", f"{score:.2f}"]
            iteration_results.append(row)
            
        with open(csv_file, mode='a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(iteration_results)
            
        target_total = sum_cent if optimize_for == "cent" else (sum_score if optimize_for == "score" else sum_rev)
        evaluated_ppms_cache[cache_key] = (target_total, ch_targets)
        return target_total, ch_targets

    current_ppms = {e: best_ppm for e in mynode_v.out_edges()}
    curr_total_target, channel_targets = evaluate_custom_ppms(current_ppms)
    
    json_file = "best_ppms.json"
    improved = True
    while improved:
        improved = False
        
        # Avoid channels being stuck with 0 target
        zero_channels = [e for e in mynode_v.out_edges() if channel_targets[e] <= 0 and current_ppms[e] > 0]
        if zero_channels:
            orig_zero_ppms = {e: current_ppms[e] for e in zero_channels}
            active_zeros = zero_channels[:]
            
            while active_zeros:
                for e in active_zeros:
                    current_ppms[e] -= 1
                    
                temp_target_total, temp_ch_targets = evaluate_custom_ppms(current_ppms)
                
                active_zeros = [e for e in active_zeros if temp_ch_targets[e] <= 0 and current_ppms[e] > 0]
                
            for e in zero_channels:
                if temp_ch_targets[e] <= 0:
                    current_ppms[e] = orig_zero_ppms[e]
                else:
                    improved = True
                    logger.info(f"Restored target on {e_short_id[e]} by reducing PPM to {current_ppms[e]}")
                    
            if improved:
                curr_total_target, channel_targets = evaluate_custom_ppms(current_ppms)
                continue

        sorted_edges = sorted(channel_targets.keys(), key=lambda e: channel_targets[e], reverse=True)
        curr_score = (curr_total_target, sum(1 for v in channel_targets.values() if v > 0))
        
        for e in sorted_edges:
            orig_ppm = current_ppms[e]
            
            # Try +1
            current_ppms[e] = orig_ppm + 1
            target_plus, ch_target_plus = evaluate_custom_ppms(current_ppms)
            plus_score = (target_plus, sum(1 for v in ch_target_plus.values() if v > 0))
            
            # Try -1
            target_minus, ch_target_minus = -1, {}
            minus_score = (-1, -1)
            if orig_ppm > 0:
                current_ppms[e] = orig_ppm - 1
                target_minus, ch_target_minus = evaluate_custom_ppms(current_ppms)
                minus_score = (target_minus, sum(1 for v in ch_target_minus.values() if v > 0))
                
            if plus_score > curr_score and plus_score >= minus_score:
                current_ppms[e] = orig_ppm + 1
                curr_total_target, channel_targets = target_plus, ch_target_plus
                curr_score = plus_score
                logger.info(f"Increased PPM on {e_short_id[e]} to {orig_ppm + 1}. New total {optimize_for}: {curr_total_target}")
                improved = True
                break
            elif minus_score > curr_score:
                current_ppms[e] = orig_ppm - 1
                
                dropped_channels = [other_e for other_e in mynode_v.out_edges() 
                                    if other_e != e and ch_target_minus[other_e] <= 0 and channel_targets[other_e] > 0 and current_ppms[other_e] > 0]
                
                if dropped_channels:
                    orig_dropped_ppms = {other_e: current_ppms[other_e] for other_e in dropped_channels}
                    
                    active_dropped = dropped_channels[:]
                    while active_dropped:
                        for other_e in active_dropped:
                            current_ppms[other_e] -= 1
                            
                        temp_target, temp_ch_target = evaluate_custom_ppms(current_ppms)
                        
                        active_dropped = [other_e for other_e in active_dropped if temp_ch_target[other_e] <= 0 and current_ppms[other_e] > 0]
                                
                    target_combined, ch_target_combined = temp_target, temp_ch_target
                    combined_score = (target_combined, sum(1 for v in ch_target_combined.values() if v > 0))
                    
                    if combined_score >= minus_score:
                        curr_total_target, channel_targets = target_combined, ch_target_combined
                        curr_score = combined_score
                        logger.info(f"Decreased PPM on {e_short_id[e]} to {orig_ppm - 1} AND adjusted {len(dropped_channels)} affected channels. New total {optimize_for}: {curr_total_target}")
                    else:
                        for other_e in dropped_channels:
                            current_ppms[other_e] = orig_dropped_ppms[other_e]
                        curr_total_target, channel_targets = target_minus, ch_target_minus
                        curr_score = minus_score
                        logger.info(f"Decreased PPM on {e_short_id[e]} to {orig_ppm - 1}. New total {optimize_for}: {curr_total_target}")
                else:
                    curr_total_target, channel_targets = target_minus, ch_target_minus
                    curr_score = minus_score
                    logger.info(f"Decreased PPM on {e_short_id[e]} to {orig_ppm - 1}. New total {optimize_for}: {curr_total_target}")
                    
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
    print(f"\n=== Final Optimized Total {optimize_for} of {curr_total_target} was achieved ===")


if __name__ == "__main__":
    # execute only if run as a script
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69")
    parser.add_argument("--input-csv", type=str, default=None, help="Previous CSV results file to continue from")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for tie-breaking epsilon")
    parser.add_argument("--refresh-graph", action="store_true", help="Fetch a new graph from the node instead of using cache")
    parser.add_argument("--optimize-for", type=str, choices=["cent", "revenue", "score"], default="revenue", help="Metric to optimize for")
    parser.add_argument("--max-ppm", type=int, default=32, help="Maximum PPM bound for the optimization interval")
    args = parser.parse_args()
    
    run_centrality_sweep(args.node, args.input_csv, args.seed, args.refresh_graph, args.optimize_for, args.max_ppm)






