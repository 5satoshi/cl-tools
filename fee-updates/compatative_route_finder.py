#!/usr/bin/python

import sys, math, os, random, logging
import graph_tool.all as gt
import matplotlib.pyplot as plt
from datetime import datetime
import argparse
from tqdm import tqdm
import csv
from skopt import Optimizer
from skopt.space import Integer
import numpy as np
import warnings
from graph_helper import get_graph_from_cli

warnings.filterwarnings("ignore", message="The objective has been evaluated at point.*")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("RouteFinder")


def run_centrality_sweep(mynode, input_csv=None):
    
    rpc = os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc"
    G = get_graph_from_cli(rpc)
    
    e_active = G.edge_properties["active"]
    wDG = gt.GraphView(G, efilt=e_active)
    
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
    tx_sat_cent = 80000
    e_weight = DG.new_edge_property("double")
    e_epsilon = DG.new_edge_property("double")
    for e in DG.edges():
        e_epsilon[e] = random.uniform(0.0001, 0.00011)
        
    logger.info("Initializing dynamic iterative PPM optimization...")
    
    current_ppms = {}
    channel_history = {}
    optimizers = {}
    
    best_total_revenue = -1
    best_iteration = -1
    iteration_revenues = {}
    
    for e in mynode_v.out_edges():
        ch_id = e_short_id[e]
        current_ppms[ch_id] = 1
        channel_history[ch_id] = []
        # Initialize Tree-Based Bayesian Optimizer for each channel (search space: 1 to 100 PPM)
        optimizers[ch_id] = Optimizer(
            dimensions=[Integer(1, 100, prior='log-uniform')],
            base_estimator="RF"
        )
        
    start_iteration = 0
    if input_csv and os.path.exists(input_csv):
        logger.info(f"Loading previous results from {input_csv}...")
        
        with open(input_csv, mode='r') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            
            has_iteration = header and "Iteration" in header
            
            for row in reader:
                if not row or len(row) < 4:
                    continue
                    
                if has_iteration and len(row) >= 5:
                    it_str, ch_id, ppm_str, cent_str, rev_str = row
                    it_num = int(it_str)
                    if it_num >= 0:
                        start_iteration = max(start_iteration, it_num + 1)
                else:
                    ch_id, ppm_str, cent_str, rev_str = row
                    it_num = 0
                    
                ppm = int(ppm_str)
                cent = int(cent_str)
                rev = int(float(rev_str))
                
                if has_iteration:
                    results.append([it_num, ch_id, ppm, cent_str, rev_str])
                else:
                    results.append([it_num, ch_id, ppm, cent_str, rev_str])
                    
                if it_num not in iteration_revenues:
                    iteration_revenues[it_num] = 0
                iteration_revenues[it_num] += rev
                
                if ch_id in current_ppms:
                    channel_history[ch_id].append((ppm, rev))
                        
        for it_n, tot_rev in iteration_revenues.items():
            if tot_rev > best_total_revenue:
                best_total_revenue = tot_rev
                best_iteration = it_n

        for ch_id in current_ppms:
            if channel_history[ch_id]:
                # Feed past history into the Bayesian optimizer
                seen_ppms = set()
                for past_ppm, past_rev in channel_history[ch_id]:
                    if past_ppm not in seen_ppms:
                        seen_ppms.add(past_ppm)
                        try:
                            optimizers[ch_id].tell([past_ppm], -past_rev) # Minimize negative revenue
                        except Exception:
                            pass
                
                # Ask the optimizer for the next best PPM to test
                current_ppms[ch_id] = int(optimizers[ch_id].ask()[0])
    else:
        logger.info("No input CSV provided or file not found. Starting all channels at PPM 1.")
        
    max_iterations = 10
    
    for iteration in tqdm(range(start_iteration, start_iteration + max_iterations), desc="Optimizing PPM"):
        logger.info(f"Iteration {iteration + 1} (Total steps)")
        
        # Update mynode out-edges PPM dynamically per channel
        for e in mynode_v.out_edges():
            ch_id = e_short_id[e]
            e_fee_rate[e] = current_ppms[ch_id]
            
        # Compute edge weights for the whole graph based on 80k sat tx
        for e in DG.edges():
            a = e_base_fee[e]
            b = e_fee_rate[e] / 1000000.0
            e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + e_epsilon[e]
            
        # Compute betweenness
        _, e_betw = gt.betweenness(DG, weight=e_weight, norm=False)
        
        sum_cent = 0
        sum_rev = 0
        
        # Record results and calculate gradient-based next step per channel
        for e in mynode_v.out_edges():
            ch_id = e_short_id[e]
            ppm = current_ppms[ch_id]
            cent = int(round(e_betw[e]))
            revenue = cent * ppm
            
            sum_cent += cent
            sum_rev += revenue
            
            results.append([iteration, ch_id, ppm, f"{cent}", f"{revenue}"])
            channel_history[ch_id].append((ppm, revenue))
            
            # Bayesian Optimization update
            try:
                optimizers[ch_id].tell([ppm], -revenue)  # Minimize negative revenue
                next_ppm = int(optimizers[ch_id].ask()[0])
            except Exception as e:
                logger.error(f"Optimizer error for channel {ch_id}: {e}. Falling back to +1 step.")
                next_ppm = ppm + 1
                
            current_ppms[ch_id] = next_ppm
            
        if sum_rev > best_total_revenue:
            best_total_revenue = sum_rev
            best_iteration = iteration
            
        logger.info(f"Iteration {iteration + 1} completed | Sum Centrality: {sum_cent} | Total Revenue: {sum_rev}")
            
    csv_file = "centrality_sweep_results.csv"
    with open(csv_file, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Iteration", "Channel", "PPM", "Edge_Centrality", "Revenue_Potential"])
        writer.writerows(results)
        
    logger.info(f"Results saved to {csv_file}")
    
    print(f"\n=== Best overall Total Revenue of {best_total_revenue} was achieved at Iteration {best_iteration} (Internal Index) ===")


if __name__ == "__main__":
    # execute only if run as a script
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69")
    parser.add_argument("--input-csv", type=str, default=None, help="Previous CSV results file to continue from")
    args = parser.parse_args()
    
    run_centrality_sweep(args.node, args.input_csv)






