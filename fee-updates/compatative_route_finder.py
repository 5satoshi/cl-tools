#!/usr/bin/python

import sys, math, os, random, logging
import graph_tool.all as gt
import pandas as pd
import matplotlib.pyplot as plt
from pyln.client import LightningRpc
from datetime import datetime
import argparse
from tqdm import tqdm
import csv
from skopt import Optimizer
from skopt.space import Integer
import numpy as np


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("RouteFinder")


def get_graph_from_cli(rpc=".lightning/bitcoin/lightning-rpc"):
    
    l1 = LightningRpc(rpc)
    
    channels = l1.listchannels()
    
    dfc = pd.DataFrame(channels["channels"])
    
    DG = gt.Graph(directed=True)
    v_id = DG.new_vertex_property("string")
    DG.vertex_properties["id"] = v_id
    
    e_active = DG.new_edge_property("bool")
    e_base_fee = DG.new_edge_property("double")
    e_fee_rate = DG.new_edge_property("double")
    e_satoshis = DG.new_edge_property("double")
    e_short_id = DG.new_edge_property("string")
    
    DG.edge_properties["active"] = e_active
    DG.edge_properties["base_fee_millisatoshi"] = e_base_fee
    DG.edge_properties["fee_per_millionth"] = e_fee_rate
    DG.edge_properties["satoshis"] = e_satoshis
    DG.edge_properties["short_channel_id"] = e_short_id

    vertex_map = {}
    for _, row in dfc.iterrows():
        u_id = row['source']
        v_id_str = row['destination']
        
        if u_id not in vertex_map:
            v = DG.add_vertex()
            v_id[v] = u_id
            vertex_map[u_id] = v
        if v_id_str not in vertex_map:
            v = DG.add_vertex()
            v_id[v] = v_id_str
            vertex_map[v_id_str] = v
            
        e = DG.add_edge(vertex_map[u_id], vertex_map[v_id_str])
        e_active[e] = row['active']
        e_base_fee[e] = row['base_fee_millisatoshi']
        e_fee_rate[e] = row['fee_per_millionth']
        
        # Handle newer CLN versions that use amount_msat instead of satoshis
        amt_msat = row.get('amount_msat', 0)
        if isinstance(amt_msat, str) and amt_msat.endswith('msat'):
            amt_msat = int(amt_msat[:-4])
        elif isinstance(amt_msat, dict) and 'msat' in amt_msat:
            amt_msat = amt_msat['msat']
        
        sat = row.get('satoshis', amt_msat / 1000.0)
        e_satoshis[e] = float(sat)
        
        e_short_id[e] = row['short_channel_id']
    
    return DG


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
    
    logger.info("Starting dynamic iterative PPM optimization...")
    
    current_ppms = {}
    channel_history = {}
    optimizers = {}
    
    best_total_revenue = -1
    best_iteration = -1
    iteration_revenues = {}
    
    for e in mynode_v.out_edges():
        ch_id = e_short_id[e]
        current_ppms[ch_id] = 10000
        channel_history[ch_id] = []
        # Initialize Bayesian Optimizer for each channel (search space: 1 to 10000 PPM)
        optimizers[ch_id] = Optimizer(dimensions=[Integer(1, 10000)], random_state=42)
        
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
                    start_iteration = max(start_iteration, it_num + 1)
                else:
                    ch_id, ppm_str, cent_str, rev_str = row
                    it_num = 0
                    
                ppm = int(ppm_str)
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
                for past_ppm, past_rev in channel_history[ch_id]:
                    try:
                        optimizers[ch_id].tell([past_ppm], -past_rev) # Minimize negative revenue
                    except Exception:
                        pass
                
                # Ask the optimizer for the next best PPM to test
                current_ppms[ch_id] = int(optimizers[ch_id].ask()[0])
    else:
        logger.info("No input CSV provided or file not found. Starting all channels at PPM 10000.")
        
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
            revenue = max(0, cent - 1) * ppm
            
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






