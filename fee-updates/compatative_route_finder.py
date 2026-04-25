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
    
    logger.info("Starting dynamic iterative PPM optimization...")
    
    channel_best = {}
    for e in mynode_v.out_edges():
        ch_id = e_short_id[e]
        channel_best[ch_id] = {'best_ppm': 1, 'max_rev': -1.0}
        
    current_global_ppm = 1
    
    if input_csv and os.path.exists(input_csv):
        logger.info(f"Loading previous results from {input_csv}...")
        highest_ppm = 1
        with open(input_csv, mode='r') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for row in reader:
                if not row or len(row) < 4:
                    continue
                ch_id, ppm_str, cent_str, rev_str = row
                ppm = int(ppm_str)
                rev = float(rev_str)
                
                results.append([ch_id, ppm, cent_str, rev_str])
                
                if ch_id in channel_best:
                    if rev > channel_best[ch_id]['max_rev']:
                        channel_best[ch_id]['max_rev'] = rev
                        channel_best[ch_id]['best_ppm'] = ppm
                
                if ppm > highest_ppm:
                    highest_ppm = ppm
                    
        # Find the last revenues at highest_ppm to calculate next step
        expected_next_ppms = []
        last_revs = {ch_id: 0.0 for ch_id in channel_best}
        for res in results:
            if res[1] == highest_ppm and res[0] in channel_best:
                last_revs[res[0]] = float(res[3])
                
        for ch_id, last_rev in last_revs.items():
            if last_rev > 0:
                if last_rev < channel_best[ch_id]['max_rev']:
                    expected = math.ceil((channel_best[ch_id]['max_rev'] / last_rev) * highest_ppm)
                    expected_next_ppms.append(expected)
                else:
                    expected_next_ppms.append(highest_ppm + 1)
                    
        if expected_next_ppms:
            current_global_ppm = min(expected_next_ppms)
        else:
            current_global_ppm = highest_ppm + 1
    else:
        logger.info("No input CSV provided or file not found. Starting at PPM 1.")
        
    max_iterations = 10
    
    for iteration in tqdm(range(max_iterations), desc="Optimizing PPM"):
        logger.info(f"Iteration {iteration + 1}/{max_iterations} - Testing global PPM: {current_global_ppm}")
        # Update mynode out-edges PPM dynamically using the global PPM
        for e in mynode_v.out_edges():
            e_fee_rate[e] = current_global_ppm
            
        # Compute edge weights for the whole graph based on 80k sat tx
        for e in DG.edges():
            a = e_base_fee[e]
            b = e_fee_rate[e] / 1000000.0
            e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + random.uniform(0, 0.0001)
            
        # Compute betweenness
        _, e_betw = gt.betweenness(DG, weight=e_weight)
        
        expected_next_ppms = []
        
        # Record results and calculate next expected step
        for e in mynode_v.out_edges():
            ch_id = e_short_id[e]
            ppm = current_global_ppm
            cent = e_betw[e]
            revenue = cent * ppm
            
            results.append([ch_id, ppm, f"{cent:.8f}", f"{revenue:.8f}"])
            
            # Update max revenue
            is_max = False
            if revenue >= channel_best[ch_id]['max_rev']:
                channel_best[ch_id]['max_rev'] = revenue
                channel_best[ch_id]['best_ppm'] = ppm
                is_max = True
                
            if revenue > 0:
                if is_max:
                    expected_next_ppms.append(ppm + 1)
                else:
                    expected = math.ceil((channel_best[ch_id]['max_rev'] / revenue) * ppm)
                    expected_next_ppms.append(expected)
                
        # Determine actual next step ensuring we move forward
        if expected_next_ppms:
            current_global_ppm = min(expected_next_ppms)
        else:
            current_global_ppm += 1
            
    csv_file = "centrality_sweep_results.csv"
    with open(csv_file, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Channel", "PPM", "Edge_Centrality", "Revenue_Potential"])
        writer.writerows(results)
        
    logger.info(f"Results saved to {csv_file}")
    
    print("\n=== Optimal Revenue PPM per Channel ===")
    total_opt_revenue = 0.0
    for ch, data in sorted(channel_best.items()):
        print(f"Channel {ch}: Optimal PPM = {data['best_ppm']} | Max Revenue Potential = {data['max_rev']:.8f}")
        if data['max_rev'] > 0:
            total_opt_revenue += data['max_rev']
            
    print(f"\nTotal Node Revenue Potential across optimized channels = {total_opt_revenue:.8f}")


if __name__ == "__main__":
    # execute only if run as a script
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69")
    parser.add_argument("--input-csv", type=str, default=None, help="Previous CSV results file to continue from")
    args = parser.parse_args()
    
    run_centrality_sweep(args.node, args.input_csv)






