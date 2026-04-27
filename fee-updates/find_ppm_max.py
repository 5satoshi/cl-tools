#!/usr/bin/python

import sys, math, os, random, logging, json, argparse
import graph_tool.all as gt
from graph_helper import get_graph_from_cli

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("PpmMaxFinder")

def run_ppm_max_search(mynode):
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
    mynode_v = gt.find_vertex(DG, v_id, mynode)[0]
    
    e_base_fee = DG.edge_properties["base_fee_millisatoshi"]
    e_fee_rate = DG.edge_properties["fee_per_millionth"]
    e_short_id = DG.edge_properties["short_channel_id"]
    
    for e in mynode_v.out_edges():
        e_base_fee[e] = 0
        
    tx_sat_cent = 80000
    e_weight = DG.new_edge_property("double")
    e_epsilon = DG.new_edge_property("double")
    for e in DG.edges():
        e_epsilon[e] = random.uniform(0.0001, 0.00011)

    logger.info("Computing baseline centrality (PPM=1000000)...")
    baseline_cent = {}
    for e in mynode_v.out_edges():
        e_fee_rate[e] = 1000000
        
    for e in DG.edges():
        a = e_base_fee[e]
        b = e_fee_rate[e] / 1000000.0
        e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + e_epsilon[e]
        
    _, e_betw_base = gt.betweenness(DG, weight=e_weight, norm=False)
    for e in mynode_v.out_edges():
        ch_id = e_short_id[e]
        baseline_cent[ch_id] = int(round(e_betw_base[e]))

    results = {}
    
    for target_e in mynode_v.out_edges():
        target_ch_id = e_short_id[target_e]
        logger.info(f"Finding ppm_max for channel {target_ch_id}...")
        
        # Keep all other channels at 0 PPM during this channel's search
        for e in mynode_v.out_edges():
            e_fee_rate[e] = 0
            
        mode = 'exp'
        ppm = 1
        lower = 0
        upper = None
        max_valid = 0
        
        while mode != 'done':
            e_fee_rate[target_e] = ppm
            
            for e in DG.edges():
                a = e_base_fee[e]
                b = e_fee_rate[e] / 1000000.0
                e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + e_epsilon[e]
                
            _, e_betw = gt.betweenness(DG, weight=e_weight, norm=False)
            
            cent = int(round(e_betw[target_e]))
            has_revenue = cent > baseline_cent[target_ch_id]
            
            if mode == 'exp':
                if has_revenue:
                    max_valid = ppm
                    lower = ppm
                    ppm *= 2
                    if ppm > 1000000:  # Safety cap
                        mode = 'done'
                else:
                    upper = ppm
                    mode = 'bin'
                    if upper - lower <= 1:
                        mode = 'done'
                    else:
                        ppm = (lower + upper) // 2
            elif mode == 'bin':
                if has_revenue:
                    max_valid = ppm
                    lower = ppm
                else:
                    upper = ppm
                    
                if upper - lower <= 1:
                    mode = 'done'
                else:
                    ppm = (lower + upper) // 2
                    
        results[target_ch_id] = max_valid
        logger.info(f"Channel {target_ch_id} ppm_max = {max_valid}")
    output_file = "ppm_max.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=4)
    
    logger.info(f"Finished. Results saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69")
    args = parser.parse_args()
    run_ppm_max_search(args.node)
