#!/usr/bin/python

import sys, math, os, random, logging, csv, argparse
import graph_tool.all as gt
from graph_helper import load_or_fetch_graph, get_filtered_graph_and_node

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("EdgeCaseCent")


def run_edge_cases(mynode, seed=42, refresh_graph=False):
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
    
    for e in mynode_v.out_edges():
        e_base_fee[e] = 0
        
    e_weight = DG.new_edge_property("double")
    e_epsilon = DG.new_edge_property("double")
    for e in DG.edges():
        e_epsilon[e] = random.uniform(0.0001, 0.00011)

    target_ppms = [1000000, 1, 0]
    results = {}
    
    for e in mynode_v.out_edges():
        ch_id = e_short_id[e]
        results[ch_id] = {}

    for ppm in target_ppms:
        logger.info(f"Computing centrality for PPM = {ppm}...")
        for e in mynode_v.out_edges():
            e_fee_rate[e] = ppm
            
        for e in DG.edges():
            a = e_base_fee[e]
            b = e_fee_rate[e] / 1000000.0
            e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + e_epsilon[e]
            
        _, e_betw = gt.betweenness(DG, weight=e_weight, norm=False)
        
        for e in mynode_v.out_edges():
            ch_id = e_short_id[e]
            cent = int(round(e_betw[e]))
            results[ch_id][ppm] = cent

    csv_file = "edge_case_centrality_results.csv"
    with open(csv_file, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Channel", "Cent_1M_PPM", "Cent_1_PPM_Norm", "Cent_0_PPM_Norm"])
        for ch_id, data in sorted(results.items()):
            cent_1m = data[1000000]
            norm_1 = data[1] / cent_1m if cent_1m > 0 else 0.0
            norm_0 = data[0] / cent_1m if cent_1m > 0 else 0.0
            writer.writerow([ch_id, cent_1m, f"{norm_1:.4f}", f"{norm_0:.4f}"])
            
    logger.info(f"Edge case results saved to {csv_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute and compare node channel centralities at extreme fee settings (0, 1, and 1,000,000 PPM).")
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69", help="Pubkey of the node to analyze")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for tie-breaking epsilon")
    parser.add_argument("--refresh-graph", action="store_true", help="Fetch a new graph from the node instead of using cache")
    args = parser.parse_args()
    
    run_edge_cases(args.node, args.seed, args.refresh_graph)
