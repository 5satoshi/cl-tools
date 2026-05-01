#!/usr/bin/python

import sys, math, os, random, logging, csv, argparse
import graph_tool.all as gt
from graph_helper import load_or_fetch_graph

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("EdgeCaseCent")


def run_edge_cases(mynode, seed=42, refresh_graph=False):
    random.seed(seed)
    rpc = os.environ.get('HOME', '') + "/.lightning/bitcoin/lightning-rpc"
    G = load_or_fetch_graph(rpc, refresh=refresh_graph)
    
    tx_sat_cent = 80000
    tx_msat = tx_sat_cent * 1000
    
    e_active = G.edge_properties["active"]
    e_htlc_max = G.edge_properties["htlc_maximum_msat"]
    
    e_filt = G.new_edge_property("bool")
    e_filt.a = e_active.a & (e_htlc_max.a >= tx_msat)
    
    wDG = gt.GraphView(G, efilt=e_filt)
    
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for tie-breaking epsilon")
    parser.add_argument("--refresh-graph", action="store_true", help="Fetch a new graph from the node instead of using cache")
    args = parser.parse_args()
    
    run_edge_cases(args.node, args.seed, args.refresh_graph)
