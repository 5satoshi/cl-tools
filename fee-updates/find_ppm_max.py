#!/usr/bin/python

import sys, math, os, random, logging, json, argparse
import graph_tool.all as gt
from graph_helper import load_or_fetch_graph

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("PpmMaxFinder")

def run_ppm_max_search(mynode, refresh_graph=False):
    rpc = os.environ.get('HOME', '') + "/.lightning/bitcoin/lightning-rpc"
    G = load_or_fetch_graph(rpc, refresh=refresh_graph)
    
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
    
    _, pred_map_base = gt.shortest_distance(DG, source=mynode_v, weights=e_weight, pred_map=True)
    e_counts_base = DG.new_edge_property("int")
    for v in DG.vertices():
        if v == mynode_v:
            continue
        curr = v
        while curr != mynode_v:
            p = pred_map_base[curr]
            if p == int(curr):
                break
            v_p = DG.vertex(p)
            e_edge = DG.edge(v_p, curr)
            if e_edge:
                e_counts_base[e_edge] += 1
            curr = v_p
            
    for e in mynode_v.out_edges():
        ch_id = e_short_id[e]
        baseline_cent[ch_id] = max(0, int(round(e_betw_base[e])) - e_counts_base[e])

    # Initialize search state for all channels simultaneously
    states = {}
    for e in mynode_v.out_edges():
        ch_id = e_short_id[e]
        states[ch_id] = {
            'mode': 'exp',
            'ppm': 1,
            'lower': 0,
            'upper': None,
            'max_valid': 0
        }

    iteration = 0
    while True:
        active_channels = [ch for ch, s in states.items() if s['mode'] != 'done']
        if not active_channels:
            break

        iteration += 1
        logger.info(f"Iteration {iteration}: {len(active_channels)} channels still searching...")

        for e in mynode_v.out_edges():
            ch_id = e_short_id[e]
            e_fee_rate[e] = states[ch_id]['ppm']
            
        for e in DG.edges():
            a = e_base_fee[e]
            b = e_fee_rate[e] / 1000000.0
            e_weight[e] = math.floor(a + tx_sat_cent * b * 1000) + e_epsilon[e]
            
        _, e_betw = gt.betweenness(DG, weight=e_weight, norm=False)
        
        _, pred_map = gt.shortest_distance(DG, source=mynode_v, weights=e_weight, pred_map=True)
        e_counts = DG.new_edge_property("int")
        for v in DG.vertices():
            if v == mynode_v:
                continue
            curr = v
            while curr != mynode_v:
                p = pred_map[curr]
                if p == int(curr):
                    break
                v_p = DG.vertex(p)
                e_edge = DG.edge(v_p, curr)
                if e_edge:
                    e_counts[e_edge] += 1
                curr = v_p

        for e in mynode_v.out_edges():
            ch_id = e_short_id[e]
            s = states[ch_id]
            if s['mode'] == 'done':
                continue

            cent = max(0, int(round(e_betw[e])) - e_counts[e])
            has_revenue = cent > baseline_cent[ch_id]
            
            tested_ppm = s['ppm']

            if s['mode'] == 'exp':
                if has_revenue:
                    s['max_valid'] = s['ppm']
                    s['lower'] = s['ppm']
                    s['ppm'] *= 2
                    if s['ppm'] > 1000000:  # Safety cap
                        s['mode'] = 'done'
                else:
                    s['upper'] = s['ppm']
                    s['mode'] = 'bin'
                    if s['upper'] - s['lower'] <= 1:
                        s['mode'] = 'done'
                        s['ppm'] = s['max_valid']
                    else:
                        s['ppm'] = (s['lower'] + s['upper']) // 2
            elif s['mode'] == 'bin':
                if has_revenue:
                    s['max_valid'] = s['ppm']
                    s['lower'] = s['ppm']
                else:
                    s['upper'] = s['ppm']
                    
                if s['upper'] - s['lower'] <= 1:
                    s['mode'] = 'done'
                    s['ppm'] = s['max_valid']
                else:
                    s['ppm'] = (s['lower'] + s['upper']) // 2
                    
            logger.info(f"Ch {ch_id} | Tested PPM: {tested_ppm} | Cent: {cent} | Has Rev: {has_revenue} -> Next Mode: {s['mode']} | Next PPM: {s['ppm']}")

    results = {ch_id: s['max_valid'] for ch_id, s in states.items()}
    output_file = "ppm_max.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=4)
    
    logger.info(f"Finished. Results saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69")
    parser.add_argument("--refresh-graph", action="store_true", help="Fetch a new graph from the node instead of using cache")
    args = parser.parse_args()
    run_ppm_max_search(args.node, args.refresh_graph)
