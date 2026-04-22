#!/usr/bin/python

import sys, math, os, random, logging
import graph_tool.all as gt
import pandas as pd
import matplotlib.pyplot as plt
from pyln.client import LightningRpc
from datetime import datetime
import argparse
from tqdm import tqdm


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


def run_route_finding(number_of_runs, mynode):
    
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
    
    ### set mynode channel fees to zero for G calc 1
    channels = {}
    best_fees = {}
    for e in mynode_v.out_edges():
        ch_id = e_short_id[e]
        channels[v_id[e.target()]] = {
            'short_id': ch_id,
            'base_fee': e_base_fee[e],
            'fee_rate': e_fee_rate[e]
        }
        # Pre-populate best_fees with all channels and default values
        best_fees[ch_id] = {'best_ppm': None, 'actual_ppm': e_fee_rate[e], 'tested': 0}
        e_base_fee[e] = 0
        e_fee_rate[e] = 0
    
    nodes = list(DG.vertices())
    
    logger.info(f"Starting {number_of_runs} route finding simulations...")
    
    for i in tqdm(range(number_of_runs), desc="Simulating routes"):
        
        tx_sat = random.randint(1,1000000)
        
        i_DG = gt.Graph(DG, prune=True)
        iv_id = i_DG.vertex_properties["id"]
        iv_mynode = gt.find_vertex(i_DG, iv_id, mynode)[0]
        
        ie_base_fee = i_DG.edge_properties["base_fee_millisatoshi"]
        ie_fee_rate = i_DG.edge_properties["fee_per_millionth"]
        ie_satoshis = i_DG.edge_properties["satoshis"]
        ie_short_id = i_DG.edge_properties["short_channel_id"]
        
        e_fee = i_DG.new_edge_property("double")
        efilt = i_DG.new_edge_property("bool", val=True)
        
        # calculate fee per tx size
        for e in i_DG.edges():
            if ie_satoshis[e] < 2.5*tx_sat:
                efilt[e] = False
            else:
                a = ie_base_fee[e]
                b = ie_fee_rate[e]/1000000.0
                e_fee[e] = math.floor(a + tx_sat*b*1000)
        
        i_DG.set_edge_filter(efilt)
        
        comp, hist = gt.label_components(i_DG, directed=True)
        if len(hist) == 0:
            continue
            
        mynode_comp = comp[iv_mynode]
        vfilt = i_DG.new_vertex_property("bool")
        vfilt.a = (comp.a == mynode_comp)
        
        ii_DG = gt.GraphView(i_DG, vfilt=vfilt)
        
        ii_mynode_list = gt.find_vertex(ii_DG, iv_id, mynode)
        if not ii_mynode_list:
            continue
        ii_mynode = ii_mynode_list[0]
        
        valid_nodes = list(ii_DG.vertices())
        if len(valid_nodes) < 3:
            continue
            
        i_i_node_v = valid_nodes[random.randint(0,len(valid_nodes)-1)]
        ii_dest_node = valid_nodes[random.randint(0,len(valid_nodes)-1)]
        
        # Ensure we pick distinct A, B, and neither is mynode
        while ii_dest_node == i_i_node_v or ii_dest_node == ii_mynode or i_i_node_v == ii_mynode:
            i_i_node_v = valid_nodes[random.randint(0,len(valid_nodes)-1)]
            ii_dest_node = valid_nodes[random.randint(0,len(valid_nodes)-1)]
            
        i_node = iv_id[i_i_node_v]
        dest_node = iv_id[ii_dest_node]
        
        for e in i_i_node_v.out_edges():
            e_fee[e] = 0
        
        dist_A, pred_A = gt.shortest_distance(ii_DG, source=i_i_node_v, weights=e_fee, pred_map=True)
        
        dist_AB = dist_A[ii_dest_node]
        if dist_AB == float('inf'):
            continue
            
        dist_from_mynode, pred_mynode = gt.shortest_distance(ii_DG, source=ii_mynode, weights=e_fee, pred_map=True)
        
        dist_A_mynode = dist_A[ii_mynode]
        dist_mynode_B = dist_from_mynode[ii_dest_node]
        
        # Check if mynode is on a shortest path
        if math.isclose(dist_AB, dist_A_mynode + dist_mynode_B, rel_tol=1e-9):
            found_competitive = False
            
            # Channel is the first hop after mynode toward destination
            curr = ii_dest_node
            while pred_mynode[curr] != ii_mynode:
                curr = ii_DG.vertex(pred_mynode[curr])
            
            channel_info = channels.get(iv_id[curr])
            if channel_info:
                channel = channel_info['short_id']
                
                # Increment how many times this channel evaluated successfully
                best_fees[channel]['tested'] += 1
                
                i_DG2 = gt.GraphView(ii_DG)
                vfilt2 = i_DG2.new_vertex_property("bool", val=True)
                vfilt2[ii_mynode] = False
                i_DG2.set_vertex_filter(vfilt2)
                
                comp_dist = gt.shortest_distance(i_DG2, source=i_i_node_v, weights=e_fee)
                dist_AB_no_mynode = comp_dist[ii_dest_node]
                
                if dist_AB_no_mynode < float('inf'):
                    max_fee = dist_AB_no_mynode - dist_AB
                    # ppm = (fee_msat - base_fee) * 1000 / tx_sat
                    best_ppm = math.floor((max_fee - channel_info['base_fee']) * 1000 / tx_sat)
                    
                    if best_fees[channel]['best_ppm'] is None or best_ppm > best_fees[channel]['best_ppm']:
                        best_fees[channel]['best_ppm'] = best_ppm

    print("\n=== Best PPM vs Actual PPM per Channel ===")
    for ch, data in sorted(best_fees.items()):
        b_ppm = data['best_ppm'] if data['best_ppm'] is not None else "N/A"
        print(f"Channel {ch}: Tested = {data['tested']} | Best PPM = {b_ppm} | Actual PPM = {data['actual_ppm']}")


if __name__ == "__main__":
    # execute only if run as a script
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=2000)
    parser.add_argument("--node", type=str, default="03fe8461ebc025880b58021c540e0b7782bb2bcdc99da9822f5c6d2184a59b8f69")
    args = parser.parse_args()
    
    run_route_finding(args.runs, args.node)






