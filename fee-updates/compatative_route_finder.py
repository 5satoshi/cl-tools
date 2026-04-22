#!/usr/bin/python

import sys, math, os, random, logging
import graph_tool.all as gt
import pandas as pd
import matplotlib.pyplot as plt
from configparser import ConfigParser
from pyln.client import LightningRpc
from datetime import datetime

import helper


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
        e_satoshis[e] = row.get('satoshis', 0)
        e_short_id[e] = row['short_channel_id']
    
    return DG


def run_route_finding(conf):
    run_conf = helper.read_config("run", conf)
    
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
    
    mynode = helper.read_config("node",conf)["id"]
    v_id = DG.vertex_properties["id"]
    mynode_v = gt.find_vertex(DG, v_id, mynode)[0]
    
    e_base_fee = DG.edge_properties["base_fee_millisatoshi"]
    e_fee_rate = DG.edge_properties["fee_per_millionth"]
    e_short_id = DG.edge_properties["short_channel_id"]
    e_satoshis = DG.edge_properties["satoshis"]
    
    ### set mynode channel fees to zero for G calc 1
    channels = {}
    for e in mynode_v.out_edges():
        e_base_fee[e] = 0
        e_fee_rate[e] = 0
        channels[v_id[e.target()]] = e_short_id[e]
    
    nodes = list(DG.vertices())
    
    best_fees = {}
    
    for i in range(int(run_conf['number_of_runs'])):
        
        i_node_v = nodes[random.randint(0,len(nodes)-1)]
        i_node = v_id[i_node_v]
        
        tx_sat = random.randint(1,1000000)
        
        i_DG = gt.Graph(DG, prune=True)
        iv_id = i_DG.vertex_properties["id"]
        iv_mynode = gt.find_vertex(i_DG, iv_id, mynode)[0]
        i_i_node_v = gt.find_vertex(i_DG, iv_id, i_node)[0]
        
        ie_base_fee = i_DG.edge_properties["base_fee_millisatoshi"]
        ie_fee_rate = i_DG.edge_properties["fee_per_millionth"]
        ie_satoshis = i_DG.edge_properties["satoshis"]
        ie_short_id = i_DG.edge_properties["short_channel_id"]
        
        for e in i_i_node_v.out_edges():
            ie_base_fee[e] = 0
            ie_fee_rate[e] = 0
        
        logging.info("---")
        logging.info("TX amount: " + str(tx_sat))
        
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
        
        found = False
        destinations = []
        
        comp, hist = gt.label_components(i_DG)
        target_comp = comp[i_i_node_v]
        vfilt = i_DG.new_vertex_property("bool")
        vfilt.a = (comp.a == target_comp)
        
        ii_DG = gt.GraphView(i_DG, vfilt=vfilt)
        
        dist, pred = gt.shortest_distance(ii_DG, source=i_i_node_v, weights=e_fee, pred_map=True)
        
        paths = {}
        for v in ii_DG.vertices():
            if dist[v] < float('inf') and v != i_i_node_v:
                path = []
                curr = v
                while curr != i_i_node_v:
                    path.append(iv_id[curr])
                    curr = ii_DG.vertex(pred[curr])
                path.append(iv_id[i_i_node_v])
                path.reverse()
                paths[iv_id[v]] = path
                
        for dest,path in paths.items():
            if mynode in path and dest!=mynode:
                found=True
                channel = channels[path[path.index(mynode)+1]]
                peer = path[path.index(mynode)-1]
                destinations.append((dest,peer,channel))
        
        if found:
            i_DG2 = gt.Graph(ii_DG, prune=True)
            vfilt2 = i_DG2.new_vertex_property("bool", val=True)
            i2_mynode = gt.find_vertex(i_DG2, i_DG2.vertex_properties["id"], mynode)[0]
            vfilt2[i2_mynode] = False
            i_DG2.set_vertex_filter(vfilt2)
            
            i2_i_node_v = gt.find_vertex(i_DG2, i_DG2.vertex_properties["id"], i_node)[0]
            e2_fee = i_DG2.edge_properties[e_fee.name] if e_fee.name else i_DG2.edge_properties.get(e_fee, e_fee)
            comp_dist = gt.shortest_distance(i_DG2, source=i2_i_node_v, weights=e_fee)
            
            comp_fees = {i_DG2.vertex_properties["id"][v]: comp_dist[v] for v in i_DG2.vertices() if comp_dist[v] < float('inf')}
            fees = {iv_id[v]: dist[v] for v in ii_DG.vertices()}
            
            found_competitive = False
            for to, peer, ch in destinations:
                theirs = comp_fees.get(to)
                if theirs is not None:
                    fee = theirs - fees[to]
                    found_competitive = True
                    if ch not in best_fees or fee > best_fees[ch]:
                        best_fees[ch] = fee
            
            if not found_competitive:
                logging.info("No compatative route")

    print("\n=== Best Fee per Channel ===")
    if not best_fees:
        print("No competitive routes found across all runs.")
    else:
        for ch, fee in sorted(best_fees.items()):
            print(f"Channel {ch}: {fee}")


if __name__ == "__main__":
    # execute only if run as a script
    cfg_file = sys.argv[1]
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    run_route_finding(cfg_file)






