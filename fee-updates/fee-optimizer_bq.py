#!/usr/bin/python

import sys, math, os, random
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
from configparser import ConfigParser
from pyln.client import LightningRpc
from datetime import datetime

from google.cloud import bigquery

import helper

bq_client = bigquery.Client()
table = bq_client.get_table("lightning-fee-optimizer.version_1.cl_tools")  ###todo move into config

def get_graph_from_cli(rpc=".lightning/bitcoin/lightning-rpc",save=True):
    
    l1 = LightningRpc(rpc)
    
    channels = l1.listchannels()
    
    dfc = pd.DataFrame(channels["channels"])
    
    DG = nx.from_pandas_edgelist(dfc,"source","destination",edge_attr=True, create_using=nx.MultiDiGraph())
    
    if save:
        prefix = datetime.now()
        nx.write_gpickle(DG,"fee-optimizer-data/" + prefix.strftime("%Y-%m-%dT%H:%M:%S")+'_lightning.pkl')
    
    return DG


def run_route_finding(conf):
    version = "0.1"
    
    data_conf = helper.read_config("data",conf)
    
    G = nx.MultiDiGraph()
    exec_time = datetime.now()
    
    if data_conf['method'] == 'file':
        G = nx.read_gpickle(data_conf['file'])
        exec_time = datetime.strptime(data_conf['datetime'], "%Y-%m-%d %H:%M:%S")### override time of execution by time of data pull as defined in config
    elif data_conf['method'] == 'cli':
        rpc = os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc"
        G = get_graph_from_cli(rpc, data_conf['save'])
    
    active_edges = (
        (source,dest,data)
        for source, dest, data
        in G.edges(data=True)
        if data['active']==True
    )
    
    wDG = nx.MultiDiGraph(active_edges)
    
    # clean for connected component of mynode
    DG = wDG.subgraph(max(nx.strongly_connected_components(wDG),key=len))
    
    mynode = helper.read_config("node",conf)["id"]
    
    ### set mynode channel fees to zero for G calc 1
    channels = {}
    for source, dest, key, data in DG.out_edges(mynode,keys=True,data=True):
        DG[source][dest][key]['base_fee_millisatoshi'] = 0
        DG[source][dest][key]['fee_per_millionth'] = 0
        channels[dest] = DG[source][dest][key]['short_channel_id']
    
    
    nodes = list(DG.nodes())
    
    for i in range(int(data_conf['number_of_runs'])):
        
        i_node = nodes[random.randint(0,len(nodes)-1)]
        
        tx_sat = random.randint(1,1000000)
        
        i_DG = nx.MultiDiGraph(DG)
        
        for source, dest, key, data in i_DG.out_edges(i_node,keys=True,data=True):
            i_DG[source][dest][key]['base_fee_millisatoshi'] = 0
            i_DG[source][dest][key]['fee_per_millionth'] = 0
        
        print("---")
        print(tx_sat)
        
        useless_edges = []
        # calculate fee per tx size
        for source, dest, key, data in i_DG.out_edges(keys=True,data=True):
            if i_DG[source][dest][key]['satoshis'] < 2.5*tx_sat:
                useless_edges.append((source, dest, key))
            else:
                a = i_DG[source][dest][key]['base_fee_millisatoshi']
                b = i_DG[source][dest][key]['fee_per_millionth']/1000000
                i_DG[source][dest][key]['fee'] = math.floor(a + tx_sat*b*1000)
        
        for s,d,k in useless_edges:
            i_DG.remove_edge(s, d, k)
        
        found = False
        destinations = []
        
        for i_nodes in nx.strongly_connected_components(i_DG):
            if i_node in i_nodes:
                break
        
        ii_DG = i_DG.subgraph(i_nodes)
        
        fees, paths = nx.single_source_dijkstra(ii_DG,i_node,weight="fee")
        for dest,path in paths.items():
            if mynode in path and dest!=mynode:
                found=True
                channel = channels[path[path.index(mynode)+1]]
                peer = path[path.index(mynode)-1]
                destinations.append((dest,peer,channel))
        
        comp_path = comp_fees = {}
        if found:
            i_DG2 = nx.MultiDiGraph(ii_DG)
            i_DG2.remove_node(mynode)
            comp_fees, comp_paths = nx.single_source_dijkstra(i_DG2,i_node,weight="fee")
            
            val = []
            for to, peer, ch in destinations:
                theirs = comp_fees.get(to)
                if theirs:
                    fee = theirs - fees[to]
                    val.append({'source':i_node,'destination':to,'node':mynode,'peer':peer,'channel_id':ch,'tx':tx_sat,'fee':fee,'gossip_date':exec_time.strftime('%Y-%m-%d %H:%M:%S'),'version':version})
            
            errors = bq_client.insert_rows_json(table, val)
            if errors == []:
                print("success")


if __name__ == "__main__":
    # execute only if run as a script
    cfg_file = sys.argv[1]
        
    run_route_finding(cfg_file)






