#!/usr/bin/python

import sys, math, os, random, logging
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
from mysql.connector import MySQLConnection, Error
from configparser import ConfigParser
from pyln.client import LightningRpc
from datetime import datetime

from google.cloud import bigquery


def get_graph_from_cli(rpc=".lightning/bitcoin/lightning-rpc",save=True):
    
    l1 = LightningRpc(rpc)
    
    channels = l1.listchannels()
    
    dfc = pd.DataFrame(channels["channels"])
    
    DG = nx.from_pandas_edgelist(dfc,"source","destination",edge_attr=True, create_using=nx.MultiDiGraph())
    
    if save:
        prefix = datetime.now()
        nx.write_gpickle(DG,"fee-optimizer-data/" + prefix.strftime("%Y-%m-%dT%H:%M:%S")+'_lightning.pkl')
    
    return DG

rpc = os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc"
G = get_graph_from_cli(rpc, False)

avg_tx_size = 80000

active_edges = (
    (source,dest,data)
    for source, dest, data
    in G.edges(data=True)
    if data['active']==True
)

wDG = nx.MultiDiGraph(active_edges)

DG = wDG.subgraph(max(nx.strongly_connected_components(wDG),key=len))

useless_edges = []
for source, dest, key, data in DG.out_edges(keys=True,data=True):
    if DG[source][dest][key]['satoshis'] < 2.5*avg_tx_size:
        #TODO remove edges with low htlc_minimum_msats
        useless_edges.append((source, dest, key))
    else:
        a = DG[source][dest][key]['base_fee_millisatoshi']
        b = DG[source][dest][key]['fee_per_millionth'] 
        DG[source][dest][key]['fee'] = math.floor(a + avg_tx_size*b*1000)

for s,d,k in useless_edges:
    DG.remove_edge(s, d, k)

centrality = nx.betweenness_centrality(DG,weight='fee')

res = sorted(centrality.items(), key=lambda x:x[1], reverse=True)

dict(list(res.items())[0:5])
list(res.keys()).index('animal')

