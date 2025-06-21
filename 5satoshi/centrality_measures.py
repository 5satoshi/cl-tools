#!/usr/bin/python

import math
import networkx as nx
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()
sql="SELECT * FROM `lightning-fee-optimizer.version_1.channels`"
channels = client.query(sql).to_dataframe()

sql="SELECT * FROM `lightning-fee-optimizer.version_1.nodes`"
nodes = client.query(sql).to_dataframe()

DG = nx.from_pandas_edgelist(channels[channels.active],"source","destination",edge_attr=True, create_using=nx.MultiDiGraph())

tx_types = [("common",80000), ("micro",200), ("macro",4000000)]
epsilon = 1

# tx_type,tx_sat = tx_types[2]

for tx_type,tx_sat in tx_types:
    #tx_sat = 4000000 #macro ~1000
    #tx_sat = 200 #micro ~0.05
    #tx_sat = 80000 #common ~20
    for source, dest, key, data in DG.out_edges(keys=True,data=True):
        a = DG[source][dest][key]['base_fee_millisatoshi']
        b = DG[source][dest][key]['fee_per_millionth']/1000000
        DG[source][dest][key]['fee'] = math.floor(a + tx_sat*b*1000) * 1000 + epsilon
    
    sufficient_edges = (
        (source,dest,data)
        for source, dest, data
        in DG.edges(data=True)
        if int(data['htlc_maximum_msat'])>tx_sat*1000 and int(data['htlc_minimum_msat'])<tx_sat*1000
    )
    
    filtered_DG = nx.MultiDiGraph(sufficient_edges)
    newDG = filtered_DG.subgraph(max(nx.strongly_connected_components(filtered_DG),key=len))
    
    start = pd.Timestamp.now()
    
    #betweenness = nx.betweenness_centrality(newDG,normalized=True,weight='fee')
    betweenness = nx.edge_betweenness_centrality(newDG,normalized=True,weight='fee')
    
    stop = pd.Timestamp.now()
    
    print('Time: ', stop - start) 
    
    nodescores = pd.DataFrame.from_dict(data=betweenness,orient='index',columns=['shortest_path_share'])
    nodescores['rank'] = nodescores['shortest_path_share'].rank(method='min',ascending=False)
    nodescores = nodescores.join(nodes[['nodeid','alias']].set_index('nodeid'))
    
    nodescores["timestamp"] = max(channels["last_update"])
    nodescores["nodeid"] = nodescores.index
    nodescores["type"] = tx_type
    
    nodescores.to_gbq("lightning-fee-optimizer.version_1.betweenness",if_exists='append')
    
    ##### Edges
    if tx_type=="common":
        
        start = pd.Timestamp.now()
        
        #betweenness = nx.betweenness_centrality(newDG,normalized=True,weight='fee')
        edge_betweenness = nx.edge_betweenness_centrality(newDG,normalized=True,weight='fee')
        
        stop = pd.Timestamp.now()
        
        print('Time: ', stop - start) 
        
        edgescores = pd.DataFrame([(k[0],k[1],k[2],v) for k,v in edge_betweenness.items()], columns=['source','destination', 'key', 'shortest_path_share'])
        
        edgescores = pd.merge(edgescores, channels[channels.active], how="left", on=['source','destination'])
        edgescores['rank'] = edgescores['shortest_path_share'].rank(method='min',ascending=False)
        
        edgescores["timestamp"] = max(channels["last_update"])
        edgescores["type"] = tx_type
        
        edgescores.to_gbq("lightning-fee-optimizer.version_1.edge_betweenness",if_exists='replace')
