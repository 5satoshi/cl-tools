#!/usr/bin/python

import sys, math, os, random
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
from mysql.connector import MySQLConnection, Error
from configparser import ConfigParser
from pyln.client import LightningRpc
from datetime import datetime


def read_config(section, filename):
    """ Read database configuration file and return a dictionary object
    :param filename: name of the configuration file
    :param section: section of database configuration
    :return: a dictionary of database parameters
    """
    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(filename)
    
    # get section
    d = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            d[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section,     filename))
    
    return d

# ----------------

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
    version = "0.2"
    
    data_conf = read_config("data",conf)
    
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
    
    mynode = read_config("node",conf)["id"]
    
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
        
        for source, dest, key, data in DG.out_edges(i_node,keys=True,data=True):
            DG[source][dest][key]['base_fee_millisatoshi'] = 0
            DG[source][dest][key]['fee_per_millionth'] = 0
        
        
        print("---")
        print(tx_sat)
        
        i_DG = nx.MultiDiGraph(DG)
        useless_edges = []
        # calculate fee per tx size
        for source, dest, key, data in i_DG.out_edges(keys=True,data=True):
            if i_DG[source][dest][key]['satoshis'] < 2.5*tx_sat:
                useless_edges.append((source, dest, key))
            else:
                a = i_DG[source][dest][key]['base_fee_millisatoshi']
                b = i_DG[source][dest][key]['fee_per_millionth']
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
            i_DG2 = nx.MultiDiGraph(i_DG)
            i_DG2.remove_node(mynode)
            comp_fees, comp_paths = nx.single_source_dijkstra(i_DG2,i_node,weight="fee")
            
            val = []
            for to, peer, ch in destinations:
                theirs = comp_fees.get(to)
                if theirs:
                    fee = theirs - fees[to]
                    val.append((i_node,to,mynode,peer,ch,tx_sat,fee,exec_time.strftime('%Y-%m-%d %H:%M:%S'),version))
            
            
            db_config = read_config("mysql",conf)
            conn = MySQLConnection(**db_config)
            
            mycursor = conn.cursor()
            
            sql = "INSERT INTO routing_competition (source, destination, node, peer, channel_id, tx, fee, gossip_date, version) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            mycursor.executemany(sql, val)
            
            conn.commit()
            
            mycursor.close()
            conn.close()



if __name__ == "__main__":
    # execute only if run as a script
    cfg_file = sys.argv[1]
        
    run_route_finding(cfg_file)






