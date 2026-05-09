import os
import pandas as pd
import graph_tool.all as gt
from pyln.client import LightningRpc
from configparser import ConfigParser

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

def get_filtered_graph_and_node(mynode, refresh_graph=False, tx_sat_cent=80000, rpc=None):
    if rpc is None:
        rpc = os.environ.get('HOME', '') + "/.lightning/bitcoin/lightning-rpc"
    G = load_or_fetch_graph(rpc, refresh=refresh_graph)
    
    if tx_sat_cent is not None:
        tx_msat = tx_sat_cent * 1000
        e_active = G.edge_properties["active"]
        e_htlc_max = G.edge_properties["htlc_maximum_msat"]
        e_filt = G.new_edge_property("bool")
        e_filt.a = e_active.a & (e_htlc_max.a >= tx_msat)
        wDG = gt.GraphView(G, efilt=e_filt)
    else:
        e_active = G.edge_properties["active"]
        wDG = gt.GraphView(G, efilt=e_active)
        
    comp, hist = gt.label_components(wDG)
    largest_comp = hist.argmax()
    v_filt = wDG.new_vertex_property("bool")
    v_filt.a = (comp.a == largest_comp)
    DG = gt.GraphView(wDG, vfilt=v_filt)
    
    v_id = DG.vertex_properties["id"]
    try:
        mynode_v = gt.find_vertex(DG, v_id, mynode)[0]
    except IndexError:
        mynode_v = None
        
    return DG, mynode_v

def load_or_fetch_graph(rpc=".lightning/bitcoin/lightning-rpc", cache_file="graph.gt", refresh=False):
    if not refresh and os.path.exists(cache_file):
        return gt.load_graph(cache_file)
    DG = get_graph_from_cli(rpc)
    DG.save(cache_file)
    return DG

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
    e_htlc_max = DG.new_edge_property("double")
    
    DG.edge_properties["active"] = e_active
    DG.edge_properties["base_fee_millisatoshi"] = e_base_fee
    DG.edge_properties["fee_per_millionth"] = e_fee_rate
    DG.edge_properties["satoshis"] = e_satoshis
    DG.edge_properties["short_channel_id"] = e_short_id
    DG.edge_properties["htlc_maximum_msat"] = e_htlc_max

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
        
        htlc_max_msat = row.get('htlc_maximum_msat', 0)
        if isinstance(htlc_max_msat, str) and htlc_max_msat.endswith('msat'):
            htlc_max_msat = int(htlc_max_msat[:-4])
        elif isinstance(htlc_max_msat, dict) and 'msat' in htlc_max_msat:
            htlc_max_msat = htlc_max_msat['msat']
        e_htlc_max[e] = float(htlc_max_msat)
        
        e_short_id[e] = row['short_channel_id']
    
    return DG
