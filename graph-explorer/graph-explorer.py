from pyln.client import LightningRpc
import pandas
import networkx as nx
import matplotlib.pyplot as plt

l1 = LightningRpc(".lightning/bitcoin/lightning-rpc")
info = l1.getinfo()

channels = l1.listchannels()
channels.keys()

dfc = pandas.DataFrame(channels["channels"])

nodes = l1.listnodes()
nodes.keys()

dfn = pandas.DataFrame(nodes["nodes"])


# Create empty graph
g = nx.Graph()

# Add edges and edge attributes
for i, elrow in dfc.iterrows():
    g.add_edge(elrow[0], elrow[1], attr_dict=elrow[2:].to_dict())

# Add node attributes
for i, nlrow in dfn.iterrows():
    g.add_node[nlrow['nodeid']] = nlrow[1:].to_dict()


print('# of edges: {}'.format(g.number_of_edges()))
print('# of nodes: {}'.format(g.number_of_nodes()))

# Calculate list of nodes with odd degree
nodes_odd_degree = [v for v, d in g.degree_iter() if d % 2 == 1]

