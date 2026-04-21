from pyln.client import LightningRpc
import pandas as pd
from graph_tool.all import Graph, vertex_average, pseudo_diameter, global_clustering, edge_reciprocity

# Initialize RPC
l1 = LightningRpc(".lightning/bitcoin/lightning-rpc")
info = l1.getinfo()

print("Fetching channels...")
channels = l1.listchannels()
dfc = pd.DataFrame(channels["channels"])

print("Fetching nodes...")
nodes = l1.listnodes()
dfn = pd.DataFrame(nodes["nodes"])

# Create empty directed graph (Lightning channels are directional)
g = Graph(directed=True)

print("Building graph...")
# Create mapping from nodeid string to graph-tool vertex object
node_map = {}
for node_id in dfn['nodeid']:
    if node_id not in node_map:
        node_map[node_id] = g.add_vertex()

# Add edges
for _, row in dfc.iterrows():
    src = row['source']
    dst = row['destination']
    
    # Ensure nodes exist even if they weren't in listnodes
    if src not in node_map:
        node_map[src] = g.add_vertex()
    if dst not in node_map:
        node_map[dst] = g.add_vertex()
        
    g.add_edge(node_map[src], node_map[dst])

print("\n--- Network KPIs ---")
print(f"Number of nodes: {g.num_vertices()}")
print(f"Number of edges: {g.num_edges()}")

# 1. Average Degree
avg_in_degree, _ = vertex_average(g, "in")
avg_out_degree, _ = vertex_average(g, "out")
print(f"Average In-Degree:  {avg_in_degree:.2f}")
print(f"Average Out-Degree: {avg_out_degree:.2f}")

# 2. Global Clustering Coefficient
g_clust, _ = global_clustering(g)
print(f"Global Clustering Coefficient: {g_clust:.4f}")

# 3. Edge Reciprocity
recip = edge_reciprocity(g)
print(f"Edge Reciprocity: {recip:.4f}")

# 4. Pseudo-diameter (Approximation of the longest shortest path)
print("Calculating pseudo-diameter...")
pdiam, _ = pseudo_diameter(g)
print(f"Pseudo-diameter: {pdiam}")

