import os
from pyln.client import LightningRpc
import pandas as pd
from graph_tool.all import Graph, vertex_average, pseudo_diameter, global_clustering, edge_reciprocity

# Initialize RPC
l1 = LightningRpc(os.environ['HOME'] + "/.lightning/bitcoin/lightning-rpc")
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

# 1. Average Degree
avg_in_degree, _ = vertex_average(g, "in")
avg_out_degree, _ = vertex_average(g, "out")

# 2. Global Clustering Coefficient
g_clust, _ = global_clustering(g)

# 3. Edge Reciprocity
recip = edge_reciprocity(g)

# 4. Pseudo-diameter (Approximation of the longest shortest path)
print("Calculating pseudo-diameter...")
pdiam, _ = pseudo_diameter(g)

report_content = f"""# Lightning Network Topology Report

## 1. Network Size
- **Number of nodes:** {g.num_vertices()}
- **Number of edges (directed channels):** {g.num_edges()}

**Interpretation:** This represents the raw size of the visible public network. Each node is a Lightning Network participant, and each edge is a directional routing channel between them.

## 2. Connectivity
- **Average In-Degree:**  {avg_in_degree:.2f}
- **Average Out-Degree:** {avg_out_degree:.2f}

**Interpretation:** The average degree indicates how many channels a typical node has. Since every channel has a source and destination, the average in-degree and out-degree are equal. A value around 4 implies the average node is connected to 4 other peers, providing multiple routing alternatives.

## 3. Clustering
- **Global Clustering Coefficient:** {g_clust:.4f}

**Interpretation:** The clustering coefficient measures the "cliquishness" of the network—what fraction of a node's neighbors are also connected to each other. A lower value (like 0.06 or 6%) suggests a sparse web characteristic of networks with large routing hubs, rather than dense, fully-connected cliques.

## 4. Reciprocity
- **Edge Reciprocity:** {recip:.4f}

**Interpretation:** This is the fraction of edges that are mutual. A high value (e.g., > 0.90) is expected in Lightning because channels are inherently bidirectional. Non-reciprocal edges usually occur when a channel's policy is updated, broken, or disabled in only one direction.

## 5. Distance
- **Pseudo-diameter:** {pdiam}

**Interpretation:** This represents the longest shortest-path across the network (the maximum number of hops between any two nodes). A low diameter confirms the "small world" nature of the Lightning Network, meaning payments can be routed between almost any two nodes in just a few hops.
"""

report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "network_report.md")
with open(report_path, "w") as f:
    f.write(report_content)

print(f"\nReport successfully generated and saved to: {report_path}")

