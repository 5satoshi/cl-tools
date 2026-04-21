# 5satoshi Core Lightning Tools

This directory contains various tools, scripts, and plugins for managing a Core Lightning node and gathering analytics data for the [5satoshi.com](https://5satoshi.com) web page.

## Overview

The scripts in this folder are designed to interact with Core Lightning via RPC, process network graph data, update fees, and store historical routing information.

## Script Documentation

### Data Collection & Syncing to BigQuery
- **`store-graph-data.py`**: Fetches the public Lightning Network topology (`listchannels`, `listnodes`) via RPC and directly pushes the data to Google BigQuery tables.
- **`store-peerchannels.py`**: Gathers detailed local channel data (`listpeerchannels`), performs backward compatibility mappings (e.g., `to_us_msat` to `msatoshi_to_us`), and uploads the active peer states to BigQuery.
- **`store-forwards.py`**: A robust syncing script that queries recent forwardings (routing events) via Lightning RPC and synchronizes them to BigQuery. It uses dry-run capabilities, enforces strict schemas/data types, and utilizes BigQuery `MERGE` operations to keep records up-to-date.

### Network Graph Analytics
- **`betweenness_centrality.py`**: A high-performance pipeline using the `graph-tool` library to calculate network betweenness centrality for both nodes and edges. It pulls the network graph from BigQuery, simulates different transaction sizes (micro, common, macro), computes routing probabilities considering fees, and uploads the centrality metrics back to BigQuery.
- **`centrality_measures.py`**: An alternative centrality calculation script relying on the `networkx` library. It filters out edges that cannot route specific HTLC sizes and calculates the edge/node betweenness metrics before storing them in BigQuery.

### Node Operations & Fee Management
- **`fee-updates_plugin.py`**: A Core Lightning (CLN) plugin that dynamically manages channel fees and HTLC limits. It quantizes liquidity into specific tiers (micro, common, macro) and calculates a power-based fee based on channel balance. It listens to `forward_event` and `channel_state_changed` to trigger automatic channel fee updates.
- **`analyse-closure.py`**: A quick utility script that fetches the most recently closed channels via RPC and parses the `close_info` to print out a human-readable summary of why the channel was closed and by whom.
