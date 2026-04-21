# Graph Explorer

This directory contains tools designed to interact with and explore the Lightning Network graph topology.

## Scripts

- **`graph-explorer.py`**: A script that connects to Core Lightning, builds a directed graph using `graph-tool`, and calculates key network topology metrics (Average Degree, Global Clustering, Edge Reciprocity, and Strongly Connected Components). It automatically generates a detailed `network_report.md` file with the results and their interpretations.
