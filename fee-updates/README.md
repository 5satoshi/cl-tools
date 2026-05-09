# Fee Updates & Routing Tools

This directory contains various scripts and utilities for managing channel fees, analyzing competitive routes, and synchronizing network data for a Core Lightning node.

## Script Overview

### Routing & Fees
- **`compatative_route_finder.py`**: A tool that interacts with the Lightning RPC to find and compare alternative routing paths across the Lightning Network.

### Utilities
- **`graph_helper.py`**: Contains shared utility functions, such as fetching network graphs (`load_or_fetch_graph`) and reading configuration files (`read_config`), used by the other scripts in this directory.
