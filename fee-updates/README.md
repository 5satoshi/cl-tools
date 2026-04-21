# Fee Updates & Routing Tools

This directory contains various scripts and utilities for managing channel fees, analyzing competitive routes, and synchronizing network data for a Core Lightning node.

## Script Overview

### Routing & Fees
- **`fee-updates.py`**: Handles the calculation and application of fee adjustments to channels.
- **`compatative_route_finder.py`**: A tool that interacts with the Lightning RPC to find and compare alternative routing paths across the Lightning Network.
- **`channel-updates.py`**: Manages and applies channel-specific configuration and fee updates.

### Data Synchronization & Updates
- **`forwards-transfer.py` & `forwards-updates.py`**: Scripts focused on extracting, transferring, and updating forwarding (routing) event data.
- **`nodes-updates.py`**: Synchronizes or updates network node information.
- **`peer-updates.py`**: Updates states and metrics related to direct channel peers.
- **`table-transfer.py`**: A utility script for migrating or transferring data tables, likely between local databases and analytics backends.

### Utilities
- **`helper.py`**: Contains shared utility functions, such as reading configuration files (`read_config`), used by the other scripts in this directory.
