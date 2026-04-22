# Fee Updates & Routing Tools

This directory contains various scripts and utilities for managing channel fees, analyzing competitive routes, and synchronizing network data for a Core Lightning node.

## Script Overview

### Routing & Fees
- **`compatative_route_finder.py`**: A tool that interacts with the Lightning RPC to find and compare alternative routing paths across the Lightning Network.

### Data Synchronization & Updates
- **`forwards-transfer.py`**: Script focused on extracting and transferring forwarding (routing) event data.
- **`table-transfer.py`**: A utility script for migrating or transferring data tables, likely between local databases and analytics backends.

### Utilities
- **`helper.py`**: Contains shared utility functions, such as reading configuration files (`read_config`), used by the other scripts in this directory.
