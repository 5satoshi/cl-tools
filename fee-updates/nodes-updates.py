#!/usr/bin/python

import sys, os, logging
import pandas as pd
from pyln.client import LightningRpc

import helper

if __name__ == "__main__":
    # execute only if run as a script
    cfg_file = sys.argv[1]
    #cfg_file = "nodes-updates.conf"
    log_config = helper.read_config("logging",cfg_file)

    rpc = os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc"
    l1 = LightningRpc(rpc)

    nodes = l1.listnodes()

    dfn = pd.DataFrame(nodes["nodes"])
    dfn['last_timestamp'] = pd.to_datetime(dfn['last_timestamp'], unit='s')

    dfn.to_gbq(helper.read_config("bigquery",cfg_file)["table"],if_exists='replace')

