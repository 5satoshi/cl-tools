#!/usr/bin/python

import sys, os, logging
import pandas as pd
from pyln.client import LightningRpc

import helper

if __name__ == "__main__":
    # execute only if run as a script
    cfg_file = sys.argv[1]
    #cfg_file = "channel-updates.conf" #sys.argv[1]
    log_config = helper.read_config("logging",cfg_file)

    rpc = os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc"
    l1 = LightningRpc(rpc)

    channels = l1.listchannels()

    dfc = pd.DataFrame(channels["channels"])
    dfc['last_update'] = pd.to_datetime(dfc['last_update'], unit='s')

    dfc.to_gbq(helper.read_config("bigquery",cfg_file)["table"],if_exists='replace')

