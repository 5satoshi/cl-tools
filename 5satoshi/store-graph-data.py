from pyln.client import LightningRpc
import pandas
from pandas_gbq import to_gbq
import sys, os, logging
from datetime import datetime, date, timedelta
from google.cloud import bigquery

client = bigquery.Client()

l1 = LightningRpc(os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc")

### Channels ---------------------------------------------

channels = l1.listchannels()

dfc = pandas.DataFrame(channels["channels"])
dfc['last_update'] = pandas.to_datetime(dfc['last_update'], unit='s')

to_gbq(dfc,"lightning-fee-optimizer.version_1.channels",if_exists='replace')

### Nodes ---------------------------------------------

nodes = l1.listnodes()

dfn = pandas.DataFrame(nodes["nodes"])
dfn['last_timestamp'] = pandas.to_datetime(dfn['last_timestamp'], unit='s')

to_gbq(dfn,"lightning-fee-optimizer.version_1.nodes",if_exists='replace')





