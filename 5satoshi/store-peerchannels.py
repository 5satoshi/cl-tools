from pyln.client import LightningRpc, Millisatoshi
import pandas
import math, time
import sys, os, logging
import random


### db update -------------------------------------------------
l1 = LightningRpc(os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc")
channels = l1.listpeerchannels()

dfp = pandas.DataFrame(channels["channels"])
### backward compatability
dfp['msatoshi_to_us'] = dfp['to_us_msat']
dfp['msatoshi_to_us_max'] = dfp['max_to_us_msat']
dfp['msatoshi_to_us_min'] = dfp['min_to_us_msat']
dfp['out_msatoshi_fulfilled'] = dfp['out_fulfilled_msat']
dfp['msatoshi_total'] = dfp['total_msat']
dfp['id'] = dfp['peer_id']

dfp = dfp.select_dtypes(include=['int64', 'float64', 'object', 'bool', 'datetime64[ns]'])
dfp.to_gbq("lightning-fee-optimizer.version_1.peers",if_exists='replace')


