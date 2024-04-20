from pyln.client import LightningRpc
import pandas
import math, time
import sys, os, logging
from datetime import datetime, date, timedelta
from google.cloud import bigquery
#fwds = l1.listforwards(index='created',start=0,limit=10)

client = bigquery.Client()

status_query = """
    SELECT status, min(received_time) as mintime, max(received_time) as maxtime, min(created_index) as minindex, max(created_index) as maxindex
    FROM `lightning-fee-optimizer.version_1.forwardings` group by status
"""

status_result = client.query(status_query).to_dataframe() 

offered_minindex = status_result.minindex[status_result.status=='offered']

if offered_minindex.empty:
    index_start = status_result.maxindex.max() + 1
else:
    index_start = offered_minindex.iloc[0]
    
    del_query = """DELETE 
        FROM `lightning-fee-optimizer.version_1.forwardings` 
        WHERE created_index >= """
    del_result = client.query(del_query + index)

### Forwardings --------------------------------------------


l1 = LightningRpc(os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc")
#index_start = 2692198

forwards = l1.listforwards(index='created',start=int(index_start))

dff = pandas.DataFrame(forwards["forwards"])
dff["received_time"] = pandas.to_datetime(dff["received_time"], unit = 's')
dff["resolved_time"] = pandas.to_datetime(dff["resolved_time"], unit = 's')

ddf.to_gbq("lightning-fee-optimizer.version_1.forwardings",if_exists='append')



### Channels ---------------------------------------------

channels = l1.listchannels()

dfc = pandas.DataFrame(channels["channels"])
dfc['last_update'] = pandas.to_datetime(dfc['last_update'], unit='s')

dfc.to_gbq("lightning-fee-optimizer.version_1.channels",if_exists='replace')

### Nodes ---------------------------------------------

nodes = l1.listnodes()

dfn = pandas.DataFrame(nodes["nodes"])
dfn['last_timestamp'] = pandas.to_datetime(dfn['last_timestamp'], unit='s')

dfn.to_gbq("lightning-fee-optimizer.version_1.nodes",if_exists='replace')


