from pyln.client import LightningRpc
import pandas
import math, time
import sys, os, logging
from datetime import datetime, date, timedelta
from google.cloud import bigquery

client = bigquery.Client()

l1 = LightningRpc(os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc")

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


### Forwardings --------------------------------------------


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
    del_result = client.query(del_query + str(index_start))


#index_start = 2692198
forwards = l1.listforwards(index='created',start=int(index_start))
dff = pandas.DataFrame(forwards["forwards"])
dff["received_time"] = dff["received_time"]*10**6
dff["resolved_time"] = dff["resolved_time"]*10**6

#chunk_size=200000
#all_fwds = []
#index_start = 0

#for i in range(14):
    #print(i)
    #forwards = l1.listforwards(index='created',start=index_start+1,limit=chunk_size)
    #all_fwds = all_fwds + forwards["forwards"]
    #index_start = index_start + chunk_size

### strange workaround to get the dtypes mapped
event_query = """SELECT * FROM (SELECT * FROM `lightning-fee-optimizer.version_1.forwardings` where style is not null and failreason is not null LIMIT 1) A 
UNION ALL 
SELECT * FROM (SELECT * FROM `lightning-fee-optimizer.version_1.forwardings` where style is not null and updated_index is not null and resolved_time is not null LIMIT 1) B"""
exmpl_evt = client.query(event_query).to_dataframe()

all_fwds = exmpl_evt.to_dict('records') + dff.to_dict('records')

df = pandas.DataFrame(all_fwds)
df.drop([0,1]).to_gbq("lightning-fee-optimizer.version_1.forwardings",if_exists='append')




