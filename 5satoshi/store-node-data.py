from pyln.client import LightningRpc
import pandas
import math, time
import sys, os, logging
from datetime import datetime, date, timedelta
from google.cloud import bigquery

l1 = LightningRpc(os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc")


### Forwardings --------------------------------------------

forwards = l1.listforwards()

dff = pandas.DataFrame(forwards["forwards"])
dff["received_time"] = pandas.to_datetime(dff["received_time"], unit = 's')
dff["resolved_time"] = pandas.to_datetime(dff["resolved_time"], unit = 's')

client = bigquery.Client()

status_query = """
    SELECT status, min(received_time) as mintime, max(received_time) as maxtime
    FROM `lightning-fee-optimizer.version_1.forwardings` group by status
"""

status_result = client.query(status_query).to_dataframe() 

offered_mintimes = status_result.mintime[status_result.status=='offered']
if offered_mintimes.empty:
    qtime = status_result.maxtime.max() + pandas.DateOffset(microseconds=1000)
else:
    qtime = offered_mintimes.iloc[0]
    
    del_query = """DELETE 
        FROM `lightning-fee-optimizer.version_1.forwardings` 
        WHERE received_time >= """
    del_result = client.query(del_query+ "'" + qtime.strftime('%Y-%m-%d %X') + "'")

filtered_df = dff.loc[(dff["received_time"] >= qtime.tz_localize(None) )]

filtered_df.to_gbq("lightning-fee-optimizer.version_1.forwardings",if_exists='append')

### Peer -------------------------------------------------

peers = l1.listpeers()

dfp = pandas.json_normalize(peers["peers"],record_path=["channels"],meta=['id', 'connected'],sep="_")
dfp = dfp.drop(columns=['features', 'state_changes','status','htlcs'])

dfp.to_gbq("lightning-fee-optimizer.version_1.peers",if_exists='replace')


### Channels ---------------------------------------------

channels = l1.listchannels()

dfc = pandas.DataFrame(channels["channels"])
dfc['last_update'] = pandas.to_datetime(dfc['last_update'], unit='s')

dfc.to_gbq("lightning-fee-optimizer.version_1.channels",if_exists='replace')






