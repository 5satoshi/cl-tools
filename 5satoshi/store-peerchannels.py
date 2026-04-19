from pyln.client import LightningRpc, Millisatoshi
import pandas
import pandas_gbq
import math, time
import sys, os, logging
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

### db update -------------------------------------------------
logger.info("Starting store-peerchannels script")
l1 = LightningRpc(os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc")
logger.info("Fetching peer channels from Lightning RPC")
channels = l1.listpeerchannels()

dfp = pandas.DataFrame(channels["channels"])
logger.info(f"Retrieved {len(dfp)} peer channels")
### backward compatability
dfp['msatoshi_to_us'] = dfp['to_us_msat']
dfp['msatoshi_to_us_max'] = dfp['max_to_us_msat']
dfp['msatoshi_to_us_min'] = dfp['min_to_us_msat']
dfp['out_msatoshi_fulfilled'] = dfp['out_fulfilled_msat']
dfp['msatoshi_total'] = dfp['total_msat']
dfp['id'] = dfp['peer_id']

dfp = dfp.select_dtypes(include=['int64', 'float64', 'object', 'bool', 'datetime64[ns]'])

logger.info("Uploading data to BigQuery: lightning-fee-optimizer.version_1.peers")
pandas_gbq.to_gbq(dfp, "lightning-fee-optimizer.version_1.peers", if_exists='replace')
logger.info("Upload complete")


