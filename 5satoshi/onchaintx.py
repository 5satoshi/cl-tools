
import sys, os, logging
import pandas
from pyln.client import LightningRpc

from bloxplorer import bitcoin_explorer


l1 = LightningRpc(os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc")
txs = l1.listtransactions()
dfp = pandas.DataFrame(txs["transactions"])

onchaintx = dfp[dfp.blockheight>0]

result = bitcoin_explorer.tx.get(dfp.hash[1])
