from pyln.client import LightningRpc
import pandas
import math, time
import sys, os, logging

logging.basicConfig(filename=os.environ['HOME']+'/logs/fees.log', level=logging.INFO,format='%(asctime)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filemode = 'a')

l1 = LightningRpc(os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc")

peers = l1.listpeers()

dfp = pandas.DataFrame(peers["peers"])

for i, row in dfp.iterrows():
    if len(row["channels"])>0:
        channel_id = row["channels"][0]["channel_id"]
        
        msat_to_us = row["channels"][0]["msatoshi_to_us"]
        msat_total = row["channels"][0]["msatoshi_total"]
        
        val = 1 # 128
        factor = 0.95
        balance = (msat_to_us+1000000)/msat_total
        new_fee = val*pow(math.floor(1/(balance*factor)),2)-1
        base_fee = 1
        if balance>1:
            new_fee=0
        
        ppm = row["channels"][0]["fee_proportional_millionths"]
        
        if ppm!=new_fee :
            logging.info("Update fee:")
            logging.info("Channel balance for " + channel_id + " is "+ str(balance) )
            logging.info("Old ppm " + str(ppm) + "; new "+ str(new_fee) )
            l1.setchannelfee(channel_id,base_fee,new_fee)
            
            time.sleep(5)
