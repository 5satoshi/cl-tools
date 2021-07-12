from pyln.client import LightningRpc
import pandas
import math
import time

l1 = LightningRpc("~/.lightning/bitcoin/lightning-rpc")

peers = l1.listpeers()

dfp = pandas.DataFrame(peers["peers"])

for i, row in dfp.iterrows():
    channel_id = row["channels"][0]["channel_id"]
    
    msat_to_us = row["channels"][0]["msatoshi_to_us"]
    msat_total = row["channels"][0]["msatoshi_total"]
    
    print(msat_to_us)
    print(msat_total)
    
    factor = 0.95
    balance = msat_to_us/(msat_total+1)
    new_fee = 256*pow(math.floor(1/(balance*factor)),2)-1
    
    ppm = row["channels"][0]["fee_proportional_millionths"]
    
    print(new_fee)
    
    if ppm!=new_fee :
        l1.setchannelfee(channel_id,0,new_fee)
        time.sleep(5)
