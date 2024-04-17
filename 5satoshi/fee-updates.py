from pyln.client import LightningRpc, Millisatoshi
import pandas
import math, time
import sys, os, logging
import random


def update_fees(rpcpath,test=False,update_all = False):
    l1 = LightningRpc(rpcpath)
    
    peers = l1.listpeers()
    
    dfp = pandas.DataFrame(peers["peers"])
    
    
    for i, row in dfp.iterrows():
        if len(row["channels"])>0:
            if row["channels"][0]["state"]=="CHANNELD_NORMAL":
                channel_id = row["channels"][0]["channel_id"]
                
                msat_to_us = row["channels"][0]["to_us_msat"].millisatoshis
                msat_total = row["channels"][0]["total_msat"].millisatoshis
                
                balance = (msat_to_us+1)/msat_total
                new_fee = pow(math.floor(1/balance),2)
                if new_fee>10000:
                    new_fee=10000
                base_fee = 0
                
                ppm = row["channels"][0]["fee_proportional_millionths"]
                htlc_max = int(Millisatoshi(row["channels"][0]["maximum_htlc_out_msat"]))
                new_htlc_max = int(msat_to_us - random.random() * 0.1 * msat_total)
                if new_htlc_max < 0:
                    new_htlc_max = msat_to_us
                
                if msat_to_us - (0.1 * msat_total) > htlc_max  or msat_to_us < htlc_max or update_all or balance>1:
                    logging.info("Update fee:")
                    logging.info("Channel balance for " + channel_id + " is "+ str(balance) )
                    logging.info("Liquidity is now "+ str(msat_to_us) )
                    logging.info("Old htlc_max " + str(htlc_max) + "; new "+ str(new_htlc_max) )
                    logging.info("Old ppm " + str(ppm) + "; new "+ str(new_fee) )
                    if not test:
                        l1.setchannel(id=channel_id,feebase=base_fee,feeppm=new_fee,htlcmax=new_htlc_max)
                    else:
                        print(channel_id + " feebase=", str(base_fee) + ",feeppm=" + str(new_fee) + ",htlcmax=" + str(new_htlc_max))
                    
                    time.sleep(5)


logging.basicConfig(filename=os.environ['HOME']+'/logs/fees.log', level=logging.INFO,format='%(asctime)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filemode = 'a')

update_fees(os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc")

### db update -------------------------------------------------
l1 = LightningRpc(rpcpath)
peers = l1.listpeers()

dfp = pandas.json_normalize(peers["peers"],record_path=["channels"],meta=['id', 'connected'],sep="_")
dfp = dfp.drop(columns=['features', 'state_changes','status','htlcs'])
dfp['msatoshi_to_us'] = dfp['to_us_msat'].apply(lambda x: x.millisatoshis)
dfp['msatoshi_total'] = dfp['total_msat'].apply(lambda x: x.millisatoshis)

dfp.to_gbq("lightning-fee-optimizer.version_1.peers",if_exists='replace')


### btcbrother -------------------------------------------------


logging.basicConfig(filename=os.environ['HOME']+'/logs/fees-btc.log', level=logging.INFO,format='%(asctime)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filemode = 'a')

update_fees(os.environ['HOME']+"/.lightning-btc/bitcoin/lightning-rpc")

