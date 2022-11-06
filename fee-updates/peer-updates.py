from pyln.client import LightningRpc
import pandas
import math, time
import sys, os, logging
from sqlalchemy import create_engine
from datetime import datetime, date, timedelta

import helper

cfg_file = sys.argv[1]
#cfg_file = "peers.conf"

log_config = helper.read_config("logging",cfg_file)
logging.basicConfig(filename=os.environ['HOME']+'/'+log_config["path"], level=logging.INFO,format='%(asctime)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filemode = 'a')


l1 = LightningRpc(os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc")

peers = l1.listpeers()

dfp = pandas.DataFrame(peers["peers"])

db_config = helper.read_config("db",cfg_file)
if db_config["database"]=="bq":
    dfp.to_gbq(db_config["table"],if_exists='replace')
    
else:
    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=db_config["host"], db=db_config["database"], user=db_config["user"], pw=db_config["password"]))
    # Convert dataframe to sql table
    dfp.to_sql('peers', engine, index=False, if_exists="replace")

