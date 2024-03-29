from pyln.client import LightningRpc
import pandas
import math, time
import sys, os, logging
from sqlalchemy import create_engine
from datetime import datetime, date, timedelta

import helper

cfg_file = sys.argv[1]
#cfg_file = "forwardings.conf"

log_config = helper.read_config("logging",cfg_file)
logging.basicConfig(filename=os.environ['HOME']+'/'+log_config["path"], level=logging.INFO,format='%(asctime)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filemode = 'a')

l1 = LightningRpc(os.environ['HOME']+"/.lightning/bitcoin/lightning-rpc")

forwards = l1.listforwards(timelimit=str(int(time.time())-60*60*24*7)+"000000000")

dfp = pandas.DataFrame(forwards["forwards"])
dfp["received_time"] = pandas.to_datetime(dfp["received_time"], unit = 's')
dfp["resolved_time"] = pandas.to_datetime(dfp["resolved_time"], unit = 's')

yesterday = date.today() - timedelta(days=1)
filtered_df = dfp.loc[(dfp["received_time"].dt.date == yesterday)]

db_config = helper.read_config("db",cfg_file)
if db_config["database"]=="bq":
    filtered_df.to_gbq(db_config["table"],if_exists='append')
    
else:
    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=db_config["host"], db=db_config["database"], user=db_config["user"], pw=db_config["password"]))
    # Convert dataframe to sql table
    filtered_df.to_sql('forwardings', engine, index=False, if_exists="append")
