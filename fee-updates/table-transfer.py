#!/usr/bin/python

import pandas as pd
import math, time
import sys, os, logging

from sqlalchemy import create_engine
from datetime import datetime, date, timedelta

import helper

if __name__ == "__main__":
    cfg_file = sys.argv[1]
    #cfg_file = "forwards-transfer.conf"
    db_config = helper.read_config("mysql",cfg_file)

    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=db_config["host"], db=db_config["database"], user=db_config["user"], pw=db_config["password"]))

    table_name = db_config["table"]
    table_df = pd.read_sql_table(
        table_name,
        con=engine
    )

    table_df.to_gbq(helper.read_config("bigquery",cfg_file)["table"],if_exists='replace')
