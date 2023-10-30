#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 15:21:54 2023

@author: denny
"""
from datetime import timedelta, datetime
import shioaji as sj
import time
import concurrent.futures
import numpy as np
import pandas as pd
from itertools import repeat
from sqlalchemy import create_engine
from sqlalchemy import text
import keyring


DATE_FORMATTER = "%Y-%m-%d"
DATA_ROWS = 100
STOCK_ID = "0056"
SQL_TABLE = "stock"
SQL_DB = "webpool"
QUERY_CACHE_ROM_DB = True

def db_connect():
    username = keyring.get_password("db", "username")
    password = keyring.get_password("db", "password")
    try:
        engine = create_engine('mysql+pymysql://%s:%s@localhost/'%(username,password)  + SQL_DB)
    except Exception as ex:
        print(ex)
    return engine



def db_insert(conn, df):
    df.to_sql(con=conn, name=SQL_TABLE, if_exists='append')
    

def db_query(engine,  stock_id_str, date_str):
    queryCmd = ("SELECT Price FROM %s where Stock_id = '%s' and Date  = '%s'" ) \
        %(SQL_TABLE, stock_id_str, date_str)
    # print(queryCmd)
    with engine.connect() as connection:
        #result = connection.execute(text("SELECT * FROM :table WHERE Date = :date"), dict(date=date_str, table = SQL_TABLE))
        result = connection.execute(text(queryCmd)).fetchone()
    #result = cursor.fetchall()
    if(result != None):
        # print(result[0])
        return result
    else:
        return None


def query(date_str, stock_id_str, engine): 
    # print(date_str)
    if(QUERY_CACHE_ROM_DB):
        db_result = db_query(engine, STOCK_ID, date_str)
        if(db_result != None):
            print("cache hit")
            return db_result;
    ticks = api.ticks(
        contract=api.Contracts.Stocks[stock_id_str], 
        date=date_str,
        query_type=sj.constant.TicksQueryType.LastCount,
        last_cnt=1,
    )
    return (ticks['close'])
    
def toPrice(value):
    print(value[1])
  
    
    if(len(value[1]) > 0):
        return value[1][0]
    else:
        return 0

def toDateStr(value):
    print(value[1])
    #print(time.time())
    if(len(value[1]) > 0):
        return datetime.fromtimestamp(value[1][0]//10e8).strftime(DATE_FORMATTER)
    else:
        return ""

# ts = time.clock_gettime(time.CLOCK_REALTIME)
api = sj.Shioaji(simulation=True) # simulate
print(sj.__version__)
db_engine = db_connect()

api.login(
    api_key=keyring.get_password("shioaji", "api_key"),     # @api key
    secret_key=keyring.get_password("shioaji", "secret_key")  # @secret key
)

timeDeataOneDay = timedelta(days=1)

date_str = "2023-07-17"
curr_date = datetime.strptime(date_str, DATE_FORMATTER)

step = 0
ts = time.clock_gettime(time.CLOCK_REALTIME)
date_strs  = []
ticks = []
while (step < DATA_ROWS):
    curr_date += timeDeataOneDay
    date_str = curr_date.strftime(DATE_FORMATTER)
    date_strs.append(date_str)
    step += 1

with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
    ticksGenerator = executor.map(query, date_strs, repeat(STOCK_ID), repeat(db_engine))


dfTicks = pd.DataFrame(ticksGenerator)

#data from server
print(dfTicks)
print("query time")
print(time.clock_gettime(time.CLOCK_REALTIME) - ts)


tickFramesDump2 =  pd.DataFrame()
tickFramesDump2['Price'] = pd.DataFrame(dfTicks)
tickFramesDump2['Stock_id'] = pd.DataFrame(np.repeat(STOCK_ID, DATA_ROWS))
tickFramesDump2['Date'] = pd.DataFrame(date_strs)

df = tickFramesDump2[~np.isnan(tickFramesDump2.Price)]
print(df)
#print(type(df[0]))
  
db_insert(db_engine, tickFramesDump2)
