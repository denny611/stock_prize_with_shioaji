#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""



@author: Daniel
"""
!pip install shioaji

from datetime import timedelta, datetime
import shioaji as sj #https://sinotrade.github.io/
import time
import concurrent.futures
import numpy as np
import pandas as pd
from itertools import repeat
from sqlalchemy import create_engine #pip install pandas sqlalchemy
from sqlalchemy import text
from sqlalchemy import Table, Column, Integer, String, MetaData
from google.colab import drive
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from google.colab import userdata
import urllib.request
import json

DATE_FORMATTER = "%Y-%m-%d"
DATA_ROWS = 100
STOCK_ID = "0050"
SQL_TABLE = "stock"
SQL_DB = "/content/drive/MyDrive/py_stock/webpool.db"
DATE_FROM = "2023-05-25"
QUERY_CACHE_ROM_DB = True
PRINT_DEBUG_MSG = True


drive.mount('/content/drive')
def db_connect():
    try:
        drive.mount('/content/drive')
        engine = create_engine(f'sqlite:///{SQL_DB}')
    except Exception as ex:
        print(ex)
    return engine

def db_create_table(engine):
    meta = MetaData()

    students = Table(
    SQL_TABLE, meta,
    Column('index', Integer),
    Column('stock_id', String),
    Column('PRICE', Integer),
    Column('date', String),
    )
    meta.create_all(engine)


def db_insert(conn, df):
    df.to_sql(con=conn, name=SQL_TABLE, if_exists='append')

def db_query(engine,  stock_id_str, date_str):
    queryCmd = ("SELECT Price FROM %s where Stock_id = '%s' and Date  = '%s'" ) \
        %(SQL_TABLE, stock_id_str, date_str)
    with engine.connect() as connection:
        result = connection.execute(text(queryCmd)).fetchone()

    if(result != None):
        return result
    else:
        return None

def db_close(engine):
    engine.dispose()

def query(date_str, stock_id_str, engine):
    if(QUERY_CACHE_ROM_DB):
        db_result = db_query(engine, STOCK_ID, date_str)
        if(db_result != None):
            return ([db_result[0], True])
    try:
        ticks = api.ticks(
            contract=api.Contracts.Stocks[stock_id_str],
            date=date_str,
            query_type=sj.constant.TicksQueryType.LastCount,
            last_cnt=1,
        )
    except Exception as ex:
        if(PRINT_DEBUG_MSG):
            print(ex)
        raise
    #print(len(ticks['close']))
    if(len(ticks['close']) == 0):
        price = np.nan
    else:
        price = ticks['close'][0]
    return ([price, False])

api = sj.Shioaji(simulation=True) # simulate
print(f"sj ver:{sj.__version__}")
db_engine = db_connect()
db_create_table(db_engine)
print("++api login")
api.login(
    api_key=userdata.get("api_key"),     # api key
    secret_key=userdata.get("secret_key")  # secret key
)
print("--api login")

time_deta_one_day = timedelta(days=1)

date_str = DATE_FROM
curr_date = datetime.strptime(date_str, DATE_FORMATTER)

step = 0
ts = time.clock_gettime(time.CLOCK_REALTIME)
date_strs  = []
ticks = []
while (step < DATA_ROWS):
    curr_date += time_deta_one_day
    date_str = curr_date.strftime(DATE_FORMATTER)
    date_strs.append(date_str)
    step += 1

with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
    ticksGenerator = executor.map(query, date_strs, repeat(STOCK_ID), repeat(db_engine))

try:
    dfTicks = pd.DataFrame(ticksGenerator)
except Exception as ex:
    if(PRINT_DEBUG_MSG):
        print(ex)
    print("***********Fail to get data for STOCK id: %s"%STOCK_ID);
    exit(1)

print("query time")
print(time.clock_gettime(time.CLOCK_REALTIME) - ts)

#data from server
print(dfTicks)


tickFramesDump2 =  pd.DataFrame()
tickFramesDump2['Price'] = pd.DataFrame(dfTicks[0])
tickFramesDump2['Cached'] = pd.DataFrame(dfTicks[1])
tickFramesDump2['Stock_id'] = pd.DataFrame(np.repeat(STOCK_ID, DATA_ROWS))
tickFramesDump2['Date'] = pd.DataFrame(date_strs)
print(tickFramesDump2)
#tickFramesDump2 = tickFramesDump2.loc[[0]]
df = tickFramesDump2[~np.isnan(tickFramesDump2.Price)]
print(df)
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
plt.plot(df['Date'], df['Price'])
plt.Axes
numsCount = len(df['Date'])
freq_x = 7
plt.xticks(np.arange(0, numsCount, freq_x), rotation = 50)
tickFramesDump2 = tickFramesDump2.loc[tickFramesDump2['Cached'] == False]
print(tickFramesDump2)
tickFramesDump2 = tickFramesDump2.drop('Cached', axis = 'columns')
print(tickFramesDump2)
db_insert(db_engine, tickFramesDump2)
db_close(db_engine)
plt.show()
