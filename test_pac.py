#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 15:21:54 2023

@author: Daniel
"""
from datetime import timedelta, datetime
import shioaji as sj #https://sinotrade.github.io/
import time
import concurrent.futures
import numpy as np
import pandas as pd
from itertools import repeat
from sqlalchemy import create_engine
from sqlalchemy import text
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import keyring
import urllib.request
import json

DATE_FORMATTER = "%Y-%m-%d"
DATA_ROWS = 100
STOCK_ID = "0050"
SQL_TABLE = "stock"
SQL_DB = "webpool"
DATE_FROM = "2023-05-25"
QUERY_CACHE_ROM_DB = True
PRINT_DEBUG_MSG = True

def make_url_data_day_avg():
    url_day_avg_twse = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_AVG_ALL"
    headers = {'accept' : 'application/json ',}  #  application/json default
    # headers = {'accept' : 'text/csv',}  # text/csv
    return url_day_avg_twse, headers

def url_get_data(url, headers):
    req = urllib.request.Request(url, headers = headers)
    with urllib.request.urlopen(req) as response:
        data = response.read().decode()
        # print(data)
        # print(type(data))
        return data

def get_avg_price_from_jason_str(data):
    data = json.loads(data)
    df = pd.DataFrame(data)
    df = df.loc[df['Code'] == STOCK_ID]
    print(df)
    font = FontProperties(fname='eduSong_Unicode.ttf', size = 20)
    textstr="[%s]Last Price: %s, Monthly Average Price :%s\nQuery in %s" \
            %(df['Name'].to_list()[0], df['ClosingPrice'].to_list()[0], \
            df['MonthlyAveragePrice'].to_list()[0], \
            datetime.now().strftime(DATE_FORMATTER))
    ax.legend([textstr], prop=font)

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
    with engine.connect() as connection:
        result = connection.execute(text(queryCmd)).fetchone()

    if(result != None):
        return result
    else:
        return None


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
print(sj.__version__)
db_engine = db_connect()

api.login(
    api_key=keyring.get_password("shioaji", "api_key"),     # @api key
    secret_key=keyring.get_password("shioaji", "secret_key")  # @secret key
)

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
numsCount = len(df['Date'])
freq_x = 7
plt.xticks(np.arange(0, numsCount, freq_x), rotation = 50)
tickFramesDump2 = tickFramesDump2.loc[tickFramesDump2['Cached'] == False]
print(tickFramesDump2)
tickFramesDump2 = tickFramesDump2.drop('Cached', axis = 'columns')
print(tickFramesDump2)
db_insert(db_engine, tickFramesDump2)
make_url_data_day_avg()
url, headers = make_url_data_day_avg()
data = url_get_data(url, headers)
get_avg_price_from_jason_str(data)
plt.show()
