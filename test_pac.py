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
import keyring


DATE_FORMATTER = "%Y-%m-%d"
DATA_ROWS = 10
STOCK_ID = "0050"



def query(date_str, stock_id_str): 
    # print(date_str)
    ticks = api.ticks(
        contract=api.Contracts.Stocks[stock_id_str], 
        date=date_str,
        query_type=sj.constant.TicksQueryType.LastCount,
        last_cnt=1,
    )
    return (ticks)
    
def toPrice(value):
    print(value[1])
  
    
    if(len(value[1]) > 0):
        return value[1][0]
    else:
        return 0

def toDateStr(value):
    print(value[1])
    print(time.time())
    if(len(value[1]) > 0):
        return datetime.fromtimestamp(value[1][0]//10e8).strftime(DATE_FORMATTER)
    else:
        return ""
    
# ts = time.clock_gettime(time.CLOCK_REALTIME)
api = sj.Shioaji(simulation=True) # simulate
print(sj.__version__)


api.login(
    api_key=keyring.get_password("shioaji", "api_key"),     # @api key
    secret_key=keyring.get_password("shioaji", "secret_key")  # @secret key
)

timeDeataOneDay = timedelta(days=1)

date_str = "2020-10-12"
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
    ticksGenerator = executor.map(query, date_strs, repeat(STOCK_ID))

print(time.clock_gettime(time.CLOCK_REALTIME) - ts)
dfTicks = pd.DataFrame(ticksGenerator)

#data from server
print(dfTicks)
