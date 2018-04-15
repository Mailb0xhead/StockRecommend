# Loads historical stock data into files
#import pandas as pd
import datetime
from pandas_datareader import data
import io
import requests
import time
import sys


 
def get_history(symbol, startdate, enddate):
    stk = data.DataReader(symbol, 'morningstar', startdate, enddate,2,.1,10)
    stk.reset_index(inplace=True,drop=False)
    stk.set_index('Date',inplace=True)
    return stk

def get_index_history(symbol, startdate, enddate):
    idx = data.DataReader(symbol,'fred',startdate, enddate)
    return idx


## MAIN CODE BODY # 
##symbol = 'GE'
##start = datetime.datetime(2018, 1, 1) # as example
##end = datetime.datetime(2018, 1, 20)
##stock = get_history(symbol, start, end)
##idx = get_index_history('DJIA', start, end)
##
##print (stock.ix['2018-01-01']['Close'])

