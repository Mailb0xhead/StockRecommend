################################################################################
## REFRESH DATA
## Reads stocks loaded into database
## Updates for new data
## RUN FOR DAILY UPDATES ON DATA ALREADY LOADED
################################################################################


import sys
import datetime
import get_data
import date_range
import pyodbc
import stock_procs
import logging
import holidays

logging.basicConfig(filename='refresh.log',level=logging.INFO, format='%(asctime)s %(message)s')

cnx = pyodbc.connect("DSN=stock64")
cursor = cnx.cursor()

################################################################################
# MAIN PROGRAM
################################################################################
unloaded_tickers = ''
# USE RUN_DATE TO AVOID WEEKENDS AND HOLIDAYS
run_date = datetime.datetime.combine(datetime.date.today(), datetime.time(0,0)) - datetime.timedelta(days = 1)
start_date = datetime.datetime.combine(datetime.date.today(), datetime.time(0,0)) - datetime.timedelta(days = 180)
end_date = run_date
idx = get_data.get_index_history('DJIA',start_date, end_date)  # Change this to run from today to end_date (using db_info)
ticker_list = stock_procs.get_tickers('stock_change')
for ticker in ticker_list:
        print('Updating for ticker: '+ticker)
        start_date = stock_procs.get_cursor_range(ticker)
        if start_date < end_date:       
                fetch_start = start_date - datetime.timedelta(days = 1) 
                try:
                        stock = get_data.get_history(ticker, fetch_start, end_date) 
                except:
                        logging.warning(ticker + ' not in Index or invalid date range')
                        unloaded_tickers += ticker + ', '
                        continue
                for row in stock.itertuples():
                        if row.Index == stock.index[0]:
                                startprice = stock.ix[0]['Close'] # first time through is the starting price
                                idxstart = idx.DJIA[stock.index[0]]
                                lastprice = startprice
                                idxlast = idxstart
                                continue
                        else:
                                if row.Volume==0:
                                        continue
                                sChange = (row.Close - lastprice)/lastprice
                                idxChange = (idx.DJIA[row.Index] - idxlast)/idxlast
                                perfChange = sChange - idxChange
                                lastprice = row.Close
                                idxlast = idx.DJIA[row.Index]
        #                        print('Date: '+str(row.Index)+'  stock change: '+str(round((sChange*100),2))+'%  idxChange: '+str(round((idxChange*100),2)))
                                stock_procs.insert_history(row.Index,ticker,row.Close,sChange, idxChange, perfChange)
        else:
                logging.warning('No Data for ' + ticker)
logging.info('The following tickers were not found in the index:  ' + unloaded_tickers)
# stock_procs.elim_under_perf()  # GET RID OF ANY UNDERPERFORMERS
        

