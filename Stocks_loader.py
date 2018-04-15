################################################################################
## STOCKS_DAILY_CHANGE
## Reads loader stock file
## Gets index data via Pandas Datareader
## Loads data into the database
## RUN TO LOAD NEW DATA FROM FILE
################################################################################
import csv
import sys
import os
import datetime
import get_data
import date_range
import pyodbc
import stock_procs

import logging
logging.basicConfig(filename='stocks.log',level=logging.INFO, format='%(asctime)s %(message)s')

cnx = pyodbc.connect("DSN=stock64")
cursor = cnx.cursor()
        

# SET PARAMETERS
myPathA = 'd:\\Woodstock\\stock trader\\'
myPathB = 'C:\\Users\\woodd\\Google Drive\\Personal\\Python\\stock trader\\'
myFile2 = 'stock_list.csv'
myFile = 'stock_list_unloaded.csv'

################################################################################
# MAIN PROGRAM
################################################################################
logging.basicConfig(format='%(asctime)s %(message)s')
myPath = myPathA
try:
        openFile = open((myPath+myFile2),'rt')
except:
        myPath = myPathB
        openFile = open((myPath+myFile2),'rt')
openWriteFile = open((myPath+myFile),'w',newline='')
outputWriter = csv.writer(openWriteFile)
outputWriter.writerow(['Ticker'])
unloaded_tickers = ''           # Used to capture any tickers not loaded and reported out at end
reader = csv.DictReader(openFile)

start_date = datetime.datetime.combine(datetime.date.today(), datetime.time(0,0)) - datetime.timedelta(days = 180)
end_date = datetime.datetime.combine(datetime.date.today(), datetime.time(0,0)) - datetime.timedelta(days = 2)
idx = get_data.get_index_history('DJIA', start_date, end_date)  # Change this to run from today to end_date (using db_info)

for row in reader:
        ticker = row['Ticker']

        # Don't try to process tickers already loaded or eliminated
        elim_tickers = stock_procs.get_tickers('eliminated')
        if ticker in elim_tickers:
                logging.info(ticker+' was not loaded as it was previously eliminated ') 
                continue
        loaded_tickers = stock_procs.get_tickers('stock_change')
        if ticker in loaded_tickers:
                logging.info(ticker+' was already loaded')
                continue
        
        total_change = 0
        start_date = datetime.datetime.combine(datetime.date.today(), datetime.time(0,0)) - datetime.timedelta(days = 180)
        end_date = datetime.datetime.combine(datetime.date.today(), datetime.time(0,0)) - datetime.timedelta(days = 2)
        print('Running for '+ticker)
        
        try:
                stock = get_data.get_history(ticker, start_date, end_date)  # Change this to run from today to end_date (using db info)
        except:
                logging.warning(ticker + ' not in Index or no new data to process')
                unloaded_tickers += ticker + ', '
                outputWriter.writerow([ticker])
                continue

#### CHANGE THIS TO TRAVESE THE CLASS OBJECT FROM RECORD 1 TO END - DATES ARE NO LONGER NEEDED  #####

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
                        total_change += perfChange
#                        print('Date: '+str(row.Index)+'  stock change: '+str(round((sChange*100),2))+'%  idxChange: '+str(round((idxChange*100),2)))
                        stock_procs.insert_history(row.Index,ticker,row.Close,sChange, idxChange, perfChange)
                        
        logging.info('The total change for ' + ticker + ' was: '+str(round(total_change*100,2))+'%  for the period from '+str(stock.index[0].strftime('%d %b %Y'))+' to '+str(stock.index[-1].strftime('%d %b %Y')))
logging.info('The following tickers were not found in the index:  ' + unloaded_tickers)
##stock_procs.elim_under_perf()
openFile.close()
openWriteFile.close()
os.remove(myPath+myFile2)
os.rename(myPath+myFile,myPath+myFile2)


        

