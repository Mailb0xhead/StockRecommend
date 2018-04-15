#import csv
import sys
#import mysql.connector
import datetime
#import get_data
import date_range
import pyodbc
import logging


cnx = pyodbc.connect("DSN=stock64")
cursor = cnx.cursor()
##logging.basicConfig(filename='stock_procs.log',level=logging.INFO, format='%(asctime)s %(message)s')


################################################################################################
## PROC GET TICKERS:
## Loads unique ticker list from sql database
################################################################################################
def get_tickers(db):
    tickers = []
    query = ('SELECT DISTINCT ticker from stocks.{0}')
    cursor.execute(query.format(db))
    for row in cursor:
        tickers.append(row.ticker)
    return tickers

################################################################################
## Proc:  INSERT HISTORY
## Loads passed information into stock_change table
################################################################################
def insert_history(s_date,ticker,close,stk_change, idx_change, perf_change):
        query = ('INSERT INTO stock_change (stk_date, ticker, close, stk_change, idx_change, perf_change) VALUES ("{0}","{1}",{2},{3},{4},{5})')
#        print(query.format(str(s_date),ticker,str(close),str(round(stk_change,4)),str(round(idx_change,4)),str(round(perf_change,4))))
        cursor.execute(query.format(str(s_date),ticker,str(close),str(round(stk_change,4)),str(round(idx_change,4)),str(round(perf_change,4))))
        cnx.commit()

              
################################################################################
## Proc:  GET CURSOR RANGE
## Gets the last date loaded in the database for a ticker so only new is loaded
################################################################################
def get_cursor_range(tkr):
        query = ('SELECT max(stk_date) as stk_date FROM stock_change WHERE ticker = "{0}"')
        cursor.execute(query.format(tkr))
        for stk_date in cursor:
                max_date = stk_date[0]
                try:
                        c = max_date - datetime.timedelta(days = 180)
                except:
                        max_date = datetime.datetime.combine(datetime.date.today(), datetime.time(0,0)) - datetime.timedelta(days = 180)
#                       If here is no data in the database, go back half a year
        return(max_date)

################################################################################################
## PROC CHECK TOTAL RETURN:
## Runs query to find the sum/max/min of a given ticker in a given date range
################################################################################################
def check_total_return(tkr, start_date, end_date, field, xtype):
    query = ('SELECT {4}({0}) as tot_return from stocks.stock_change where ticker = "{1}" and stk_date > "{2}" and stk_date < "{3}" ')
##    print(query.format(field, tkr, start_date, end_date, xtype))
    cursor.execute(query.format(field, tkr, start_date, end_date, xtype))
    for row in cursor:
        t_return = row.tot_return
        #print('The return for '+tkr+' was: '+str(row.tot_return))
    return(t_return)


################################################################################
## Proc:  DELETE LOAD HISTORY
## Deletes ALL data from loader table based on ticker
################################################################################
def del_stock(ticker):
    query1 = 'INSERT INTO stock_change_archive select * from stock_change WHERE ticker = "{0}"'
    query2 = 'DELETE FROM stock_change WHERE ticker = "{0}"'
    query3 = 'DELETE FROM ticker_score where ticker = "{0}"'
    if if_exists('ticker', 'portfolio', ticker) is None:
        cursor.execute(query1.format(ticker))
        cnx.commit()
        cursor.execute(query2.format(ticker))
        logging.info(ticker+' was archived from the loader history database')
        cnx.commit()
        cursor.execute(query3.format(ticker))
        cnx.commit()
        

################################################################################
## Proc:  ELIMINATE UNDERPERFORMERS
## removes data from loader table 
################################################################################
def del_under_perf(ticker, end_date, reason,e_type):
    query = 'INSERT INTO eliminated (ticker, e_date, e_reason, e_type) VALUES ("{0}","{1}","{2}","{3}")'
    cursor.execute(query.format(ticker,str(end_date),reason,e_type))
    cnx.commit()
    del_stock(ticker)
    
################################################################################
## Proc:  ELIMINATE UNDERPERFORMERS
## Gets the last date loaded in the database for a ticker so only new is loaded
################################################################################
def elim_under_perf():
    start_date = datetime.datetime.combine(datetime.date.today(), datetime.time(0,0)) - datetime.timedelta(days = 1800)
    end_date = datetime.datetime.combine(datetime.date.today(), datetime.time(0,0))
    stocks = get_tickers('stock_change')
    reason = ''
    for ticker in stocks:
        if if_exists('ticker','portfolio',ticker):
            logging.info(ticker+' has dropped to IGNORE rating but is in portfolio')
            return()
        s_return = check_total_return(ticker, start_date, end_date, 'stk_change', 'sum')
        s_perf = check_total_return(ticker, start_date, end_date, 'perf_change', 'sum')
        if s_return < .05:
            reason = 'Return from '+str(start_date)+' is less than 5% ('+str(round(s_return*100,2))+'%)'
            del_under_perf(ticker, end_date, reason, 'Eliminated')
        elif s_perf < 0:
            reason = 'Stock did not beat index from '+str(start_date)+' ('+str(round(s_return*100,2))+'%)'
            del_under_perf(ticker, end_date, reason, 'Eliminated')
    
            
################################################################################
## Proc:  RECORD SCORE
## Records the stock score to the history database

            
## TO BE REPLACED BY VIEW IN DATABASE            


################################################################################
def record_score(ticker, end_date, perf_score, reccomendation, buy_warning):
    warning = 0
    if buy_warning:
        warning = 1
    query = 'select max(h_date) as h_date from stocks.stock_history where ticker = "{0}"'
    cursor.execute(query.format(ticker))
    for row in cursor:
        try:
            max_date = row.h_date
            if max_date == end_date:
                return()
        except:
             max_date = get_cursor_range(ticker)
    query2 = 'insert into stocks.stock_history (ticker, h_date, h_score, h_recc, buy_warning) values ("{0}","{1}",{2},"{3}",{4})'
    cursor.execute(query2.format(ticker, end_date, perf_score, reccomendation, warning))
    cnx.commit()

################################################################################
## Proc:  IF EXISTS
## checks to see if a given item exists in a given table
################################################################################
def if_exists(field, table, value):
    query = 'select distinct {0} from {1} where {0} = "{2}"'
    results = cursor.execute(query.format(field, table, value))
    result = None
    for row in results:
        result = row
    return(result)

################################################################################
## Proc:  BUY STOCKS
## Moves high rated stocks to the portfolio
################################################################################
def buy_stocks(run_date):
    cursor2 = cnx.cursor()
    query1 = 'select ticker, h_score, buy_warning from stocks.stock_history where h_date = "{0}" and h_score > 20'
    query2 = 'insert into stocks.portfolio (ticker, buy_date, buy_price, action_date, action_price, status) values ("{0}","{1}",{2},"{3}",{4},"{5}")'
    query3 = 'select close from stocks.stock_change where ticker = "{0}" and stk_date = "{1}"'
    query4 = 'update stocks.portfolio set action_date = "{1}", action_price = {2} where ticker = "{0}"'
    query5 = 'update stocks.stock_history set h_action = "{0}" where ticker = "{1}" and h_date = "{2}"'
    buy_list = cursor2.execute(query1.format(run_date))
    for tkr in buy_list:
        ticker = tkr[0]
        max_date = get_cursor_range(ticker)
        price = cursor.execute(query3.format(ticker, run_date))
        for row in price:
            curr_price = row.close
#### NEED TO ACCOUNT FOR LOGIC ON A REBUY, NOT JUST FOR A NEW BUY
        if if_exists('ticker', 'portfolio', ticker) is None:
            if tkr.buy_warning == 0:
                #print(query2.format(ticker,max_date, curr_price, max_date, curr_price, 'BOUGHT'))
                cursor.execute(query2.format(ticker,run_date, curr_price, run_date, curr_price, 'BOUGHT'))
                cursor.commit()
                cursor.execute(query5.format('BOUGHT',ticker,run_date))
                logging.info('bought '+ticker+' at '+str(curr_price))
            else:
                logging.info('DID NOT BUY '+ticker+' due to warnings')

        else:
            #print(query4.format(ticker,max_date, curr_price))
            cursor.execute(query4.format(ticker,run_date, curr_price))
            logging.info('updated '+ticker+' at '+str(curr_price))
    cursor.commit()

################################################################################
## Proc:  SELL STOCKS
## Changes status of portfolio stocks whose rating has dropped to SOLD
################################################################################
def sell_stocks(run_date):
    cursor2 = cnx.cursor()
    query1 = 'select ticker, status from portfolio'
    query2 = 'select ticker, h_date, h_score from stock_history where ticker = "{0}" and h_date = "{1}"'
    query3 = 'update portfolio set status = "{0}" where ticker = "{1}"'
    query5 = 'update stocks.stock_history set h_action = "{0}" where ticker = "{1}" and h_date = "{2}"'
##    query2 = 'select ticker, h_date, h_score from stock_history where ticker = "{0}" and idstock_history in ' + \
##                '(select max(idstock_history) from stock_history group by ticker)'
    portfolio = list(cursor.execute(query1))
    for ticker in portfolio:
        if ticker.status == 'BOUGHT':
    ##        print(query2.format(ticker.ticker))
            curr_score = cursor2.execute(query2.format(ticker.ticker, run_date))
            score = 0
            for row in curr_score:
                score = row.h_score
            if score < 0:
                cursor2.execute(query3.format('SOLD',ticker.ticker))
                cursor2.commit()
                cursor2.execute(query5.format('SOLD',ticker.ticker,run_date))
                cursor2.commit()
                print('SELL '+ticker.ticker+' the score has dropped below 0 ('+str(score)+')')

################################################################################
## Proc:  LOAD VALUES
## Gets the passed value from the passed table in the DB
################################################################################
def load_values(table,v_field,v_where):
    if v_where == '':
        query = 'select {0} from {1}'
        return (list(cursor.execute(query.format(v_field, table))))
        
    else:
        query = 'select {0} from {1} where {2}'
        return (list(cursor.execute(query.format(v_field,table,v_where))))
################################################################################
## Proc:  GET SCORE
## Changes status of portfolio stocks whose rating has dropped to SOLD
################################################################################
def get_score(table, v_field, v_where):
    result = load_values(table, v_field, v_where)[0]
    for item in result:
        score = item
    return(score)
        
##cursor2 = cnx.cursor()
##query1 = 'select ticker, status from portfolio'
##query2 = 'select ticker, h_date, h_score from stock_history where ticker = "{0}" and idstock_history in ' + \
##            '(select max(idstock_history) from stock_history group by ticker)'
##portfolio = cursor.execute(query1)
##for ticker in portfolio:
##    if ticker.status == 'BOUGHT':
####        print(query2.format(ticker.ticker))
##        curr_score = cursor2.execute(query2.format(ticker.ticker))
##        for row in curr_score:
##            score = row.h_score
##            if score < 0:
##                print('SELL '+ticker.ticker+' the score has dropped below 0 ('+str(score)+')')
##
    
                
