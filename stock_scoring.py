import sys
import datetime
import date_range
import pyodbc
import logging
import stock_procs
import holidays

cnx = pyodbc.connect("DSN=stock64")
cursor = cnx.cursor()

##################################################################################################
#### PROC CHECK TOTAL RETURN:
#### Runs query to find the sum/max/min of a given ticker in a given date range
##################################################################################################
##def check_total_return(tkr, start_date, end_date, field, xtype):
##    query = ('SELECT {4}({0}) as tot_return from stocks.stock_change where ticker = "{1}" and stk_date > "{2}" and stk_date <= "{3}" ')
###    print(query.format(field, tkr, start_date, end_date, xtype))
##    cursor.execute(query.format(field, tkr, start_date, end_date, xtype))
##    for row in cursor:
##        t_return = row.tot_return
##        #print('The return for '+tkr+' was: '+str(row.tot_return))
##    return(t_return)

################################################################################################
## PROC CHECK TOTAL RETURN:
## Runs query to find the sum/max/min of a given ticker in a given date range
################################################################################################
def check_total_return2(tkr, start_date, end_date, field, xtype, where):
    if where == 'na':
        query = ('SELECT {4}({0}) as tot_return from stocks.stock_change where ticker = "{1}" and stk_date > "{2}" and stk_date <= "{3}" ')
        cursor.execute(query.format(field, tkr, start_date, end_date, xtype))
    else:
        query = ('SELECT {4}({0}) as tot_return from stocks.stock_change where {5} and ticker = "{1}" and stk_date > "{2}" and stk_date <= "{3}" ')
#        print(query.format(field, tkr, start_date, end_date, xtype, where))
        cursor.execute(query.format(field, tkr, start_date, end_date, xtype, where))
        
#    print(query.format(field, tkr, start_date, end_date, xtype))
    for row in cursor:
        t_return = row.tot_return
        #print('The return for '+tkr+' was: '+str(row.tot_return))
    return(t_return)

################################################################################################
## PROC Check Volatility:
## Runs a sql query to count for a passed condition (usually change amount) against a
## ticker and date range
################################################################################################
def check_vol(field, where, start_date, end_date, tkr):
    query = 'select {0} as num from stocks.stock_change where {1} and stk_date > "{2}" and stk_date <= "{3}" and ticker = "{4}"'
##    print(query.format(field, start_date, end_date, tkr))
    cursor.execute(query.format(field, where, start_date, end_date, tkr))
    for row in cursor:
        t_return = row.num
    return(t_return)

################################################################################################
## PROC Record Score:
## Records score for scoring algorithm into ticker score data table
################################################################################################
def record_score(ticker, score_name, score, as_of_date):
    if score == 0:
        return
    query = 'INSERT INTO ticker_score (ticker, score_name, score, as_of_date, run_date) VALUES ("{0}","{1}",{2},"{3}","{4}")'
##    print(query.format(field, start_date, end_date, tkr))
    cursor.execute(query.format(ticker, score_name, score, as_of_date,datetime.datetime.combine(datetime.date.today(), datetime.time(0,0))))
    cnx.commit()
    return

################################################################################################
## PROC Score Check:
## Scores the total return for a given period (assume 6 months)
## Eliminates and under or over performers
## ticker = stock ticker
## start_date = the earliest date to start the comparison
## end_date = the last date for the comparison
## compare = the operator for the comparison (>, <, =, etc...)
## elim = whether or not the ticker should be archived and removed if the test fails
## score_name = tha name of the score that this test should run against (from DB)
## field = the name of the field that should be used for the comparison (stk, perf, or idx)
## calc = the type of group calculation that should be used (sum, max, min, etc...)
## msg = the message that should be recorded along with the score / elimination
## fail = whether the penalty multiplier is to be scored when the test fails
################################################################################################
def score_check(ticker, start_date, end_date, compare, elim, score_name, field, calc, msg, fail, run_date):
    tot_return = check_total_return2(ticker, start_date, end_date, field, calc, 'na')
    score_set = stock_procs.load_values('algorithm','setting, score, fail_score','name="'+score_name+'"')
    if eval(str(tot_return)+compare+str(score_set[0][0])):
        record_score(ticker, score_name, tot_return*score_set[0][1], run_date)
##        print(ticker+' has passed')
        return(True)
    else:
        if fail:
            record_score(ticker, score_name, tot_return*score_set[0][2], run_date)
##            print(ticker+' FAILED')
        if elim:
            print(ticker+' has been ELIMINATED due to total return of '+str(tot_return))
            stock_procs.del_under_perf(ticker, end_date, score_name, msg)
        return(False)

    
def vol_check(ticker, start_date, end_date, compare, elim, score_name, field, calc, msg, fail, run_date):
    score_set = stock_procs.load_values('algorithm','setting, score, fail_score','name="'+score_name+'"')
    volatility = check_total_return2(ticker, start_date, end_date, field, calc, field+compare+str(score_set[0][0]))
 #   print(ticker+' has '+str(volatility)+' major drops.  Score = '+str(volatility*score_set[0][2]))
    record_score(ticker, score_name, volatility*score_set[0][2], run_date)
    return(True)

