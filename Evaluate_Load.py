#import csv
import sys
#import mysql.connector
import datetime
#import get_data
import date_range
import pyodbc
import logging
import stock_procs
import stock_scoring
import holidays

logging.basicConfig(filename='evaluate.log',level=logging.INFO, format='%(asctime)s %(message)s')
us_holidays = holidays.UnitedStates()
cnx = pyodbc.connect("DSN=stock64")
cursor = cnx.cursor()


################################################################################################
## PROC CHECK TOTAL RETURN:
## Runs query to find the sum/max/min of a given ticker in a given date range
################################################################################################
def check_total_return(tkr, start_date, end_date, field, xtype):
    query = ('SELECT {4}({0}) as tot_return from stocks.stock_change where ticker = "{1}" and stk_date > "{2}" and stk_date <= "{3}" ')
##    print(query.format(field, tkr, start_date, end_date, xtype))
    cursor.execute(query.format(field, tkr, start_date, end_date, xtype))
    for row in cursor:
        t_return = row.tot_return
        #print('The return for '+tkr+' was: '+str(row.tot_return))
    return(t_return)

################################################################################################
## PROC Check Volatility:
## Runs a sql query to count for a passed condition (usually change amount) against a
## ticker and date range
################################################################################################
def check_vol(field, start_date, end_date, tkr):
    query = 'select count(*) as num from stocks.stock_change where {0} and stk_date > "{1}" and stk_date <= "{2}" and ticker = "{3}"'
##    print(query.format(field, start_date, end_date, tkr))
    cursor.execute(query.format(field, start_date, end_date, tkr))
    for row in cursor:
        t_return = row.num
    return(t_return)
    

#############################################################################################
# MAIN PROGRAM

# SETTINGS

##v_mo_vol = 4            # Amount of allowed volatility changes in a month
##v_jump_pct_tot = .50    # Highest volatility change allowed as a % of total change in 6 months
##v_big_drop = .025       # Threshhold for what is a "big" drop
##v_mo_return = .05       # Threshold for minimum return in last 30 days
##v_hi_vol = .02          # High volatility amount
##v_mid_vol = .015        # Medium volatility amount
##v_low_vol = .01         # Low volatility amount
##v_hi_vol_tol = 2        # Amount of high volatility changes allowed in total sample range (default 6 mo)
##v_tot_vol_tol = 10      # Total amount of allowed volatility changes in sample range
##v_idx_perf = 0          # Minimum amount of performance against index allowed to be considered
##v_month_return_goal = .05   
##v_strong_buy = 40       # Score needed for Strong Buy
##v_buy = 20              # Score needed for Buy
##v_consider = 0          # Score needed for Consider
##v_investigate = -20     # Score needed for Investigate
##v_tot_return_hi = 1   # Return seen as excessive for a six month period
##
##v_tot_return = .05      # Minimum total return allowed to be considered
##v_good_return = .15     # Good return amount for sample range
##
##
### SCORES
##s_good_return = 5      # Good return for the period being measured
##s_last_week = 20        # Positive previous 7 days v index
##s_last_2_weeks = 15     # Positive 7 < x < 14 return v index 
##s_last_3_weeks = 10     # Positive 14 < x < 28 return v index
##s_last_month_v_inx = 20 # Positive previous 30 days v index
##s_jump_v_total = 10     # Highest one day move < tolerance on total return
##s_stk_has_big_drop = 5 # No major drops
##s_drop_3_mos = 5       # No major drops last 3 mos
##s_drop_v_gain = 5      # Number of drops < no of gains
##s_last_mo_beat_idx = 10 # Return from last 30 days > index return
##s_last_4_wk_ea_beat_idx = 20    # Last 4 weeks all beat index individually
##s_last_mo_return = 5   # Return on stock beat goal for last month
##s_last_mo_neg_pos_vol = 5      # Last month volatility was within tolerance
##s_tot_vol = 5          # Total volatility was within tolerance
##s_month_return_goal = 5        # Last month return goal was met
##s_recent_v = 5         # There was no major recent volatility

#stock_procs.elim_under_perf()  # Eliminate poor performing stocks before processing (from load just in case)
ticker_list = stock_procs.get_tickers('stock_change') # Load tickers


## Setup Run Length (cmd line input for scheduled runs)
num_days_back = 1  # Used to populate old data for testing - REMOVE WHEN SCHEDULED AS A JOB
num_days_run = 1

    
for runtimes in range(0, num_days_run):  # To process old data
    
    run_date = datetime.datetime.combine(datetime.date.today(), datetime.time(0,0)) - datetime.timedelta(days = num_days_back)
    if (run_date in us_holidays or run_date.weekday() > 4):
        print('not processing for '+str(run_date)+' due to weekend/holiday')
        num_days_back -= 1
        continue
    print('run date is:  '+str(run_date))
    logging.info('\n *********** Running for date:  '+str(run_date)+' **************)')
    
    for ticker in ticker_list:
        perf_score = 0  #set score counter to zero for each run through (ticker)
        buy_warning = False
        start_date = run_date - datetime.timedelta(days = 180) # reset start date each run
        end_date = run_date #reset end date each run

    # SCORE TOTAL RETURN FOR 6 MONTHS - ELIMINATE OVER/UNDER PERFORMERS
        if not (stock_scoring.score_check(ticker, start_date, end_date, '>', True, 'min_return', 'stk_change', 'sum','POOR RETURN', False, run_date)):
            print(ticker + ' FAILED TOTAL RETURN - REMOVED FROM CONSIDERATION & ARCHIVED')
            continue
        if not (stock_scoring.score_check(ticker, start_date, end_date, '<', True, 'excess_return', 'stk_change', 'sum', 'EXCESSIVE RETURN', False, run_date)):
            print(ticker + ' FAILED EXCESS RETURN - REMOVED FROM CONSIDERATION & ARCHIVED')
            continue
    
    # CHECK TO SEE IF IT RETURNED MORE THAN INDEX
        if not stock_scoring.score_check(ticker, start_date, end_date, '>', True, 'idx_perf', 'perf_change', 'sum', 'UNDERPERFORMED INDEX', False, run_date):
            print(ticker+' FAILED TO BEAT INDEX')
            continue
        
    # CHECK BIGGEST DROP
        big_drop = stock_scoring.score_check(ticker, start_date, end_date, '<', False, 'big_drop', 'stk_change', 'min', 'NA', False, run_date)

    # SCORE LAST 4  WEEK's PERFORMANCE VS INDEX
        x = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 7), end_date, '>', False, '1w_perf', 'perf_change', 'sum', 'NA', True, run_date)
        x = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 14), run_date - datetime.timedelta(days = 7), '>', False, '2w_perf', 'perf_change', 'sum', 'NA', True, run_date)
        x = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 21), run_date - datetime.timedelta(days = 14), '>', False, '3w_perf', 'perf_change', 'sum', 'NA', True, run_date)
        x = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 28), run_date - datetime.timedelta(days = 21), '>', False, '4w_perf', 'perf_change', 'sum', 'NA', True, run_date)

    # CHECK TO SEE IF EACH OF THE LAST 4 WEEKS ALL BEAT INDEX
        wk1 = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 7), end_date, '>', False, 'idx_perf', 'perf_change', 'sum', 'NA', False, run_date)
        wk2 = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 14), run_date - datetime.timedelta(days = 7), '>', False, 'idx_rg_perf', 'perf_change', 'sum', 'NA', False, run_date)
        wk3 = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 21), run_date - datetime.timedelta(days = 14), '>', False, 'idx_rg_perf', 'perf_change', 'sum', 'NA', False, run_date)
        wk4 = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 28), run_date - datetime.timedelta(days = 21), '>', False, 'idx_rg_perf', 'perf_change', 'sum', 'NA', False, run_date)
        if wk1 and wk2 and wk3 and wk4:
            stock_scoring.record_score(ticker,'4_wk_beat_return',20,run_date)

    # CHECK LAST FEW MONTHS OF RETURN
        x1mo = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 60), run_date - datetime.timedelta(days = 30), '>', False, '1mo_perf', 'perf_change', 'sum', 'NA', True, run_date)
        x2mo = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 90), run_date - datetime.timedelta(days = 60), '>', False, '2mo_perf', 'perf_change', 'sum', 'NA', True, run_date)
        x3mo = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 120), run_date - datetime.timedelta(days = 90), '>', False, '3mo_perf', 'perf_change', 'sum', 'NA', True, run_date)
        x4mo = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 150), run_date - datetime.timedelta(days = 120), '>', False, '4mo_perf', 'perf_change', 'sum', 'NA', True, run_date)
        x5mo = stock_scoring.score_check(ticker, run_date - datetime.timedelta(days = 180), run_date - datetime.timedelta(days = 150), '>', False, '5mo_perf', 'perf_change', 'sum', 'NA', True, run_date)
        
        

### VOLATILITY #####
            
        big_drops = stock_scoring.vol_check(ticker, run_date - datetime.timedelta(days = 90), run_date, '<', False, 'lg_drop', 'stk_change', 'count', 'na', False, run_date)
        big_gains = stock_scoring.vol_check(ticker, run_date - datetime.timedelta(days = 90), run_date, '>', False, 'lg_gain', 'stk_change', 'count', 'na', False, run_date)
        recent_drops = stock_scoring.vol_check(ticker, run_date - datetime.timedelta(days = 30), run_date, '<', False, 'recent_drop', 'stk_change', 'count', 'na', False, run_date)
        recent_gains = stock_scoring.vol_check(ticker, run_date - datetime.timedelta(days = 30), run_date, '>', False, 'recent_gain', 'stk_change', 'count', 'na', False, run_date)

#### DIFFERENT THAN OTHER CALCS _ DO LAST    # CHECK BIGGEST JUMP IS MORE THAN A BIG % OF TOTAL
        max_return = stock_scoring.check_total_return2(ticker, start_date, run_date, 'stk_change', 'max', 'na')
        total_return = stock_scoring.check_total_return2(ticker, start_date, run_date, 'stk_change', 'sum', 'na')
        score_set = stock_procs.load_values('algorithm','setting, score, fail_score','name="large_one_day_rtn"')
        if max_return*score_set[0][0] > total_return:
            stock_scoring.record_score(ticker, 'large_one_day_rtn', max_return*score_set[0][2], run_date)

##    # CALCULATE AND REPORT SCORE
##        end_date = run_date
##        if perf_score > v_strong_buy:
##            reccomendation = 'STRONG BUY'
##        elif perf_score > v_buy:
##            reccomendation = 'BUY'
##        elif perf_score > v_consider:
##            reccomendation = 'CONSIDER'
##        elif perf_score > v_investigate:
##            reccomendation = 'INVESTIGATE'
##        elif perf_score <= v_investigate:
##            reccomendation = 'IGNORE'
##            reason = 'Score was below tolerance'
##            stock_procs.del_under_perf(ticker, end_date, reason,reccomendation)
##            continue
##    # Reset the end data to the last loaded date in the DB
##        max_date = stock_procs.get_cursor_range(ticker) 
##        if end_date >= max_date:
##            end_date = max_date
##        stock_procs.record_score(ticker, end_date, perf_score, reccomendation, buy_warning)        
##        print(ticker+' has a performance rating of:  '+str(perf_score)+' the reccomendation is:  '+reccomendation)
##
##    # EXECUTE PORTFOLIO CHANGES BASED ON DATA
##    stock_procs.buy_stocks(run_date)
##    stock_procs.sell_stocks(run_date)
    num_days_back -= 1 # Used for loading back data
##    
