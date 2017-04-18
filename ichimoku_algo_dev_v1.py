import numpy as np
import pandas as pd
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
rcParams['figure.figsize'] = 15, 6
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.arima_model import ARIMA
from datetime import datetime
import statsmodels.api as sm

from pandas.stats import moments
from functools import wraps

import os
os.system('clear')

def print_full(x):
    pd.set_option('display.max_rows', len(x))
    print(x)
    pd.reset_option('display.max_rows')

def series_indicator(col):
    def inner_series_indicator(f):
        @wraps(f)
        def wrapper(s, *args, **kwargs):
            if isinstance(s, pd.DataFrame):
                s = s[col]
            return f(s, *args, **kwargs)
        return wrapper
    return inner_series_indicator

@series_indicator('High')
def hhv(s, n):
    return s.rolling(n).max()

@series_indicator('Low')
def llv(s, n):
    return s.rolling(n).min()

def ichimoku(s, n1=9, n2=26, n3=52):
    conv = (hhv(s, n1) + llv(s, n1)) / 2
    base = (hhv(s, n2) + llv(s, n2)) / 2

    spana = (conv + base) / 2
    spanb = (hhv(s, n3) + llv(s, n3)) / 2

    return pd.DataFrame(dict(conv=conv, base=base, spana=spana,#.shift(n2),
                          spanb=spanb, lspan=s.Close.shift(-n2)))#.shift(n2), lspan=s.Close.shift(-n2)))

def score_generator(df):
    close = 0
    base = 0
    long_trigger = False
    short_trigger = False
    incloud = False
    for row in df.itertuples():
        # print(row)
        # input('press enter...')
        """ Long """
        # score = 0
        # if incloud == True and row[10] > row[6] and row[10] > row[7] and row[10] > base and row[10] > row[1]: #(row[6] > row[7] and close < row[6] and row[10] > row[6]) or (row[6] < row[7] and close < row[7] and row[10] > row[7]):
        #     long_trigger = True
        #     incloud = False
        # else:
        #     long_trigger = False
        # if long_trigger:
        #     if row[2] > row[1]:     # Conv > Base
        #         score += 1
        #     if row[3] > row[8] and row[3] > row[9] and row[3] > row[11]:    # Lag above both clouds and rolling close price
        #         score += 1
        #     if row[4] >= row[5]:    # Cloud is Green
        #         if row[4] >= row[6] and row[4] >= row[7]:   # row[4] implies that the cloud is rising
        #             score += 2
        #         elif row[4] >= row[6] or row[4] >= row[7]:   # row[4] implies that the cloud is rising
        #             score += 1
        #     # if row[4] >= row[5]:    # Cloud is Green
        #     #     score += 1
        #     # if row[2] >= row[6] and row[2] >= row[7]:   # conv above full cloud
        #     #     score += 2
        #     # elif row[2] > row[6] or row[2] > row[7]:    # conv above one or other span
        #     #     score += 1
        # if row[10] < row[6] or row[10] < row[7]:
        #     incloud = True
        # base = row[1]
        # yield score
        # # input('press enter...')

        """ Short """
        score = 0
        if incloud == True and row[10] < row[6] and row[10] < row[7] and row[10] < base and row[10] < row[1]: #(row[6] > row[7] and close < row[6] and row[10] > row[6]) or (row[6] < row[7] and close < row[7] and row[10] > row[7]):
            short_trigger = True
            incloud = False
        else:
            short_trigger = False
        if short_trigger:
            if row[2] < row[1]:     # Conv > Base
                score += 1
            if row[3] < row[8] and row[3] < row[9] and row[3] < row[11]:    # Lag above both clouds and rolling close price
                score += 1
            if row[4] <= row[5]:    # Cloud is Green
                if row[4] <= row[6] and row[4] <= row[7]:   # row[4] implies that the cloud is rising
                    score += 2
                elif row[4] <= row[6] or row[4] <= row[7]:   # row[4] implies that the cloud is rising
                    score += 1
            # if row[4] <= row[5]:    # Cloud is Green
            #     score += 1
            # if row[2] <= row[6] and row[2] <= row[7]:   # conv above full cloud
            #     score += 2
            # elif row[2] < row[6] or row[2] < row[7]:    # conv above one or other span
            #     score += 1
        if row[10] > row[6] or row[10] > row[7]:
            incloud = True
        base = row[1]
        yield score

def hourly_data_generator(df):
    Open = 0
    Close = 0
    High = 0
    Low = 0
    for row in df.itertuples():
        # print(row)
        # input('press enter...')
        if row[0] == 0:
            Open = row[4]
            High = row[5]
            Low = row[6]
            Close = row[7]
        if int(row[3]) % 100 == 1:
            Open = row[4]
            High = row[5]
            Low = row[6]
        if int(row[3]) % 100 == 0:
            Close = row[7]
        if row[5] > High:
            High = row[5]
        if row[6] < Low:
            Low = row[6]
        pair = row[1]
        date = row[2]
        time = row[3]
        minute = row[8]
        yield pair, date, time, Open, High, Low, Close, minute

def returns_calculator(df):
    trade_count = 0
    ret = 0
    price = 0
    cum_ret = 0
    stop = 0
    total_trades = 0
    concurrent_trades = 0
    trade_scores = [0] * 10
    trade_counts = [0] * 6
    score1ret = 0
    score2ret = 0
    score3ret = 0
    score4ret = 0
    score5ret = 0
    for row in df.itertuples():
        """ Long """
        # print(row)
        # input('press enter...')
        # score1 = 0
        # score2 = 0
        # score3 = 0
        # score4 = 0
        # score5 = 0
        # if row[6] <= stop and trade_count > 0:
        #     ret = (stop - price)
        #     concurrent_trades = 0
        #     for i in range(len(trade_scores)):
        #         if trade_scores[i] == 1:
        #             score1 += 1
        #         elif trade_scores[i] == 2:
        #             score2 += 1
        #         elif trade_scores[i] == 3:
        #             score3 += 1
        #         elif trade_scores[i] == 4:
        #             score4 += 1
        #         elif trade_scores[i] == 5:
        #             score5 += 1
        #     score1ret = ret * score1
        #     score2ret = ret * score2
        #     score3ret = ret * score3
        #     score4ret = ret * score4
        #     score5ret = ret * score5
        #     ret = trade_count * ret#(stop - price)
        #     trade_count = 0
        #     trade_scores = [0] * 10
        # else:
        #     ret = (row[7] - price)
        #     for i in range(len(trade_scores)):
        #         if trade_scores[i] == 1:
        #             score1 += 1
        #         elif trade_scores[i] == 2:
        #             score2 += 1
        #         elif trade_scores[i] == 3:
        #             score3 += 1
        #         elif trade_scores[i] == 4:
        #             score4 += 1
        #         elif trade_scores[i] == 5:
        #             score5 += 1
        #     score1ret = ret * score1
        #     score2ret = ret * score2
        #     score3ret = ret * score3
        #     score4ret = ret * score4
        #     score5ret = ret * score5
        #     ret = trade_count * ret#(stop - price)
        # if row[9] > 0:
        #     trade_count += 1
        #     total_trades += 1
        #     concurrent_trades += 1
        #     trade_counts[row[9]] += 1
        #     trade_scores[trade_scores.index(next(filter(lambda x: x==0, trade_scores)))] = row[9]
        # cum_ret += ret
        # price = row[7]
        # stop = row[8]
        # yield price, trade_count, ret, cum_ret, stop, total_trades, concurrent_trades, score1ret, score2ret, score3ret, score4ret, score5ret, trade_counts

        """ Short """
        score1 = 0
        score2 = 0
        score3 = 0
        score4 = 0
        score5 = 0
        if row[5] >= stop and trade_count > 0:
            ret = (price - stop)
            concurrent_trades = 0
            for i in range(len(trade_scores)):
                if trade_scores[i] == 1:
                    score1 += 1
                elif trade_scores[i] == 2:
                    score2 += 1
                elif trade_scores[i] == 3:
                    score3 += 1
                elif trade_scores[i] == 4:
                    score4 += 1
                elif trade_scores[i] == 5:
                    score5 += 1
            score1ret = ret * score1
            score2ret = ret * score2
            score3ret = ret * score3
            score4ret = ret * score4
            score5ret = ret * score5
            ret = trade_count * ret
            trade_count = 0
            trade_scores = [0] * 10
        else:
            ret = (price - row[7])
            for i in range(len(trade_scores)):
                if trade_scores[i] == 1:
                    score1 += 1
                elif trade_scores[i] == 2:
                    score2 += 1
                elif trade_scores[i] == 3:
                    score3 += 1
                elif trade_scores[i] == 4:
                    score4 += 1
                elif trade_scores[i] == 5:
                    score5 += 1
            score1ret = ret * score1
            score2ret = ret * score2
            score3ret = ret * score3
            score4ret = ret * score4
            score5ret = ret * score5
            ret = trade_count * ret
        if row[9] > 0:
            trade_count += 1
            total_trades += 1
            concurrent_trades += 1
            trade_counts[row[9]] += 1
            trade_scores[trade_scores.index(next(filter(lambda x: x==0, trade_scores)))] = row[9]
        cum_ret += ret
        price = row[7]
        stop = row[8]
        yield price, trade_count, ret, cum_ret, stop, total_trades, concurrent_trades, score1ret, score2ret, score3ret, score4ret, score5ret, trade_counts

pair_list = []
return_list = []
trades_list = []
pairs = ['AUDCAD','AUDJPY','AUDNZD','AUDUSD','CADJPY','EURAUD','EURCAD','EURGBP','EURJPY','EURNZD','EURUSD','GBPAUD','GBPCAD','GBPJPY','GBPNZD','GBPUSD','NZDCAD','NZDJPY','NZDUSD','USDCAD','USDJPY','XAGUSD','XAUUSD']
# pairs = ['EURAUD']
for pair in pairs:
    # print(pair)
    df = pd.read_csv('{}.csv'.format(pair))#,parse_dates={'DateTime':['Date','Time']})

    df['Date'] = df['Date'].astype(int).astype(str)
    df['Time'] = (df['Time']/100).astype(int).astype(str)
    df['Time'] = df['Time'].apply(lambda x: x.zfill(4))

    df['year'] = df['Date'].str.slice(0,4)
    df['minute'] = df['Time'].str.slice(2,4)
    df['year'] = df['year'].astype(int)
    df['minute'] = df['minute'].astype(int)


    df = df[df['year'] > 2005]
    df.reset_index(inplace=True)
    df.drop(['index','year'],axis=1,inplace=True)


    hour_df = pd.DataFrame(hourly_data_generator(df),columns = ['Pair','Date','Time','Open','High','Low','Close','minute'])
    # hour_df.head(20000).to_csv('hour_df_full_data.csv',sep=',')
    hour_df = hour_df[hour_df['minute'] % 100 == 0]


    hour_df.drop(['minute'],axis=1,inplace=True)
    print(hour_df.head(10))
    input('press enter...')

    ich = ichimoku(hour_df)
    ich['Cloud_Current_A'] = ich.spana.shift(26)
    ich['Cloud_Current_B'] = ich.spanb.shift(26)
    ich['Cloud_Lagged_A'] = ich.spana.shift(52)
    ich['Cloud_Lagged_B'] = ich.spanb.shift(52)
    ich['Close'] = df['Close']
    ich['Rolling_Close'] = ich.Close.rolling(26).max()

    trade_score = pd.DataFrame(score_generator(ich),columns=['score'])
    # print(len(trade_score))
    # print(len(hour_df))
    # input('press enter...')


    hour_df['stop_value'] = ich.base
    hour_df.reset_index(inplace=True)
    hour_df['trade_score'] = trade_score.score
    hour_df.drop(['index'],axis=1,inplace=True)
    hour_df.round({'Open':5,'High':5,'Low':5,'Close':5,'stop_value':5})

    returns_df = pd.DataFrame(returns_calculator(hour_df),columns=['price','trade_count','ret','cum_ret','stop','total_trades','concurrent_trades', 'score1ret','score2ret','score3ret','score4ret','score5ret','trade_counts'])#,'trade_score'])
    returns_df.round({'price':5,'stop':5})

    pair_list.append(pair)
    return_list.append(returns_df.cum_ret.iloc[-1])
    trades_list.append(returns_df.total_trades.iloc[-1])
    
    breakdown_list = []
    breakdown_list.append(returns_df.score1ret.sum())
    breakdown_list.append(returns_df.score2ret.sum())
    breakdown_list.append(returns_df.score3ret.sum())
    breakdown_list.append(returns_df.score4ret.sum())
    breakdown_list.append(returns_df.score5ret.sum())
    for i in range(len(returns_df.trade_counts.iloc[-1])):
        breakdown_list.append(returns_df.trade_counts.iloc[-1][i])
    breakdown_list_df = pd.DataFrame(breakdown_list)
    breakdown_list_df.to_csv('Pre-Crash_{}_Trades_Breakdown_By_Score.csv'.format(pair),sep=',')
    # group_list.append(returns_df.groupby(['trade_score'])['ret'].sum()[1:])
    print('Pair: '+pair+' Return: '+str(returns_df.cum_ret.iloc[-1])+' Trades: '+str(returns_df.total_trades.iloc[-1])+' Max Concurrent Trades: '+str(returns_df.concurrent_trades.max()))
    print('Score1: '+str(returns_df.score1ret.sum())+' Score2: '+str(returns_df.score2ret.sum())+' Score3: '+str(returns_df.score3ret.sum())+' Score4: '+str(returns_df.score4ret.sum())+' Score5: '+str(returns_df.score5ret.sum()))
    print(breakdown_list[-6:])

    del breakdown_list, breakdown_list_df
    ich.reset_index(inplace=True)
    ich.drop(['index'],axis=1,inplace=True)
    returns_df.reset_index(inplace=True)
    returns_df.drop(['index'],axis=1,inplace=True)
    # ich.to_csv('{}-Ich.csv'.format(pair),sep=',')
    hour_df = pd.concat([hour_df,ich,returns_df],axis=1)
    hour_df.to_csv('Pre-Crash-{}-Ichimoku-Trades.csv'.format(pair),sep=',')
    # returns_df.to_csv('{}_Returns_df.csv'.format(pair),sep=',')

    # trade_score.to_csv('AUDUSD-Trade_Score.csv',sep=',')
    # ich['base'].to_csv('AUDUSD-Stop.csv',sep=',')


try:
    longs_df = pd.DataFrame({'Pair': pair_list,'Cum_Ret': return_list,'Total_Trades': trades_list})
    # longs_df.to_csv('Pre-Crash_Long_Results.csv',sep=',',index=False)
    longs_df.to_csv('Pre-Crash_Short_Results.csv',sep=',',index=False)
except Exception as e:
    print(str(e))
    try:
        for i in range(len(pair_list)):
            print('Pair: '+pair_list[i]+' Return: '+str(return_list[i])+' Total Trades: '+str(trades_list[i]))
    except Exception as e:
        print(str(e))
        print((pair,returns,trades) for (pair,returns,trades) in zip(pair_list,return_list,trades_list))
