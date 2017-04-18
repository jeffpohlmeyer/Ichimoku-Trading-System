"""


Python parallelization - https://blog.dominodatalab.com/simple-parallelization/


"""


















import numpy as np
import pandas as pd
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
rcParams['figure.figsize'] = 15, 6
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.arima_model import ARIMA
from datetime import datetime
import statsmodels.api as sm

from joblib import Parallel, delayed
import multiprocessing

import os
os.system('clear')

def test_stationarity(timeseries):
	
	# #Determing rolling statistics
	# rolmean = timeseries.rolling(window=12).mean()
	# rolstd = timeseries.rolling(window=12).std()

	# #Plot rolling statistics:
	# orig = plt.plot(timeseries, color='blue',label='Original')
	# mean = plt.plot(rolmean, color='red', label='Rolling Mean')
	# std = plt.plot(rolstd, color='black', label = 'Rolling Std')
	# plt.legend(loc='best')
	# plt.title('Rolling Mean & Standard Deviation')
	# plt.show(block=False)
	
	#Perform Dickey-Fuller test:
	print('Results of Dickey-Fuller Test:')
	dftest = adfuller(timeseries, autolag='AIC')
	dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','#Lags Used','Number of Observations Used'])
	for key,value in dftest[4].items():
		dfoutput['Critical Value (%s)'%key] = value
	print(dfoutput)

def pos_generator(df):
	pos_val = 0
	Long = False
	Short = False
	Entry = 0
	Exit = 0
	PL = 0
	for row in df.itertuples():
		if row[3] < 0:
			pos = -1
		elif row[3] > 0:
			pos = 1
		else:
			pos = pos_val
		pos_val = pos
		if pos == 1 and Long == False:
			Long = True
			if Short == True:
				Exit = row[1]
				PL = PL + Entry - Exit
			else:
				Exit = 0
			Short = False
			Entry = row[1]
		elif pos == -1 and Short == False:
			Short = True
			if Long == True:
				Exit = row[1]
				PL = PL + Exit - Entry
			else:
				Exit = 0
			Entry = row[1]
			Long = False
		else:
			Entry = Entry
			Exit = 0

		yield pos, Long, Short, Entry, Exit, PL

# parse = lambda x: pd.datetime.strptime(dates, '%Y-%m-%d %H:%M')
df = pd.read_csv('AUDUSD.csv')#,parse_dates={'DateTime':['Date','Time']})
# df['Date'] = int(df['Date'])
# print(type(df.ix[2,'Time']))
df['Date'] = df['Date'].astype(int).astype(str)
df['Time'] = (df['Time']/100).astype(int).astype(str)
df['Time'] = df['Time'].apply(lambda x: x.zfill(4))

# format = "%Y-%m-%d %H:%M"
# times = pd.to_datetime(df.Date + ' ' + df.Time, format=format)
# df.set_index(times, inplace=True)
# # and maybe for cleanup
# df = df.drop(['Date','Time'], axis=1)

df['year'] = df['Date'].str.slice(0,4)
df['month'] = df['Date'].str.slice(4,6)
df['day'] = df['Date'].str.slice(6,8)
df['minute'] = df['Time'].str.slice(2,4)
df['hour'] = df['Time'].str.slice(0,2)
df['year'] = df['year'].astype(int)
df['month'] = df['month'].astype(int)
df['day'] = df['day'].astype(int)
df['minute'] = df['minute'].astype(int)
df['hour'] = df['hour'].astype(int)
df = df[df['minute'] % 15 == 0]
df = df[df['year'] > 2010]
df['DateTime'] = pd.to_datetime(df[['year', 'month', 'day', 'hour', 'minute']])
df = df.drop(['Date','Time','year','month','day','hour','minute','Pair'],axis=1)
df.set_index('DateTime',inplace=True)
trainset = df[:datetime(2016,1,1)]
testset = df[datetime(2016,1,1):]
del df

lags = 20
bic = []

# def lag_calc(lag,trainset):
#	 print(lag)
#	 model = ARIMA(trainset,(lag,0,0)).fit(disp=0)
#	 return lag, model.bic

# num_cores = multiprocessing.cpu_count() - 1

# lag_vals, bic = Parallel(n_jobs=num_cores)(delayed(lag_calc)(lag,trainset) for lag in range(lags))

# print(lag_vals, bic)

# for lag in range(lags):
# 	print(lag)
# 	model = ARIMA(trainset,(lag,0,0)).fit(disp=0)
# 	bic.append(model.bic)
# val, idx = min((val, idx) for (idx, val) in enumerate(bic))
# print('{}:{}'.format(idx,val))
bic = 2
yF = []
for t in range(len(testset)):
	if t % 100 == 0:
		print(t)
	try:
		# if t >= 50:
		try:
			forecast_vec = testset.ix[t-100:t]
			arma_res = ARIMA(forecast_vec,(bic,0,0)).fit(disp=0)
			preds, _, _ = arma_res.forecast(1)
			yF.append(preds)
		except Exception as e:
			print('t={}, ARIMA error: '.format(t)+str(e))
			forecast_vec = testset.ix[:t]
			arma_res = ARIMA(forecast_vec,(bic,0,0)).fit(disp=0)
			preds, _, _ = arma_res.forecast(1)
			yF.append(preds)
		# arma_res = ARIMA(forecast_vec,(bic,0,0)).fit(disp=0)
		# preds, _, _ = arma_res.forecast(1)
		# yF.append(preds)
	except Exception as e:
		print('t={}, Second error: '.format(t)+str(e))
		yF.append(testset.ix[t,'Close'])

try:
	for i in range(len(yF),len(testset)):
		yF.append(testset.ix[i,'Close'])
except Exception as e:
	print(str(e))
testset['yF'] = yF
testset['DeltayF'] = testset.yF - testset.Close
# testset['pos'] = 0
testset[['Position','Long','Short','Entry','Exit','PL']] = pd.DataFrame(pos_generator(testset),columns=['Position','Long','Short','Entry','Exit','PL'],index=testset.index)
print(testset.head(50))
print(testset.tail(50))
testset.to_csv('AUDUSD-2016.csv',sep=',')
# print(testset.tail(10))
# print(pos.head(10))
# yF_df = pd.DataFrame(yF,columns=)
# deltayF = yF - testset.Close


# actual = plt.plot(testset.Close,color='blue',label='Actual')
# forecasted = plt.plot(yF,color='red',label='Forecast')
# plt.legend(loc='best')
# plt.show()
# print(trainset.tail())
# print(testset.head())

# test_stationarity(df['Close'])
# timeseries = df['Close']
# #Determing rolling statistics
# rolmean = timeseries.rolling(window=12,min_periods=12).mean()
# rolstd = timeseries.rolling(window=12,min_periods=12).std()
# print(rolmean.tail(20))
# print(rolstd.tail(20))

# #Plot rolling statistics:
# orig = plt.plot(timeseries, color='blue',label='Original')
# mean = plt.plot(rolmean, color='red', label='Rolling Mean')
# std = plt.plot(rolstd, color='black', label = 'Rolling Std')
# plt.legend(loc='best')
# plt.title('Rolling Mean & Standard Deviation')
# plt.show()