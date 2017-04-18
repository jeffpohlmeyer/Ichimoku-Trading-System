import pandas as pd
import numpy as np
import os

os.system('clear')

def unique_pairs(day,month,year):
	df = pd.read_csv(str(year)+'/'+str(day).zfill(2)+str(month).zfill(2)+str(year-2000).zfill(2)+'.txt')
	return df['<TICKER>'].unique()

def pull_data_by_pair(pair):
	count = 1
	if not os.path.exists('{}.csv'.format(pair)):
		for year in range(2001,2017):
			for month in range(1,13):
				print '{}, {}'.format(month,year)
				for day in range(1,32):
					try:
						if count == 1:
							df = pd.read_csv(str(year)+'/'+str(day).zfill(2)+str(month).zfill(2)+str(year-2000).zfill(2)+'.txt')#,parse_dates={'datetime': ['<DTYYYYMMDD>', '<TIME>']})#, date_parser=dateparse)
							df.columns = ['Pair','Date','Time','Open','High','Low','Close']
							# df.drop(['Open','High','Low'],axis=1,inplace=True)
							data = df[df['Pair'] == pair]
							count = 0
						else:
							df = pd.read_csv(str(year)+'/'+str(day).zfill(2)+str(month).zfill(2)+str(year-2000).zfill(2)+'.txt')#,parse_dates={'datetime': ['<DTYYYYMMDD>', '<TIME>']})#, date_parser=dateparse)
							df.columns = ['Pair','Date','Time','Open','High','Low','Close']
							# df.drop(['Open','High','Low'],axis=1,inplace=True)
							temp_data = df[df['Pair'] == pair]
							data = data.append(temp_data)
					except Exception as e:
						print str(e)
		data.to_csv('{}.csv'.format(pair),index=False)


pairs = list(pd.read_csv('Unique_Pairs.csv')['Ticker'])
for pair in pairs:
	print pair
	pull_data_by_pair(pair)

# pair = 'EURUSD'
# month = 3
# day = 27
# year = 2007

# pull_data_by_pair(pair)
# dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

# df = pd.read_csv(str(year)+'/'+str(day).zfill(2)+str(month).zfill(2)+str(year-2000).zfill(2)+'.txt',parse_dates={'datetime': ['<DTYYYYMMDD>', '<TIME>']})#, date_parser=dateparse)
# df.columns = ['DateTime','Pair','Open','High','Low','Close']
# data = df[df['Pair'] == pair]
# data.set_index('DateTime',inplace=True)

# day = 28
# df = pd.read_csv(str(year)+'/'+str(day).zfill(2)+str(month).zfill(2)+str(year-2000).zfill(2)+'.txt',parse_dates={'datetime': ['<DTYYYYMMDD>', '<TIME>']})#, date_parser=dateparse)
# df.columns = ['DateTime','Pair','Open','High','Low','Close']
# temp_data = df[df['Pair'] == pair]
# temp_data.set_index('DateTime',inplace=True)
# data = data.append(temp_data)
# data.to_csv('temp.csv')