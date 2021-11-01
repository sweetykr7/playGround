#import pybithumb
import pandas as pd
import pyupbit
from datetime import datetime, time, date, timedelta
from calendar import monthrange
from time import sleep

#df = pybithumb.get_ohlcv("BTC")
#df = pybithumb.get_ohlcv("MITH")
#print(df.tail())


coin_list=['BTC', 'ETH', 'DASH', 'LTC', 'ETC', 'XRP', 'BCH', 'XMR', 'ZEC', 'QTUM', 'BTG', 'EOS',
           'ICX', 'VET', 'TRX', 'ELF', 'MITH', 'MCO', 'OMG', 'KNC', 'GNT', 'HSR', 'ZIL', 'ETHOS',
           'PAY', 'WAX', 'POWR', 'LRC', 'GTO', 'STEEM', 'STRAT', 'ZRX', 'REP', 'AE', 'XEM', 'SNT',
           'ADA', 'PPT', 'CTXC', 'CMT', 'THETA', 'WTC', 'ITC', 'TRUE', 'ABT', 'RNT', 'PLY', 'WAVES',
           'LINK', 'ENJ', 'PST']


for coin_name in coin_list:
    try:
        df = pybithumb.get_ohlcv(coin_name)
        print(f'''{coin_name}, {len(df)}개  ''')
    except:
        pass