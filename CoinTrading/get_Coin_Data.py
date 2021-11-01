
import pandas as pd
import pyupbit
from datetime import datetime, time, date, timedelta
from calendar import monthrange
from time import sleep

#df = pybithumb.get_ohlcv("BTC")
#df = pybithumb.get_ohlcv("MITH")
#print(df.tail())

class get_Coin_Data():
    def __init__(self):
        self.variable_setting()


    def variable_setting(self):
        test=2

    def coin_main(self):
        print("ok")

#
# coin_list=pyupbit.get_tickers()
#
# print(coin_list)
# print(len(coin_list))
#
# #print(pyupbit.get_ohlcv(coin_list[0]))
#
# #a=pyupbit.get_ohlcv(coin_list[0], interval="minute1")
# daily_coin_data=pyupbit.get_ohlcv(coin_list[0])
# daily_coin_data_df=pd.DataFrame(daily_coin_data,columns=['open', 'high', 'low', 'close', 'volume', 'value'])
#
# test=0


if __name__ == '__main__':
    coin=get_Coin_Data()
    coin.coin_main()
