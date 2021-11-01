
import pandas as pd
import pyupbit
from datetime import datetime, time, date, timedelta
from calendar import monthrange
from time import sleep
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *

from library import coin_cf as cf


from sqlalchemy import create_engine, event, Text, Float
from sqlalchemy.pool import Pool
import pymysql
pymysql.install_as_MySQLdb()



class get_Coin_Data():
    def __init__(self):
        self.variable_setting()


    def variable_setting(self):
        self.coin_daily_craw_engine = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_daily_craw",
            encoding='utf-8')
        self.coin_min_craw_engine = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_min_craw",
            encoding='utf-8')
        self.coin_test_engine = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_test",
            encoding='utf-8')

    def coin_main(self):
        test=0
        #coin_list = pyupbit.get_tickers()
        #this_coin_list = pyupbit.get_ohlcv("KRW-BTC",count=10000)
        #print(this_coin_list)

        # long_coin=''
        # long_coin_length=0
        # for coin in coin_list:
        #     this_coin_list=pyupbit.get_ohlcv(coin, count=10000)
        #     try:
        #         if len(this_coin_list)>long_coin_length:
        #
        #             long_coin_length=len(this_coin_list)
        #             long_coin=coin
        #     except:
        #         continue
        #     print(f'''coin : {coin}''')



        #print(f'''long_Coin : {long_coin}, long_Coin_length : {long_coin_length}''')
    def coin_update(self):
        coin_list = pyupbit.get_tickers()
        print("coin_list를 가져옵니다.")
        count=0
        for coin in coin_list:
            this_coin = pyupbit.get_ohlcv(coin, count=10000)
            df_this_coin = pd.DataFrame(this_coin, columns=['open', 'high', 'low', 'close', 'volume', 'value'])
            df_this_coin['date'] = d

            df_this_coin.to_sql(name=coin.lower(), con=self.coin_daily_craw_engine, if_exists='append')
            print(f'''{coin.lower()}을 업데이트하고 있습니다.--{count}''')
            count=count+1

        #this_coin_list = pyupbit.get_ohlcv("KRW-BTC",count=10000)
        #print(this_coin_list)

        # long_coin=''
        # long_coin_length=0
        # for coin in coin_list:
        #     this_coin_list=pyupbit.get_ohlcv(coin, count=10000)
        #     try:
        #         if len(this_coin_list)>long_coin_length:
        #
        #             long_coin_length=len(this_coin_list)
        #             long_coin=coin
        #     except:
        #         continue
        #     print(f'''coin : {coin}''')


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
    coin.coin_update()