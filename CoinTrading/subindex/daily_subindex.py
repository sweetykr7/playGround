import numpy as np
import datetime
import pymysql
pymysql.install_as_MySQLdb()

from library import coin_cf as cf

from sqlalchemy import create_engine, event, Text, Float
from sqlalchemy.pool import Pool

import pandas as pd
import pymysql.cursors

from library.logging_pack import *



class daily_subindex():
    def __init__(self):
        self.variable_setting()

    def variable_setting(self):
        self.today = datetime.datetime.today().strftime("%Y%m%d")
        self.today_detail = datetime.datetime.today().strftime("%Y%m%d%H%M")
        self.start_date = cf.start_date
        self.db_setting_etc(cf.real_db_name)
        self.date_rows_setting()

    def db_setting_etc(self, db_name):
        self.coin_daily_craw_engine = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_daily_craw",
            encoding='utf-8')
        self.coin_daily_list_engine = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_daily_list",
            encoding='utf-8')
        self.coin_min_craw_engine = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_min_craw",
            encoding='utf-8')
        self.engine_daily_subindex = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_daily_subindex",
            encoding='utf-8')

    def date_rows_setting(self):
        sql=f'''        
            select date from `btc-doge` where date >= {cf.start_date} group by date
                '''
        self.date_rows=self.coin_daily_craw_engine.execute(sql).fetchall()

    def is_table_exist_daily_subindex(self, date):
        sql = "select 1 from information_schema.tables where table_schema ='coin_daily_subindex' and table_name = '%s'"
        rows = self.engine_daily_subindex.execute(sql % (date)).fetchall()

        if len(rows) == 1:
            return True
        elif len(rows) == 0:
            return False

    def daily_subindex_main(self):
        #self.get_stock_item_all()

        for k in range(len(self.date_rows)):

            print(str(k) + " 번째 : " + self.today)

            if self.is_table_exist_daily_subindex(self.date_rows[k][0]) == True:
                # continue
                print(self.date_rows[k][0] + "테이블은 존재한다 !! continue!! ")
                continue
            else:
                print(self.date_rows[k][0] + "테이블은 존재하지 않는다 !!!!!!!!!!! table create !! ")

                sql = f'''
                        select *
                        from `subindex` 
                        where date = '{self.date_rows[k][0]}'
                        '''


                # daily craw에서 subindex 만들기 위한 날짜 data를 가져옴.
                rows = self.coin_daily_list_engine.execute(sql).fetchall()


                if len(rows) != 0:
                    df_temp = pd.DataFrame(rows,
                                        columns=[
                                            'index','date','code','open','close','low','high','volume','noise',
                                            'switch_line','standard_line','backspan','prespan1',
                                            'prespan2','ma19','ma20','avg_momentum_plus_12month',
                                            'avg_momentum_20day','bband_1month','cci','rsi',
                                            'macd','macd_signal','macd_hist','stoch_slowk',
                                            'stoch_slowd','BBAND_L','BBAND_M','BBAND_U','OBV',
                                            'avg_noise','best_52','high_60days','low_60days'
                                        ])
                    df_temp.drop(['index'],axis=1,inplace=True) #위에 리스트 형태 변환하기 싫어서 그냥 이렇게 함.
                    df_temp.to_sql(name=self.date_rows[k][0], con=self.engine_daily_subindex, if_exists='replace')
                elif len(rows) ==0:
                    continue




    def get_stock_item_all(self):
        print("get_stock_item_all!!!!!!")
        sql = "select code_name,code from stock_item_all"
        self.stock_item_all = self.engine_daily_buy_list.execute(sql).fetchall()

    def is_table_exist_daily_craw(self, code, code_name):
        sql = "select 1 from information_schema.tables where table_schema ='daily_craw' and table_name = '%s'"
        rows = self.engine_daily_craw.execute(sql % (code_name)).fetchall()

        if len(rows) == 1:
            # print(code + " " + code_name + " 테이블 존재한다!!!")
            return True
        elif len(rows) == 0:
            # print("####################" + code + " " + code_name + " no such table!!!")
            # self.create_new_table(self.cc.code_df.iloc[i][0])
            return False

    def run(self):

        self.transaction_info()

        # print("run end")
        return 0





if __name__ == "__main__":
    subindex = daily_subindex()
    subindex.daily_subindex_main()

