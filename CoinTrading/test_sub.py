ver = "#version 1.3.8"
print(f"simulator_func_mysql Version: {ver}")
import sys
import numpy as np
is_64bits = sys.maxsize > 2 ** 32
if is_64bits:
    print('64bit 환경입니다.')
else:
    print('32bit 환경입니다.')

from sqlalchemy import event
from sqlalchemy.exc import ProgrammingError
from datetime import datetime, time, date, timedelta

from library.logging_pack import logger
import pymysql.cursors

from datetime import timedelta

from pandas import DataFrame

from library import coin_cf as cf

from sqlalchemy import create_engine, event, Text, Float
from sqlalchemy.pool import Pool
import pandas as pd
from library import coin_collector_api
import math




class test_this():

    def __init__(self):
        test=0
        self.engine_daily_buy_list = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_daily_list",
            encoding='utf-8')

    def test_main(self):


        sql = f'''
                                            select DAY.*
                                            from coin_daily_list.`20210805` DAY,
                                                coin_daily_subindex.`20210805` subindex
                                            where DAY.code=subindex.code
                                            and left(DAY.code,3)='KRW'
                                            group by DAY.code
                                            order by (rank() over(order by subindex.avg_noise))*1 +
                                                (rank() over(order by DAY.close*DAY.volume desc))*1
                                            limit 10
                                        '''

        realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        save_list = []
        for i in range(len(realtime_daily_buy_list)):
            code = realtime_daily_buy_list[i][4]
            avg_score = self.rarry_setting_invest_unit(code)

            if avg_score < 3:
                continue
            else:
                save_list.append(realtime_daily_buy_list[i])

        realtime_daily_buy_list = save_list


    def rarry_setting_invest_unit(self, code):
        sql = f'''
                                select clo3,clo5,clo10,clo20,close 
                                from coin_daily_list.`20210805` 
                                where code='{code}'
                            '''
        avg_result = self.engine_daily_buy_list.execute(sql).fetchall()

        clo3 = avg_result[0][0]
        clo5 = avg_result[0][1]
        clo10 = avg_result[0][2]
        clo20 = avg_result[0][3]
        current_price = avg_result[0][4]

        avg_score = 0

        if current_price >= clo3:
            avg_score += 1

        if current_price >= clo5:
            avg_score += 1

        if current_price >= clo10:
            avg_score += 1

        if current_price >= clo20:
            avg_score += 1

        return avg_score


if __name__ == '__main__':
    test = test_this()
    test.test_main()
