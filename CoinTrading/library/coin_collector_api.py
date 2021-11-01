
import pandas as pd
import pyupbit
from datetime import datetime, time, date, timedelta
from calendar import monthrange
from time import sleep as timesleep
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *

from integer import Integer
import numpy

from library import coin_cf as cf


from sqlalchemy import create_engine, event, Text, Float
from sqlalchemy.pool import Pool
import pymysql
pymysql.install_as_MySQLdb()
from library.logging_pack import logger

from subindex import daily_subindex
from subindex import subindex
from library.coin_open_api import *



class get_Coin_Data():
    def __init__(self):
        self.system_mode = 0  # 'simulator'이 1 , real이 0
        self.today = datetime.today().strftime("%Y%m%d")
        self.db_name = self.db_name_setting()
        self.db_setting(self.db_name)
        self.open_api=open_api()
        self.variable_setting()

        #
        # #db 초기화
        # if not self.is_simul_table_exist(self.db_name, "setting_data"):
        #     self.init_db_setting_data()
        # else:
        #     logger.debug("setting_data db 존재한다!!!")




    def variable_setting(self):

        self.db_setting_etc(self.db_name)

        # 만약에 setting_data 테이블이 존재하지 않으면 구축 하는 로직
        # if not self.is_simul_table_exist(self.db_name, "setting_data"):
        #     self.init_db_setting_data()
        # else:
        #     logger.debug("setting_data db 존재한다!!!")

        self.get_code_list()
        #종목가져오기


    def db_name_setting(self):
        if self.system_mode==1:
            db_name=1
        elif self.system_mode==0:
            db_name=cf.real_db_name
        return db_name

    def db_setting(self, db_name):

        logger.debug("db name !!! : %s", db_name)
        conn = pymysql.connect(
            host=cf.db_ip,
            port=int(cf.db_port),
            user=cf.db_id,
            password=cf.db_passwd,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            if not self.is_database_exist(cursor):
                self.create_database(cursor)
            self.engine_JB = create_engine(
                "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/" + db_name,
                encoding='utf-8'
            )
            self.basic_db_check(cursor)

    def db_setting_etc(self,db_name):
        self.coin_daily_craw_engine = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_daily_craw",
            encoding='utf-8')
        self.coin_daily_list_engine = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_daily_list",
            encoding='utf-8')
        self.coin_min_craw_engine = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_min_craw",
            encoding='utf-8')

    # 봇 데이터 베이스를 만드는 함수
    def create_database(self, cursor):
        logger.debug("create_database!!! {}".format(self.db_name))
        sql = 'CREATE DATABASE {}'
        cursor.execute(sql.format(self.db_name))

    # 봇 데이터 베이스 존재 여부 확인 함수
    def is_database_exist(self, cursor):
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = '{}'"
        if cursor.execute(sql.format(self.db_name)):
            logger.debug("%s 데이터 베이스가 존재한다! ", self.db_name)
            return True
        else:
            logger.debug("%s 데이터 베이스가 존재하지 않는다! ", self.db_name)
            return False

    def coin_update(self):
        sql = "select code_update,jango_data_db_check, possessed_item, today_profit, final_chegyul_check, db_to_buy_list,today_buy_list, daily_crawler , min_crawler, daily_buy_list, daily_sector_crawler, monthly_gpa_crawling, subindex,daily_subindex from setting_data limit 1"
        rows = self.engine_JB.execute(sql).fetchall()

        if rows[0][7] != self.today:
            self.coin_daily_craw()

        if rows[0][9] != self.today:
            self.coin_daily_list()


        if rows[0][12] != self.today:
            self.subindex_collecting = subindex.subindex()
            self.subindex_collecting.collecting()
            logger.debug("subindex success !!!")

            sql = "UPDATE setting_data SET subindex='%s' limit 1"
            self.engine_JB.execute(sql % (self.today))

        if rows[0][13] != self.today:
            # daily_subindex로 만드는 코딩
            self.daily_subindex_collecting = daily_subindex.daily_subindex()
            self.daily_subindex_collecting.daily_subindex_main()
            logger.debug("daily_subindex success !!!")

            sql = "UPDATE setting_data SET daily_subindex='%s' limit 1"
            self.engine_JB.execute(sql % (self.today))

        # min_craw db (분별 데이터) 업데이트
        # if rows[0][8] != self.today:
        #     self.coin_min_craw()

        if rows[0][6] != self.today:
            self.realtime_daily_buy_list_check()
            #self.chatbot.rdb_load()


    def coin_daily_craw(self):
        coin_list=self.stock_item_all
        print(f'''coin list를 가져왔습니다. 총 {len(coin_list)}개를 가져왔습니다.''')
        count=0

        #num = len(coin_list)
        #latest_index=self.get_latest_index(coin_list)

        for coin in coin_list:
            if self.is_daily_date_exist(coin,self.today):
                continue

            self.need_collecting_days = self.get_need_collecting_days(coin.lower())
            collecting_days =self.need_collecting_days

            if self.coin_daily_craw_data_exist :
                this_coin = pyupbit.get_ohlcv(coin, count=(collecting_days+120)) #불러오기
            else:
                this_coin = pyupbit.get_ohlcv(coin, count=(collecting_days))

            try:
                this_coin=this_coin.reset_index().rename(columns={"index":"date"}) #index에 있는거 끌어나오게 하는거
                this_coin['date']=pd.to_datetime(this_coin['date']).apply(lambda x: x.date().strftime("%Y%m%d")) #형식변환
            except:
                continue

            this_coin['code']=coin
            update_coin=this_coin.reindex(columns=['code','date','open','high','low','close','volume','value'])
            final_df = self.create_coin_daily_craw_table(update_coin)


            if self.coin_daily_craw_data_exist:
                #final_df=final_df.tail(n=collecting_days)
                # final_df = base_time.strftime("%Y%m%d%H%M")
                #base_date = datetime.strptime(self.final_collecting_date, "%Y%m%d")
                #test=0
                final_df=final_df[final_df.date > self.final_collecting_date]

            final_df.to_sql(name=coin.lower(), con=self.coin_daily_craw_engine, if_exists='append')

            update_sql = "UPDATE stock_item_all SET check_daily_crawler='%s' WHERE code='%s'"
            self.coin_daily_list_engine.execute(update_sql % ('1', coin))
            print(f'''{coin.lower()}을 업데이트하고 있습니다.--{count}''')

            count=count+1

        logger.debug("Coin_daily_crawler success !!!")

        sql = "UPDATE setting_data SET daily_crawler='%s' limit 1"
        self.engine_JB.execute(sql % (self.today))

    def coin_daily_list(self):
        self.date_rows_setting()
        for k in range(len(self.date_rows)):

            print(str(k) + " 번째 : " + self.date_rows[k][0])
            # daily 테이블 존재하는지 확인
            if self.is_simul_table_exist('coin_daily_list',self.date_rows[k][0]) == True:
                # continue
                print(self.date_rows[k][0] + "테이블은 존재한다 !! continue!! ")
                continue
            else:
                print(self.date_rows[k][0] + "테이블은 존재하지 않는다 !!!!!!!!!!! table create !! ")

                multi_list = list()

                for i in range(len(self.stock_item_all)):
                    code = self.stock_item_all[i]
                    if self.is_simul_table_exist('coin_daily_craw', code) == False:
                        print("daily_craw db에 " + str(code) + " 테이블이 존재하지 않는다 !!")
                        continue

                    sql = "select * from `" + self.stock_item_all[i] + "` where date = '{}' group by date"
                    # daily_craw에서 해당 날짜의 row를 한 줄 가져오는 것
                    rows = self.coin_daily_craw_engine.execute(sql.format(self.date_rows[k][0])).fetchall()
                    multi_list += rows
                test=0
                if len(multi_list) != 0:
                    df_temp = pd.DataFrame(multi_list,
                                        columns=['index', 'date', 'check_item', 'code', 'code_name', 'd1_diff_rate',
                                                 'close', 'open', 'high', 'low',
                                                 'volume', 'clo3','clo5', 'clo10', 'clo20', 'clo40', 'clo60',
                                                 'clo100', 'clo120', "clo3_diff_rate","clo5_diff_rate", "clo10_diff_rate",
                                                 "clo20_diff_rate", "clo40_diff_rate", "clo60_diff_rate",
                                                  "clo100_diff_rate", "clo120_diff_rate",
                                                 'yes_clo3', 'yes_clo5', 'yes_clo10', 'yes_clo20', 'yes_clo40', 'yes_clo60',
                                                 'yes_clo100', 'yes_clo120',
                                                 'vol3','vol5', 'vol10', 'vol20', 'vol40', 'vol60',
                                                 'vol100', 'vol120'
                                                 ])

                    df_temp.to_sql(name=self.date_rows[k][0], con=self.coin_daily_list_engine, if_exists='replace')


        logger.debug("Coin_daily_list success !!!")

        sql = "UPDATE setting_data SET daily_buy_list='%s' limit 1"
        self.engine_JB.execute(sql % (self.today))

    def coin_min_craw(self):
        test=0
        self.db_to_min_craw()
        logger.debug("min_crawler success !!!")

        sql = "UPDATE setting_data SET min_crawler='%s' limit 1"
        self.engine_JB.execute(sql % (self.today))

    def db_to_min_craw(self):
        logger.debug("db_to_min_craw!!!!!!")
        sql = "select code,code_name, check_min_crawler from stock_item_all"
        target_code = self.coin_daily_list_engine.execute(sql).fetchall()


        num = len(target_code)
        # for i in range(num):
        #     code = target_code[i]
        #     code_name = target_code[i]
        #     self.set_min_crawler_table(code, code_name)

        for i in range(num):
            # check_item 확인
            if int(target_code[i][2]) != 0:
                continue
            test = 0
            code = target_code[i][0]
            code_name = target_code[i][1]

            if code[:3]=='KRW':
                pass
            else:
                continue


            logger.debug("++++++++++++++" + str(code_name) + "++++++++++++++++++++" + str(i + 1) + '/' + str(num))

            self.need_collecting_days_for_min_craw = self.get_need_collecting_days_for_min_craw(code)

            check_item_gubun=self.get_total_data_min(code, code_name, self.today)
            test=0

            #check_item_gubun=1
            sql = "UPDATE stock_item_all SET check_min_crawler='%s' WHERE code='%s'"
            self.coin_daily_list_engine.execute(sql % (check_item_gubun, code))

    def set_min_crawler_table(self, code, code_name, df):

        df_temp = pd.DataFrame(df,
                            columns=['date', 'check_item', 'code', 'code_name', 'd1_diff_rate', 'close', 'open', 'high',
                                     'low',
                                     'volume', 'sum_volume', 'clo5', 'clo10', 'clo20', 'clo40', 'clo60', 'clo80',
                                     'clo100', 'clo120', "clo5_diff_rate", "clo10_diff_rate",
                                     "clo20_diff_rate", "clo40_diff_rate", "clo60_diff_rate",
                                     "clo80_diff_rate", "clo100_diff_rate", "clo120_diff_rate",
                                     'yes_clo5', 'yes_clo10', 'yes_clo20', 'yes_clo40', 'yes_clo60', 'yes_clo80',
                                     'yes_clo100', 'yes_clo120',
                                     'vol5', 'vol10', 'vol20', 'vol40', 'vol60', 'vol80',
                                     'vol100', 'vol120'
                                     ])

        df_temp = df_temp.sort_values(by=['date'], ascending=True)

        df_temp['code'] = code
        # # 뒤에 0없애기 (초)
        df_temp['code_name'] = code_name
        d1_diff_rate = round((df_temp['close'] - df_temp['close'].shift(1)) / df_temp['close'].shift(1) * 100, 2)
        df_temp['d1_diff_rate'] = d1_diff_rate.replace(numpy.inf, numpy.nan)

        # 하나씩 추가할때는 append 아니면 replace
        clo5 = df_temp['close'].rolling(window=5).mean()
        clo10 = df_temp['close'].rolling(window=10).mean()
        clo20 = df_temp['close'].rolling(window=20).mean()
        clo40 = df_temp['close'].rolling(window=40).mean()
        clo60 = df_temp['close'].rolling(window=60).mean()
        clo80 = df_temp['close'].rolling(window=80).mean()
        clo100 = df_temp['close'].rolling(window=100).mean()
        clo120 = df_temp['close'].rolling(window=120).mean()
        df_temp['clo5'] = round(clo5, 2)
        df_temp['clo10'] = round(clo10, 2)
        df_temp['clo20'] = round(clo20, 2)
        df_temp['clo40'] = round(clo40, 2)
        df_temp['clo60'] = round(clo60, 2)
        df_temp['clo80'] = round(clo80, 2)
        df_temp['clo100'] = round(clo100, 2)
        df_temp['clo120'] = round(clo120, 2)

        df_temp['clo5_diff_rate'] = round((df_temp['close'] - clo5) / clo5 * 100, 2)
        df_temp['clo10_diff_rate'] = round((df_temp['close'] - clo10) / clo10 * 100, 2)
        df_temp['clo20_diff_rate'] = round((df_temp['close'] - clo20) / clo20 * 100, 2)
        df_temp['clo40_diff_rate'] = round((df_temp['close'] - clo40) / clo40 * 100, 2)
        df_temp['clo60_diff_rate'] = round((df_temp['close'] - clo60) / clo60 * 100, 2)
        df_temp['clo80_diff_rate'] = round((df_temp['close'] - clo80) / clo80 * 100, 2)
        df_temp['clo100_diff_rate'] = round((df_temp['close'] - clo100) / clo100 * 100, 2)
        df_temp['clo120_diff_rate'] = round((df_temp['close'] - clo120) / clo120 * 100, 2)

        df_temp['yes_clo5'] = df_temp['clo5'].shift(1)
        df_temp['yes_clo10'] = df_temp['clo10'].shift(1)
        df_temp['yes_clo20'] = df_temp['clo20'].shift(1)
        df_temp['yes_clo40'] = df_temp['clo40'].shift(1)
        df_temp['yes_clo60'] = df_temp['clo60'].shift(1)
        df_temp['yes_clo80'] = df_temp['clo80'].shift(1)
        df_temp['yes_clo100'] = df_temp['clo100'].shift(1)
        df_temp['yes_clo120'] = df_temp['clo120'].shift(1)

        df_temp['vol5'] = df_temp['volume'].rolling(window=5).mean()
        df_temp['vol10'] = df_temp['volume'].rolling(window=10).mean()
        df_temp['vol20'] = df_temp['volume'].rolling(window=20).mean()
        df_temp['vol40'] = df_temp['volume'].rolling(window=40).mean()
        df_temp['vol60'] = df_temp['volume'].rolling(window=60).mean()
        df_temp['vol80'] = df_temp['volume'].rolling(window=80).mean()
        df_temp['vol100'] = df_temp['volume'].rolling(window=100).mean()
        df_temp['vol120'] = df_temp['volume'].rolling(window=120).mean()

        # if self.open_api.craw_table_exist:
        #     df_temp = df_temp[df_temp.date > self.open_api.craw_db_last_min]
        #
        # if len(df_temp) == 0:
        #     logger.debug("이미 min_craw db의 " + code_name + " 테이블에 콜렉팅 완료 했다! df_temp가 비었다!!")
        #
        #     # 이렇게 안해주면 아래 프로세스들을 안하고 바로 넘어가기때문에 그만큼 tr 조회 하는 시간이 짧아지고 1초에 5회 이상의 조회를 할 수 가있다 따라서 비었을 경우는 sleep해줘야 안멈춘다
        #     time.sleep(0.03)
        #     check_item_gubun = 3
        #     return check_item_gubun
        #
        test=0
        df_temp[['close', 'open', 'high', 'low', 'volume', 'sum_volume', 'clo5', 'clo10', 'clo20', 'clo40', 'clo60',
                 'clo80', 'clo100', 'clo120',
                 'yes_clo5', 'yes_clo10', 'yes_clo20', 'yes_clo40', 'yes_clo60', 'yes_clo80', 'yes_clo100',
                 'yes_clo120',
                 'vol5', 'vol10', 'vol20', 'vol40', 'vol60', 'vol80', 'vol100', 'vol120']] = \
            df_temp[
                ['close', 'open', 'high', 'low', 'volume', 'sum_volume', 'clo5', 'clo10', 'clo20', 'clo40', 'clo60',
                 'clo80', 'clo100', 'clo120',
                 'yes_clo5', 'yes_clo10', 'yes_clo20', 'yes_clo40', 'yes_clo60', 'yes_clo80', 'yes_clo100',
                 'yes_clo120',
                 'vol5', 'vol10', 'vol20', 'vol40', 'vol60', 'vol80', 'vol100', 'vol120']].fillna(0).astype(float)

        return df_temp



    def get_total_data_min(self, code, code_name, start):
        #self.ohlcv = defaultdict(list)



        self.craw_table_exist = False

        if self.is_min_craw_table_exist(code_name):
            self.craw_table_exist = True
            self.craw_db_last_min = self.get_craw_db_last_min(code_name)
            if self.craw_db_last_min[:8]==self.today:
                return 1
            #self.craw_db_last_min_sum_volume = self.get_craw_db_last_min_sum_volume(code_name)
        # else:
        #     self.craw_db_last_min = str(0)
        #     self.craw_db_last_min_sum_volume = 0


        #초반에 없을때 들어가는 메뉴
        if self.craw_table_exist == False:
            from_date_sql=f'''
                            select date from coin_daily_craw.`{code}` order by date limit 1
                            '''
            base_date = self.coin_daily_list_engine.execute(from_date_sql).fetchall()[0][0]
            base_date = datetime.strptime(base_date, "%Y%m%d")
            pre_date = base_date

            base_date+=timedelta(days=1)
            base_time = datetime.combine(base_date, time(9, 0, 0))
            pre_base_time = datetime.combine(pre_date, time(9, 0, 0))
            #self.min_craw_exist_data=False
            need_days = self.need_collecting_days_for_min_craw
        #업데이트 할때 들어가는 곳
        else:
            #update원활하게 해주려고 이렇게 해서 하루를 더 가져옴. 나중에 tail로 끈어줄거임
            need_days=self.need_collecting_days_for_min_craw
            #self.min_craw_exist_data = True

            base_date=datetime.strptime(self.craw_db_last_min[:8], "%Y%m%d")
            base_time = datetime.combine(base_date, time(9, 0, 0))

            pre_base_time = base_time
            base_time+=timedelta(hours=24, minutes=0)

        pre_base_time = pre_base_time.strftime("%Y%m%d%H%M")



        test=0

        #min_count = (need_days * 1440) - 1400

        total_coin={'code':[], 'date':[], 'open':[], 'high':[], 'low':[], 'close':[], 'volume':[], 'value':[]}

        df_total_coin = pd.DataFrame(total_coin,columns=['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'value'])


        log_cnt=0


        while need_days > 0:
            if int(base_time.year)<int('2019'):
                need_days = need_days - 1
                pre_base_time = base_time.strftime("%Y%m%d%H%M")
                base_time += timedelta(hours=24, minutes=0)
                logger.debug(f'''날짜 pass day:{pre_base_time}''')
                continue

            this_coin = pyupbit.get_ohlcv(code, count=(1440 + 120), to=base_time, interval='minute1')
            test=0
            if this_coin is None:
                need_days = need_days - 1
                pre_base_time = base_time.strftime("%Y%m%d%H%M")
                base_time += timedelta(hours=24, minutes=0)
                logger.debug(f'''{code}의 일자 {pre_base_time}가 Data가 없어 pass합니다. 남은 일수 {need_days}''')
                continue

            try:
                this_coin = this_coin.reset_index().rename(columns={"index": "date"})  # index에 있는거 끌어나오게 하는거
                this_coin['date'] = pd.to_datetime(this_coin['date']).apply(
                    lambda x: x.strftime("%Y%m%d%H%M"))  # 형식변환
            except:
                continue
            test=0
            this_coin['code'] = code
            update_coin = this_coin.reindex(
                columns=['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'value'])

            #df_total_coin = pd.concat([df_total_coin,update_coin], axis=0)
            result_df=self.set_min_crawler_table(code,code,this_coin)
            test=0

            # if len(result_df)==1560:
            #     result_df=result_df.tail(n=1440)
            # else:

            base_time_for_compare = base_time.strftime("%Y%m%d%H%M")
            time_9=pre_base_time[:8]+'0900'
            time_12=pre_base_time[:8] + '1200'

            result_df=result_df[result_df.date < base_time_for_compare]
            result_df = result_df[result_df.date >= pre_base_time]
            # result_df = result_df[result_df.date >= time_9]
            # result_df = result_df[result_df.date <= time_12]


            result_df.to_sql(name=code_name, con=self.coin_min_craw_engine, if_exists='append')

            pre_base_time = base_time.strftime("%Y%m%d%H%M")
            base_time += timedelta(hours=24, minutes=0)
            #base_time_after_delta = base_time.strftime("%Y%m%d%H%M")




            logger.debug(f'''{code}의 일자 {pre_base_time}를 업데이트중입니다.남은 일수 : {need_days}일''')
            #log_cnt=log_cnt+1
            need_days = need_days - 1

        return 1


        #return df_total_coin

    # min_craw db 특정 종목의 테이블에서 마지막으로 콜렉팅한 date를 가져오는 함수
    def get_craw_db_last_min(self, code_name):
        sql = "SELECT date from `" + code_name + "` order by date desc limit 1"
        rows = self.coin_min_craw_engine.execute(sql).fetchall()
        test=0
        if len(rows):
            return rows[0][0]
        # 신생
        else:
            self.craw_table_exist=False
            return str(0)
    # min_craw 테이블에서 마지막 콜렉팅한 row의 sum_volume을 가져오는 함수
    def get_craw_db_last_min_sum_volume(self, code_name):
        sql = "SELECT sum_volume from `" + code_name + "` order by date desc limit 1"
        rows = self.coin_min_craw_engine.execute(sql).fetchall()
        if len(rows):
            return rows[0][0]
        # 신생
        else:
            return str(0)

    def is_min_craw_table_exist(self, code_name):
        # #jackbot("******************************** is_craw_table_exist !!")
        sql = "select 1 from information_schema.tables where table_schema ='coin_min_craw' and table_name = '{}'"
        rows = self.coin_min_craw_engine.execute(sql.format(code_name)).fetchall()
        if rows:
            return True
        else:
            logger.debug(str(code_name) + " min_craw db에 없다 새로 생성! ", )
            return False


    def get_code_list(self):
        self.stock_item_all = pyupbit.get_tickers()
        code_list=pd.DataFrame(self.stock_item_all,columns=['code'])
        logger.debug("get_code_list")

        if self.is_simul_table_exist('coin_daily_list','stock_item_all') and self.stock_item_all_code_update_check():
            pass
        else:
            self.stock_item_all_db_setting(code_list)

    def stock_item_all_code_update_check(self):
        sql = f'''
                select code_update from setting_data limit 1
                            '''
        code_update_confirm = self.engine_JB.execute(sql).fetchall()[0][0]

        if code_update_confirm == self.today:
            return True
        else:
            return False

    def stock_item_all_db_setting(self,code_list):

        # sector all (업종별 종합지수)
        df_sector_all_temp = {'id': [], 'code': [], 'code_name': [], 'check_item': [], 'check_daily_crawler': [], 'check_min_crawler': []}
        self.df_sector_all = pd.DataFrame(df_sector_all_temp,
                                       columns=['code', 'code_name', 'check_item', 'check_daily_crawler','check_min_crawler'],
                                       index=df_sector_all_temp['id'])

        test=0
        # code : 업종코드 = 001:종합(KOSPI), 002:대형주, 003:중형주, 004:소형주 101:종합(KOSDAQ), 201:KOSPI200, 302:KOSTAR, 701: KRX100
        # 원하는 업종코드를 추가해 준다. 현재는 KOSPI 와 KOSDAQ 만 넣었다.
        self.df_sector_all['code'] = code_list['code']
        #this_coin['date'] = pd.to_datetime(this_coin['date']).apply(lambda x: x.date().strftime("%Y%m%d"))
        self.df_sector_all['code_name'] = code_list['code']

        self.df_sector_all['check_item'] = int(0)
        # 이렇게 str로 선언안하면 포맷 자체가 int 로 바뀌게 되고 나중에 20190101.0 이런식으로 date 찍힌다
        self.df_sector_all['check_daily_crawler'] = str(0)
        self.df_sector_all['check_min_crawler'] = str(0)
        self.df_sector_all.to_sql('stock_item_all', self.coin_daily_list_engine, if_exists='replace')

        update_sql = f'''
                                UPDATE setting_data SET code_update = {self.today}
                            '''
        self.engine_JB.execute(update_sql)


    def _stock_to_sql(self, origin_df, type):
        checking_stocks = ['kosdaq', 'kospi', 'konex', 'etf']
        stock_df = pd.DataFrame()
        stock_df['code'] = origin_df['code']
        name_list = []
        for KIND_info in origin_df.itertuples():
            kiwoom_name = self.open_api.dynamicCall("GetMasterCodeName(QString)", KIND_info.code).strip()
            name_list.append(kiwoom_name)
            if not kiwoom_name:
                if type in checking_stocks:
                    logger.error(
                        f"종목명이 비어있습니다. - "
                        f"종목: {KIND_info.code_name}, "
                        f"코드: {KIND_info.code}"
                    )

        stock_df['code_name'] = name_list
        stock_df['check_item'] = 0
        if type in checking_stocks:
            stock_df = stock_df[stock_df['code_name'].map(len) > 0]

        if type == 'item_all':
            stock_df['check_daily_crawler'] = "0"
            stock_df['check_min_crawler'] = "0"

        dtypes = dict(zip(list(stock_df.columns), [Text] * len(stock_df.columns)))  # 모든 타입을 Text로
        dtypes['check_item'] = Integer  # check_item만 int로 변경

        stock_df.to_sql(f'stock_{type}', self.open_api.engine_daily_buy_list, if_exists='replace', dtype=dtypes)
        return stock_df

    def create_coin_daily_craw_table(self,previous_table):



        df_initial={'date':[],'check_item':[],'code':[],'code_name':[],'d1_diff_rate':[],
            'close':[],'open':[],'high':[],'low':[],'volume':[],'clo3':[],'clo5':[],'clo10':[],'clo20':[],
            'clo40':[],'clo60':[],'clo100':[],'clo120':[],'clo3_diff_rate':[],'clo5_diff_rate':[],
            'clo10_diff_rate':[],'clo20_diff_rate':[],'clo40_diff_rate':[],'clo60_diff_rate':[]
            ,'clo100_diff_rate':[],'clo120_diff_rate':[],'yes_clo3':[],'yes_clo5':[],
            'yes_clo10':[],'yes_clo20':[],'yes_clo40':[],'yes_clo60':[],
            'yes_clo100':[],'yes_clo120':[],'vol3':[],'vol5':[],'vol10':[],'vol20':[],'vol40':[],
            'vol60':[],'vol100':[],'vol120':[]}

        df_temp=pd.DataFrame(df_initial,columns=[
            'date', 'check_item', 'code', 'code_name', 'd1_diff_rate', 'close', 'open', 'high',
            'low','volume','clo3', 'clo5', 'clo10', 'clo20', 'clo40', 'clo60',
            'clo100', 'clo120', "clo3_diff_rate","clo5_diff_rate", "clo10_diff_rate",
            "clo20_diff_rate", "clo40_diff_rate", "clo60_diff_rate",
             "clo100_diff_rate", "clo120_diff_rate",'yes_clo3',
            'yes_clo5', 'yes_clo10', 'yes_clo20', 'yes_clo40', 'yes_clo60',
            'yes_clo100', 'yes_clo120','vol3',
            'vol5', 'vol10', 'vol20', 'vol40', 'vol60',
            'vol100', 'vol120'])
        # 'code', 'date', 'open', 'high', 'low', 'close', 'volume', 'value'

        df_temp['code'] = previous_table['code']
        df_temp['code_name'] = previous_table['code']
        df_temp['close']=previous_table['close']
        df_temp['date'] = previous_table['date']
        df_temp['open'] = previous_table['open']
        df_temp['high'] = previous_table['high']
        df_temp['low'] = previous_table['low']
        df_temp['volume'] = previous_table['volume']

        df_temp['d1_diff_rate'] = round(
            (df_temp['close'] - df_temp['close'].shift(1)) / df_temp['close'].shift(1) * 100, 2)

        # 하나씩 추가할때는 append 아니면 replace
        clo3 = df_temp['close'].rolling(window=3).mean()
        clo5 = df_temp['close'].rolling(window=5).mean()
        clo10 = df_temp['close'].rolling(window=10).mean()
        clo20 = df_temp['close'].rolling(window=20).mean()
        clo40 = df_temp['close'].rolling(window=40).mean()
        clo60 = df_temp['close'].rolling(window=60).mean()
        clo100 = df_temp['close'].rolling(window=100).mean()
        clo120 = df_temp['close'].rolling(window=120).mean()
        df_temp['clo3'] = clo3
        df_temp['clo5'] = clo5
        df_temp['clo10'] = clo10
        df_temp['clo20'] = clo20
        df_temp['clo40'] = clo40
        df_temp['clo60'] = clo60
        df_temp['clo100'] = clo100
        df_temp['clo120'] = clo120

        df_temp['clo3_diff_rate'] = round((df_temp['close'] - clo3) / clo3 * 100, 2)
        df_temp['clo5_diff_rate'] = round((df_temp['close'] - clo5) / clo5 * 100, 2)
        df_temp['clo10_diff_rate'] = round((df_temp['close'] - clo10) / clo10 * 100, 2)
        df_temp['clo20_diff_rate'] = round((df_temp['close'] - clo20) / clo20 * 100, 2)
        df_temp['clo40_diff_rate'] = round((df_temp['close'] - clo40) / clo40 * 100, 2)
        df_temp['clo60_diff_rate'] = round((df_temp['close'] - clo60) / clo60 * 100, 2)
        df_temp['clo100_diff_rate'] = round((df_temp['close'] - clo100) / clo100 * 100, 2)
        df_temp['clo120_diff_rate'] = round((df_temp['close'] - clo120) / clo120 * 100, 2)

        df_temp['yes_clo3'] = df_temp['clo3'].shift(1)
        df_temp['yes_clo5'] = df_temp['clo5'].shift(1)
        df_temp['yes_clo10'] = df_temp['clo10'].shift(1)
        df_temp['yes_clo20'] = df_temp['clo20'].shift(1)
        df_temp['yes_clo40'] = df_temp['clo40'].shift(1)
        df_temp['yes_clo60'] = df_temp['clo60'].shift(1)
        df_temp['yes_clo100'] = df_temp['clo100'].shift(1)
        df_temp['yes_clo120'] = df_temp['clo120'].shift(1)

        df_temp['vol3'] = df_temp['volume'].rolling(window=3).mean()
        df_temp['vol5'] = df_temp['volume'].rolling(window=5).mean()
        df_temp['vol10'] = df_temp['volume'].rolling(window=10).mean()
        df_temp['vol20'] = df_temp['volume'].rolling(window=20).mean()
        df_temp['vol40'] = df_temp['volume'].rolling(window=40).mean()
        df_temp['vol60'] = df_temp['volume'].rolling(window=60).mean()
        df_temp['vol100'] = df_temp['volume'].rolling(window=100).mean()
        df_temp['vol120'] = df_temp['volume'].rolling(window=120).mean()

        return df_temp

    def get_need_collecting_days(self,coin):
        try:
            sql=f'''select date from `{coin}` order by date desc limit 1'''
            self.final_collecting_date = self.coin_daily_craw_engine.execute(sql).fetchall()[0][0]
            if len(self.final_collecting_date)>0:
                self.coin_daily_craw_data_exist=True
        except:
            self.coin_daily_craw_data_exist = False
            return 5000



        now = datetime.now()

        diff = now - datetime.strptime(self.final_collecting_date, "%Y%m%d")
        #diff=datetime.self.final_collecting_date-now
        return diff.days

    def get_need_collecting_days_for_min_craw(self,coin):
        try:
            #마지막 꺼 가져오는거
            sql=f'''select date from `{coin}` order by date desc limit 1'''
            self.final_collecting_date = self.coin_min_craw_engine.execute(sql).fetchall()[0][0]

        except:
            except_sql=f'''
                        select * from `{coin}`
                            '''
            return len(self.coin_daily_craw_engine.execute(except_sql).fetchall())



        #now = datetime.now().strftime("%Y%m%d")

        diff = datetime.now() - datetime.strptime(self.final_collecting_date[:8], "%Y%m%d")
        return diff.days



    # def get_latest_index(self,target_code):
    #
    #     try:
    #
    #
    #         latest_code_sql = f'''
    #                     select code from information_schema.tables
    #                     where table_schema ='coin_daily_craw' and table_name = {self.today}
    #                         '''
    #
    #
    #         latest_code=self.coin_daily_craw_engine.execute(latest_code_sql).fetchall()[0][0]
    #
    #
    #         for i, (scode, _) in enumerate(target_code):
    #             if scode == latest_code:
    #                 latest_index = i+1
    #     except :  # 아직 한번도 데이터를 넣지 않아 테이블이 존재하지 않을 시
    #         latest_index = 0
    #     return latest_index

    def date_rows_setting(self):
        sql=f'''        
            select date from `btc-doge` where date >= {cf.start_date} group by date
                '''
        self.date_rows=self.coin_daily_craw_engine.execute(sql).fetchall()



    def is_simul_table_exist(self,db_name, table_name):
        sql = "select 1 from information_schema.tables where table_schema = '%s' and table_name = '%s'"
        rows = self.engine_JB.execute(sql % (db_name, table_name)).fetchall()
        if len(rows) == 1:
            return True
        else:
            return False

    def is_daily_date_exist(self,code,date):
        try:
            sql=f'''
                    select * from coin_daily_craw.`{code}` where date={date}
                '''
            exist = self.coin_daily_craw_engine.execute(sql).fetchall()
        except:
            return False

        if len(exist)>0:
            return True
        else:
            return False

    def init_db_setting_data(self):
        logger.debug("init_db_setting_data !! ")

        #  추가하면 여기에도 추가해야함
        df_setting_data_temp = {'loan_money': [], 'limit_money': [], 'invest_unit': [], 'max_invest_unit': [],
                                'min_invest_unit': [],
                                'set_invest_unit': [], 'code_update': [], 'today_buy_stop': [],
                                'jango_data_db_check': [], 'possessed_item': [], 'today_profit': [],
                                'final_chegyul_check': [],
                                'db_to_buy_list': [], 'today_buy_list': [], 'daily_crawler': [],
                                'monthly_gpa_crawling':[],'subindex':[],'daily_subindex':[],
                                'daily_buy_list': [],'daily_sector_crawler': [],
                                'MDD_Money_Max':[],'MDD_Money_Min':[],'MDD_Max':[],'MDD_Min':[],'MDD_yes':[],
                                'Reval_date': [],'realtime_daily_buy_list_length':[],'trading_start':[]
                                }

        df_setting_data = pd.DataFrame(df_setting_data_temp,
                                    columns=['loan_money', 'limit_money', 'invest_unit', 'max_invest_unit',
                                             'min_invest_unit',
                                             'set_invest_unit', 'code_update', 'today_buy_stop',
                                             'jango_data_db_check', 'possessed_item', 'today_profit',
                                             'final_chegyul_check',
                                             'db_to_buy_list', 'today_buy_list', 'daily_crawler',
                                             'daily_buy_list','daily_sector_crawler','monthly_gpa_crawling',
                                             'subindex','daily_subindex',
                                             'MDD_Money_Max','MDD_Money_Min','MDD_Max', 'MDD_Min', 'MDD_yes',
                                             'Reval_date','realtime_daily_buy_list_length','trading_start'])

        # 자료형
        df_setting_data.loc[0, 'loan_money'] = int(0)
        df_setting_data.loc[0, 'limit_money'] = int(0)
        df_setting_data.loc[0, 'invest_unit'] = int(0)
        df_setting_data.loc[0, 'max_invest_unit'] = int(0)
        df_setting_data.loc[0, 'min_invest_unit'] = int(0)

        df_setting_data.loc[0, 'set_invest_unit'] = str(0)
        df_setting_data.loc[0, 'code_update'] = str(0)
        df_setting_data.loc[0, 'today_buy_stop'] = str(0)
        df_setting_data.loc[0, 'jango_data_db_check'] = str(0)

        df_setting_data.loc[0, 'possessed_item'] = str(0)
        df_setting_data.loc[0, 'today_profit'] = str(0)
        df_setting_data.loc[0, 'final_chegyul_check'] = str(0)
        df_setting_data.loc[0, 'db_to_buy_list'] = str(0)
        df_setting_data.loc[0, 'today_buy_list'] = str(0)
        df_setting_data.loc[0, 'daily_crawler'] = str(0)
        df_setting_data.loc[0, 'min_crawler'] = str(0)
        df_setting_data.loc[0, 'daily_buy_list'] = str(0)

        df_setting_data.loc[0, 'daily_sector_crawler'] = str(0)



        df_setting_data.loc[0, 'monthly_gpa_crawling'] = str(0)

        df_setting_data.loc[0, 'subindex'] = str(0)
        df_setting_data.loc[0, 'daily_subindex'] = str(0)


        df_setting_data.loc[0, 'MDD_Max'] = str(0)
        df_setting_data.loc[0, 'MDD_Min'] = str(0)
        df_setting_data.loc[0, 'MDD_yes'] = str(0)
        df_setting_data.loc[0, 'MDD_Money_Max'] = str(0)
        df_setting_data.loc[0, 'MDD_Money_Min'] = str(0)

        df_setting_data.loc[0, 'Reval_date'] = str(0)
        df_setting_data.loc[0, 'realtime_daily_buy_list_length'] = str(0)

        df_setting_data.loc[0, 'trading_start'] = str(0)





        df_setting_data.to_sql('setting_data', self.engine_JB, if_exists='replace')



    def basic_db_check(self, cursor):
        check_list = ['coin_daily_craw', 'coin_daily_list', 'coin_min_craw']
        sql = "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA"
        cursor.execute(sql)
        rows = cursor.fetchall()
        db_list = [n['SCHEMA_NAME'].lower() for n in rows]
        create_db_tmp = "CREATE DATABASE {}"
        has_created = False
        for check_name in check_list:
            if check_name not in db_list:
                has_created = True
                logger.debug(f'{check_name} DB가 존재하지 않아 생성 중...')
                create_db_sql = create_db_tmp.format(check_name)
                cursor.execute(create_db_sql)
                logger.debug(f'{check_name} 생성 완료')

        if has_created and self.engine_JB.has_table('setting_data'):
            self.engine_JB.execute("""
                UPDATE setting_data SET code_update = '0';
            """)

    #
    # # 실전 봇, 모의 봇 매수 종목 세팅 + all_item_db 업데이트 함수
    # def realtime_daily_buy_list_check(self):
    #
    #     today=self.today
    #     #today='20211001'
    #     if self.open_api.sf.is_date_exist(today):
    #         logger.debug("daily_buy_list DB에 {} 테이블이 있습니다. jackbot DB에 realtime_daily_buy_list 테이블을 생성합니다".format(
    #             today))
    #
    #         self.date_rows_setting()
    #         # 첫 번째 파라미터는 여기서는 의미가 없다.
    #         # 두 번째 파라미터에 오늘 일자를 넣는 이유는 매수를 하는 시점인 내일 기준으로 date_rows_yesterday가 오늘 이기 때문
    #         #!@!$
    #         for i in range(len(self.open_api.cf.algo_df)):
    #             logger.debug(f'''Algorithm : {self.open_api.cf.algo_df.iloc[i][0]}, {self.open_api.cf.algo_df.iloc[i][3]}''')
    #             self.open_api.sf.db_name = self.open_api.cf.algo_df.iloc[i][3]
    #             self.open_api.sf.db_name_setting()
    #             self.open_api.db_name_setting(self.open_api.cf.algo_df.iloc[i][3])
    #             self.open_api.sf.simul_num=int(self.open_api.cf.algo_df.iloc[i][2])
    #             self.open_api.sf.variable_setting()
    #
    #             self.open_api.sf.db_to_realtime_daily_buy_list(today, today,
    #                                                            len(self.open_api.sf.date_rows))
    #
    #             # all_item_db에서 open, clo5~120, volume 등을 오늘 일자 데이터로 업데이트 한다.
    #             self.open_api.sf.update_all_db_by_date(today)
    #             self.open_api.rate_check()
    #
    #
    #         # realtime_daily_buy_list(매수 리스트) 테이블 세팅을 완료 했으면 아래 쿼리를 통해 setting_data의 today_buy_list에 오늘 날짜를 찍는다.
    #         sql = "UPDATE setting_data SET today_buy_list='%s' limit 1"
    #         self.engine_JB.execute(sql % (today))
    #     else:
    #         logger.debug(
    #             """daily_buy_list DB에 {} 테이블이 없습니다. jackbot DB에 realtime_daily_buy_list 테이블을 생성 할 수 없습니다.
    #             realtime_daily_buy_list는 daily_buy_list DB 안에 오늘 날짜 테이블이 만들어져야 생성이 됩니다.
    #             realtime_daily_buy_list 테이블을 생성할 수 없는 이유는 아래와 같습니다.
    #             1. 장이 열리지 않은 날 혹은 15시 30분 ~ 23시 59분 사이에 콜렉터를 돌리지 않은 경우
    #             2. 콜렉터를 오늘 날짜 까지 돌리지 않아 daily_buy_list의 오늘 날짜 테이블이 없는 경우
    #             """.format(self.open_api.today))

    def realtime_daily_buy_list_check(self):
        if self.open_api.sf.is_date_exist(self.today):
            logger.debug("daily_buy_list DB에 {} 테이블이 있습니다. jackbot DB에 realtime_daily_buy_list 테이블을 생성합니다".format(
                self.today))

            self.open_api.sf.get_date_for_simul()
            # 첫 번째 파라미터는 여기서는 의미가 없다.
            # 두 번째 파라미터에 오늘 일자를 넣는 이유는 매수를 하는 시점인 내일 기준으로 date_rows_yesterday가 오늘 이기 때문
            self.open_api.sf.db_to_realtime_daily_buy_list(self.today, self.today,
                                                           len(self.open_api.sf.date_rows))

            # all_item_db에서 open, clo5~120, volume 등을 오늘 일자 데이터로 업데이트 한다.
            self.open_api.sf.update_all_db_by_date(self.today)
            self.open_api.rate_check()
            # realtime_daily_buy_list(매수 리스트) 테이블 세팅을 완료 했으면 아래 쿼리를 통해 setting_data의 today_buy_list에 오늘 날짜를 찍는다.
            sql = "UPDATE setting_data SET today_buy_list='%s' limit 1"
            self.engine_JB.execute(sql % (self.today))
        else:
            logger.debug(
                """daily_buy_list DB에 {} 테이블이 없습니다. jackbot DB에 realtime_daily_buy_list 테이블을 생성 할 수 없습니다.
                """.format(self.today))



if __name__ == '__main__':
    coin=get_Coin_Data()
    coin.coin_update()