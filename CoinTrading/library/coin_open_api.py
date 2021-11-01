ver = "#version 1.0.0"
print(f"Coin_open_api Version: {ver}")


from library.coin_simulator_func import *
from datetime import datetime,time,date, timedelta

from library import coin_cf as cf
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

import pyupbit
access_key = cf.access_key
secret_key = cf.secret_key

upbit = pyupbit.Upbit(access_key, secret_key)

from sqlalchemy import create_engine, event, Text, Float
from sqlalchemy.pool import Pool

from library.logging_pack import logger

import pymysql
pymysql.install_as_MySQLdb()

from collections import defaultdict

import time as timesleep


# from sqlalchemy import Integer, Text, Float
# import sys
# from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
# import time

# from pandas import DataFrame
# import re
# import pandas as pd
# import os



class open_api():
    def __init__(self):
        super().__init__()
        self.date_setting()
        self.variable_setting()
        self.sf_variable_setting()
        #self.get_item_list()
        self.db_to_possesed_item()

        test=0


    def variable_setting(self):
        logger.debug("variable_setting 함수에 들어왔다.")
        self.set_limit_invest_unit=0
        self.get_today_buy_list_code = 0 #?? 뭔지 모르겠음
        self.cf = cf

        self.system_mode=0 # 0이 실전, 1이 시뮬 db


        self.db_name=self.db_name_setting()
        self.db_setting(self.db_name)
        self.db_setting_etc(self.db_name)
        self.invest_unit = 0
        self.sf = simulator_func_mysql(self.simul_num, 'real', self.db_name)


        logger.debug("self.sf.simul_num(알고리즘 번호) : %s", self.sf.simul_num)
        logger.debug("self.sf.db_to_realtime_daily_buy_list_num : %s", self.sf.db_to_realtime_daily_buy_list_num)
        logger.debug("self.sf.sell_list_num : %s", self.sf.sell_list_num)

        # 만약에 setting_data 테이블이 존재하지 않으면 구축 하는 로직
        if not self.sf.is_simul_table_exist(self.db_name, "setting_data"):
            self.init_db_setting_data()
        else:
            logger.debug("setting_data db 존재한다!!!")

        self.set_invest_unit()
        test=0



        #jango check 여기에다가 해야함



        #self.jango_is_null = True

        #self.py_gubun = False

    def date_setting(self):
        self.today = datetime.today().strftime("%Y%m%d")
        self.today_detail = datetime.today().strftime("%Y%m%d%H%M")

    def db_name_setting(self):
        if self.system_mode==1:
            db_name=1
        elif self.system_mode==0:
            db_name=cf.real_db_name
            self.simul_num = cf.real_simul_num
            self.mod_gubun = 100  # 실전, 금일 수익률 표시 하는게 달라서(중요X)고 써져있음, 모의는 1이였음.
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

    # 봇 데이터 베이스 존재 여부 확인 함수
    def is_database_exist(self, cursor):
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = '{}'"
        if cursor.execute(sql.format(self.db_name)):
            logger.debug("%s 데이터 베이스가 존재한다! ", self.db_name)
            return True
        else:
            logger.debug("%s 데이터 베이스가 존재하지 않는다! ", self.db_name)
            return False

    #매수금액 설정, 중요
    def set_invest_unit(self):

        self.jango_check()
        test=0

        self.total_invest = self.jango_money + self.total_eval_price

        test = 0
        self.invest_unit = int(float(self.total_invest) *
                        (1 / self.sf.divide_invest_unit) * self.sf.avg_momentum_rate )


        sql = "UPDATE setting_data SET invest_unit='%s',set_invest_unit='%s' limit 1"
        self.engine_JB.execute(sql % (self.invest_unit, self.today))

    # 잔액 체크 함수
    def jango_check(self):
        logger.debug("jango_check 함수에 들어왔습니다!")
        get_balance = upbit.get_balances()

        #print(get_balance)
        df_balance = pd.DataFrame(get_balance,columns=['currency','balance','locked','avg_buy_price',
                                                   'avg_buy_price_modified','unit_currency'])

        #KRW로 된 잔고만 추출, list 행으로 추출, 안하면 int64로 이상하게 나옴.
        krw_balance_index_no=df_balance.index[(df_balance['currency'] == 'KRW') & (df_balance['unit_currency'] == 'KRW')].tolist()

        self.krw_balance=float(df_balance.iloc[krw_balance_index_no[0]][1]) #리스트의 0번째

        self.total_eval_price=float(0)
        cnt=0
        for i in range(len(df_balance)):
            if cnt==krw_balance_index_no[0]:
                cnt += 1
                continue
            self.total_eval_price+=(float(df_balance.iloc[i][1])*float(df_balance.iloc[i][3]))
            cnt+=1

        self.jango_money=self.krw_balance

        self.set_limit_invest_unit=self.invest_unit/2

        if self.jango_money > self.set_limit_invest_unit and self.jango_money > self.invest_unit :
            self.jango_is_null = False
            logger.debug("돈안부족해 투자 가능!!!!!!!!")
            return True
        else:
            logger.debug("돈부족해서 invest 불가!!!!!!!!")
            self.jango_is_null = True
            return False

        #print(self.krw_balance)

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

    # all_item_db에 추가하는 함수
    def db_to_all_item(self, order_num, code, chegyul_check, purchase_price, rate):
        logger.debug("db_to_all_item 함수에 들어왔다!!!")
        #self.date_setting()
        self.sf.init_df_all_item()
        self.sf.df_all_item.loc[0, 'order_num'] = order_num
        self.sf.df_all_item.loc[0, 'code'] = str(code)
        self.sf.df_all_item.loc[0, 'code_name'] = str(code)
        self.sf.df_all_item.loc[0, 'rate'] = float(rate)

        self.sf.df_all_item.loc[0, 'buy_date'] = self.today_detail
        # 사는 순간 chegyul_check 1 로 만드는거다.
        self.sf.df_all_item.loc[0, 'chegyul_check'] = chegyul_check
        # int로 넣어야 나중에 ++ 할수 있다.
        self.sf.df_all_item.loc[0, 'reinvest_date'] = '#'
        # df_all_item.loc[0, 'reinvest_count'] = int(0)
        # 다음에 투자할 금액은 invest_unit과 같은 금액이다.
        self.sf.df_all_item.loc[0, 'invest_unit'] = self.invest_unit
        # df_all_item.loc[0, 'reinvest_unit'] = self.invest_unit
        self.sf.df_all_item.loc[0, 'purchase_price'] = purchase_price

        # 신규 매수의 경우
        if order_num != 0:
            recent_daily_buy_list_date = self.sf.get_recent_daily_buy_list_date()
            if recent_daily_buy_list_date:
                df = self.sf.get_daily_buy_list_by_code(code, recent_daily_buy_list_date)
                if not df.empty:
                    self.sf.df_all_item.loc[0, 'code_name'] = df.loc[0, 'code_name']
                    self.sf.df_all_item.loc[0, 'close'] = df.loc[0, 'close']
                    self.sf.df_all_item.loc[0, 'open'] = df.loc[0, 'open']
                    self.sf.df_all_item.loc[0, 'high'] = df.loc[0, 'high']
                    self.sf.df_all_item.loc[0, 'low'] = df.loc[0, 'low']
                    self.sf.df_all_item.loc[0, 'volume'] = df.loc[0, 'volume']
                    self.sf.df_all_item.loc[0, 'd1_diff_rate'] = float(df.loc[0, 'd1_diff_rate'])
                    self.sf.df_all_item.loc[0, 'clo3'] = df.loc[0, 'clo3']
                    self.sf.df_all_item.loc[0, 'clo5'] = df.loc[0, 'clo5']
                    self.sf.df_all_item.loc[0, 'clo10'] = df.loc[0, 'clo10']
                    self.sf.df_all_item.loc[0, 'clo20'] = df.loc[0, 'clo20']
                    self.sf.df_all_item.loc[0, 'clo40'] = df.loc[0, 'clo40']
                    self.sf.df_all_item.loc[0, 'clo60'] = df.loc[0, 'clo60']

                    self.sf.df_all_item.loc[0, 'clo100'] = df.loc[0, 'clo100']
                    self.sf.df_all_item.loc[0, 'clo120'] = df.loc[0, 'clo120']
                    if df.loc[0, 'clo3_diff_rate'] is not None:
                        self.sf.df_all_item.loc[0, 'clo3_diff_rate'] = float(df.loc[0, 'clo3_diff_rate'])
                    if df.loc[0, 'clo5_diff_rate'] is not None:
                        self.sf.df_all_item.loc[0, 'clo5_diff_rate'] = float(df.loc[0, 'clo5_diff_rate'])
                    if df.loc[0, 'clo10_diff_rate'] is not None:
                        self.sf.df_all_item.loc[0, 'clo10_diff_rate'] = float(df.loc[0, 'clo10_diff_rate'])
                    if df.loc[0, 'clo20_diff_rate'] is not None:
                        self.sf.df_all_item.loc[0, 'clo20_diff_rate'] = float(df.loc[0, 'clo20_diff_rate'])
                    if df.loc[0, 'clo40_diff_rate'] is not None:
                        self.sf.df_all_item.loc[0, 'clo40_diff_rate'] = float(df.loc[0, 'clo40_diff_rate'])

                    if df.loc[0, 'clo60_diff_rate'] is not None:
                        self.sf.df_all_item.loc[0, 'clo60_diff_rate'] = float(df.loc[0, 'clo60_diff_rate'])

                    if df.loc[0, 'clo100_diff_rate'] is not None:
                        self.sf.df_all_item.loc[0, 'clo100_diff_rate'] = float(df.loc[0, 'clo100_diff_rate'])
                    if df.loc[0, 'clo120_diff_rate'] is not None:
                        self.sf.df_all_item.loc[0, 'clo120_diff_rate'] = float(df.loc[0, 'clo120_diff_rate'])

        # 컬럼 중에 nan 값이 있는 경우 0으로 변경 -> 이렇게 안하면 아래 데이터베이스에 넣을 때
        # AttributeError: 'numpy.int64' object has no attribute 'translate' 에러 발생
        self.sf.df_all_item = self.sf.df_all_item.fillna(0)
        self.sf.df_all_item.to_sql('all_item_db', self.engine_JB, if_exists='append', dtype={
            'code_name': Text,
            'rate': Float,
            'sell_rate': Float,
            'purchase_rate': Float,
            'sell_date': Text,
            'd1_diff_rate': Float,
            'clo3_diff_rate': Float,
            'clo5_diff_rate': Float,
            'clo10_diff_rate': Float,
            'clo20_diff_rate': Float,
            'clo40_diff_rate': Float,
            'clo60_diff_rate': Float,

            'clo100_diff_rate': Float,
            'clo120_diff_rate': Float
        })

    def init_df_all_item(self):
        df_all_item_temp = {'id': []}

        self.df_all_item = DataFrame(df_all_item_temp,
                                     columns=['id', 'order_num', 'code', 'code_name', 'rate', 'purchase_rate',
                                              'purchase_price',
                                              'present_price', 'valuation_price',
                                              'valuation_profit', 'holding_amount', 'buy_date', 'item_total_purchase',
                                              'chegyul_check', 'reinvest_count', 'reinvest_date', 'invest_unit',
                                              'reinvest_unit',
                                              'sell_date', 'sell_price', 'sell_rate', 'rate_std', 'rate_std_mod_val',
                                              'rate_std_htr', 'rate_htr',
                                              'rate_std_mod_val_htr', 'yes_close', 'close', 'd1_diff_rate', 'd1_diff',
                                              'open', 'high',
                                              'low',
                                              'volume','clo3', 'clo5', 'clo10', 'clo20', 'clo40', 'clo60',
                                              'clo100', 'clo120',"clo3_diff_rate", "clo5_diff_rate", "clo10_diff_rate",
                                              "clo20_diff_rate", "clo40_diff_rate", "clo60_diff_rate",
                                               "clo100_diff_rate", "clo120_diff_rate"])

    def sf_variable_setting(self):
        self.date_rows_yesterday = self.sf.get_recent_daily_buy_list_date()


        #Revalencing Date를 불러옴
        #self.revalencing_Date=self.get_Revalencing_Date()


        if not self.sf.is_simul_table_exist(self.db_name, "all_item_db"):
            logger.debug("all_item_db 없어서 생성!! init !! ")
            self.invest_unit = 0
            self.db_to_all_item(0, 0, 0, 0, 0)
            self.delete_all_item("0")

        # setting_data에 invest_unit값이 설정 되어 있는지 확인
        if not self.check_set_invest_unit():
            # setting_data에 invest_unit 값이 설정 되어 있지 않으면 세팅
            self.set_invest_unit()
        # setting_data에 invest_unit값이 설정 되어 있으면 해당 값을 가져온다.
        else:
            self.invest_unit = self.get_invest_unit()
            self.sf.invest_unit = self.invest_unit
        # setting_data에 invest_unit값이 설정 되어 있는지 확인 하는 함수

    def delete_all_item(self, code):
        logger.debug("delete_all_item!!!!!!!!")

        # 팔았으면 즉각 possess db에서 삭제한다. 왜냐하면 checgyul_check 들어가기 직전에 possess_db를 최신화 하긴 하지만 possess db 최신화와 chegyul_check 사이에 매도가 이뤄져서 receive로 가게 되면 sell_date를 찍어버리기 때문에 checgyul_check 입장에서는 possess에는 존재하고 all_db는 sell_date찍혀있다고 판단해서 새롭게 all_db추가해버린다.
        sql = "delete from all_item_db where code = '%s'"
        # self.engine_JB.execute(sql % (code,))
        # self.jackbot_db_con.commit()
        self.engine_JB.execute(sql % (code))

        logger.debug("delete_all_item!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        logger.debug(code)

    # setting_data에 invest_unit값이 설정 되어 있는지 확인 하는 함수
    def check_set_invest_unit(self):
        sql = "select invest_unit, set_invest_unit from setting_data limit 1"
        rows = self.engine_JB.execute(sql).fetchall()
        if rows[0][1] == self.today:
            self.invest_unit = rows[0][0]
            return True
        else:
            return False

    # invest_unit을 가져오는 함수
    def get_invest_unit(self):
        logger.debug("get_invest_unit 함수에 들어왔습니다!")
        sql = "select invest_unit from setting_data limit 1"
        # 데이타 Fetch
        # rows 는 list안에 튜플이 있는 [()] 형태로 받아온다
        return self.engine_JB.execute(sql).fetchall()[0][0]

    def get_item_list(self):
        logger.debug("get_item_list 함수에 들어왔습니다!")
        get_balance = upbit.get_balances()
        # print(get_balance)
        df_balance = pd.DataFrame(get_balance, columns=['currency', 'balance', 'locked', 'avg_buy_price',
                                                        'avg_buy_price_modified', 'unit_currency'])
        test = 0
        # KRW로 된 잔고만 추출, list 행으로 추출, 안하면 int64로 이상하게 나옴.
        krw_balance_index_no = df_balance.index[
            (df_balance['currency'] == 'KRW') & (df_balance['unit_currency'] == 'KRW')].tolist()
        SYNX_balance_index_no = df_balance.index[
            (df_balance['currency'] == 'SYNX') & (df_balance['unit_currency'] == 'KRW')].tolist()
        VTHO_balance_index_no = df_balance.index[
            (df_balance['currency'] == 'VTHO') & (df_balance['unit_currency'] == 'KRW')].tolist()
        USDT_balance_index_no = df_balance.index[
            (df_balance['currency'] == 'USDT') & (df_balance['unit_currency'] == 'KRW')].tolist()

        try:
            self.df_total_item_list = df_balance.drop([df_balance.index[krw_balance_index_no[0]]])
        except:
            pass
        try:
            self.df_total_item_list = self.df_total_item_list.drop([df_balance.index[SYNX_balance_index_no[0]]])
        except:
            pass

        try:
            self.df_total_item_list = self.df_total_item_list.drop([df_balance.index[VTHO_balance_index_no[0]]])
        except:
            pass
        try:
            self.df_total_item_list = self.df_total_item_list.drop([df_balance.index[USDT_balance_index_no[0]]])
        except:
            pass

        test=0

    # 실제로 키움증권에서 보유한 종목들의 리스트를 가져오는 함수
    def db_to_possesed_item(self):
        logger.debug("db_to_possesed_item 함수에 들어왔습니다!")
        self.get_item_list()
        item_count = len(self.df_total_item_list)

        possesed_item_temp = {'date': [], 'code': [], 'code_name': [], 'holding_amount': [], 'purchase_price': [],
                              'present_price': [], 'valuation_profit': [], 'rate': [], 'item_total_purchase': []}

        possesed_item = DataFrame(possesed_item_temp,
                                  columns=['date', 'code', 'code_name', 'holding_amount', 'purchase_price',
                                           'present_price', 'valuation_profit', 'rate', 'item_total_purchase'])

        # 'currency', 'balance', 'locked', 'avg_buy_price',
        # 'avg_buy_price_modified', 'unit_currency'

        for i in range(item_count):
            row = self.df_total_item_list.iloc[i,:]
            code=(row[5]+'-'+row[0]).lower()
            # 오늘 일자
            test=0
            possesed_item.loc[i, 'date'] = self.today
            possesed_item.loc[i, 'code'] = code
            possesed_item.loc[i, 'code_name'] = code
            # 보유량
            this_coin_holding_amount=float(row[1])
            possesed_item.loc[i, 'holding_amount'] = this_coin_holding_amount
            # 매수가
            this_coin_purchase_price=float(row[3])
            possesed_item.loc[i, 'purchase_price'] = this_coin_purchase_price
            # 현재가
            try:
                this_coin_current_price=float(pyupbit.get_current_price(code))
            except:
                try:
                    this_coin_current_price = float(pyupbit.get_current_price(code))
                except:
                    this_coin_current_price=0
            possesed_item.loc[i, 'present_price'] = this_coin_current_price


            # valuation_profit은 사실상 의미가 없다. 백만원 어치 종목 매도 시 한번에 매도 되는게 아니고
            # 10만원 씩 10번 체결 될 수 있기 때문에
            # 마지막 체결 된 10만원이 possessd_item 테이블의 valuation_profit컬럼에 적용이 된다. 따라서 그냥 무시
            possesed_item.loc[i, 'valuation_profit'] = this_coin_current_price * this_coin_holding_amount
            # 수익률, 반드시 float로 넣어줘야한다.
            try:
                possesed_item.loc[i, 'rate'] = round((1-(this_coin_current_price * this_coin_holding_amount)/(this_coin_purchase_price * this_coin_holding_amount))*100,2)
            except:
                possesed_item.loc[i, 'rate'] = 0

            if this_coin_current_price==0:
                possesed_item.loc[i, 'rate'] = 0

            # 총 매수 금액
            possesed_item.loc[i, 'item_total_purchase'] = this_coin_purchase_price * this_coin_holding_amount

        # possessed_item 테이블에 현재 보유 종목을 넣는다.
        possesed_item.to_sql('possessed_item', self.engine_JB, if_exists='replace')
        self.chegyul_sync()
        test = 0

    def chegyul_sync(self):
        # 먼저 possessd_item 테이블에는 있는데 all_item_db에 없는 종목들 추가해준다
        sql = """select code, code_name, purchase_price, rate from possessed_item p
            where p.code not in (select a.code from all_item_db a
                                 where a.sell_date = '0' group by a.code)
            group by p.code"""

        rows = self.engine_JB.execute(sql).fetchall()

        logger.debug("possess_item 테이블에는 있는데 all_item_db에 없는 종목들 처리!!!")
        logger.debug(rows)

        for r in rows:
            #test=0
            self.db_to_all_item(0, r.code, 0, r.purchase_price, r.rate)

    # 현재 잔액 부족, 매수할 종목 리스트가 없는 경우로 인해
    # setting_data 테이블의 today_buy_stop 컬럼에 오늘 날짜가 찍혀있는지 확인하는 함수
    # setting_data 테이블의 today_buy_stop에 날짜가 찍혀 있으면 매수 중지, 0이면 매수 진행 가능
    def buy_check(self):
        logger.debug("buy_check 함수에 들어왔습니다!")
        sql = "select today_buy_stop from setting_data limit 1"
        rows = self.engine_JB.execute(sql).fetchall()[0][0]

        if rows != self.today:
            logger.debug("GoGo Buying!!!!!!")
            return True
        else:
            logger.debug("Stop Buying!!!!!!")
            return False

    # 매도 했는데 bot이 꺼져있을때 매도해서 possessed_item 테이블에는 없는데 all_item_db에 sell_date 안찍힌 종목들 처리해준다.
    def final_chegyul_check(self):
        sql = "select code from all_item_db a where (a.sell_date = '%s' or a.sell_date ='%s') and a.code not in ( select code from possessed_item) and a.chegyul_check != '%s'"

        rows = self.engine_JB.execute(sql % (0, "", 1)).fetchall()
        logger.debug("possess_item 테이블에는 없는데 all_item_db에 sell_date가 없는 리스트 처리!!!")
        logger.debug(rows)
        num = len(rows)

        for t in range(num):
            logger.debug(f"t!!! {t}")
            self.sell_final_check2(rows[t][0])

        # 오늘 리스트 다 뽑았으면 today를 setting_data에 체크
        sql = "UPDATE setting_data SET final_chegyul_check='%s' limit 1"
        self.engine_JB.execute(sql % (self.today))

    def sell_final_check2(self, code):
        logger.debug(f"sell_final_check2 possessed_item에는 없는데 all_item_db에 sell_date 추가 안된 종목 처리 !!! {code}")
        sql = "UPDATE all_item_db SET chegyul_check='%s', sell_date ='%s' WHERE code='%s' and sell_date ='%s' ORDER BY buy_date desc LIMIT 1"

        self.engine_JB.execute(sql % (0, self.today_detail, code, 0))

    # all_item_db의 rate를 업데이트 한다.
    def rate_check(self):
        logger.debug("rate_check!!!")
        sql = "select code ,holding_amount, purchase_price, present_price, valuation_profit, rate,item_total_purchase from possessed_item group by code"
        rows = self.engine_JB.execute(sql).fetchall()

        logger.debug("rate 업데이트 !!!")
        logger.debug(rows)
        num = len(rows)

        for k in range(num):
            # logger.debug("k!!!")
            # logger.debug(k)
            code = rows[k][0]
            holding_amount = rows[k][1]
            purchase_price = rows[k][2]
            present_price =rows[k][3]
            valuation_profit=rows[k][4]
            rate = rows[k][5]
            item_total_purchase = rows[k][6]
            # print("rate!!", rate)
            sql = "update all_item_db set holding_amount ='%s', purchase_price ='%s', present_price='%s',valuation_profit='%s',rate='%s',item_total_purchase='%s' where code='%s' and sell_date = '%s'"
            self.engine_JB.execute(sql % (holding_amount,purchase_price,present_price,valuation_profit,float(rate),item_total_purchase, code, 0))

    # 보유량 가져오는 함수
    def get_holding_amount(self, code):
        logger.debug("get_holding_amount 함수에 들어왔습니다!")
        sql = "select holding_amount from possessed_item where code = '%s' group by code"
        rows = self.engine_JB.execute(sql % (code)).fetchall()
        if len(rows):
            return rows[0][0]
        else:
            logger.debug("get_holding_amount 비어있다 !")
            return False

    def sell_order(self, code,holding_amount):
        ret=upbit.sell_market_order(code.upper(), holding_amount)
        logger.debug(f'''매도 주문합니다. {ret}''')
        # uuid = ret[0]['uuid']
        # 안정성을 위해 0.1초 딜레이

        #미체결 대상 확인 하는 것
        result_ret=len(upbit.get_order(code))

        #미체결 대상 남으면 계속 미체결 대상 있는지 확인하는 것
        cnt=0
        while result_ret>0 and cnt < 10:
            timesleep.sleep(0.05)
            result_ret = len(upbit.get_order(code))
            cnt+=1

        if result_ret==0:
            self.sell_final_check(code)

    # 매도 후 all item db 에 작업하는거
    def sell_final_check(self, code):
        logger.debug("sell_final_check")

        # sell_price가 없어서 에러가났음
        get_list = self.engine_JB.execute(f"""
            SELECT valuation_profit, rate, item_total_purchase, present_price 
            FROM possessed_item WHERE code='{code}' LIMIT 1
        """).fetchall()
        if get_list:
            item = get_list[0]
            sql = f"""UPDATE all_item_db
                SET item_total_purchase = {item.item_total_purchase}, chegyul_check = 0,
                 sell_date = '{self.today_detail}', valuation_profit = {item.valuation_profit},
                 sell_rate = {item.rate}, sell_price = {item.present_price}
                WHERE code = '{code}' and sell_date = '0' ORDER BY buy_date desc LIMIT 1"""
            self.engine_JB.execute(sql)

            # 팔았으면 즉각 possess db에서 삭제한다. 왜냐하면 checgyul_check 들어가기 직전에 possess_db를 최신화 하긴 하지만 possess db 최신화와 chegyul_check 사이에 매도가 이뤄져서 receive로 가게 되면 sell_date를 찍어버리기 때문에 checgyul_check 입장에서는 possess에는 존재하고 all_db는 sell_date찍혀있다고 판단해서 새롭게 all_db추가해버린다.
            self.engine_JB.execute(f"DELETE FROM possessed_item WHERE code = '{code}'")

            logger.debug(f"delete {code}!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        else:
            logger.debug("possess가 없다!!!!!!!!!!!!!!!!!!!!!")



        # sql = f'''update sell set holding_amount ='%s', purchase_price ='%s', present_price='%s',valuation_profit='%s',rate='%s',item_total_purchase='%s' where code='%s''''
        # self.engine_JB.execute(sql % (sql))

    def buy_order(self, code, buy_money):

        ret = upbit.buy_market_order(code, buy_money)
        logger.debug(f'''매수 주문합니다. {ret}''')
        # uuid = ret[0]['uuid']
        # 안정성을 위해 0.1초 딜레이

        # 미체결 대상 확인 하는 것
        result_ret = len(upbit.get_order(code))

        # 미체결 대상 남으면 계속 미체결 대상 있는지 확인하는 것
        cnt = 0
        while result_ret > 0 and cnt < 10:
            timesleep.sleep(0.05)
            result_ret = len(upbit.get_order(code))
            cnt += 1
        # uuid = ret[0]['uuid']
        if result_ret==0:
            self.end_invest_count_check(code)

    # 오늘 매수 할 종목들을 가져오는 함수
    def get_today_buy_list(self):
        logger.debug("get_today_buy_list 함수에 들어왔습니다!")
        logger.debug("self.today : %s , self.date_rows_yesterday : %s !", self.today, self.date_rows_yesterday)

        if self.sf.is_simul_table_exist(self.db_name, "realtime_daily_buy_list"):
            logger.debug("realtime_daily_buy_list 생겼다!!!!! ")
            self.sf.get_realtime_daily_buy_list()
            if self.sf.len_df_realtime_daily_buy_list == 0:
                logger.debug("realtime_daily_buy_list 생겼지만 아직 data가 없다!!!!! ")
                return
        else:
            logger.debug("realtime_daily_buy_list 없다 !! ")
            return


        logger.debug("self.sf.len_df_realtime_daily_buy_list 이제 사러간다!! ")
        logger.debug("매수 리스트!!!!")
        logger.debug(self.sf.df_realtime_daily_buy_list)
        # 만약에 realtime_daily_buy_list 의 종목 수가 1개 이상이면 아래 로직을 들어간다
        for i in range(self.sf.len_df_realtime_daily_buy_list):
            # code를 가져온다
            code = self.sf.df_realtime_daily_buy_list.loc[i, 'code']
            # 종가를 가져온다
            close = self.sf.df_realtime_daily_buy_list.loc[i, 'close']
            # 이미 오늘 매수 한 종목이면 check_item은 1 / 아직 매수 안했으면 0
            check_item = self.sf.df_realtime_daily_buy_list.loc[i, 'check_item']



            if self.jango_is_null:
                break
            # 이미 매수한 종목은 넘기고 다음 종목을 사라는 의미
            if check_item == True:
                continue
            else:
                # (추가) 매수 조건 함수(trade_check) ##########################################
                # trade_check_num(실시간 조건 체크-> 실시간으로 조건 비교 하여 매수하는 경우)
                # 고급챕터에서 수업 할 때 아래 주석을 풀어주세요!
                if self.sf.trade_check_num:
                    # 시작가를 가져온다
                    current_open = self.get_one_day_option_data(code, self.today, 'open')
                    current_price = self.get_one_day_option_data(code, self.today, 'close')
                    current_sum_volume = self.get_one_day_option_data(code, self.today, 'volume')
                    if not self.sf.trade_check(self.sf.df_realtime_daily_buy_list.loc[i], current_open, current_price, current_sum_volume):
                        continue
                ###################################################################################

                self.get_today_buy_list_code = code
                self.get_today_buy_list_close = close
                #test=0
                #break
                if self.jango_check():
                    self.trade()
                else:
                    break

        # 모든 매수를 마쳤으면 더이상 매수 하지 않도록 설정하는 함수
        if self.sf.only_nine_buy:
            self.buy_check_stop()


    # 매수 함수
    def trade(self):
        logger.debug("trade 함수에 들어왔다!")
        logger.debug("매수 대상 종목 코드! " + self.get_today_buy_list_code)

        # 실시간 현재가(close) 가져오는 함수
        # close는 종가 이지만, 현재 시점의 종가를 가져오기 때문에 현재가를 가져온다.
        current_price = self.get_one_day_option_data(self.get_today_buy_list_code, self.today, 'close')

        #여기서 모멘텀함수 다녀와서 종목 적용하면 될듯.
        #

        if current_price == False:
            logger.debug(self.get_today_buy_list_code + " 의 현재가가 비어있다 !!!")
            return False

        # 매수 가격 최저 범위
        min_buy_limit = int(self.get_today_buy_list_close) * self.sf.invest_min_limit_rate
        # 매수 가격 최고 범위
        max_buy_limit = int(self.get_today_buy_list_close) * self.sf.invest_limit_rate
        # 현재가가 매수 가격 최저 범위와 매수 가격 최고 범위 안에 들어와 있다면 매수 한다.
        test=0
        if min_buy_limit < current_price < max_buy_limit:
            #실투시에 이부분을 수정하면 종목별 모멘텀 적용 가능할듯.


            #원래 0.98 곱해줬는데 삭제함.
            #다시 0.98 곱해준 이유는, invest_unit이 됐는데도 못사는 경우가 발생함.
            buy_num = self.buy_num_count(self.invest_unit, current_price)


            buy_money = self.sf.limit_money + self.invest_unit * 1.1
            if self.jango_money < buy_money :
                logger.debug(f'''(조금만) 투자가능해서 투자금 조정!''')
                #buy_num = self.buy_num_count(int(self.invest_unit*0.75), int(current_price))
                buy_num = self.buy_num_count(self.jango_money * 0.95, current_price)



            logger.debug(
                "매수!!!!+-+-+-+-+-+-+-+-+-+-+-+-+-+-+- code :%s, 목표가: %s, 현재가: %s, 매수량: %s, min_buy_limit: %s, max_buy_limit: %s , invest_limit_rate: %s,예수금: %s , today : %s, today_min : %s, date_rows_yesterday : %s, invest_unit : %s, real_invest_unit : %s +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-",
                self.get_today_buy_list_code, self.get_today_buy_list_close, current_price, buy_num, min_buy_limit,
                max_buy_limit, self.sf.invest_limit_rate, self.jango_money, self.today, self.today_detail,
                self.date_rows_yesterday, self.invest_unit, int(current_price) * int(buy_num))

            # 매수 하기 전에 해당 종목의 check_item을 1로 변경. 즉, 이미 매수 했으니까 다시 매수 하지말라고 체크 하는 로직
            sql = "UPDATE realtime_daily_buy_list SET check_item='%s' WHERE code='%s'"
            self.engine_JB.execute(sql % (1, self.get_today_buy_list_code))

            # 03 시장가 매수
            # 4번째 인자: 1: 신규매수 / 2: 신규매도 / 3:매수취소 / 4:매도취소 / 5: 매수정정 / 6:매도정정
            #
            # #if self.trade
            # if QTime.currentTime() >= QTime(9, 0, 0):
            #     self.chatbot.buy_alarm(str(self.get_today_buy_list_code), str(current_price), str(buy_num),
            #                            str(self.invest_unit))

            self.buy_order(self.get_today_buy_list_code,current_price*buy_num)

            # 만약 sf.only_nine_buy가 False 이면 즉, 한번 매수하고 금일 매수를 중단하는 것이 아니라면, 매도 후에 잔액이 생기면 다시 매수를 시작
            # sf.only_nine_buy가 True이면 1회만 매수, 1회 매수 시 잔액이 부족해지면 바로 매수 중단
            if not self.jango_check() and self.sf.only_nine_buy:
                logger.debug("하나 샀더니 잔고가 부족해진 구간!!!!!")
                # setting_data에 today_buy_stop을 1 로 설정
                self.buy_check_stop()
        else:
            logger.debug(
                "invest_limit_rate 만큼 급등 or invest_min_limit_rate 만큼 급락 해서 매수 안함 !!! code :%s, 목표가: %s , 현재가: %s, invest_limit_rate: %s , invest_min_limit_rate : %s, today : %s, today_min : %s, date_rows_yesterday : %s",
                self.get_today_buy_list_code, self.get_today_buy_list_close, current_price, self.sf.invest_limit_rate,
                self.sf.invest_min_limit_rate, self.today, self.today_detail, self.date_rows_yesterday)

    # 몇 개의 주를 살지 계산 하는 함수
    def buy_num_count(self, invest_unit, present_price):
        logger.debug("buy_num_count 함수에 들어왔습니다!")
        return float(invest_unit / present_price)

    # 투자 가능한 잔액이 부족한 경우이거나, 매수할 종목이 더이상 없는 경우
    # setting_data의 today_buy_stop 옵션을 1로 변경-> 더이상 매수 하지 않는다.
    def buy_check_stop(self):
        logger.debug("buy_check_stop!!!")
        sql = "UPDATE setting_data SET today_buy_stop='%s' limit 1"
        self.engine_JB.execute(sql % (self.today))


    # get_one_day_option_data : 특정 종목의 특정 일 open(시작가), high(최고가), low(최저가), close(종가), volume(거래량) 조회 함수
    # 사용방법
    # code : 종목코드
    # start : 조회 일자
    # option : open(시작가), high(최고가), low(최저가), close(종가), volume(거래량)
    def get_one_day_option_data(self, code, start, option):
        self.ohlcv = defaultdict(list)
        #9시 기준 시가
        this_coin_day = pyupbit.get_ohlcv(code, count=1)
        this_coin_minute = pyupbit.get_ohlcv(code, count=1, interval='minute1')
        open = this_coin_day.iloc[0][2]
        current_price  = pyupbit.get_current_price(code)
        #1분전 거래량으로 가져옴
        current_volume = this_coin_minute.iloc[0][5]
        #9시 기준으로
        current_high = this_coin_day.iloc[0][3]
        current_low = this_coin_day.iloc[0][4]



        if option == 'open':
            return open
        elif option == 'high':
            return current_high
        elif option == 'low':
            return current_low
        elif option == 'close':
            return current_price
        elif option == 'volume':
            return current_volume
        else:
            return False

    def end_invest_count_check(self, code):
        logger.debug("end_invest_count_check 함수로 들어왔습니다!")
        logger.debug("end_invest_count_check_code!!!!!!!!")
        logger.debug(code)

        sql = "UPDATE all_item_db SET chegyul_check='%s' WHERE code='%s' and sell_date = '%s' ORDER BY buy_date desc LIMIT 1"

        self.engine_JB.execute(sql % (0, code, 0))

        # 중복적으로 possessed_item 테이블에 반영되는 이슈가 있어서 일단 possesed_item 테이블에서 해당 종목을 지운다.
        # 어차피 다시 possessed_item은 업데이트가 된다.
        sql = "delete from possessed_item where ifnull(code,0) ='%s'"
        self.engine_JB.execute(sql % (code))

    # all_item_db 보유한 종목이 있는지 확인 (sell_date가 0이거나 비어있으면 아직 매도하지 않고 보유한 종목이다)
    # 보유한 경우 true 반환, 보유 하지 않았으면 False 반환
    def is_all_item_db_check(self, code):
        logger.debug(f"is_all_item_db_check code!! {code}")
        sql = "select code from all_item_db where code='%s' and (sell_date ='%s' or sell_date='%s') ORDER BY buy_date desc LIMIT 1"

        rows = self.engine_JB.execute(sql % (code, 0, "")).fetchall()
        if len(rows) != 0:
            return True
        else:
            return False










    # def sell_table(self):
    #     temp_sell_table = {'date':[],'code': [], 'sell_check': [], 'uuid':[]}
    #     self.sell_table = DataFrame(temp_sell_table,
    #                               columns=['date','code', 'sell_check','uuid'])
    #
    #     self.sell_table.to_sql('sell_table', self.engine_JB, if_exists='append')





if __name__ == '__main__':
    coin=open_api()





