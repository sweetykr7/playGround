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


class simulator_func_mysql:
    def __init__(self, simul_num, op, db_name):
        self.simul_num = int(simul_num)

        # scraper할 때 start date 가져오기 위해서
        if self.simul_num == -1:
            self.date_setting()

        # option이 reset일 경우 실행
        elif op == 'reset':
            self.op = 'reset'
            self.simul_reset = True
            self.variable_setting()
            self.rotate_date()

        # option이 real일 경우 실행(시뮬레이터와 무관)
        elif op == 'real':
            self.op = 'real'
            self.simul_reset = False
            self.db_name = db_name
            self.variable_setting()

        #  option이 continue 일 경우 실행
        elif op == 'continue':
            self.op = 'continue'
            self.simul_reset = False
            self.variable_setting()
            self.rotate_date()
        else:
            print("simul_num or op 어느 것도 만족 하지 못함 simul_num : %s ,op : %s !!", simul_num, op)

    # 마지막으로 구동했던 시뮬레이터의 날짜를 가져온다.
    def get_jango_data_last_date(self):
        sql = "SELECT date from jango_data order by date desc limit 1"
        return self.engine_simulator.execute(sql).fetchall()[0][0]

    # 모든 테이블을 삭제 하는 함수
    def delete_table_data(self):
        logger.info('delete_table_data !!!!')
        if self.is_simul_table_exist(self.db_name, "all_item_db"):
            sql = "drop table all_item_db"
            self.engine_simulator.execute(sql)
            # 만약 jango data 컬럼을 수정하게 되면 테이블을 삭제하고 다시 생성이 자동으로 되는데 이때 삭제했으면 delete가 안먹힌다. 그래서 확인 후 delete

        if self.is_simul_table_exist(self.db_name, "jango_data"):
            sql = "drop table jango_data"
            self.engine_simulator.execute(sql)

        if self.is_simul_table_exist(self.db_name, "realtime_daily_buy_list"):
            sql = "drop table realtime_daily_buy_list"
            self.engine_simulator.execute(sql)

    # realtime_daily_buy_list 테이블의 check_item컬럼에 특정 종목의 매수 시간을 넣는 함수
    def update_realtime_daily_buy_list(self, code, min_date):
        sql = "update realtime_daily_buy_list set check_item = '%s' where code = '%s'"
        self.engine_simulator.execute(sql % (min_date, code))

    def def_realtime_daily_buy_list_len(self):
        try:
            sql = "SELECT * from realtime_daily_buy_list "
            return_rows = self.engine_simulator.execute(sql).fetchall()
        except:
            return_rows = []
        return return_rows

    # 시뮬레이션 옵션 설정 함수
    def variable_setting(self):
        # 아래 if문으로 들어가기 전까지의 변수들은 모든 알고리즘에 공통적으로 적용 되는 설정
        # 오늘 날짜를 설정
        self.date_setting()
        # 시뮬레이팅이 끝나는 날짜.
        self.simul_end_date = self.today
        self.start_min = "0900"

        self.realtime_daily_buy_list_len = len(self.def_realtime_daily_buy_list_len())

        # 아래 3개는 분별시뮬레이션 옵션
        # (use_min, only_nine_buy 변수만 각각의 알고리즘에 붙여 넣기 해서 사용)
        # 분별 시뮬레이션을 사용하고 싶을 경우 아래 옵션을 True로 변경하여 사용
        self.use_min = False
        # 아침 9시에만 매수를 하고 싶은 경우 True, 9시가 아니어도 매수를 하고 싶은 경우 False(분별 시뮬레이션 적용 가능 / 일별 시뮬레이션은 9시에만 매수, 매도)
        self.only_nine_buy = False
        # self.buy_stop옵션은 수정 필요가 없음. self.only_nine_buy 옵션을 True로 하게 되면 시뮬레이터가 9시에 매수 후에 self.buy_stop을 true로 변경해서 당일에는 더이상 매수하지 않도록 설정함
        self.buy_stop = False

        # AI알고리즘 사용 여부 (고급 챕터에서 소개)
        self.use_ai = False  # ai 알고리즘 사용 시 True 사용 안하면 False
        self.ai_filter_num = 1  # ai 알고리즘 선택

        # 실시간 조건 매수 옵션 (고급 챕터에서 소개)
        # self.only_nine_buy 옵션을 반드시 False로 설정해야함
        # self.use_min 옵션이 반드시 True로 설정이 되어야함
        # 실시간 조건 매수 알고리즘 선택 (1,2,3..)
        self.trade_check_num = False

        # 실시간 조건 list용
        self.realtime_daily_buy_list_condition = []
        self.sell_list_condition = []

        # 섀넌의 포트폴리오
        self.shannon_rate = 1
        self.shannon_rate_on = 0  # on/off 조정

        # 실시간 조건 list용
        self.condition_trade = False

        # 실시간 조건 list용
        self.realtime_daily_buy_list_condition = []
        self.sell_list_condition = []

        ##########################################평균 모멘텀 스코어 적용##########################################
        self.avg_momentum_rate = 1
        # 평균 모멘텀 스코어 적용할지 말지 결정
        self.avg_momentum_on = 0

        # 몇 분할로 투자할 것인지.(전체 계좌 금액에서 나눔, 평균모멘텀 적용시 그 비율에서 나눔)
        self.divide_invest_unit = 20

        # 평균 모멘텀 스코어를 적용할때 적정 점수 이하에만 적용하는 방식, self.avg_momentum_month(적용 개월수)를 같이 쓴다.
        self.avg_momentum_apply_month = 0
        # 평균 모멘텀 스코어를 몇개월 단위로 적용시킬지(기본은 12개월)
        self.avg_momentum_period = 12
        # 평균 모멘텀을 한달단위로 할지 일단위로 할지 결정, 20은 한달임, 20이 기본 설정임.
        self.avg_momentum_day = 20

        # 평균 모멘텀 특정 이하에서 투자를 아예 안해버리는 것
        self.avg_momentum_invest_zero = 0

        # 각 종목별 모멘텀 on/off, ref.는 0임 off
        self.avg_momentum_each_stock_on = 0
        # 각 종목별 모멘텀 비율 초기화
        self.avg_momentum_each_stock_rate = 1

        # avg_momentum에서 특정점수 이하일때 inbus 적용할지 여부
        self.avg_momentum_apply_inbus_on = 0
        # avg_momentum에서 inbus적용시에 투자금액
        self.avg_momentum_apply_inbus_invest = 0
        # avg_momentum에서 적용시에 비율
        self.avg_momentum_apply_inbus_rate = 0
        # avg_momentum을 realtime에서 추가할지 말지를 결정하는 함수, 특정점수 이하일때 1로 되게 셋팅되어 있음
        self.avg_momentum_apply_inbus_real_on = 0

        # control the prespan1's rate
        self.prespan1_rate = 0.95

        # MDD 적용 할지 말지의 여부
        self.MDD_on = 0
        self.MDD_Min = 0
        self.MDD_Max = 0

        # MDD에 따라서 전체 투자금액 조정하기
        self.divide_rate_using_MDD_on = 0  # 켤지 말지
        self.divide_rate_using_MDD = 1
        self.divide_rate_using_MDD_setting = 1

        self.MDD_condition_changing_on = 0  # 조건 변경 신설함.
        self.MDD_zero_check = 0

        self.MDD_variable_on = 0
        self.MDD_variable_rate = 1
        self.MDD_privious_max = 0
        self.MDD_privious = 0
        self.MDD_privious_min = 0

        self.mdd_attack_avg_momentum_month = 3
        self.mdd_attack_divide_invest_unit = 30
        self.mdd_attack_avg_momentum_each_stock_on = 0

        self.mdd_defence_avg_momentum_month = 5
        self.mdd_defence_divide_invest_unit = 30
        self.mdd_defence_avg_momentum_each_stock_on = 0

        self.mdd_compensation_rate = 10

        # MDD 전역 변수 설정
        self.mdd_value = 0
        self.mdd_losscut = 15

        self.mdd_losscut_on = 0

        self.mdd_sell_count = 0  # mdd로 팔은 횟수 카운트
        self.mdd_sell_next_step = 5  # mdd로 팔고나서 다음 스텝에서 얼마되면 손절할건지
        self.mdd_sell_check = 0
        self.final_mdd_losscut = self.mdd_losscut

        self.pbr_rate = 1

        # noise rate을 rarryk에 적용하기

        self.noise_rate_on = 0

        ##########################################평균 모멘텀 스코어 적용##########################################

        self.rarry_k = 0.5
        self.noise_rate_on = 0
        self.divide_invest_unit = 20

        # 실시간 일목균형표 적용위해
        self.backspan_day = 0

        # 분별 시뮬레이션 당일 rate control 하기
        self.min_rate_control_on = False
        self.min_rate_control = 1

        # 중첩 데이터 삭제 안하기
        self.overlap_data_delete_on = True

        # 볼밴 상단에서 비율 벗어나면 매도하는 계수
        self.bband_u_rate = 1

        self.bband_u_buy_rate = 1
        self.bband_u_sell_rate = 1

        # 볼밴 상단 돌파한 기간 얼마인지의 변수
        self.bband_1month_period = 1

        # pbr rate, gpa적용하려고 넣어둠.
        self.pbr_rate = 1
        self.for_gpa_pbr_order_limit = 100
        self.for_gpa_gpa_order_limit = 100
        self.for_gpa_gpa_rate_order_limit = 50

        # prespan으로 손절하는 알고리즘 변수
        self.prespan1_losscut = 1
        self.prespan2_losscut = 1

        # 시가 총액 제한
        self.market_cap = 1000

        # 시가 총액 비율로 조정
        self.marketcap_rate = 20

        # 거래대금 제한
        self.trading_money = 1000 * 10000

        # 분별 시뮬레이션 시간 제한 두기
        self.trading_time_control_for_min_simul = False

        ##########################################평균 모멘텀 스코어 적용##########################################

        print("self.simul_num!!! ", self.simul_num)

        ###!@####################################################################################################################
        # 아래 부터는 알고리즘 별로 별도의 설정을 해주는 부분

        if self.simul_num in (1, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18,
                              921,  # 저pbr만 매수
                              1509,  # 저pbr+고gpa+시가총액 1000억 이하
                              1548,  # 저pbr(100)+시가총액 1000억 이하+고gpa_yoy_rate(50)+고gpa
                              1604,  # 저 pbr(100)+고gpa_yoy_rate(50)+고gpa+시가총액 하위 20%+거래대금 천이상+10분할
                              1644,  # 1604에 etc_info 주식개장일 적용해서 다시 만듬
                              1668,  # 1644에 15분할 120/60으로 변경함, Algo1
                              1802,  # 회사가치(상속세법) 저 평가 된 대상 매수, Algo2
                              1899,  # 회사가치(상속세법) > gpa, 시가총액 0~50%, limit 100, 20분할, 손절-30%, 카운트손절도입

                              2073,
                              # Redesign Algo1, 저PBR + 고GPA + GPA_qq_YOY_rate 1 : 3 : 2비율, 특정 매수리스트 갯수 이하시 매수금지/전량매도
                              2091,  # Redesign Algo2, 회사가치(상속세법) + 고GPA + 고 gpa_qq_YOY_rate 2:3:1 비율
                              2224,  # Redesign Algo3, #2222알고리즘으로 안전하게 변형시켜둠(20210901)
                              2248,  # Algo4(5번계좌)
        ########################## Coin Simulation
                              5001, #Test Coin
                              5002, #2248에서 이것저것 추가
                              5003, #
                              10001,  # 실전 봇 현재는 5003을 복사함.
                              #실시간 매매 준비  
                              5004, # 래리윌리엄스 전략 준비 노이즈 비율
                              5005,
                              5006, # rarry_k로 다시함
                              5007,  # avg_noise
                              5008, #평균이평으로 투자금 조절하는거 빼봄
                              5009, # 코인여러개로 함. 노이즈레벨 낮은순으로 10개를 선정한 담에 래리윌리엄스 적용
                              5010, #5009추가로 하나 만듬
                              5011, #5010에 익절 30%, 손절 -10%,살 대상을 거래대금까지 같이 해서 비교
                              5012, #거래대금을 20일 평균으로 바꿈
                              5013, #거래대금을 60일 평균으로 바꿈, 노이즈:거래대금=3:1로 맞춤, 매수쿼리문에 vol60을 추가함. 2달 이후꺼로 한다는 소리임
                              5014, #거래대금만으로
                              5015, #BTC/ETH/LTC/EOS/XRP
                              5016,  #5015에 손절,익절 없앰. btc안함
                              5017 #btc추가
                            ):

            # 시뮬레이팅 시작 일자(분 별 시뮬레이션의 경우 최근 1년 치 데이터만 있기 때문에 start_date 조정 필요)
            self.simul_start_date = "20190101"

            ######### 알고리즘 선택 #############
            # 매수 리스트 설정 알고리즘 번호
            self.db_to_realtime_daily_buy_list_num = 1

            # 매도 리스트 설정 알고리즘 번호
            self.sell_list_num = 1
            ###################################

            # 초기 투자자금(시뮬레이션에서의 초기 투자 금액. 모의투자는 신청 당시의 금액이 초기 투자 금액이라고 보시면 됩니다)
            # 주의! start_invest_price 는 모의투자 초기 자본금과 별개. 시뮬레이션에서만 적용.
            # 키움증권 모의투자의 경우 초기에 모의투자 신청 할 때 설정 한 금액으로 자본금이 설정됨
            self.start_invest_price = 20000000

            # 매수 금액
            self.invest_unit = 1000000

            # 자산 중 최소로 남겨 둘 금액
            self.limit_money = 3000000

            # 익절 수익률 기준치
            self.sell_point = 10

            # 손절 수익률 기준치
            self.losscut_point = -2

            # 실전/모의 봇 돌릴 때 매수하는 순간 종목의 최신 종가 보다 1% 이상 오른 경우 사지 않도록 하는 설정(변경 가능)
            self.invest_limit_rate = 1.03  # 원래 1.01이였음.
            # 실전/모의 봇 돌릴 때 매수하는 순간 종목의 최신 종가 보다 -2% 이하로 떨어진 경우 사지 않도록 하는 설정(변경 가능)
            self.invest_min_limit_rate = 0.97  # 원래  0.98이였음.

            if self.simul_num == 4:
                self.db_to_realtime_daily_buy_list_num = 4
                self.interval_month = 3
                self.invest_unit = 50000

            elif self.simul_num == 5:
                self.db_to_realtime_daily_buy_list_num = 5
                self.total_transaction_price = 10000000000
                self.interval_month = 3
                self.vol_mul = 3
                self.d1_diff = 2
                # self.use_min= True
                # self.only_nine_buy = False

            # 절대 모멘텀 / 상대 모멘텀
            elif self.simul_num in (7, 8, 9, 10):
                # 매수 리스트 설정 알고리즘 번호(절대모멘텀 code ver)
                self.db_to_realtime_daily_buy_list_num = 7
                # 매도 리스트 설정 알고리즘 번호(절대모멘텀 code ver)
                self.sell_list_num = 4
                # 시뮬레이팅 시작 일자(분 별 시뮬레이션의 경우 최근 1년 치 데이터만 있기 때문에 start_date 조정 필요)
                self.simul_start_date = "20200101"
                # n일 전 종가 데이터를 가져올지 설정 (ex. 20 -> 장이 열리는 날 기준 20일 이니까 기간으로 보면 약 한 달, 250일->1년)
                self.day_before = 20  # 단위 일
                # n일 전 종가 대비 현재 종가(현재가)가 몇 프로 증가 했을 때 매수, 몇 프로 떨어졌을 때 매도 할 지 설정(0으로 설정 시 단순히 증가 했을 때 매수, 감소 했을 때 매도)
                self.diff_point = 1  # 단위 %
                # 분별 시뮬레이션 옵션
                self.use_min = True
                self.only_nine_buy = True

                if self.simul_num == 8:
                    # 매수 리스트 설정 알고리즘 번호 (절대모멘텀 query ver)
                    self.db_to_realtime_daily_buy_list_num = 8
                    # 매도 리스트 설정 알고리즘 번호 (절대모멘텀 query ver)
                    self.sell_list_num = 5

                elif self.simul_num == 9:
                    # 매수 리스트 설정 알고리즘 번호 (절대모멘텀 query ver)
                    self.db_to_realtime_daily_buy_list_num = 8
                    # 매도 리스트 설정 알고리즘 번호 (절대모멘텀 query ver + losscut point 추가)
                    self.sell_list_num = 6
                    # 손절 수익률 기준치
                    self.losscut_point = -2

                elif self.simul_num == 10:
                    # 매수 리스트 설정 알고리즘 번호 (상대모멘텀 query ver)
                    self.db_to_realtime_daily_buy_list_num = 9
                    # 매도 리스트 설정 알고리즘 번호 (절대모멘텀 query ver + losscut point 추가)
                    self.sell_list_num = 5

            elif self.simul_num == 11:
                self.use_ai = True  # ai 알고리즘 사용 시 True 사용 안하면 False
                self.ai_filter_num = 1  # ai 알고리즘 선택

            # 실시간 조건 매수
            elif self.simul_num in (12, 13, 14):
                self.simul_start_date = "20200101"
                self.use_min = True
                # 아침 9시에만 매수를 하고 싶은 경우 True, 9시가 아니어도 매수를 하고 싶은 경우 False(분별 시뮬레이션, trader 적용 가능 / 일별 시뮬레이션은 9시에만 매수, 매도)
                self.only_nine_buy = False
                # 실시간 조건 매수 옵션 (고급 챕터에서 소개) self.only_nine_buy 옵션을 반드시 False로 설정해야함
                self.trade_check_num = 1  # 실시간 조건 매수 알고리즘 선택 (1,2,3..)
                # 특정 거래대금 보다 x배 이상 증가 할 경우 매수
                self.volume_up = 2
                #
                if self.simul_num == 13:
                    self.trade_check_num = 2
                    # 매수하는 순간 종목의 최신 종가 보다 1% 이상 오른 경우 사지 않도록 하는 설정(변경 가능)
                    self.invest_limit_rate = 1.01
                    # 매수하는 순간 종목의 최신 종가 보다 -2% 이하로 떨어진 경우 사지 않도록 하는 설정(변경 가능)
                    self.invest_min_limit_rate = 0.98

                # 래리윌리엄스 변동성 돌파 전략
                elif self.simul_num == 14:
                    self.trade_check_num = 3
                    self.rarry_k = 0.5

            elif self.simul_num == 16:
                self.db_to_realtime_daily_buy_list_num = 11

            elif self.simul_num == 17:
                self.db_to_realtime_daily_buy_list_num = 12
            elif self.simul_num == 18:
                self.db_to_realtime_daily_buy_list_num = 13
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20201016"




            elif self.simul_num == 921:
                self.db_to_realtime_daily_buy_list_num = 921
                self.sell_list_num = 921
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20031229"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 10
                # 손절 수익률 기준치
                self.losscut_point = -30
                # 몇 분할 투자할지
                self.divide_invest_unit = 30
                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0  # 공격에 종목별 모멘텀 적용

                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100

                self.overlap_data_delete_on = False

            elif self.simul_num == 1509:
                self.db_to_realtime_daily_buy_list_num = 1509
                self.sell_list_num = 1509
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20210201"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 10
                # 손절 수익률 기준치
                self.losscut_point = -25
                # 몇 분할 투자할지
                self.divide_invest_unit = 20

                self.prespan1_rate = 0.978
                self.overlap_data_delete_on = False

                self.pbr_rate = 1
                self.for_gpa_pbr_order_limit = 100

                self.bband_1month_period = 3

                self.mdd_losscut_on = True
                self.mdd_losscut = 20
                self.mdd_sell_next_step = 3

                # 시가 총액 제한
                self.market_cap = 1000

            elif self.simul_num == 1548:
                self.db_to_realtime_daily_buy_list_num = 1548
                self.sell_list_num = 1509
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20031229"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 10
                # 손절 수익률 기준치
                self.losscut_point = -25
                # 몇 분할 투자할지
                self.divide_invest_unit = 20

                self.prespan1_rate = 0.978
                self.overlap_data_delete_on = False

                self.pbr_rate = 1
                self.for_gpa_pbr_order_limit = 100
                self.for_gpa_gpa_rate_order_limit = 50

                self.bband_1month_period = 3

                self.mdd_losscut_on = True
                self.mdd_losscut = 15
                self.mdd_sell_next_step = 3

                # 시가 총액 제한
                self.market_cap = 1000

            elif self.simul_num == 1604:
                self.db_to_realtime_daily_buy_list_num = 1604
                self.sell_list_num = 1604
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20200501"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 10
                # 손절 수익률 기준치
                self.losscut_point = -25
                # 몇 분할 투자할지
                self.divide_invest_unit = 10

                self.prespan1_rate = 0.978

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.pbr_rate = 1
                self.for_gpa_pbr_order_limit = 120
                self.for_gpa_gpa_rate_order_limit = 50

                self.bband_1month_period = 3

                self.mdd_losscut_on = False
                self.mdd_losscut = 15
                self.mdd_sell_next_step = 3
                self.trading_money = 1000 * 10000

                self.marketcap_rate = 20


            elif self.simul_num == 1644:
                self.db_to_realtime_daily_buy_list_num = 1644
                self.sell_list_num = 1644
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20200501"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 10
                # 손절 수익률 기준치
                self.losscut_point = -25
                # 몇 분할 투자할지
                self.divide_invest_unit = 10

                self.prespan1_rate = 0.978

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.pbr_rate = 1
                self.for_gpa_pbr_order_limit = 120
                self.for_gpa_gpa_rate_order_limit = 50

                self.bband_1month_period = 3

                self.mdd_losscut_on = False
                self.mdd_losscut = 15
                self.mdd_sell_next_step = 3
                self.trading_money = 1000 * 10000

                self.marketcap_rate = 20




            elif self.simul_num == 1668:
                self.db_to_realtime_daily_buy_list_num = 1668
                self.sell_list_num = 1644
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20200501"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 10
                # 손절 수익률 기준치
                self.losscut_point = -25
                # 몇 분할 투자할지
                self.divide_invest_unit = 15

                self.prespan1_rate = 0.978

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.pbr_rate = 1
                self.for_gpa_pbr_order_limit = 120
                self.for_gpa_gpa_rate_order_limit = 60

                self.bband_1month_period = 3

                self.mdd_losscut_on = False
                self.mdd_losscut = 15
                self.mdd_sell_next_step = 3
                self.trading_money = 1000 * 10000

                self.marketcap_rate = 15  # 코스닥,코스피로만으로 수정함


            elif self.simul_num == 1802:
                self.db_to_realtime_daily_buy_list_num = 1802
                self.sell_list_num = 1802
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20070101"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -30
                # 몇 분할 투자할지
                self.divide_invest_unit = 10

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.marketcap_rate = 21

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.profit_qq = 0  # 십만이 일억
                self.loan_rate = 120

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

            elif self.simul_num == 1899:
                self.db_to_realtime_daily_buy_list_num = 1899
                self.sell_list_num = 1899
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20070101"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -30
                # 몇 분할 투자할지
                self.divide_invest_unit = 20

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.marketcap_rate = 10
                self.market_start = 0
                self.market_gap = 5  # gap을 설정하면 marketcap_rate * market_gap 만큼 limit에서 더해진다. 10(시작점) + 30(갭)

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 1

                self.profit_qq = 0  # 십만이 일억
                self.loan_rate_tt = 1.5

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                # 특정 손절 라인 아래로 여러개의 종목이 걸리면 전체 다 팔리게 하는 알고리즘
                self.lisk_sensing_rate = -10
                self.lisk_sensing_count = 5

                self.company_value_ss_rate_limit = 100


            elif self.simul_num == 2073:
                self.db_to_realtime_daily_buy_list_num = 2073
                self.sell_list_num = 2073
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20070101"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 10
                # 손절 수익률 기준치
                self.losscut_point = -30
                # 몇 분할 투자할지
                self.divide_invest_unit = 20

                self.prespan1_rate = 0.978

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.pbr_rate = 0.2
                self.for_gpa_pbr_order_limit = 150

                self.for_gpa_yoy_rate = 0

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.marketcap_rate = 20

                self.pbr_proportion = 1
                self.gpa_proportion = 3
                self.gpa_yoy_rate_proportion = 2

                self.buy_list_length_limit = 10

            elif self.simul_num == 2091:
                self.db_to_realtime_daily_buy_list_num = 2091
                self.sell_list_num = 2091
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20070101"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -30
                # 몇 분할 투자할지
                self.divide_invest_unit = 10

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.marketcap_rate = 20

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                # self.profit_qq = 0  # 십만이 일억

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.company_value_ss_proportion = 2
                self.gpa_proportion = 3
                self.gpa_yoy_rate_proportion = 1

            elif self.simul_num == 2224:
                self.db_to_realtime_daily_buy_list_num = 2224
                self.sell_list_num = 2224
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20070101"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -30
                # 몇 분할 투자할지
                self.divide_invest_unit = 10

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.marketcap_rate = 20

                self.trading_money = 10000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                # self.profit_qq = 0  # 십만이 일억

                self.realtime_daily_buy_list_count = 3

                self.revalancing_date = 30

                self.por_proportion = 2
                self.opm_proportion = 1
                self.gpa_proportion = 1
                self.gross_total_yoy_rate_proportion = 1
                self.asset_turnover_ratio_yoy_rate_proportion = 1

            elif self.simul_num == 2248:
                self.db_to_realtime_daily_buy_list_num = 2248
                self.sell_list_num = 2248
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20070101"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -30
                # 몇 분할 투자할지
                self.divide_invest_unit = 10

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3
                self.for_gpa_pbr_order_limit = 100

                self.marketcap_rate = 20

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.profit_qq = 0  # 십만이 일억

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.pbr_proportion = 2
                self.gpa_proportion = 3
                self.gpa_yoy_rate_proportion = 1

            elif self.simul_num == 5001:
                self.db_to_realtime_daily_buy_list_num = 5001
                self.sell_list_num = 5001
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20070101"
                # self.simul_start_date = "20031229"
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -30
                # 몇 분할 투자할지
                self.divide_invest_unit = 10

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3
                self.for_gpa_pbr_order_limit = 100

                self.marketcap_rate = 20

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.profit_qq = 0  # 십만이 일억

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.pbr_proportion = 2
                self.gpa_proportion = 3
                self.gpa_yoy_rate_proportion = 1

            elif self.simul_num == 5002:
                self.db_to_realtime_daily_buy_list_num = 5002
                self.sell_list_num = 5002
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20170101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -30
                # 몇 분할 투자할지
                self.divide_invest_unit = 10
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3


                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.profit_qq = 0  # 십만이 일억

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.pbr_proportion = 2
                self.gpa_proportion = 3
                self.gpa_yoy_rate_proportion = 1

                self.shannon_rate = 0.5
                self.shannon_rate_on = 1  # on/off 조정

            elif self.simul_num == 10001:
                self.db_to_realtime_daily_buy_list_num = 5003
                self.sell_list_num = 5002
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20170101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -30
                # 몇 분할 투자할지
                self.divide_invest_unit = 10
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1
                # 실전/모의 봇 돌릴 때 매수하는 순간 종목의 최신 종가 보다 1% 이상 오른 경우 사지 않도록 하는 설정(변경 가능)
                self.invest_limit_rate = 1.05
                # 실전/모의 봇 돌릴 때 매수하는 순간 종목의 최신 종가 보다 -2% 이하로 떨어진 경우 사지 않도록 하는 설정(변경 가능)
                self.invest_min_limit_rate = 0.95



            elif self.simul_num == 5003:
                self.db_to_realtime_daily_buy_list_num = 5003
                self.sell_list_num = 5002
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20170101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -30
                # 몇 분할 투자할지
                self.divide_invest_unit = 5
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

            elif self.simul_num == 5004:
                self.db_to_realtime_daily_buy_list_num = 5004
                self.sell_list_num = 5004
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -20
                # 몇 분할 투자할지
                self.divide_invest_unit = 2
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =4

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = True
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=1

            elif self.simul_num == 5005:
                self.db_to_realtime_daily_buy_list_num = 5004
                self.sell_list_num = 5004
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -20
                # 몇 분할 투자할지
                self.divide_invest_unit = 2
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =3

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = True
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=0

            elif self.simul_num == 5006:
                self.db_to_realtime_daily_buy_list_num = 5004
                self.sell_list_num = 5004
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -20
                # 몇 분할 투자할지
                self.divide_invest_unit = 2
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =3

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = True
                self.trading_buy_start_time='0901'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0900'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=0

            elif self.simul_num == 5007:
                self.db_to_realtime_daily_buy_list_num = 5004
                self.sell_list_num = 5004
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -20
                # 몇 분할 투자할지
                self.divide_invest_unit = 2
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =4

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = False
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=1

            elif self.simul_num == 5008:
                self.db_to_realtime_daily_buy_list_num = 5004
                self.sell_list_num = 5004
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -20
                # 몇 분할 투자할지
                self.divide_invest_unit = 2
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =4

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = False
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=0


            elif self.simul_num == 5009:
                self.db_to_realtime_daily_buy_list_num = 5009
                self.sell_list_num = 5004
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 2
                # 손절 수익률 기준치
                self.losscut_point = -20
                # 몇 분할 투자할지
                self.divide_invest_unit = 10
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =4

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = False
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=0

            elif self.simul_num == 5010:
                self.db_to_realtime_daily_buy_list_num = 5009
                self.sell_list_num = 5004
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 30
                # 손절 수익률 기준치
                self.losscut_point = -10
                # 몇 분할 투자할지
                self.divide_invest_unit = 10
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =4

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = False
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=0

            elif self.simul_num == 5011:
                self.db_to_realtime_daily_buy_list_num = 5011
                self.sell_list_num = 5011
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 30
                # 손절 수익률 기준치
                self.losscut_point = -10
                # 몇 분할 투자할지
                self.divide_invest_unit = 10
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =4

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = False
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=0

            elif self.simul_num == 5012:
                self.db_to_realtime_daily_buy_list_num = 5012
                self.sell_list_num = 5011
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 30
                # 손절 수익률 기준치
                self.losscut_point = -10
                # 몇 분할 투자할지
                self.divide_invest_unit = 10
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =4

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = False
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=0

            elif self.simul_num == 5013:
                self.db_to_realtime_daily_buy_list_num = 5013
                self.sell_list_num = 5011
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 30
                # 손절 수익률 기준치
                self.losscut_point = -10
                # 몇 분할 투자할지
                self.divide_invest_unit = 10
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =4

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = False
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=0

                self.subindex_avg_noise_proporsion=3
                self.trade_money_proporsion=1

            elif self.simul_num == 5014:
                self.db_to_realtime_daily_buy_list_num = 5013
                self.sell_list_num = 5011
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 30
                # 손절 수익률 기준치
                self.losscut_point = -10
                # 몇 분할 투자할지
                self.divide_invest_unit = 10
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =4

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = False
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=0

                self.subindex_avg_noise_proporsion=0
                self.trade_money_proporsion=1

            elif self.simul_num == 5015:
                self.db_to_realtime_daily_buy_list_num = 5015
                self.sell_list_num = 5011
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 30
                # 손절 수익률 기준치
                self.losscut_point = -10
                # 몇 분할 투자할지
                self.divide_invest_unit = 5
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =4

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = False
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=0

                self.subindex_avg_noise_proporsion=0
                self.trade_money_proporsion=1

            elif self.simul_num == 5016:
                self.db_to_realtime_daily_buy_list_num = 5015
                self.sell_list_num = 5011
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 30
                # 손절 수익률 기준치
                self.losscut_point = -10
                # 몇 분할 투자할지
                self.divide_invest_unit = 5
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =4

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = False
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=0

                self.subindex_avg_noise_proporsion=0
                self.trade_money_proporsion=1

            elif self.simul_num == 5017:
                self.db_to_realtime_daily_buy_list_num = 5017
                self.sell_list_num = 5011
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20190101"
                self.coin_market='KRW'
                self.start_invest_price = 10000000
                # 매수 금액
                # 자산 중 최소로 남겨 둘 금액
                self.limit_money = 0
                # 익절 수익률 기준치
                self.sell_point = 30
                # 손절 수익률 기준치
                self.losscut_point = -10
                # 몇 분할 투자할지
                self.divide_invest_unit = 5
                self.divide_invest_rate = 0.1

                self.prespan1_rate = 0.978

                self.bband_1month_period = 3

                self.trading_money = 5000 * 10000

                self.overlap_data_delete_on = True  # False면 중첩 데이터 삭제 안하겠다는 것임
                self.r_d_b_overlap_save_on_off = True  # True가 따로 테이블 생성하는 것임

                self.r_d_b_limit_control = True
                self.r_d_b_unit_limit = self.divide_invest_unit * 2

                self.realtime_daily_buy_list_count = 5

                self.revalancing_date = 30

                self.shannon_rate = 1
                self.shannon_rate_on = 1  # on/off 조정

                self.noise_proportion=0
                self.avg_noise_proportion=1
                self.trading_money_proportion=1

                self.use_min =True
                self.only_nine_buy =False
                self.trade_check_num =4

                self.rarry_k=0.5

                self.trading_time_control_for_min_simul = False
                self.trading_buy_start_time='0905'
                self.trading_buy_end_time = '1200'
                self.trading_sell_start_time = '0900'
                self.trading_sell_end_time = '0904'

                self.get_min_table_use = True

                self.rarry_setting_invest_unit_on=0

                self.subindex_avg_noise_proporsion=0
                self.trade_money_proporsion=1


















        elif self.simul_num == 2:
            # 시뮬레이팅 시작 일자
            self.simul_start_date = "20190101"

            ######### 알고리즘 선택 #############
            # 매수 리스트 설정 알고리즘 번호
            self.db_to_realtime_daily_buy_list_num = 1
            # 매도 리스트 설정 알고리즘 번호
            self.sell_list_num = 2
            ###################################
            # 초기 투자자금
            # 주의! start_invest_price 는 모의투자 초기 자본금과 별개. 시뮬레이션에서만 적용.
            # 키움증권 모의투자의 경우 초기에 모의투자 신청 할 때 설정 한 금액으로 자본금이 설정됨
            self.start_invest_price = 10000000
            # 매수 금액
            self.invest_unit = 1000000

            # 자산 중 최소로 남겨 둘 금액
            self.limit_money = 1000000
            # # 익절 수익률 기준치
            self.sell_point = False
            # 손절 수익률 기준치
            self.losscut_point = -2
            # 실전/모의 봇 돌릴 때 매수하는 순간 종목의 최신 종가 보다 1% 이상 오른 경우 사지 않도록 하는 설정(변경 가능)
            self.invest_limit_rate = 1.01
            # 실전/모의 봇 돌릴 때 매수하는 순간 종목의 최신 종가 보다 -2% 이하로 떨어진 경우 사지 않도록 하는 설정(변경 가능)
            self.invest_min_limit_rate = 0.98


        elif self.simul_num == 3:

            # 시뮬레이팅 시작 일자

            self.simul_start_date = "20190101"

            ######### 알고리즘 선택 #############

            # 매수 리스트 설정 알고리즘 번호

            self.db_to_realtime_daily_buy_list_num = 3

            # 매도 리스트 설정 알고리즘 번호

            self.sell_list_num = 2

            ###################################

            # 초기 투자자금
            # 주의! start_invest_price 는 모의투자 초기 자본금과 별개. 시뮬레이션에서만 적용.
            # 키움증권 모의투자의 경우 초기에 모의투자 신청 할 때 설정 한 금액으로 자본금이 설정됨
            self.start_invest_price = 10000000

            # 매수 금액
            self.invest_unit = 3000000

            # 자산 중 최소로 남겨 둘 금액
            self.limit_money = 1000000

            # 익절 수익률 기준치
            self.sell_point = 10

            # 손절 수익률 기준치
            self.losscut_point = -2

            # 실전/모의 봇 돌릴 때 매수하는 순간 종목의 최신 종가 보다 1% 이상 오른 경우 사지 않도록 하는 설정(변경 가능)
            self.invest_limit_rate = 1.01
            # 실전/모의 봇 돌릴 때 매수하는 순간 종목의 최신 종가 보다 -2% 이하로 떨어진 경우 사지 않도록 하는 설정(변경 가능)
            self.invest_min_limit_rate = 0.98
        else:
            logger.error(
                f"입력 하신 {self.simul_num}번 알고리즘에 대한 설정이 없습니다. simulator_func_mysql.py 파일의 variable_setting함수에 알고리즘을 설정해주세요. ")
            sys.exit(1)

        #########################################################################################################################
        self.db_name_setting()

        if self.op != 'real':
            # database, table 초기화 함수
            self.table_setting()

            # 시뮬레이팅 할 날짜를 가져 오는 함수
            self.get_date_for_simul()

            # 매도를 한 종목들 대상 수익
            self.total_valuation_profit = 0

            # 실제 수익 : 매도를 한 종목들 대상 수익 + 현재 보유 중인 종목들의 수익
            self.sum_valuation_profit = 0

            # 전재산 : 투자금액 + 실제 수익(self.sum_valuation_profit)
            self.total_invest_price = self.start_invest_price

            # 현재 총 투자한 금액
            self.total_purchase_price = 0

            # 현재 투자 가능한 금액(예수금) = (초기자본 + 매도한 종목의 수익) - 현재 총 투자 금액
            self.d2_deposit = self.start_invest_price

            # 일별 정산 함수
            self.check_balance()

            # 매수할때 수수료 한번, 매도할때 전체금액에 세금, 수수료
            self.tax_rate = 0
            self.fees_rate = 0.0005



            # 시뮬레이터를 멈춘 지점 부터 다시 돌리기 위해 사용하는 변수(중요X)
            self.simul_reset_lock = False

    # 데이터베이스와 테이블을 세팅하기 위한 함수
    def table_setting(self):
        print("self.simul_reset" + str(self.simul_reset))
        # 시뮬레이터를 초기화 하고 처음부터 구축하기 위한 로직
        if self.simul_reset:
            print("table reset setting !!! ")
            self.init_database()
        # 시뮬레이터를 초기화 하지 않고 마지막으로 끝난 시점 부터 구동하기 위한 로직
        else:
            # self.simul_reset 이 False이고, 시뮬레이터 데이터베이스와, all_item_db 테이블, jango_table이 존재하는 경우 이어서 시뮬레이터 시작
            if self.is_simul_database_exist() and self.is_simul_table_exist(self.db_name,
                                                                            "all_item_db") and self.is_simul_table_exist(
                self.db_name, "jango_data"):
                self.init_df_jango()
                self.init_df_all_item()
                # 마지막으로 구동했던 시뮬레이터의 날짜를 가져온다.
                self.last_simul_date = self.get_jango_data_last_date()
                print("self.last_simul_date: " + str(self.last_simul_date))
            #    초반에 reset 으로 돌다가 멈춰버린 경우 다시 init 해줘야함
            else:
                print("초반에 reset 으로 돌다가 멈춰버린 경우 다시 init 해줘야함 ! ")
                self.init_database()
                self.simul_reset = True

    # 데이터베이스 초기화 함수
    def init_database(self):
        self.drop_database()
        self.create_database()
        self.init_df_jango()
        self.init_df_all_item()

    # 데이터베이스를 생성하는 함수
    def create_database(self):
        if self.is_simul_database_exist() == False:
            sql = 'CREATE DATABASE %s'
            self.db_conn.cursor().execute(sql % (self.db_name))
            self.db_conn.commit()

    # 데이터베이스를 삭제하는 함수
    def drop_database(self):
        if self.is_simul_database_exist():
            print("drop!!!!")
            sql = "drop DATABASE %s"
            self.db_conn.cursor().execute(sql % (self.db_name))
            self.db_conn.commit()

    # 데이터베이스의 존재 유무를 파악하는 함수.
    def is_simul_database_exist(self):
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = '%s'"
        rows = self.engine_daily_buy_list.execute(sql % (self.db_name)).fetchall()
        print("rows : ", rows)
        if len(rows):
            return True
        else:
            return False

    # 오늘 날짜를 설정하는 함수
    def date_setting(self):
        self.today = datetime.today().strftime("%Y%m%d")
        self.today_detail = datetime.today().strftime("%Y%m%d%H%M")
        self.today_date_form = datetime.strptime(self.today, "%Y%m%d").date()

    # DB 이름 세팅 함수
    def db_name_setting(self):
        if self.op == "real":
            self.engine_simulator = create_engine(
                "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/" + str(
                    self.db_name),
                encoding='utf-8')

        else:
            # db_name을 setting 한다.
            self.db_name = "Coin_simulator" + str(self.simul_num)
            self.engine_simulator = create_engine(
                "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/" + str(
                    self.db_name), encoding='utf-8')

        self.engine_daily_craw = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_daily_craw",
            encoding='utf-8')

        self.engine_craw = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_min_craw",
            encoding='utf-8')
        self.engine_daily_buy_list = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_daily_list",
            encoding='utf-8')



        # 특정 데이터 베이스가 아닌, mysql 에 접속하는 객체
        self.db_conn = pymysql.connect(host=cf.db_ip, port=int(cf.db_port), user=cf.db_id, password=cf.db_passwd,
                                       charset='utf8')

    # 매수 함수
    def invest_send_order(self, date, code, code_name, price, yes_close, j):
        # print("invest_send_order!!!")
        # 시작가가 투자하려는 금액 보다 작아야 매수가 가능하기 때문에 아래 조건
        # !!!!!!!@ 원래 주식에서는 아래와 같이 하였으나, 비트코인은 소수점단위로 매수하기 때문에 삭제함.
        # if price < self.invest_unit:
        print(code_name, " 매수!!!!!!!!!!!!!!!")

        # 매수를 하게 되면 all_item_db 테이블에 반영을 한다.
        self.db_to_all_item(date, self.df_realtime_daily_buy_list, j,
                            code,
                            code_name, price,
                            yes_close)

        # 매수를 성공적으로 했으면 realtime_daily_buy_list 테이블의 check_item 에 매수 시간을 설정
        self.update_realtime_daily_buy_list(code, date)

        # 일별, 분별 정산 함수
        self.check_balance()

    # code명으로 code_name을 가져오는 함수
    def get_name_by_code(self, code):

        sql = "select code_name from stock_item_all where code = '%s'"
        code_name = self.engine_daily_buy_list.execute(sql % (code)).fetchall()
        print(code_name)
        if code_name:
            return code_name[0][0]
        else:
            return False

    # 실제 매수하는 함수
    def auto_trade_stock_realtime(self, min_date, date_rows_today, date_rows_yesterday):
        print("auto_trade_stock_realtime 함수에 들어왔다!!")
        # self.df_realtime_daily_buy_list 에 있는 모든 종목들을 매수한다
        for j in range(self.len_df_realtime_daily_buy_list):

            if self.jango_check():

                # 종목 코드를 가져온다.
                code = str(self.df_realtime_daily_buy_list.loc[j, 'code']).rjust(6, "0")

                # 종목명을 가져온다.
                code_name = self.df_realtime_daily_buy_list.loc[j, 'code_name']

                # (촬영 후 추가 코드) 매수 들어가기전에 db에 테이블이 존재하는지 확인
                # 분별 시뮬레이팅 인 경우
                if self.use_min:
                    # print("code_name!!", code_name)
                    # min_craw db에 종목이 없으면 매수 하지 않는다.
                    if not self.is_min_craw_table_exist(code_name):
                        continue
                # 일별 시뮬레이팅 인 경우
                else:
                    # daily_craw db에 종목이 없으면 매수 하지 않는다.
                    if not self.is_daily_craw_table_exist(code_name):
                        continue

                # 아래 if else 구문은 영상 촬영 후 수정 하였습니다. open_price 를 가져오는 것을 분별/일별 시뮬레이션 구분하여 설정하였습니다.
                # 분별 시뮬레이션이 아닌 일별 시뮬레이션의 경우
                if not self.use_min:
                    # 매수 당일 시작가를 가져온다.
                    price = self.get_now_open_price_by_date(code, date_rows_today)
                # 분별 시뮬레이션의 경우
                else:
                    # 매수 시점의 가격을 가져온다.
                    if self.get_min_table_use:
                        #price = self.get_now_close_price_by_get_min_table(code_name, min_date)
                        price = self.get_now_close_price_by_get_min_table(code_name, min_date)
                    else:
                        price = self.get_now_close_price_by_min(code_name, min_date)

                # 어제 종가를 가져온다.
                yes_close = self.get_yes_close_price_by_date(code, date_rows_yesterday)

                # False는 데이터가 없는것
                if code_name == False or price == 0 or price == False:
                    continue
                test=0
                # 촬영 후 아래 if 문 추가 (향후 실시간 조건 매수 시 사용) ###################
                if self.use_min and not self.only_nine_buy and self.trade_check_num:
                    # 시작가
                    # 예를들어 20190103이라고 했을때 20190103~20190104까지이다.
                    # 그러므로 오늘 것이 아닌 하루전꺼를 가져와야 한다.
                    #open = self.get_now_open_price_by_date(code, date_rows_today)
                    open = self.get_now_open_price_by_date(code, date_rows_today)
                    self.date_rows_today_for_trade_check=date_rows_yesterday
                    # 당일 누적 거래량
                    if self.get_min_table_use:
                        sum_volume = self.get_now_volume_by_get_min_table(code_name, min_date)
                    else:
                        sum_volume = self.get_now_volume_by_min(code_name, min_date)

                    # open, sum_volume 값이 존재 할 경우
                    if open and sum_volume:
                        # 매수 할 종목에 대한 dataframe row와, 시작가, 현재가, 분별 누적 거래량 정보를 전달
                        if not self.trade_check(self.df_realtime_daily_buy_list.loc[j], open, price, sum_volume):
                            # 실시간 매수 조건에 맞지 않는 경우 pass
                            continue
                ################################################################

                # 매수 주문에 들어간다.
                self.invest_send_order(min_date, code, code_name, price, yes_close, j)
            else:
                break;

    # 최근 daily_buy_list의 날짜 테이블에서 code에 해당 하는 row만 가져오는 함수
    def get_daily_buy_list_by_code(self, code, date):
        # print("get_daily_buy_list_by_code 함수에 들어왔습니다!")

        sql = "select * from `" + date + "` where code = '%s' group by code"

        daily_buy_list = self.engine_daily_buy_list.execute(sql % (code)).fetchall()

        df_daily_buy_list = DataFrame(daily_buy_list,
                                      columns=['index', 'index2', 'date', 'check_item',
                                               'code', 'code_name', 'd1_diff_rate', 'close', 'open',
                                               'high', 'low',
                                               'volume','clo3',
                                               'clo5', 'clo10', 'clo20', 'clo40', 'clo60',
                                               'clo100', 'clo120',"clo3_diff_rate",
                                               "clo5_diff_rate", "clo10_diff_rate", "clo20_diff_rate",
                                               "clo40_diff_rate", "clo60_diff_rate",
                                                "clo100_diff_rate",
                                               "clo120_diff_rate",
                                               'yes_clo3',
                                               'yes_clo5', 'yes_clo10', 'yes_clo20', 'yes_clo40',
                                               'yes_clo60',

                                               'yes_clo100', 'yes_clo120','vol3',
                                               'vol5', 'vol10', 'vol20', 'vol40', 'vol60',
                                               'vol100', 'vol120'])
        return df_daily_buy_list

    # realtime_daily_buy_list 테이블의 매수 리스트를 가져오는 함수
    def get_realtime_daily_buy_list(self):
        print("get_realtime_daily_buy_list 함수에 들어왔습니다!")
        #
        # 여기서부터 다시해야함.

        # list를 바꿔치기 한다면?
        # day에서 해당코드 가져온다음에 update를 해버리자.

        #
        # if self.condition_trade :
        #
        #     self.realtime_daily_buy_list_condition = []
        #     realtime_daily_buy_list = []
        #     self.open_api.get_condition()
        #
        #     for code in self.realtime_daily_buy_list_condition:
        #         sql = f'''
        #                                        SELECT DAY.*
        #                                        FROM `{date_rows_yesterday}` DAY
        #                                        where DAY.code = '{code}'
        #                                    '''
        #
        #         realtime_daily_buy_list_temp = self.engine_daily_buy_list.execute(sql).fetchall()
        #         realtime_daily_buy_list.append(realtime_daily_buy_list_temp)

        # 이 부분은 촬영 후 코드를 간소화 했습니다. 조건문 모두 없앴습니다.
        # check_item = 매수 했을 시 날짜가 찍혀 있다. 매수 하지 않았을 때는 0
        sql = "select * from realtime_daily_buy_list where check_item = '%s' group by code"

        realtime_daily_buy_list = self.engine_simulator.execute(sql % (0)).fetchall()

        self.df_realtime_daily_buy_list = DataFrame(realtime_daily_buy_list,
                                                    columns=['index', 'index2', 'index3', 'date', 'check_item',
                                                             'code', 'code_name', 'd1_diff_rate', 'close', 'open',
                                                             'high', 'low',
                                                             'volume','clo3',
                                                             'clo5', 'clo10', 'clo20', 'clo40', 'clo60',
                                                             'clo100', 'clo120',"clo3_diff_rate",
                                                             "clo5_diff_rate", "clo10_diff_rate", "clo20_diff_rate",
                                                             "clo40_diff_rate", "clo60_diff_rate",
                                                              "clo100_diff_rate",
                                                             "clo120_diff_rate",
                                                             'yes_clo3',
                                                             'yes_clo5', 'yes_clo10', 'yes_clo20', 'yes_clo40',
                                                             'yes_clo60',

                                                             'yes_clo100', 'yes_clo120',
                                                             'vol3','vol5', 'vol10', 'vol20', 'vol40', 'vol60',
                                                             'vol100', 'vol120'])

        self.len_df_realtime_daily_buy_list = len(self.df_realtime_daily_buy_list)
        self.rdb_code_list_for_min_trading = self.df_realtime_daily_buy_list.code

    # 가장 최근의 daily_buy_list에 담겨 있는 날짜 테이블 이름을 가져오는 함수
    def get_recent_daily_buy_list_date(self):
        sql = "select TABLE_NAME from information_schema.tables where table_schema = 'coin_daily_list' and TABLE_NAME like '%s' order by table_name desc limit 1"
        row = self.engine_daily_buy_list.execute(sql % ("20%%")).fetchall()

        if len(row) == 0:
            return False
        return row[0][0]

    # 실시간 주가 분석 알고리즘 함수 (느낌표 골뱅이 추가하면 검색 시 편합니다) (고급클래스에서 소개)
    def trade_check(self, df_row, open_price, current_price, current_sum_volume):
        '''
        :param df_row: 매수 종목 리스트(realtime_daily_buy_list)
        :param current_price: (현재가)
        :param current_sum_volume: (현재 누적 거래량)
        :return: True (매수), False(매수 X)
        '''
        code_name = df_row['code_name']
        yes_vol20 = df_row['vol20']
        yes_close = df_row['close']
        yes_high = df_row['high']
        yes_low = df_row['low']
        yes_volume = df_row['volume']

        # 실시간 거래 대금 체크 알고리즘
        if self.trade_check_num == 1:
            # 어제 거래 대금
            yes_total_tr_price = yes_close * yes_volume
            # 현재 거래 대금
            current_total_tr_price = current_price * current_sum_volume
            # 어제 종가 보다 현재가가 증가했고, 거래 대금이 어제 거래대금에 비해서 x배 올라갔을 때 매수
            if current_price > yes_close and current_total_tr_price > yes_total_tr_price * self.volume_up:
                return True
            else:
                return False

        elif self.trade_check_num == 2:
            # 매수 가격 최저 범위
            min_buy_limit = int(yes_close) * self.invest_min_limit_rate
            # 매수 가격 최고 범위
            max_buy_limit = int(yes_close) * self.invest_limit_rate
            # 현재가가 매수 가격 최저 범위와 매수 가격 최고 범위 안에 들어와 있다면 매수 한다.
            if min_buy_limit < current_price < max_buy_limit:
                return True
            else:
                return False

        # 래리 윌리엄스 변동성 돌파 알고리즘(매수)
        elif self.trade_check_num == 3:
            # 변동폭(_range): 전일 고가(yes_high)에서 전일 저가(yes_low)를 뺀 가격
            # 매수시점 : 현재가 > 시작가 + (변동폭 * k)  [k는 0~1 사이 수]
            _range = yes_high - yes_low
            if open_price + _range * self.rarry_k < current_price:
                return True
            else:
                return False

        elif self.trade_check_num == 4:
            # 변동폭(_range): 전일 고가(yes_high)에서 전일 저가(yes_low)를 뺀 가격
            # 매수시점 : 현재가 > 시작가 + (변동폭 * k)  [k는 0~1 사이 수]
            avg_noise_sql=f'''
                        select avg_noise from coin_daily_subindex.`{self.date_rows_today_for_trade_check}`
                        where code='{code_name}'
                '''
            avg_noise = self.engine_daily_buy_list.execute(avg_noise_sql).fetchall()[0][0]

            _range = yes_high - yes_low
            if open_price + _range * avg_noise < current_price:
                test=0
                return True
            else:
                return False

        elif self.trade_check_num == 301:
            return True

        else:
            logger.debug("trade_check 함수에 self.trade_check_num = {} 에 맞는 알고리즘이 없습니다. ".format(self.trade_check_num))
            exit(1)

    # 여기서 sql문의 date는 반드시 어제 일자여야 한다. -> 어제 일자 기준 반영된 데이터로 종목을 선정해야함.
    ##!@####################################################################################################################################################################################
    # 매수 할 종목의 리스트를 선정 알고리즘
    def db_to_realtime_daily_buy_list(self, date_rows_today, date_rows_yesterday, i):
        # 5 / 20 골든크로스 buy
        if self.db_to_realtime_daily_buy_list_num == 1:
            # orderby는 거래량 많은 순서
            date_rows_yesterday = '20210917'
            sql = "select * from `" + date_rows_yesterday + "` a where yes_clo20 > yes_clo5 and clo5 > clo20 " \
                                                            "and NOT exists (select null from stock_konex b where a.code=b.code) " \
                                                            "and close < '%s' group by code"
            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql % (self.invest_unit)).fetchall()


        # 5 / 40 골든크로스 buy
        elif self.db_to_realtime_daily_buy_list_num == 2:
            # orderby는 거래량 많은 순서
            sql = "select * from `" + date_rows_yesterday + "` a where yes_clo40 > yes_clo5 and clo5 > clo40 " \
                                                            "and NOT exists (select null from stock_konex b where a.code=b.code) " \
                                                            "and close < '%s' group by code"
            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql % (self.invest_unit)).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 3:
            sql = "select * from `" + date_rows_yesterday + "` a where d1_diff_rate > 1 " \
                                                            "and NOT exists (select null from stock_konex b where a.code=b.code) " \
                                                            "and close < '%s' group by code"
            # 아래 명령을 통해 테이블로 부터 데이터를 가져오면 리스트 형태로 realtime_daily_buy_list 에 담긴다.
            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql % (self.invest_unit)).fetchall()



        elif self.db_to_realtime_daily_buy_list_num == 4:
            sql = "select * from `" + date_rows_yesterday + "` a " \
                                                            "where yes_clo20 > yes_clo5 and clo5 > clo20 " \
                                                            "and NOT exists (select null from stock_konex b where a.code=b.code)" \
                                                            "and NOT exists (select null from stock_managing c where a.code=c.code and c.code_name != '' group by c.code) " \
                                                            "and NOT exists (select null from stock_insincerity d where a.code=d.code and d.code_name !='' group by d.code) " \
                                                            "and NOT exists (select null from stock_invest_caution e where a.code=e.code and DATE_SUB('%s', INTERVAL '%s' MONTH ) < e.post_date and e.post_date < Date('%s') and e.type != '투자경고 지정해제' group by e.code)" \
                                                            "and NOT exists (select null from stock_invest_warning f where a.code=f.code and f.post_date <= DATE('%s') and (f.cleared_date > DATE('%s') or f.cleared_date is null) group by f.code)" \
                                                            "and NOT exists (select null from stock_invest_danger g where a.code=g.code and g.post_date <= DATE('%s') and (g.cleared_date > DATE('%s') or g.cleared_date is null) group by g.code)" \
                                                            "and a.close < '%s'"

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql % (
                date_rows_yesterday, self.interval_month, date_rows_yesterday, date_rows_yesterday, date_rows_yesterday,
                date_rows_yesterday, date_rows_yesterday, self.invest_unit)).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 5:
            sql = "select * from `" + date_rows_yesterday + "` a " \
                                                            "where yes_clo20 > yes_clo5 and clo5 > clo20 " \
                                                            "and volume * close > '%s' " \
                                                            "and vol20 * '%s' < volume " \
                                                            "and d1_diff_rate > '%s' " \
                                                            "and NOT exists (select null from stock_konex b where a.code=b.code)" \
                                                            "and NOT exists (select null from stock_managing c where a.code=c.code and c.code_name != '' group by c.code) " \
                                                            "and NOT exists (select null from stock_insincerity d where a.code=d.code and d.code_name !='' group by d.code) " \
                                                            "and NOT exists (select null from stock_invest_caution e where a.code=e.code and DATE_SUB('%s', INTERVAL '%s' MONTH ) < e.post_date and e.post_date < Date('%s') and e.type != '투자경고 지정해제' group by e.code)" \
                                                            "and NOT exists (select null from stock_invest_warning f where a.code=f.code and f.post_date <= DATE('%s') and (f.cleared_date > DATE('%s') or f.cleared_date is null) group by f.code)" \
                                                            "and NOT exists (select null from stock_invest_danger g where a.code=g.code and g.post_date <= DATE('%s') and (g.cleared_date > DATE('%s') or g.cleared_date is null) group by g.code)" \
                                                            "and a.close < '%s'" \
                                                            "order by volume * close desc"
            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql % (
                self.total_transaction_price, self.vol_mul, self.d1_diff, date_rows_yesterday, self.interval_month,
                date_rows_yesterday, date_rows_yesterday, date_rows_yesterday, date_rows_yesterday, date_rows_yesterday,
                self.invest_unit)).fetchall()

        # 절대 모멘텀 전략 : 특정일 전의 종가 보다 n% 이상 상승한 종목 매수 (code version)
        elif self.db_to_realtime_daily_buy_list_num == 7:
            # 아래에서 필터링 된 매수종목을 append 해주기 위해 비어있는 리스트를 만들어준다.
            realtime_daily_buy_list = []
            if i < self.day_before + 1:
                pass
            else:
                sql = "SELECT * FROM `" + date_rows_yesterday + "` a " \
                                                                "WHERE NOT exists (SELECT null FROM stock_konex b WHERE a.code=b.code) " \
                                                                "AND close < '%s' "

                # realtime_daily_buy_list_temp 로 일단 위 조건의 종목을을받는다.
                realtime_daily_buy_list_temp = self.engine_daily_buy_list.execute(sql % (self.invest_unit)).fetchall()
                for row in realtime_daily_buy_list_temp:
                    # 종목코드
                    code = row[4]
                    # 어제 종가
                    yes_close = row[7]
                    # date_rows_yesterday 가 self.date_rows[i-1] 값이다.
                    # 어제 일자 기준 n 일전 날짜
                    date_before = self.date_rows[i - 1 - self.day_before][0]
                    # 어제 일자 기준 n 일전 종가
                    date_before_close = self.get_now_close_price_by_date(code, date_before)
                    if date_before_close != 0 and date_before_close != False:
                        # 모멘텀 계산 : n일전 종가 대비 수익률
                        diff_point_calc = (yes_close - date_before_close) / date_before_close * 100
                        # 모멘텀(수익률)이 self.diff_point 보다 높을 경우 realtime_daily_buy_list에 append
                        if diff_point_calc > self.diff_point:
                            realtime_daily_buy_list.append(row)

        # 절대 모멘텀 전략 : 특정일 전의 종가 보다 n% 이상 상승한 종목 매수 (qeury vesrion)
        elif self.db_to_realtime_daily_buy_list_num == 8:
            if i < self.day_before + 1:
                realtime_daily_buy_list = []
                pass
            else:
                date_before = self.date_rows[i - 1 - self.day_before][0]
                sql = "SELECT YES_DAY.* " \
                      "FROM `" + date_before + "` BEFORE_DAY, `" + date_rows_yesterday + "` YES_DAY " \
                                                                                         "WHERE BEFORE_DAY.code = YES_DAY.code " \
                                                                                         "AND (YES_DAY.close - BEFORE_DAY.close) / BEFORE_DAY.close * 100 > '%s' " \
                                                                                         "AND NOT exists (SELECT null FROM stock_konex b WHERE YES_DAY.code=b.code)" \
                                                                                         "AND YES_DAY.close < '%s'"

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(
                    sql % (self.diff_point, self.invest_unit)).fetchall()

        # 상대 모멘텀 전략 : 특정일 전의 종가 보다 n% 이상 상승한 종목 중 가장 많이 상승한 종목 순으로 매수 (내림차순) (query version)
        elif self.db_to_realtime_daily_buy_list_num == 9:
            if i < self.day_before + 1:
                realtime_daily_buy_list = []
                pass
            else:
                date_before = self.date_rows[i - 1 - self.day_before][0]
                sql = "SELECT YES_DAY.* " \
                      "FROM `" + date_before + "` BEFORE_DAY, `" + date_rows_yesterday + "` YES_DAY " \
                                                                                         "WHERE BEFORE_DAY.code = YES_DAY.code " \
                                                                                         "AND (YES_DAY.close - BEFORE_DAY.close) / BEFORE_DAY.close * 100 > '%s' " \
                                                                                         "AND NOT exists (SELECT null FROM stock_konex b WHERE YES_DAY.code=b.code)" \
                                                                                         "AND YES_DAY.close < '%s'" \
                                                                                         "ORDER BY (YES_DAY.close - BEFORE_DAY.close) / BEFORE_DAY.close * 100 DESC"

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(
                    sql % (self.diff_point, self.invest_unit)).fetchall()

        # ETF
        elif self.db_to_realtime_daily_buy_list_num == 11:
            sql = f"SELECT * from `{date_rows_yesterday}` YES_DAY " \
                  "WHERE yes_clo20 > yes_clo5 and clo5 > clo20 " \
                  "AND EXISTS (SELECT null FROM stock_etf ETF WHERE YES_DAY.code=ETF.code) " \
                  f"AND close < {self.invest_unit} " \
                  "GROUP BY code"

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 12:
            sql = f'''
                SELECT day.* FROM `{date_rows_yesterday}` day, stock_info info
                WHERE day.code = info.code
                AND info.stock_market IN ("거래소", "코스닥")
                AND info.category0 IN ("우량기업", "신성장기업")
                AND info.audit = '정상'
                AND info.margin <= 40
                AND info.remarks NOT LIKE "%관리종목%"
                AND info.remarks NOT LIKE "%거래정지%"
            '''
            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 13:
            sql = f'''
                SELECT DAY.*
                FROM `{date_rows_yesterday}` DAY, stock_finance FIN, stock_info INF
                WHERE DAY.code = FIN.code
                AND DAY.code = INF.code
                AND FIN.date = '{date_rows_yesterday}'
                AND FIN.PER != '' AND FIN.PBR != ''
                AND DAY.close < {self.invest_unit}
                AND INF.stock_market in ('거래소', '코스닥')
                AND INF.category0 IN ("우량기업", "신성장기업")
                AND INF.audit = '정상'
                AND INF.margin <= 40
                AND INF.remarks NOT LIKE "%관리종목%"
                AND INF.remarks NOT LIKE "%거래정지%"
                ORDER BY FIN.PER+FIN.PBR
                LIMIT 100;
            '''
            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 201:
            twodaysago = self.date_rows[-3][0]
            sql = f'''
                SELECT DAY.*
                FROM `{date_rows_yesterday}` DAY, stock_finance FIN, stock_info INF,
                (select code,cci from subindex where date='{date_rows_yesterday}' and cci>-100) TODAYCCI,
                (select code,cci from subindex where date='{twodaysago}' and cci<-100) YESCCI
                WHERE DAY.code = FIN.code
                AND DAY.code = INF.code
                AND DAY.code = TODAYCCI.code
                AND DAY.code = YESCCI.code
                AND FIN.date = '20210108'
                AND FIN.PER != '' AND FIN.PBR != ''
                AND DAY.close < {self.invest_unit}
                AND INF.stock_market in ('거래소', '코스닥')
                AND INF.category1 in ('대형주','중형주')
                AND INF.audit = '정상'
                AND INF.remarks NOT LIKE "%관리종목%"
                AND INF.remarks NOT LIKE "%거래정지%"
                ORDER BY FIN.PER+FIN.PBR
                LIMIT 30;
            '''

            # AND INF.category1 = '대형주'
            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 202:
            setting_day = 1
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                twodaysago = self.date_rows[i - 1 - setting_day][0]
                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, stock_finance FIN, stock_info INF,
                    (select code,cci from subindex where date='{date_rows_yesterday}' and cci>-100) TODAYCCI,
                    (select code,cci from subindex where date='{twodaysago}' and cci<-100) YESCCI
                    WHERE DAY.code = FIN.code
                    AND DAY.code = INF.code
                    AND DAY.code = TODAYCCI.code
                    AND DAY.code = YESCCI.code
                    AND FIN.date = '20210108'
                    AND FIN.PER != '' AND FIN.PBR != ''
                    AND DAY.close < {self.invest_unit}
                    AND INF.stock_market in ('거래소', '코스닥')
                    AND INF.category1 in ('대형주')
                    AND INF.audit = '정상'
                    AND INF.remarks NOT LIKE "%관리종목%"
                    AND INF.remarks NOT LIKE "%거래정지%"
                    ORDER BY FIN.PER+FIN.PBR
                    LIMIT 3;
                '''

                # AND INF.category1 = '대형주'
                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 207:
            setting_day = 1
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                twodaysago = self.date_rows[i - 1 - setting_day][0]
                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, stock_finance FIN, stock_info INF,
                    (select code,cci from subindex where date='{date_rows_yesterday}' and cci>-100) TODAYCCI,
                    (select code,cci from subindex where date='{twodaysago}' and cci<-100) YESCCI
                    WHERE DAY.code = FIN.code
                    AND DAY.code = INF.code
                    AND DAY.code = TODAYCCI.code
                    AND DAY.code = YESCCI.code
                    AND FIN.date = '20210108'
                    AND FIN.PER != '' AND FIN.PBR != ''
                    AND DAY.close < {self.invest_unit}
                    AND INF.stock_market in ('거래소', '코스닥')
                    AND INF.category1 in ('대형주','중형주')
                    AND INF.audit = '정상'
                    AND INF.remarks NOT LIKE "%관리종목%"
                    AND INF.remarks NOT LIKE "%거래정지%"
                    ORDER BY FIN.PER+FIN.PBR
                    LIMIT 3;
                '''

                # AND INF.category1 = '대형주'
                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 212:

            setting_day = 1
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                twodaysago = self.date_rows[i - 1 - setting_day][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, stock_finance FIN, stock_info INF,
                    top50,
                    (select code,cci from subindex where date='{date_rows_yesterday}' and cci>-100) TODAYCCI,
                    (select code,cci from subindex where date='{twodaysago}' and cci<-100) YESCCI
                    WHERE DAY.code = FIN.code
                    AND DAY.code = INF.code
                    AND DAY.code = TODAYCCI.code
                    AND DAY.code = YESCCI.code
                    AND DAY.code = top50.code
                    AND FIN.date = '20210108'
                    AND FIN.PER != '' AND FIN.PBR != ''
                    AND DAY.close < {self.invest_unit}                
                    ORDER BY FIN.PER+FIN.PBR
                    LIMIT 3;
                '''

                # AND INF.category1 = '대형주'
                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
                check = 0

        elif self.db_to_realtime_daily_buy_list_num == 214:

            setting_day = 1
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                twodaysago = self.date_rows[i - 1 - setting_day][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, stock_finance FIN, stock_info INF,
                    top50,us_stock,
                    (select code,cci from subindex where date='{date_rows_yesterday}' and cci>-100) TODAYCCI,
                    (select code,cci from subindex where date='{twodaysago}' and cci<-100) YESCCI
                    WHERE DAY.code = FIN.code
                    AND DAY.code = INF.code
                    AND DAY.code = TODAYCCI.code
                    AND DAY.code = YESCCI.code
                    AND DAY.code = top50.code
                    AND FIN.date = '20210108'
                    AND us_stock.date='{date_rows_yesterday}'
                    AND us_stock.DOW_Plus='O'
                    AND FIN.PER != '' AND FIN.PBR != ''
                    AND DAY.close < {self.invest_unit}                
                    ORDER BY FIN.PER+FIN.PBR
                    LIMIT 3;
                '''

                # AND INF.category1 = '대형주'
                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
                check = 0

        elif self.db_to_realtime_daily_buy_list_num == 215:
            setting_day = 1
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                twodaysago = self.date_rows[i - 1 - setting_day][0]
                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, stock_info INF,
                    (select code,cci from subindex where date='{date_rows_yesterday}' and cci>-100) TODAYCCI,
                    (select code,cci from subindex where date='{twodaysago}' and cci<-100) YESCCI                    
                    where DAY.code = INF.code
                    AND DAY.code = TODAYCCI.code
                    AND DAY.code = YESCCI.code
                    AND DAY.close < {self.invest_unit}
                    AND INF.stock_market in ('거래소', '코스닥')
                    AND INF.category1 in ('대형주')
                    AND INF.audit = '정상'
                    AND INF.remarks NOT LIKE "%관리종목%"
                    AND INF.remarks NOT LIKE "%거래정지%"
                    ORDER BY DAY.close*DAY.volume desc
                    LIMIT 3;
                '''

                # AND INF.category1 = '대형주'
                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 218:

            setting_day = 1
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                twodaysago = self.date_rows[i - 1 - setting_day][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, top50,
                    (select code,cci from subindex where date='{date_rows_yesterday}' and cci>-100) TODAYCCI,
                    (select code,cci from subindex where date='{twodaysago}' and cci<-100) YESCCI                    
                    where DAY.code = TODAYCCI.code
                    AND DAY.code = YESCCI.code
                    AND DAY.code = top50.code
                    AND DAY.close < {self.invest_unit} 
                    ORDER BY DAY.close*DAY.volume desc 
                    LIMIT 3;
                '''

                # AND INF.category1 = '대형주'
                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
                check = 0

        elif self.db_to_realtime_daily_buy_list_num == 223:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku                    
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and DAY.volume > ONEDAYBEFORE.volume*10
                    AND INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.volume !=0
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 229:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku                    
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and DAY.volume > ONEDAYBEFORE.volume*10
                    AND INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.volume !=0
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 254:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku                    
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and DAY.volume > ONEDAYBEFORE.volume*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.volume !=0
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 260:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku                    
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and DAY.volume > ONEDAYBEFORE.volume*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.volume !=0
                    order by (DAY.close * DAY.volume) desc
                    limit 2
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 266:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku                    
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and DAY.volume > ONEDAYBEFORE.volume*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.volume !=0
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 276:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku                    
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and ichimoku.backspan>ichimoku.prespan1                    
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and DAY.volume > ONEDAYBEFORE.volume*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.volume !=0
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 285:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku                    
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 294:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku                    
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and DAY.volume > ONEDAYBEFORE.vol20*8
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 295:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku                    
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and DAY.volume > ONEDAYBEFORE.volume*8
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.volume !=0
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 300:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY                    
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 301:

            realtime_daily_buy_list = []

            if len(self.realtime_daily_buy_list_condition) > 0:
                for code in self.realtime_daily_buy_list_condition:
                    sql = f'''
                                SELECT DAY.*
                                FROM `{date_rows_yesterday}` DAY                    
                                where DAY.code = '{code}'
                            '''

                    realtime_daily_buy_list_temp = self.engine_daily_buy_list.execute(sql).fetchall()
                    realtime_daily_buy_list.append(realtime_daily_buy_list_temp)
                # 이 아래는 2차원 list를 1차원으로 바꾸는 방법이다.
                realtime_daily_buy_list = sum(realtime_daily_buy_list, [])

        elif self.db_to_realtime_daily_buy_list_num == 401:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku                    
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<(ichimoku.prespan1+ichimoku.prespan2)/2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and DAY.volume > ONEDAYBEFORE.volume*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.volume !=0
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 408:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku                    
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<(ichimoku.prespan1+ichimoku.prespan2)/2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 501:

            onedaybefore = self.date_rows[i - 2][0]

            sql = f'''
                           SELECT DAY.*
                           FROM `{date_rows_yesterday}` DAY, stock_info INF,
                           (select code,ma19,ma20 from subindex where date ='{date_rows_yesterday}') ma1,
                           (select code,ma19,ma20 from subindex where date ='{onedaybefore}') ma2
                           where DAY.code = ma1.code
                           and DAY.code = ma2.code
                           and DAY.code = INF.code
                           and ma1.ma19 > ma1.ma20
                           and ma2.ma19 < ma2.ma20
                           and INF.stock_market in ('거래소', '코스닥')
                           order by (DAY.close * DAY.volume) desc
                       '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 502:

            if i < 2:
                realtime_daily_buy_list = []
                pass
            else:
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                                           SELECT DAY.*
                                           FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                                           (select code,ma19,ma20 from subindex where date ='{date_rows_yesterday}') ma1,
                                           (select code,ma19,ma20 from subindex where date ='{onedaybefore}') ma2
                                           where DAY.code = ma1.code
                                           and DAY.code = ma2.code
                                           and DAY.code = INF.code
                                           and DAY.code = ONEDAYBEFORE.code
                                           and DAY.clo60 > ONEDAYBEFORE.clo60
                                           and ma1.ma19 > ma1.ma20
                                           and ma2.ma19 < ma2.ma20
                                           and INF.stock_market in ('거래소')
                                           order by (DAY.close * DAY.volume) desc
                                       '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 509:

            if i < 2:
                realtime_daily_buy_list = []
                pass
            else:
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                                           SELECT DAY.*
                                           FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                                           (select code,ma19,ma20 from subindex where date ='{date_rows_yesterday}') ma1,
                                           (select code,ma19,ma20 from subindex where date ='{onedaybefore}') ma2
                                           where DAY.code = ma1.code
                                           and DAY.code = ma2.code
                                           and DAY.code = INF.code
                                           and DAY.code = ONEDAYBEFORE.code
                                           and DAY.clo60 > ONEDAYBEFORE.clo60
                                           and ma1.ma19 > ma1.ma20
                                           and ma2.ma19 < ma2.ma20
                                           and INF.stock_market in ('거래소','코스닥')
                                           order by (DAY.close * DAY.volume) desc
                                       '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 510:

            if i < 2:
                realtime_daily_buy_list = []
                pass
            else:
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                                           SELECT DAY.*
                                           FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, 
                                           (select code,ma19,ma20 from subindex where date ='{date_rows_yesterday}') ma1,
                                           (select code,ma19,ma20 from subindex where date ='{onedaybefore}') ma2,
                                           (select code,date,year,total_value from kospi200 where year=left('{date_rows_yesterday}',4)) kospi200
                                           where DAY.code = ma1.code
                                           and DAY.code = ma2.code
                                           and DAY.code = kospi200.code
                                           and DAY.code = ONEDAYBEFORE.code
                                           and DAY.clo60 > ONEDAYBEFORE.clo60
                                           and ma1.ma19 > ma1.ma20
                                           and ma2.ma19 < ma2.ma20
                                           order by kospi200.total_value desc
                                       '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 551:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku,
                    (select code,ma19,ma20 from subindex where date='{date_rows_yesterday}') ma
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and DAY.code = ma.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and ma.ma19>ma.ma20                    
                    and INF.stock_market in ('거래소', '코스닥')                    
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 552:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku,
                    (select code,ma19,ma20 from subindex where date='{date_rows_yesterday}') ma
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and DAY.code = ma.code
                    and ichimoku.backspan>(ichimoku.prespan1*0.95)
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and ma.ma19>ma.ma20                    
                    and INF.stock_market in ('거래소', '코스닥')                    
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 553:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku,
                    (select code,ma19,ma20 from subindex where date='{date_rows_yesterday}') ma
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and DAY.code = ma.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and ma.ma19>ma.ma20
                    and DAY.volume > ONEDAYBEFORE.volume*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.volume !=0
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 561:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{backspan_day}`) ichimoku                    
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and DAY.volume > ONEDAYBEFORE.volume*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.volume !=0
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 562:

            setting_day = 25
            if i < setting_day + 1:
                realtime_daily_buy_list = []
                pass
            else:
                backspan_day = self.date_rows[i - 1 - setting_day][0]
                onedaybefore = self.date_rows[i - 2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{backspan_day}`) ichimoku,
                    (select code,ma19,ma20 from daily_subindex.`{date_rows_yesterday}`) ma
                    where DAY.code = ichimoku.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and DAY.code = ma.code
                    and ichimoku.backspan>ichimoku.prespan1
                    and ichimoku.backspan<ichimoku.prespan2
                    and ichimoku.backspan>ichimoku.switch_line
                    and ichimoku.backspan>ichimoku.standard_line
                    and ma.ma19>ma.ma20
                    and DAY.volume > ONEDAYBEFORE.volume*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.volume !=0
                    order by (DAY.close * DAY.volume) desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 651:

            confirm_sql = f'''
                                SELECT DAY.*
                                FROM `{date_rows_yesterday}` DAY, etc.`date` etc
                                where exists(select * from etc.`date` where etc.date = {date_rows_yesterday})
                                limit 1
                            '''

            confirm = self.engine_daily_buy_list.execute(confirm_sql).fetchall()

            if len(confirm) > 0:
                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, etc.`{date_rows_yesterday}` etc
                    where DAY.code=etc.code
                    and etc.PBR != 0
                    and etc.PBR > 0.2
                    order by etc.PBR
                    limit 100
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            else:
                realtime_daily_buy_list = []


        elif self.db_to_realtime_daily_buy_list_num == 658:

            confirm_sql = f'''
                                SELECT DAY.*
                                FROM `{date_rows_yesterday}` DAY, etc.`date` etc
                                where exists(select * from etc.`date` where etc.date = {date_rows_yesterday})
                                limit 1
                            '''

            confirm = self.engine_daily_buy_list.execute(confirm_sql).fetchall()

            if len(confirm) > 0:
                sql = f'''
                    select DAY.*
                    from `{date_rows_yesterday}` DAY,etc.`{date_rows_yesterday}` etc_out
                    where DAY.code=etc_out.code 
                    and etc_out.PBR IS NOT NULL
                    and etc_out.PER IS NOT NULL
                    and etc_out.PCR IS NOT NULL
                    and etc_out.PSR IS NOT NULL
                    order by ((select count(*) from etc.`{date_rows_yesterday}` etc_in where etc_in.PBR < etc_out.PBR) +
                           (select count(*) from etc.`{date_rows_yesterday}` etc_in where etc_in.PER < etc_out.PER) +
                           (select count(*) from etc.`{date_rows_yesterday}` etc_in where etc_in.PCR < etc_out.PCR) +
                           (select count(*) from etc.`{date_rows_yesterday}` etc_in where etc_in.PSR < etc_out.PSR))                    
                    limit 100
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            else:
                realtime_daily_buy_list = []

        elif self.db_to_realtime_daily_buy_list_num == 659:

            confirm_sql = f'''
                                SELECT DAY.*
                                FROM `{date_rows_yesterday}` DAY, etc.`date` etc
                                where exists(select * from etc.`date` where etc.date = {date_rows_yesterday})
                                limit 1
                            '''

            confirm = self.engine_daily_buy_list.execute(confirm_sql).fetchall()

            if len(confirm) > 0:
                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, etc.`{date_rows_yesterday}` etc
                    where DAY.code=etc.code
                    and etc.PBR != 0
                    and etc.PBR >= 0.2
                    order by etc.PBR
                    limit 100
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
            else:
                realtime_daily_buy_list = []

        elif self.db_to_realtime_daily_buy_list_num == 660:

            confirm_sql = f'''
                                SELECT DAY.*
                                FROM `{date_rows_yesterday}` DAY, etc.`date` etc
                                where exists(select * from etc.`date` where etc.date = {date_rows_yesterday})
                                limit 1
                            '''

            confirm = self.engine_daily_buy_list.execute(confirm_sql).fetchall()

            if len(confirm) > 0:
                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, etc.`{date_rows_yesterday}` etc, stock_info INF
                    where DAY.code=etc.code
                    and DAY.code = INF.code
                    and etc.PBR != 0
                    and etc.PBR >= 0.2
                    and INF.stock_market in ('거래소', '코스닥')
                    order by etc.PBR
                    limit 100
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
            else:
                realtime_daily_buy_list = []




        elif self.db_to_realtime_daily_buy_list_num == 801:

            confirm_sql = f'''
                                SELECT DAY.*
                                FROM `{date_rows_yesterday}` DAY, etc.`date` etc
                                where exists(select * from etc.`date` where etc.date = {date_rows_yesterday})
                                limit 1
                            '''

            confirm = self.engine_daily_buy_list.execute(confirm_sql).fetchall()

            if len(confirm) > 0:
                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, etc.`{date_rows_yesterday}` etc
                    where DAY.code=etc.code

                    and etc.PBR != 0
                    and etc.PBR >= 0.2
                    order by etc.PBR
                    limit 100
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
                df_temp = pd.DataFrame(realtime_daily_buy_list)
                self.avg_momentum_each_stock_list(df_temp.iloc[:30, 4])

            else:
                realtime_daily_buy_list = []


        elif self.db_to_realtime_daily_buy_list_num == 802:

            confirm_sql = f'''
                                SELECT DAY.*
                                FROM `{date_rows_yesterday}` DAY, etc.`date` etc
                                where exists(select * from etc.`date` where etc.date = {date_rows_yesterday})
                                limit 1
                            '''
            if date_rows_yesterday == '20190628':
                print("now")

            confirm = self.engine_daily_buy_list.execute(confirm_sql).fetchall()

            if len(confirm) > 0:
                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, etc.`{date_rows_yesterday}` etc,
                    daily_subindex.`{date_rows_yesterday}` subindex
                    where DAY.code=etc.code
                    and DAY.code=subindex.code
                    and DAY.close > subindex.prespan1*0.95
                    and etc.PBR != 0
                    and etc.PBR >= 0.2
                    order by etc.PBR
                    limit 100
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
                df_temp = pd.DataFrame(realtime_daily_buy_list)
                self.avg_momentum_each_stock_list(df_temp.iloc[:self.divide_invest_unit, 4])

            else:
                realtime_daily_buy_list = []

        # inbus 도입시스템 적용
        elif self.db_to_realtime_daily_buy_list_num == 821:

            confirm_sql = f'''
                                SELECT DAY.*
                                FROM `{date_rows_yesterday}` DAY, etc.`date` etc
                                where exists(select * from etc.`date` where etc.date = {date_rows_yesterday})
                                limit 1
                            '''

            confirm = self.engine_daily_buy_list.execute(confirm_sql).fetchall()

            if len(confirm) > 0:
                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, etc.`{date_rows_yesterday}` etc,
                    daily_subindex.`{date_rows_yesterday}` subindex
                    where DAY.code=etc.code
                    and DAY.code=subindex.code
                    and DAY.close > subindex.prespan1*0.95
                    and etc.PBR != 0
                    and etc.PBR >= 0.2
                    order by etc.PBR
                    limit 100
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
                df_temp = pd.DataFrame(realtime_daily_buy_list)
                self.avg_momentum_each_stock_list(df_temp.iloc[:self.divide_invest_unit, 4])

                if self.avg_momentum_apply_inbus_on == 1 and self.avg_momentum_apply_inbus_real_on == 1:
                    temp_sql = f'''
                                        SELECT DAY.*
                                        FROM `{date_rows_yesterday}` DAY
                                        where DAY.code='114800'
                                    '''
                    temp_sql_list = self.engine_daily_buy_list.execute(temp_sql).fetchall()
                    if len(temp_sql_list) > 0:
                        realtime_daily_buy_list.insert(0, temp_sql_list[0])
            else:
                realtime_daily_buy_list = []

        elif self.db_to_realtime_daily_buy_list_num == 887:

            confirm_sql = f'''
                                SELECT DAY.*
                                FROM `{date_rows_yesterday}` DAY, etc.`date` etc
                                where exists(select * from etc.`date` where etc.date = {date_rows_yesterday})
                                limit 1
                            '''

            confirm = self.engine_daily_buy_list.execute(confirm_sql).fetchall()

            if len(confirm) > 0:
                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, etc.`{date_rows_yesterday}` etc,
                    daily_subindex.`{date_rows_yesterday}` subindex
                    where DAY.code=etc.code
                    and DAY.code=subindex.code
                    and DAY.close > subindex.prespan1*0.95
                    and etc.PBR != 0
                    and etc.PBR >= 0.2
                    group by day.code
                    order by etc.PBR
                    limit 100
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
                df_temp = pd.DataFrame(realtime_daily_buy_list)
                self.avg_momentum_each_stock_list(df_temp.iloc[:self.divide_invest_unit, 4])

            else:
                realtime_daily_buy_list = []

        elif self.db_to_realtime_daily_buy_list_num == 921:
            check_sql = f'''
                     select * from jackbot921.all_item_db
                     where sell_date=0
                     and datediff('{date_rows_yesterday}',left(buy_date,8))>=29
                     order by left(buy_date,8) desc
                     limit 1
            '''
            check_sql_list = self.engine_daily_buy_list.execute(check_sql).fetchall()

            if len(check_sql_list) > 0:
                sql = f'''
                    SELECT DAY.*
                    FROM daily_buy_list.`{date_rows_yesterday}` DAY, daily_buy_list.stock_finance SF,
                    daily_subindex.`{date_rows_yesterday}` subindex
                    where DAY.code=SF.code
                    and DAY.code=subindex.code
                    and DAY.close > subindex.prespan1*0.98
                    and SF.date='{date_rows_yesterday}'
                    and SF.PBR IS NOT NULL
                    and SF.PBR >= 0.2          
                    group by DAY.code
                    order by SF.PBR
                    limit 100
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
            else:
                realtime_daily_buy_list = []

        elif self.db_to_realtime_daily_buy_list_num == 1509:
            check_sql = f'''
                     select * from jackbot1509.all_item_db
                     where sell_date=0
                     and datediff('{date_rows_yesterday}',left(buy_date,8))>=29
                     order by left(buy_date,8) desc
                     limit 1
            '''
            check_sql_list = self.engine_daily_buy_list.execute(check_sql).fetchall()

            # test용 강제로 아래 if문 들어가게함.
            # check_sql_list=[1]

            if len(check_sql_list) > 0:
                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY,  
                    (select SF.* from daily_buy_list.stock_finance SF
                    where SF.date='{date_rows_yesterday}'
                    and SF.PBR !=0 and SF.PBR >=0.2 
                    and SF.market_cap < {self.market_cap}
                    order by pbr limit {self.for_gpa_pbr_order_limit}) pbr_table,
                    daily_subindex.`{date_rows_yesterday}` subindex,
                    daily_buy_list.stock_info SI
                    where DAY.code=pbr_table.code
                    and DAY.code=subindex.code
                    and DAY.code=SI.code
                    and left(DAY.code,1) != 9
                    and (SI.audit not like '%경고%' or SI.audit not like '%주의%')
                    and DAY.close > subindex.prespan1*{self.prespan1_rate}
                    and (subindex.bband_1month=0 or subindex.bband_1month>={self.bband_1month_period})
                    and subindex.gpa>0
                    group by day.code
                    order by subindex.gpa desc
                    limit 30
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
            else:
                realtime_daily_buy_list = []

        elif self.db_to_realtime_daily_buy_list_num == 1548:
            check_sql = f'''
                     select * from jackbot1548.all_item_db
                     where sell_date=0
                     and datediff('{date_rows_yesterday}',left(buy_date,8))>=29
                     order by left(buy_date,8) desc
                     limit 1
            '''
            check_sql_list = self.engine_daily_buy_list.execute(check_sql).fetchall()

            # test용 강제로 아래 if문 들어가게함.
            # check_sql_list=[1]

            if len(check_sql_list) > 0:
                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY,  
                    (select pbr_table.* from
                    (select SF.* from daily_buy_list.stock_finance SF
                    where SF.date='{date_rows_yesterday}'
                    and SF.PBR !=0 and SF.PBR >=0.2 
                    and SF.market_cap < {self.market_cap}
                    order by pbr limit {self.for_gpa_pbr_order_limit}) pbr_table,
                    daily_subindex.`{date_rows_yesterday}` subindex_in
                    where pbr_table.code=subindex_in.code
                    order by subindex_in.gpa_yoy_rate desc limit {self.for_gpa_gpa_rate_order_limit}
                    ) gpa_rate_and_pbr_table,
                    daily_subindex.`{date_rows_yesterday}` subindex,
                    daily_buy_list.stock_info SI
                    where DAY.code=gpa_rate_and_pbr_table.code
                    and DAY.code=subindex.code
                    and DAY.code=SI.code
                    and left(DAY.code,1) != 9
                    and (SI.stock_market ='거래소' or SI.stock_market = '코스닥')
                    and (SI.audit not like '%경고%' or SI.audit not like '%주의%')
                    and DAY.close > subindex.prespan1*{self.prespan1_rate}
                    and (subindex.bband_1month=0 or subindex.bband_1month>={self.bband_1month_period})
                    and subindex.gpa_qq>0
                    group by day.code
                    order by subindex.gpa_qq desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
            else:
                realtime_daily_buy_list = []



        elif self.db_to_realtime_daily_buy_list_num == 1604:

            check_sql = f'''
                     select * from jackbot1604.all_item_db
                     where sell_date=0
                     and datediff('{date_rows_yesterday}',left(buy_date,8))>=29
                     order by left(buy_date,8) desc
                     limit 1
            '''
            check_sql_list = self.engine_daily_buy_list.execute(check_sql).fetchall()

            # test용 강제로 아래 if문 들어가게함.
            # check_sql_list=[1]

            if len(check_sql_list) > 0:

                market_percent_sql = f'''
                                                select count(*) 
                                                from daily_buy_list.`{date_rows_yesterday}`
                                                '''

                market_count = self.engine_daily_buy_list.execute(market_percent_sql).fetchall()[0][0]

                market_percent = int(market_count * (0.01 * self.marketcap_rate))

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY,  
                    (select pbr_table.* from
                    (select SF.* from daily_buy_list.stock_finance SF,
                    (select * from daily_buy_list.stock_finance
                    where date='{date_rows_yesterday}'
                    order by market_cap limit {market_percent}) marketcap,
                    daily_buy_list.stock_info SI
                    where SF.code=marketcap.code
                    and SF.code=SI.code
                    and (SI.stock_market ='거래소' or SI.stock_market = '코스닥')
                    and (SI.audit not like '%경고%')
                    and SF.date='{date_rows_yesterday}'
                    and SF.PBR !=0 and SF.PBR >=0.2 
                    order by pbr limit {self.for_gpa_pbr_order_limit}) pbr_table,
                    daily_subindex.`{date_rows_yesterday}` subindex_in
                    where pbr_table.code=subindex_in.code
                    and subindex_in.gpa_yoy_rate>0
                    order by subindex_in.gpa_yoy_rate desc limit {self.for_gpa_gpa_rate_order_limit}
                    ) gpa_rate_and_pbr_table,
                    daily_subindex.`{date_rows_yesterday}` subindex
                    where DAY.code=gpa_rate_and_pbr_table.code
                    and DAY.code=subindex.code
                    and left(DAY.code,1) != 9
                    and DAY.close > subindex.prespan1*{self.prespan1_rate}
                    and (subindex.bband_1month=0 or subindex.bband_1month>={self.bband_1month_period})
                    and subindex.gpa_qq>0
                    and DAY.clo5*DAY.vol5 > {self.trading_money}
                    group by day.code
                    order by subindex.gpa_qq desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
            else:
                realtime_daily_buy_list = []

        # elif self.db_to_realtime_daily_buy_list_num == 1644:
        #
        #     check_sql=f'''
        #              select lead(b.date, 1) over (order by b.date desc) from (
        #             select a.* from (
        #                 select * from etc_info.`etc_info_date`
        #                     where left(date,6)=left({self.today},6) ) a
        #              ) b limit 1
        #     '''
        #     check_sql_list = self.engine_daily_buy_list.execute(check_sql).fetchall()[0][0]
        #     if check_sql_list==self.today:
        #         check_pass=1
        #     else:
        #         check_pass=0
        #
        #     #test용 강제로 아래 if문 들어가게함.
        #     #check_sql_list=[1]
        #
        #     if check_pass==1:
        #
        #         market_percent_sql = f'''
        #                                         select count(*)
        #                                         from daily_buy_list.`{date_rows_yesterday}`
        #                                         '''
        #
        #         market_count = self.engine_daily_buy_list.execute(market_percent_sql).fetchall()[0][0]
        #
        #         market_percent = int(market_count * (0.01 * self.marketcap_rate))
        #
        #
        #         sql = f'''
        #             SELECT DAY.*
        #             FROM `{date_rows_yesterday}` DAY,
        #             (select pbr_table.* from
        #             (select SF.* from daily_buy_list.stock_finance SF,
        #             (select * from daily_buy_list.stock_finance
        #             where date='{date_rows_yesterday}'
        #             order by market_cap limit {market_percent}) marketcap,
        #             daily_buy_list.stock_info SI
        #             where SF.code=marketcap.code
        #             and SF.code=SI.code
        #             and (SI.stock_market ='거래소' or SI.stock_market = '코스닥')
        #             and (SI.audit not like '%경고%' )
        #             and SF.date='{date_rows_yesterday}'
        #             and SF.PBR !=0 and SF.PBR >=0.2
        #             order by pbr limit {self.for_gpa_pbr_order_limit}) pbr_table,
        #             daily_subindex.`{date_rows_yesterday}` subindex_in
        #             where pbr_table.code=subindex_in.code
        #             and subindex_in.gpa_yoy_rate>0
        #             order by subindex_in.gpa_yoy_rate desc limit {self.for_gpa_gpa_rate_order_limit}
        #             ) gpa_rate_and_pbr_table,
        #             daily_subindex.`{date_rows_yesterday}` subindex
        #             where DAY.code=gpa_rate_and_pbr_table.code
        #             and DAY.code=subindex.code
        #             and left(DAY.code,1) != 9
        #             and DAY.close > subindex.prespan1*{self.prespan1_rate}
        #             and (subindex.bband_1month=0 or subindex.bband_1month>={self.bband_1month_period})
        #             and subindex.gpa_qq>0
        #             and DAY.clo5*DAY.vol5 > {self.trading_money}
        #             group by day.code
        #             order by subindex.gpa_qq desc
        #         '''
        #
        #         realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
        #         check_pass=0
        #     else :
        #         realtime_daily_buy_list=[]

        elif self.db_to_realtime_daily_buy_list_num == 1644:

            check_sql = f'''
                     select lead(b.date, 1) over (order by b.date desc) from (
                    select a.* from (
                        select * from etc_info.`etc_info_date`
                            where left(date,6)=left({self.today},6) ) a
                     ) b limit 1
            '''
            check_sql_list = self.engine_daily_buy_list.execute(check_sql).fetchall()[0][0]
            if check_sql_list == self.today:
                check_pass = 1
            else:
                check_pass = 0

            # test용 강제로 아래 if문 들어가게함.
            # check_sql_list=[1]

            if check_pass == 1:

                market_percent_sql = f'''
                            select count(*) from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                            where sf.code=si.code
                            and si.stock_market in ('코스닥','거래소')
                            and date={date_rows_yesterday}
                                                '''

                market_count = self.engine_daily_buy_list.execute(market_percent_sql).fetchall()[0][0]

                market_percent = int(market_count * (0.01 * self.marketcap_rate))

                sql = f'''
                    SELECT DAY.*
                    FROM daily_buy_list.`{date_rows_yesterday}` DAY,
                    (select pbr_table.* from
                    (select marketcap_out.* from
                    (select SF.*,SI.audit from daily_buy_list.stock_finance SF,daily_buy_list.stock_info SI
                    where SF.code=SI.code
                    and SF.date='{date_rows_yesterday}'
                    and (SI.stock_market ='거래소' or SI.stock_market = '코스닥')
                     order by market_cap limit {market_percent})  marketcap_out
                    where marketcap_out.audit not like '%경고%'
                    and marketcap_out.PBR !=0 and marketcap_out.PBR >=0.2
                    order by marketcap_out.pbr limit {self.for_gpa_pbr_order_limit}) pbr_table,
                    daily_subindex.`{date_rows_yesterday}` subindex_in
                    where pbr_table.code=subindex_in.code
                    and subindex_in.gpa_yoy_rate>0
                    order by subindex_in.gpa_yoy_rate desc limit {self.for_gpa_gpa_rate_order_limit}
                    ) gpa_rate_and_pbr_table,
                    daily_subindex.`{date_rows_yesterday}` subindex
                    where DAY.code=gpa_rate_and_pbr_table.code
                    and DAY.code=subindex.code
                    and left(DAY.code,1) != 9
                    and subindex.gpa_qq>0
                    and DAY.clo5*DAY.vol5 > {self.trading_money}
                    and DAY.close > subindex.prespan1*{self.prespan1_rate}
                    and (subindex.bband_1month=0 or subindex.bband_1month>={self.bband_1month_period})
                    group by day.code
                    order by subindex.gpa_qq desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
                check_pass = 0
            else:
                realtime_daily_buy_list = []


        elif self.db_to_realtime_daily_buy_list_num == 1668:

            check_sql = f'''
                     select lead(b.date, 1) over (order by b.date desc) from (
                    select a.* from (
                        select * from etc_info.`etc_info_date`
                            where left(date,6)=left({self.today},6) ) a
                     ) b limit 1
            '''
            check_sql_list = self.engine_daily_buy_list.execute(check_sql).fetchall()[0][0]
            if check_sql_list == self.today:
                check_pass = 1
            else:
                check_pass = 0

            # test용 강제로 아래 if문 들어가게함.
            # check_sql_list=[1]

            if check_pass == 1:

                market_percent_sql = f'''
                            select count(*) from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                            where sf.code=si.code
                            and si.stock_market in ('코스닥','거래소')
                            and date={date_rows_yesterday}
                                                '''

                market_count = self.engine_daily_buy_list.execute(market_percent_sql).fetchall()[0][0]

                market_percent = int(market_count * (0.01 * self.marketcap_rate))

                sql = f'''
                    SELECT DAY.*
                    FROM daily_buy_list.`{date_rows_yesterday}` DAY,
                    (select pbr_table.* from
                    (select marketcap_out.*,subindex_in.prespan1, subindex_in.bband_1month,
                    subindex_in.gpa_yoy_rate,subindex_in.gpa_qq
                    from
                    (select SF.*,SI.audit from daily_buy_list.stock_finance SF,daily_buy_list.stock_info SI
                    where SF.code=SI.code
                    and SF.date={date_rows_yesterday}
                    and (SI.stock_market ='거래소' or SI.stock_market = '코스닥')
                    order by market_cap limit {market_percent})  marketcap_out,
                    daily_subindex.`{date_rows_yesterday}` subindex_in
                    where marketcap_out.code=subindex_in.code
                    and marketcap_out.audit not like '%경고%'
                    and subindex_in.code_name not like '%홀딩스%'
                    and subindex_in.code_name not like '%지주%'
                    and subindex_in.code_name not like '%은행%'
                    and subindex_in.code_name not like '%금융%'
                    and subindex_in.code_name not like '%스팩%'
                    and subindex_in.code_name not like '%증권%'
                    and marketcap_out.PBR !=0 and marketcap_out.PBR >=0.2
                    and subindex_in.gpa_yoy_rate>0
                    and left(subindex_in.code,1) != 9
                    and subindex_in.gpa_qq>0
                    order by marketcap_out.pbr limit {self.for_gpa_pbr_order_limit}) pbr_table
                    order by pbr_table.gpa_yoy_rate desc limit {self.for_gpa_gpa_rate_order_limit}
                    ) gpa_rate_and_pbr_table
                    where DAY.code=gpa_rate_and_pbr_table.code
                    and DAY.clo5*DAY.vol5 > {self.trading_money}
                    and DAY.close > gpa_rate_and_pbr_table.prespan1*{self.prespan1_rate}
                    and (gpa_rate_and_pbr_table.bband_1month=0 or gpa_rate_and_pbr_table.bband_1month>={self.bband_1month_period})
                    group by day.code
                    order by gpa_rate_and_pbr_table.gpa_qq desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
                check_pass = 0
            else:
                realtime_daily_buy_list = []

        elif self.db_to_realtime_daily_buy_list_num == 1802:

            market_percent_sql = f'''
                            select count(*) from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                            where sf.code=si.code
                            and si.stock_market in ('코스닥','거래소')
                            and date={date_rows_yesterday}
                            '''

            market_count = self.engine_daily_buy_list.execute(market_percent_sql).fetchall()[0][0]

            market_percent = int(market_count * (0.01 * self.marketcap_rate))

            sql = f'''
                select DAY.* from daily_buy_list.`{date_rows_yesterday}` DAY,
                (select sf.*,si.audit from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                where sf.code=si.code
                and si.stock_market in ('코스닥','거래소')
                and date={date_rows_yesterday}
                order by sf.market_cap
                limit {market_percent}) marketcap,
                daily_subindex.`{date_rows_yesterday}` subindex
                where DAY.code=marketcap.code
                and DAY.code=subindex.code
                and left(DAY.code,1) != 9
                and marketcap.audit not like '%경고%'
                and subindex.gpa_yoy_rate > 0
                and company_value_ss_rate > 0
                and DAY.code_name not like '%홀딩스%'
                and DAY.close > subindex.prespan1*{self.prespan1_rate}
                and (subindex.bband_1month=0 or subindex.bband_1month>={self.bband_1month_period})
                and DAY.close * DAY.volume > {self.trading_money}
                and subindex.profit_qq>{self.profit_qq}
                group by DAY.code
                order by subindex.company_value_ss_rate desc
            '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            self.realtime_daily_buy_list_len = len(realtime_daily_buy_list)

            if self.realtime_daily_buy_list_len <= self.realtime_daily_buy_list_count + 1:
                realtime_daily_buy_list = []

        elif self.db_to_realtime_daily_buy_list_num == 1899:
            check_sql = f'''
                                 select lead(b.date, 1) over (order by b.date desc) from (
                                select a.* from (
                                    select * from etc_info.`etc_info_date`
                                        where left(date,6)=left({self.today},6) ) a
                                 ) b limit 1
                        '''
            check_sql_list = self.engine_daily_buy_list.execute(check_sql).fetchall()[0][0]
            if check_sql_list == self.today:
                check_pass = 1
            else:
                check_pass = 0

            if check_pass == 1:
                market_percent_sql = f'''
                                select count(*) 
                                from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                                where sf.code=si.code
                                and si.stock_market in ('코스닥','거래소')
                                and date={date_rows_yesterday}
                                '''

                market_count = self.engine_daily_buy_list.execute(market_percent_sql).fetchall()[0][0]

                market_percent = int(market_count * (0.01 * self.marketcap_rate))
                market_start = market_percent * self.market_start
                market_gap = market_percent * self.market_gap

                sql = f'''
                    select DAY.* from daily_buy_list.`{date_rows_yesterday}` DAY,
                    (select subindex.* from (select sf.*,si.audit from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                    where sf.code=si.code
                    and si.stock_market in ('코스닥','거래소')
                    and date={date_rows_yesterday}
                    order by sf.market_cap
                    limit {market_start}, {market_gap}
                    ) marketcap,
                    daily_subindex.`{date_rows_yesterday}` subindex
                    where marketcap.code=subindex.code
                    and left(subindex.code,1) != 9
                    and marketcap.audit not like '%경고%'
                    and subindex.gpa_yoy_rate > 0
                    and subindex.company_value_ss_rate > 0
                    and subindex.code_name not like '%홀딩스%'
                    and subindex.code_name not like '%지주%'
                    and subindex.code_name not like '%은행%'
                    and subindex.code_name not like '%금융%'
                    and subindex.code_name not like '%스팩%'
                    and subindex.code_name not like '%증권%'
                    and subindex.profit_qq>{self.profit_qq}
                    and subindex.loan_t_rate <={self.loan_rate_tt}
                    order by subindex.company_value_ss_rate desc limit {self.company_value_ss_rate_limit}
                    ) subindex_out
                    where DAY.code=subindex_out.code
                    and DAY.close > subindex_out.prespan1*{self.prespan1_rate}
                    and (subindex_out.bband_1month=0 or subindex_out.bband_1month>={self.bband_1month_period})
                    and DAY.clo5 * DAY.vol5 > {self.trading_money}
                    group by DAY.code
                    order by subindex_out.gpa_qq desc
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

                self.realtime_daily_buy_list_len = len(realtime_daily_buy_list)

                if self.realtime_daily_buy_list_len <= self.realtime_daily_buy_list_count + 1:
                    realtime_daily_buy_list = []
            else:
                realtime_daily_buy_list = []




        elif self.db_to_realtime_daily_buy_list_num == 2073:

            check_sql = f'''
                     select lead(b.date, 0) over (order by b.date desc) from (
                    select a.* from (
                        select * from etc_info.`etc_info_date`
                            where left(date,6)=left({self.today},6) ) a
                     ) b limit 1
            '''
            check_sql_list = self.engine_daily_buy_list.execute(check_sql).fetchall()[0][0]
            # check_sql_list='20210819'
            if check_sql_list == self.today:
                check_pass = 1
            else:
                check_pass = 0

            # test용 강제로 아래 if문 들어가게함.
            # check_sql_list=[1]

            market_percent_sql = f'''
                        select count(*) from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                        where sf.code=si.code
                        and si.stock_market in ('코스닥','거래소')
                        and date={date_rows_yesterday}
                                            '''

            market_count = self.engine_daily_buy_list.execute(market_percent_sql).fetchall()[0][0]

            market_percent = int(market_count * (0.01 * self.marketcap_rate))

            test = 0
            sql = f'''
                SELECT DAY.*
                from daily_buy_list.`{date_rows_yesterday}` DAY,
                (select subindex_in.code,subindex_in.code_name,subindex_in.prespan1,subindex_in.bband_1month,
                ((rank() over(order by subindex_in.gpa_qq desc))*({self.gpa_proportion})
                + (rank() over(order by marketcap_out.pbr))*({self.pbr_proportion})
                + (rank() over(order by subindex_in.GPA_yoy_rate desc))*({self.gpa_yoy_rate_proportion})) score
                    from (select SF.*, SI.audit
                        from daily_buy_list.stock_finance SF,
                             daily_buy_list.stock_info SI
                             where SF.code = SI.code
                             and SF.date = {date_rows_yesterday}
                             and (SI.stock_market = '거래소' or SI.stock_market = '코스닥')
                             order by market_cap
                             limit {market_percent}) marketcap_out,
                               daily_subindex.`{date_rows_yesterday}` subindex_in
                                  where marketcap_out.code= subindex_in.code
                               and marketcap_out.audit not like '%경고%'
                               and subindex_in.code_name not like '%홀딩스%'
                               and subindex_in.code_name not like '%지주%'
                               and subindex_in.code_name not like '%은행%'
                               and subindex_in.code_name not like '%금융%'
                               and subindex_in.code_name not like '%스팩%'
                               and subindex_in.code_name not like '%증권%'
                               and marketcap_out.PBR >= 0.2
                               and left(subindex_in.code, 1) != 9
                               and subindex_in.gpa_yoy_rate>0
                               and subindex_in.gpa_qq>0
                               order by score
                               limit {self.for_gpa_pbr_order_limit}
                ) gpa_pbr_table
                where gpa_pbr_table.code=day.code
                and DAY.clo5*DAY.vol5 > {self.trading_money}
                and DAY.close > gpa_pbr_table.prespan1*{self.prespan1_rate}
                and (gpa_pbr_table.bband_1month=0 or gpa_pbr_table.bband_1month>={self.bband_1month_period})
                group by day.code
                order by gpa_pbr_table.score
            '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            self.realtime_daily_buy_list_length = len(realtime_daily_buy_list)

            self.update_realtime_daily_buy_list_length(self.realtime_daily_buy_list_length)
            # test=0
            # 리밸런싱 날짜 아니면 매수리스트 비움
            if check_pass != 1:
                realtime_daily_buy_list = []
            else:
                pass

            # 리밋 걸은 것보다 매수리스트가 적으면 안사게 만듬
            if self.realtime_daily_buy_list_length <= self.buy_list_length_limit:
                realtime_daily_buy_list = []
            else:
                pass

            test = 0

        elif self.db_to_realtime_daily_buy_list_num == 2091:
            # date_rows_yesterday='20211001'
            test = 0
            market_percent_sql = f'''
                select count(*) from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                where sf.code=si.code
                and si.stock_market in ('코스닥','거래소')
                and date={date_rows_yesterday}
                            '''

            market_count = self.engine_daily_buy_list.execute(market_percent_sql).fetchall()[0][0]

            market_percent = int(market_count * (0.01 * self.marketcap_rate))

            sql = f'''
                select DAY.* from daily_buy_list.`{date_rows_yesterday}` DAY,
                (select sf.*,si.audit from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                where sf.code=si.code
                and si.stock_market in ('코스닥','거래소')
                and date={date_rows_yesterday}
                order by sf.market_cap
                limit {market_percent}) marketcap,
                daily_subindex.`{date_rows_yesterday}` subindex
                where DAY.code=marketcap.code
                and DAY.code=subindex.code
                and left(DAY.code,1) != 9
                and marketcap.audit not like '%경고%'
                and DAY.code_name not like '%홀딩스%'
                and DAY.code_name not like '%지주%'
                and DAY.code_name not like '%은행%'
                and DAY.code_name not like '%금융%'
                and DAY.code_name not like '%스팩%'
                and DAY.code_name not like '%증권%'
                and subindex.company_value_ss_rate > 0
                and DAY.close > subindex.prespan1*{self.prespan1_rate}
                and (subindex.bband_1month=0 or subindex.bband_1month>={self.bband_1month_period})
                and DAY.close * DAY.volume > {self.trading_money}
                group by DAY.code
                order by (rank() over(order by subindex.company_value_ss_rate desc))*({self.company_value_ss_proportion})
                 +(rank() over(order by subindex.gpa_qq desc))*({self.gpa_proportion})
                 +(rank() over(order by subindex.gpa_yoy_rate desc))*({self.gpa_yoy_rate_proportion})
            '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            test = 0
            self.realtime_daily_buy_list_len = len(realtime_daily_buy_list)
            self.update_realtime_daily_buy_list_length(self.realtime_daily_buy_list_len)

            if self.realtime_daily_buy_list_len <= self.realtime_daily_buy_list_count + 1:
                realtime_daily_buy_list = []

        elif self.db_to_realtime_daily_buy_list_num == 2224:

            market_percent_sql = f'''
                            select count(*) from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                            where sf.code=si.code
                            and si.stock_market in ('코스닥','거래소')
                            and date={date_rows_yesterday}
                            '''

            market_count = self.engine_daily_buy_list.execute(market_percent_sql).fetchall()[0][0]

            market_percent = int(market_count * (0.01 * self.marketcap_rate))

            sql = f'''
                select DAY.* from daily_buy_list.`{date_rows_yesterday}` DAY,
                (select sf.*,si.audit from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                where sf.code=si.code
                and si.stock_market in ('코스닥','거래소')
                and date={date_rows_yesterday}
                order by sf.market_cap
                limit {market_percent}) marketcap,
                daily_subindex.`{date_rows_yesterday}` subindex
                where DAY.code=marketcap.code
                and DAY.code=subindex.code
                and left(DAY.code,1) != 9
                and marketcap.audit not like '%경고%'
                and DAY.code_name not like '%홀딩스%'
                and DAY.code_name not like '%지주%'
                and DAY.code_name not like '%은행%'
                and DAY.code_name not like '%금융%'
                and DAY.code_name not like '%스팩%'
                and DAY.code_name not like '%증권%'
                and subindex.company_value_ss_rate > 0
                and DAY.close > subindex.prespan1*{self.prespan1_rate}
                and (subindex.bband_1month=0 or subindex.bband_1month>={self.bband_1month_period})
                and DAY.close * DAY.volume > {self.trading_money}
                group by DAY.code
                order by (rank() over(order by 1/subindex.por desc))*({self.por_proportion})
                 +(rank() over(order by subindex.opm desc))*({self.opm_proportion})
                 +(rank() over(order by subindex.gpa_qq desc))*({self.gpa_proportion})
                 +(rank() over(order by subindex.gross_total_yoy_rate desc))*({self.gross_total_yoy_rate_proportion})
                 +(rank() over(order by subindex.asset_turnover_ratio_yoy_rate desc))*({self.asset_turnover_ratio_yoy_rate_proportion})
            '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            self.realtime_daily_buy_list_len = len(realtime_daily_buy_list)
            self.update_realtime_daily_buy_list_length(self.realtime_daily_buy_list_len)

            if self.realtime_daily_buy_list_len <= self.realtime_daily_buy_list_count + 1:
                realtime_daily_buy_list = []

        elif self.db_to_realtime_daily_buy_list_num == 2242:

            market_percent_sql = f'''
                            select count(*) 
                                from daily_financial.`{date_rows_yesterday}` fn,
                                daily_subindex.`{date_rows_yesterday}` sub,
                                daily_buy_list.`{date_rows_yesterday}` day
                                where fn.code=sub.code
                                and fn.code=day.code
                            '''

            market_count = self.engine_daily_buy_list.execute(market_percent_sql).fetchall()[0][0]

            market_percent = int(market_count * (0.01 * self.marketcap_rate))

            sql = f'''
                        SELECT DAY.*
                        FROM `{date_rows_yesterday}` DAY,  
                        (select pbr_table.* from
                        (
                        select fn.*

                        from (select fn_in.* from (
                                    select day_fn.code,day_fn.code_name,day_fn.gpa_qq,
                                    day_fn.net_profit,day_fn.GPA_qq_yoy_rate,
                                    subindex_in.prespan1,subindex_in.bband_1month,etc.pbr
                                    from daily_financial.`{date_rows_yesterday}` day_fn,
                                    daily_subindex.`{date_rows_yesterday}` subindex_in,
                                    etc_daily.`{date_rows_yesterday}` etc
                                    where day_fn.code=subindex_in.code
                                    and day_fn.code=etc.code
                                    group by day_fn.code
                                    order by day_fn.marketcap limit {market_percent} ) fn_in
                                where fn_in.code_name not like '%금융%'
                                and fn_in.code_name not like '%증권%'
                                and fn_in.code_name not like '%홀딩스%'
                                and fn_in.code_name not like '%스팩%'
                                and fn_in.code_name not like '%은행%'
                                and left(fn_in.code,1) != 9
                                and fn_in.pbr >= 0.2 
                                and fn_in.GPA_qq_yoy_rate > 0 
                                and fn_in.gpa_qq > 0 
                            ) fn
                        order by (rank() over(order by fn.gpa_qq desc))*({self.gpa_proportion})
                                + (rank() over(order by fn.pbr) )*({self.pbr_proportion})
                                + (rank() over(order by fn.GPA_qq_yoy_rate desc))*({self.gpa_yoy_rate_proportion})
                        limit {self.for_gpa_pbr_order_limit}) pbr_table
                        ) gpa_and_pbr_table
                        where DAY.code=gpa_and_pbr_table.code
                        and DAY.clo5*DAY.vol5 > {self.trading_money}
                        and DAY.close > gpa_and_pbr_table.prespan1 * {self.prespan1_rate}
                        and (gpa_and_pbr_table.bband_1month=0 or gpa_and_pbr_table.bband_1month >= {self.bband_1month_period})
                        group by day.code
                             '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            self.realtime_daily_buy_list_len = len(realtime_daily_buy_list)

            if self.realtime_daily_buy_list_len <= self.realtime_daily_buy_list_count + 1:
                realtime_daily_buy_list = []

        elif self.db_to_realtime_daily_buy_list_num == 2248:
            # date_rows_yesterday='20211001'
            test = 0
            market_percent_sql = f'''
                select count(*) from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                where sf.code=si.code
                and si.stock_market in ('코스닥','거래소')
                and date={date_rows_yesterday}
                            '''

            market_count = self.engine_daily_buy_list.execute(market_percent_sql).fetchall()[0][0]

            market_percent = int(market_count * (0.01 * self.marketcap_rate))

            sql = f'''
                select DAY.* from daily_buy_list.`{date_rows_yesterday}` DAY,
                (select subindex.*,(rank() over(order by 1/marketcap.pbr desc))*({self.pbr_proportion})
                 +(rank() over(order by subindex.gpa_qq desc))*({self.gpa_proportion})
                 +(rank() over(order by subindex.gpa_yoy_rate desc))*({self.gpa_yoy_rate_proportion}) score
                from
                (select sf.*,si.audit from daily_buy_list.stock_finance sf, daily_buy_list.stock_info si
                where sf.code=si.code
                and si.stock_market in ('코스닥','거래소')
                and date={date_rows_yesterday}
                order by sf.market_cap
                limit {market_percent}) marketcap,
                daily_subindex.`{date_rows_yesterday}` subindex,
                daily_buy_list.`{date_rows_yesterday}` DAY_in
                where DAY_in.code=marketcap.code
                and DAY_in.code=subindex.code
                and left(DAY_in.code,1) != 9
                and marketcap.audit not like '%경고%'
                and DAY_in.code_name not like '%홀딩스%'
                and DAY_in.code_name not like '%지주%'
                and DAY_in.code_name not like '%은행%'
                and DAY_in.code_name not like '%금융%'
                and DAY_in.code_name not like '%스팩%'
                and DAY_in.code_name not like '%증권%'
                and subindex.company_value_ss_rate > 0
                and marketcap.pbr > 0.2
                and subindex.gpa_qq > 0 
                and subindex.gpa_yoy_rate > 0
                order by score 
                limit {self.for_gpa_pbr_order_limit}) totaldata
                where Day.code=totaldata.code
                and DAY.close > totaldata.prespan1*{self.prespan1_rate}
                and (totaldata.bband_1month=0 or totaldata.bband_1month>={self.bband_1month_period})
                and DAY.close * DAY.volume > {self.trading_money}
                group by DAY.code
                order by score                
            '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            test = 0
            self.realtime_daily_buy_list_len = len(realtime_daily_buy_list)
            self.update_realtime_daily_buy_list_length(self.realtime_daily_buy_list_len)

            if self.realtime_daily_buy_list_len <= self.realtime_daily_buy_list_count + 1:
                realtime_daily_buy_list = []

        elif self.db_to_realtime_daily_buy_list_num == 5001:
            date_before = self.date_rows[i - 2][0]
            if i<2:
                realtime_daily_buy_list = []
                pass
            else:


                sql = f'''
                    select DAY.* from coin_daily_list.`{date_rows_yesterday}` DAY,
                    coin_daily_list.`{date_before}` before_day
                    where DAY.close > before_day.close
                    and DAY.code=before_day.code
                '''

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

                test=0

        elif self.db_to_realtime_daily_buy_list_num == 5002:
            date_sql=f'''
                    select * from coin_daily_list.`{date_rows_yesterday}`
                    where left(code,3)='{self.coin_market}'
                        '''
            date_sql_result = self.engine_daily_buy_list.execute(date_sql).fetchall()
            test=0
            self.divide_invest_unit = math.ceil(len(date_sql_result) * self.divide_invest_rate)
            if self.divide_invest_unit==0:
                self.divide_invest_unit=1

            test=0
            sql = f'''
                    select DAY.* 
                    from coin_daily_list.`{date_rows_yesterday}` DAY,
                    coin_daily_subindex.`{date_rows_yesterday}` subindex
                    where DAY.code=subindex.code
                    and left(DAY.code,3) = '{self.coin_market}'
                    and DAY.close > subindex.prespan1*{self.prespan1_rate}
                    and (subindex.bband_1month=0 or subindex.bband_1month>={self.bband_1month_period})
                    and DAY.close * DAY.volume > 0
                    group by DAY.code             
                    order by (rank() over(order by subindex.noise))*({self.noise_proportion}) +
                            (rank() over(order by subindex.avg_noise))*({self.avg_noise_proportion})

                '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            self.realtime_daily_buy_list_len = len(realtime_daily_buy_list)

        elif self.db_to_realtime_daily_buy_list_num == 5003:
            # date_sql = f'''
            #             select * from coin_daily_list.`{date_rows_yesterday}`
            #             where left(code,3)='{self.coin_market}'
            #                 '''
            # date_sql_result = self.engine_daily_buy_list.execute(date_sql).fetchall()
            # test = 0
            # self.divide_invest_unit = math.ceil(len(date_sql_result) * self.divide_invest_rate)
            # if self.divide_invest_unit == 0:
            #     self.divide_invest_unit = 1

            test = 0
            sql = f'''
                        select DAY.* 
                        from coin_daily_list.`{date_rows_yesterday}` DAY,
                        coin_daily_subindex.`{date_rows_yesterday}` subindex
                        where DAY.code=subindex.code
                        and left(DAY.code,3) = '{self.coin_market}'
                        and DAY.close > subindex.prespan1*{self.prespan1_rate}
                        and DAY.close * DAY.volume > 0
                        group by DAY.code             
                        order by (rank() over(order by subindex.noise))*({self.noise_proportion}) +
                                (rank() over(order by subindex.avg_noise))*({self.avg_noise_proportion}) +
                                (rank() over(order by DAY.close * DAY.volume desc))*({self.trading_money_proportion})

                    '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            self.realtime_daily_buy_list_len = len(realtime_daily_buy_list)
            self.update_realtime_daily_buy_list_length(self.realtime_daily_buy_list_len)

        elif self.db_to_realtime_daily_buy_list_num == 5004:


            sql = f'''
                            select DAY.* 
                            from coin_daily_list.`{date_rows_yesterday}` DAY
                            where (DAY.code_name='krw-btc' or DAY.code_name='krw-eth')
                            group by DAY.code             
                           

                        '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 5009:

            sql = f'''
                                select DAY.*
                                from coin_daily_list.`{date_rows_yesterday}` DAY,
                                    coin_daily_subindex.`{date_rows_yesterday}` subindex
                                where DAY.code=subindex.code
                                and left(DAY.code,3)='KRW'
                                group by DAY.code                                 
                                order by subindex.avg_noise
                                limit 10 


                            '''

            # sql = f'''
            #                                 select DAY.*
            #                                 from coin_daily_list.`{date_rows_yesterday}` DAY
            #                                 where DAY.code='krw-btc'
            #                             '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            #
            # self.realtime_daily_buy_list_len = len(realtime_daily_buy_list)
            # self.update_realtime_daily_buy_list_length(self.realtime_daily_buy_list_len)

        elif self.db_to_realtime_daily_buy_list_num == 5011:

            sql = f'''
                                    select DAY.*
                                    from coin_daily_list.`{date_rows_yesterday}` DAY,
                                        coin_daily_subindex.`{date_rows_yesterday}` subindex
                                    where DAY.code=subindex.code
                                    and left(DAY.code,3)='KRW'
                                    group by DAY.code
                                    order by (rank() over(order by subindex.avg_noise))*1 +
                                        (rank() over(order by DAY.close*DAY.volume desc))*1
                                    limit 10
                                '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            save_list=[]
            for i in range(len(realtime_daily_buy_list)):
                code=realtime_daily_buy_list[i][4]
                try:
                    avg_score=self.rarry_setting_invest_unit(code)
                except:
                    avg_score=0

                if avg_score<3:
                    continue
                else:
                    save_list.append(realtime_daily_buy_list[i])

            realtime_daily_buy_list=save_list[:10]

        elif self.db_to_realtime_daily_buy_list_num == 5012:

            sql = f'''
                                        select DAY.*
                                        from coin_daily_list.`{date_rows_yesterday}` DAY,
                                            coin_daily_subindex.`{date_rows_yesterday}` subindex
                                        where DAY.code=subindex.code
                                        and left(DAY.code,3)='KRW'
                                        group by DAY.code
                                        order by (rank() over(order by subindex.avg_noise))*1 +
                                            (rank() over(order by DAY.clo20*DAY.vol20 desc))*1
                                        limit 10
                                    '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            save_list = []
            for i in range(len(realtime_daily_buy_list)):
                code = realtime_daily_buy_list[i][4]
                try:
                    avg_score = self.rarry_setting_invest_unit(code)
                except:
                    avg_score = 0

                if avg_score < 3:
                    continue
                else:
                    save_list.append(realtime_daily_buy_list[i])

            realtime_daily_buy_list = save_list[:10]

        elif self.db_to_realtime_daily_buy_list_num == 5013:

            sql = f'''
                                            select DAY.*
                                            from coin_daily_list.`{date_rows_yesterday}` DAY,
                                                coin_daily_subindex.`{date_rows_yesterday}` subindex
                                            where DAY.code=subindex.code
                                            and left(DAY.code,3)='KRW'
                                            and DAY.vol60>0
                                            group by DAY.code
                                            order by (rank() over(order by subindex.avg_noise))*({self.subindex_avg_noise_proporsion}) +
                                                (rank() over(order by DAY.clo60*DAY.vol60 desc))*({self.trade_money_proporsion})
                                            limit 10
                                        '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            save_list = []
            for i in range(len(realtime_daily_buy_list)):
                code = realtime_daily_buy_list[i][4]
                try:
                    avg_score = self.rarry_setting_invest_unit(code)
                except:
                    avg_score = 0

                if avg_score < 3:
                    continue
                else:
                    save_list.append(realtime_daily_buy_list[i])

            realtime_daily_buy_list = save_list[:10]

        elif self.db_to_realtime_daily_buy_list_num == 5015:

            sql = f'''
                                                select DAY.*
                                                from coin_daily_list.`{date_rows_yesterday}` DAY,
                                                    coin_daily_subindex.`{date_rows_yesterday}` subindex
                                                where DAY.code=subindex.code
                                                and left(DAY.code,3)='KRW'
                                                and (DAY.code='krw-btc' or 
                                                    DAY.code='krw-eth' or
                                                    DAY.code='krw-ltc' or
                                                    DAY.code='krw-xrp' or
                                                    DAY.code='krw-ada'
                                                    )
                                            '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            save_list = []
            for i in range(len(realtime_daily_buy_list)):
                code = realtime_daily_buy_list[i][4]
                try:
                    avg_score = self.rarry_setting_invest_unit(code)
                except:
                    avg_score = 0

                if avg_score < 3:
                    continue
                else:
                    save_list.append(realtime_daily_buy_list[i])

            realtime_daily_buy_list = save_list[:10]

        elif self.db_to_realtime_daily_buy_list_num == 5017:

            sql = f'''
                                                    select DAY.*
                                                    from coin_daily_list.`{date_rows_yesterday}` DAY,
                                                        coin_daily_subindex.`{date_rows_yesterday}` subindex
                                                    where DAY.code=subindex.code
                                                    and left(DAY.code,3)='KRW'
                                                    and (DAY.code='krw-btc' or 
                                                    DAY.code='krw-eth' or
                                                    DAY.code='krw-ltc' or
                                                    DAY.code='krw-xrp' or
                                                    DAY.code='krw-ada'
                                                        )
                                                '''

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

            save_list = []
            for i in range(len(realtime_daily_buy_list)):
                code = realtime_daily_buy_list[i][4]
                try:
                    avg_score = self.rarry_setting_invest_unit(code)
                except:
                    avg_score = 0

                if avg_score < 3:
                    continue
                else:
                    save_list.append(realtime_daily_buy_list[i])

            realtime_daily_buy_list = save_list[:10]

            ######################################################################################################################################################################################
        else:
            print(
                f"{self.simul_num}번 알고리즘에 대한 self.db_to_realtime_daily_buy_list_num 설정이 비었습니다. variable_setting 함수에서 self.db_to_realtime_daily_buy_list_num 을 확인해주세요.")
            sys.exit(1)
        # realtime_daily_buy_list 에 종목이 하나라도 있다면, 즉 매수할 종목이 하나라도 있다면 아래 로직을 들어간다.
        if len(realtime_daily_buy_list) > 0:
            # realtime_daily_buy_list 라는 리스트를 df_realtime_daily_buy_list 라는 데이터프레임으로 변환하는 과정
            # 차이점은 리스트는 컬럼에 대한 개념이 없는데, 데이터프레임은 컬럼이 있다.

            df_realtime_daily_buy_list = DataFrame(realtime_daily_buy_list,
                                                   columns=['index', 'index2', 'date', 'check_item', 'code',
                                                            'code_name', 'd1_diff_rate', 'close', 'open', 'high',
                                                            'low', 'volume','clo3',
                                                            'clo5', 'clo10', 'clo20', 'clo40', 'clo60',
                                                            'clo100', 'clo120',"clo3_diff_rate",
                                                            "clo5_diff_rate", "clo10_diff_rate", "clo20_diff_rate",
                                                            "clo40_diff_rate", "clo60_diff_rate",
                                                            "clo100_diff_rate", "clo120_diff_rate",
                                                            'yes_clo3','yes_clo5', 'yes_clo10', 'yes_clo20', 'yes_clo40',
                                                            'yes_clo60',
                                                            'yes_clo100', 'yes_clo120',
                                                            'vol3','vol5', 'vol10', 'vol20', 'vol40', 'vol60',
                                                            'vol100', 'vol120'])



            # print("통과4")
            # lamda는 익명 함수이다. 여기서 int로 param을 보내야 6d ( 정수) 에서 안걸린다.
            # df_realtime_daily_buy_list['code'] = df_realtime_daily_buy_list['code'].apply(
            #     lambda x: "{:0>6d}".format(int(x)))

            # 시뮬레이터의 경우
            if self.op != 'real':
                df_realtime_daily_buy_list['check_item'] = int(0)
                # [to_sql]
                # df_realtime_daily_buy_list 라는 데이터프레임을
                # simulator 데이터베이스의 realtime_daily_buy_list 테이블로 만들어주는 명령
                #
                # ** if_exists 옵션 **
                # # 데이터베이스에 테이블이 존재할 때 수행 동작을 지정한다.
                # 'fail', 'replace', 'append' 중 하나를 사용할 수 있는데 기본값은 'fail'이다.
                # 'fail'은 데이터베이스에 테이블이 있다면 아무 동작도 수행하지 않는다.
                # 'replace'는 테이블이 존재하면 기존 테이블을 삭제하고 새로 테이블을 생성한 후 데이터를 삽입한다.
                # 'append'는 테이블이 존재하면 데이터만을 추가한다.
                df_realtime_daily_buy_list.to_sql('realtime_daily_buy_list', self.engine_simulator, if_exists='replace')

                r_d_b_overlap_list = []
                if self.r_d_b_limit_control:
                    r_d_b_unit_limit = self.r_d_b_unit_limit
                else:
                    r_d_b_unit_limit = self.divide_invest_unit

                try:
                    if self.r_d_b_overlap_save_on_off:
                        rbd_overlap_list_sql = f'''
                            select db.code,db.code_name,db.buy_date,db.sell_date,db.rate
                            from (select * from realtime_daily_buy_list limit {self.divide_invest_unit}) rdb
                            , all_item_db db
                            where rdb.code=db.code
                            and db.sell_date=0

                        '''

                        r_d_b_overlap_list = self.engine_simulator.execute(rbd_overlap_list_sql).fetchall()
                    else:
                        r_d_b_overlap_list = []
                except:
                    r_d_b_overlap_list = []
                if len(r_d_b_overlap_list) > 0:
                    df_rdb_overlap_list = DataFrame(r_d_b_overlap_list,
                                                    columns=['code', 'code_name',
                                                             'buy_date', 'sell_date', 'rate'])
                else:
                    df_rdb_overlap_list = pd.DataFrame(index=range(0, 0),
                                                       columns=['code', 'code_name',
                                                                'buy_date', 'sell_date', 'rate'])
                df_rdb_overlap_list.to_sql('rdb_overlap_list', self.engine_simulator, if_exists='replace')

                # 현재 보유 중인 종목은 매수 리스트(realtime_daily_buy_list) 에서 제거 하는 로직
                # !@여기에  중복을 삭제하게 했음. 굉장히 중요하고 리스크한 부분 건들었음, 혹시 나중에 다른 알고리즘 할때 해당부분 삭제해야됨.
                if self.is_simul_table_exist(self.db_name, "all_item_db") and self.overlap_data_delete_on:
                    sql = "delete from realtime_daily_buy_list where code in (select code from all_item_db where sell_date = '%s' or buy_date = '%s' or sell_date = '%s')"
                    # delete는 리턴 값이 없기 때문에 fetchall 쓰지 않는다.
                    self.engine_simulator.execute(sql % (0, date_rows_today, date_rows_today))



                # !$
                # realtime_daily_buy_list 매수할 갯수만 한정해서 갖다둠(자꾸 매수할 범위 넘어서서 매수하기때문에 설정함)
                # +1해놓은 것은 혹시나 10개 매수대상이 11개가 보유종목인데, 11개를 빼면 오류가 날수도 있기때문에 방지차원에서 설정함
                # +10으로 재설정완료
                # self.divide_invest_unit 이상해서 10으로 설정해둠.
                df_realtime_daily_buy_list = df_realtime_daily_buy_list.head(10 - len(r_d_b_overlap_list) + 1)
                df_realtime_daily_buy_list.to_sql('realtime_daily_buy_list', self.engine_simulator, if_exists='replace')

                # 영상 촬영 후 추가 된 코드입니다. AI챕터에서 다룰 예정입니다.
                # if self.use_ai:
                #     from ai_filter import ai_filter
                #     ai_filter(self.ai_filter_num, engine=self.engine_simulator, until=date_rows_yesterday)

                # 최종적으로 realtime_daily_buy_list 테이블에 저장 된 종목들을 가져온다.
                self.get_realtime_daily_buy_list()

            # 모의, 실전 투자 봇 의 경우
            else:
                # check_item 컬럼에 0 으로 setting
                df_realtime_daily_buy_list['check_item'] = int(0)
                df_realtime_daily_buy_list.to_sql('realtime_daily_buy_list', self.engine_simulator, if_exists='replace')

                ###########################중복대상 테이블 따로 담는 코드#######################
                r_d_b_overlap_list = []
                if self.r_d_b_limit_control:
                    r_d_b_unit_limit = self.r_d_b_unit_limit
                else:
                    r_d_b_unit_limit = self.divide_invest_unit
                try:
                    if self.r_d_b_overlap_save_on_off:
                        rbd_overlap_list_sql = f'''
                            select db.code,db.code_name,db.buy_date,db.sell_date,db.rate
                            from (select * from realtime_daily_buy_list limit {r_d_b_unit_limit}) rdb
                            , all_item_db db
                            where rdb.code=db.code
                            and db.sell_date=0

                        '''

                        r_d_b_overlap_list = self.engine_simulator.execute(rbd_overlap_list_sql).fetchall()
                    else:
                        r_d_b_overlap_list = []
                except:
                    r_d_b_overlap_list = []
                if len(r_d_b_overlap_list) > 0:
                    df_rdb_overlap_list = DataFrame(r_d_b_overlap_list,
                                                    columns=['code', 'code_name',
                                                             'buy_date', 'sell_date', 'rate'])
                else:
                    df_rdb_overlap_list = pd.DataFrame(index=range(0, 0),
                                                       columns=['code', 'code_name',
                                                                'buy_date', 'sell_date', 'rate'])
                df_rdb_overlap_list.to_sql('rdb_overlap_list', self.engine_simulator, if_exists='replace')

                df_realtime_daily_buy_list = df_realtime_daily_buy_list.head(self.divide_invest_unit + 1)
                df_realtime_daily_buy_list.to_sql('realtime_daily_buy_list', self.engine_simulator, if_exists='replace')

                ###########################중복대상 테이블 따로 담는 코드#######################

                # 현재 보유 중인 종목들은 삭제
                if self.overlap_data_delete_on:
                    sql = "delete from realtime_daily_buy_list where code in (select code from all_item_db where sell_date=0)"
                    self.engine_simulator.execute(sql)

                    # sql = "delete from realtime_daily_buy_list where code in (select code from possessed_item)"

                # !$
                # realtime_daily_buy_list 매수할 갯수만 한정해서 갖다둠(자꾸 매수할 범위 넘어서서 매수하기때문에 설정함)
                # +1해놓은 것은 혹시나 10개 매수대상이 11개가 보유종목인데, 11개를 빼면 오류가 날수도 있기때문에 방지차원에서 설정함
                # +10으로 재설정완료
                # self.divide_invest_unit 이상해서 10으로 설정해둠.



        # 매수할 종목이 없으면, df_realtime_daily_buy_list라는 데이터프레임의 길이를 저장하는
        # len_df_realtime_daily_buy_list에 다가 0을 넣는다.
        else:
            self.rdb_code_list_for_min_trading=[]
            self.len_df_realtime_daily_buy_list = 0
            # 강의 촬영 후 추가 코드 (매수 조건에 맞는 종목이 하나도 없을 경우 realtime_daily_buy_list 를 비워준다)
            if self.engine_simulator.dialect.has_table(self.engine_simulator, "realtime_daily_buy_list"):
                self.engine_simulator.execute("""
                    DELETE FROM realtime_daily_buy_list 
                """)

    # 현재의 주가를 all_item_db에 있는 보유한 종목들에 대해서 반영 한다.
    def db_to_all_item_present_price_update(self, code_name, d1_diff_rate, close, open, high, low, volume, clo5, clo10,
                                            clo20,
                                            clo40, clo60, clo3, clo100, clo120, option='ALL'):
        # 영상 촬영 후 아래 내용 업데이트 하였습니다.
        if self.op == 'real':  # 콜렉터에서 업데이트 할 때는 현재가를 종가로 업데이트(trader에서 실시간으로 present_price 업데이트함)
            present_price = close
        else:
            present_price = open  # 시뮬레이터에서는 open가를 현재가로 업데이트

        # option이 ALL이면 모든 데이터 업데이트
        if option == "ALL":
            sql = f"update all_item_db set d1_diff_rate = {d1_diff_rate}, close = {close}, open = {open}, high = {high}, " \
                  f"low = {low}, volume = {volume}, present_price = {present_price}, clo5 = {clo5}, clo10 = {clo10}, clo20 = {clo20}, " \
                  f"clo40 = {clo40}, clo60 = {clo60}, clo3 = {clo3}, clo100 = {clo100}, clo120 = {clo120} " \
                  f"where code_name = '{code_name}' and sell_date = {0}"
        # option이 OPEN이면 open, present_price 만 업데이트
        else:
            sql = f"update all_item_db set open = {open}, present_price = {present_price} where code_name = '{code_name}' and sell_date = {0}"

        self.engine_simulator.execute(sql)

    # jango_data 라는 테이블을 만들기 위한 self.jango 데이터프레임을 생성
    def init_df_jango(self):
        jango_temp = {'id': []}

        self.jango = DataFrame(jango_temp,
                               columns=['date', 'today_earning_rate', 'sum_valuation_profit', 'total_profit',
                                        'today_profit',
                                        'today_profitcut_count', 'today_losscut_count', 'today_profitcut',
                                        'today_losscut',
                                        'd2_deposit', 'total_possess_count', 'today_buy_count', 'today_buy_list_count',
                                        'today_reinvest_count',
                                        'today_cant_reinvest_count',
                                        'total_asset',
                                        'total_invest',
                                        'sum_item_total_purchase', 'total_evaluation', 'today_rate',
                                        'today_invest_price', 'today_reinvest_price',
                                        'today_sell_price', 'volume_limit', 'reinvest_point', 'sell_point',
                                        'max_reinvest_count', 'invest_limit_rate', 'invest_unit',
                                        'rate_std_sell_point', 'limit_money', 'total_profitcut', 'total_losscut',
                                        'total_profitcut_count',
                                        'total_losscut_count', 'loan_money', 'start_kospi_point',
                                        'start_kosdaq_point', 'end_kospi_point', 'end_kosdaq_point',
                                        'today_buy_total_sell_count',
                                        'today_buy_total_possess_count', 'today_buy_today_profitcut_count',
                                        'today_buy_today_profitcut_rate', 'today_buy_today_losscut_count',
                                        'today_buy_today_losscut_rate',
                                        'today_buy_total_profitcut_count', 'today_buy_total_profitcut_rate',
                                        'today_buy_total_losscut_count', 'today_buy_total_losscut_rate',
                                        'today_buy_reinvest_count0_sell_count',
                                        'today_buy_reinvest_count1_sell_count', 'today_buy_reinvest_count2_sell_count',
                                        'today_buy_reinvest_count3_sell_count', 'today_buy_reinvest_count4_sell_count',
                                        'today_buy_reinvest_count4_sell_profitcut_count',
                                        'today_buy_reinvest_count4_sell_losscut_count',
                                        'today_buy_reinvest_count5_sell_count',
                                        'today_buy_reinvest_count5_sell_profitcut_count',
                                        'today_buy_reinvest_count5_sell_losscut_count',
                                        'today_buy_reinvest_count0_remain_count',
                                        'today_buy_reinvest_count1_remain_count',
                                        'today_buy_reinvest_count2_remain_count',
                                        'today_buy_reinvest_count3_remain_count',
                                        'today_buy_reinvest_count4_remain_count',
                                        'today_buy_reinvest_count5_remain_count'],
                               index=jango_temp['id'])

    # all_item_db 라는 테이블을 만들기 위한 self.df_all_item 데이터프레임
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
                                              'volume', 'clo3','clo5', 'clo10', 'clo20', 'clo40', 'clo60',
                                              'clo100', 'clo120', "clo3_diff_rate", "clo5_diff_rate", "clo10_diff_rate",
                                              "clo20_diff_rate", "clo40_diff_rate", "clo60_diff_rate",
                                               "clo100_diff_rate", "clo120_diff_rate"])

    # 가장 초기에 매수 했을 때 all_item_db 에 추가하는 함수 @#
    def db_to_all_item(self, min_date, df, index, code, code_name, purchase_price, yesterday_close):
        self.df_all_item.loc[0, 'code'] = code
        self.df_all_item.loc[0, 'code_name'] = code_name
        # 초기는 반드시 rate가 -0.33 이여야한다. -> 수수료, 세금을 반영함
        self.df_all_item.loc[0, 'rate'] = float(-0.33)

        if yesterday_close:
            self.df_all_item.loc[0, 'purchase_rate'] = round(
                (float(purchase_price) - float(yesterday_close)) / float(yesterday_close) * 100, 2)

        self.df_all_item.loc[0, 'purchase_price'] = purchase_price
        self.df_all_item.loc[0, 'present_price'] = purchase_price

        # #jackbot("code_name: "+ code_name + "purchase_price: "+ str(purchase_price))
        # @#이부분 바꿔야함.

        ######################종목별 모멘텀 적용######################
        # if self.avg_momentum_each_stock_on == 0:
        #     self.df_all_item.loc[0, 'holding_amount'] = int(self.invest_unit / purchase_price)
        # elif self.avg_momentum_each_stock_on == 1:
        #     self.avg_momentum_each_stock_rate_code(code)
        #     # 여기에서 함수 들어가야함.
        #     self.df_all_item.loc[0, 'holding_amount'] \
        #         = int((self.invest_unit * self.divide_invest_unit * self.avg_momentum_each_stock_rate) / purchase_price)
        test=0
        if self.rarry_setting_invest_unit_on ==1:
            avg_score=self.rarry_setting_invest_unit(code,purchase_price)
            self.df_all_item.loc[0, 'holding_amount'] = float((self.invest_unit * (avg_score/4)) / purchase_price)
        else:
            self.df_all_item.loc[0, 'holding_amount'] = float(self.invest_unit / purchase_price)

        #inbus에 사용할려고 만들었는데 안씀.
        # if code == '114800':
        #     self.df_all_item.loc[0, 'holding_amount'] = int((self.avg_momentum_apply_inbus_invest) / purchase_price)

        ######################종목별 모멘텀 적용######################

        self.df_all_item.loc[0, 'buy_date'] = min_date
        self.df_all_item.loc[0, 'item_total_purchase'] = self.df_all_item.loc[0, 'purchase_price'] * \
                                                         self.df_all_item.loc[
                                                             0, 'holding_amount']

        # 실시간으로 오늘 투자한 금액 합산
        self.today_invest_price = self.today_invest_price + self.df_all_item.loc[0, 'item_total_purchase']

        self.df_all_item.loc[0, 'chegyul_check'] = 0
        self.df_all_item.loc[0, 'id'] = 0
        # int로 넣어야 나중에 ++ 할수 있다.
        # self.df_all_item.loc[0, 'reinvest_date'] = '#'
        # self.df_all_item.loc[0, 'reinvest_count'] = int(0)
        # 다음에 투자할 금액은 invest_unit과 같은 금액이다.
        self.df_all_item.loc[0, 'invest_unit'] = self.invest_unit
        # self.df_all_item.loc[0, 'reinvest_unit'] = self.invest_unit
        self.df_all_item.loc[0, 'sell_rate'] = float(0)
        self.df_all_item.loc[0, 'yes_close'] = yesterday_close
        self.df_all_item.loc[0, 'close'] = df.loc[index, 'close']

        self.df_all_item.loc[0, 'open'] = df.loc[index, 'open']
        self.df_all_item.loc[0, 'high'] = df.loc[index, 'high']
        self.df_all_item.loc[0, 'low'] = df.loc[index, 'low']
        self.df_all_item.loc[0, 'volume'] = df.loc[index, 'volume']

        if df.loc[index, 'd1_diff_rate'] is None:
            self.df_all_item.loc[0, 'd1_diff_rate']=0
        else:
            self.df_all_item.loc[0, 'd1_diff_rate'] = float(df.loc[index, 'd1_diff_rate'])

        self.df_all_item.loc[0, 'clo3'] = df.loc[index, 'clo3']
        self.df_all_item.loc[0, 'clo5'] = df.loc[index, 'clo5']
        self.df_all_item.loc[0, 'clo10'] = df.loc[index, 'clo10']
        self.df_all_item.loc[0, 'clo20'] = df.loc[index, 'clo20']
        self.df_all_item.loc[0, 'clo40'] = df.loc[index, 'clo40']
        self.df_all_item.loc[0, 'clo60'] = df.loc[index, 'clo60']
        self.df_all_item.loc[0, 'clo100'] = df.loc[index, 'clo100']
        self.df_all_item.loc[0, 'clo120'] = df.loc[index, 'clo120']

        if df.loc[index, 'clo3_diff_rate'] is not None:
            self.df_all_item.loc[0, 'clo3_diff_rate'] = float(df.loc[index, 'clo3_diff_rate'])

        if df.loc[index, 'clo5_diff_rate'] is not None:
            self.df_all_item.loc[0, 'clo5_diff_rate'] = float(df.loc[index, 'clo5_diff_rate'])
        if df.loc[index, 'clo10_diff_rate'] is not None:
            self.df_all_item.loc[0, 'clo10_diff_rate'] = float(df.loc[index, 'clo10_diff_rate'])
        if df.loc[index, 'clo20_diff_rate'] is not None:
            self.df_all_item.loc[0, 'clo20_diff_rate'] = float(df.loc[index, 'clo20_diff_rate'])
        if df.loc[index, 'clo40_diff_rate'] is not None:
            self.df_all_item.loc[0, 'clo40_diff_rate'] = float(df.loc[index, 'clo40_diff_rate'])

        if df.loc[index, 'clo60_diff_rate'] is not None:
            self.df_all_item.loc[0, 'clo60_diff_rate'] = float(df.loc[index, 'clo60_diff_rate'])
        if df.loc[index, 'clo100_diff_rate'] is not None:
            self.df_all_item.loc[0, 'clo100_diff_rate'] = float(df.loc[index, 'clo100_diff_rate'])
        if df.loc[index, 'clo120_diff_rate'] is not None:
            self.df_all_item.loc[0, 'clo120_diff_rate'] = float(df.loc[index, 'clo120_diff_rate'])

        self.df_all_item.loc[0, 'valuation_profit'] = int(0)

        # 컬럼 중에 nan 값이 있는 경우 0으로 변경 -> 이렇게 안하면 아래 데이터베이스에 넣을 때
        # AttributeError: 'numpy.int64' object has no attribute 'translate' 에러 발생
        self.df_all_item = self.df_all_item.fillna(0)

        if self.df_all_item.loc[0, 'item_total_purchase'] != 0:
            self.df_all_item.to_sql('all_item_db', self.engine_simulator, if_exists='append')

    # 보유한 종목들을 가져오는 함수
    # sell_date가 0이면 현재 보유 중인 종목이다. 매도를 할 경우 sell_date에 매도 한 날짜가 찍힌다.
    def get_data_from_possessed_item(self):
        sql = "SELECT code_name from all_item_db where sell_date = '%s'"
        return self.engine_simulator.execute(sql % (0)).fetchall()

    # 보유 종복 수 반환 함수
    def get_count_possessed_item(self):
        sql = "SELECT count(*) from all_item_db where sell_date = '%s'"
        return self.engine_simulator.execute(sql % (0)).fetchall()[0][0]

    # 테이블의 존재 여부를 파악하는 함수
    def is_simul_table_exist(self, db_name, table_name):
        sql = "select 1 from information_schema.tables where table_schema = '%s' and table_name = '%s'"
        rows = self.engine_simulator.execute(sql % (db_name, table_name)).fetchall()
        if len(rows) == 1:
            return True
        else:
            return False

    # 일별, 분별 정산 함수
    def check_balance(self):
        # all_item_db가 없으면 check_balance 함수를 나가라
        if self.is_simul_table_exist(self.db_name, "all_item_db") == False:
            return

        # 총 수익 금액 (종목별 평가 금액 합산)
        sql = "SELECT sum(valuation_profit) from all_item_db"
        self.sum_valuation_profit = self.engine_simulator.execute(sql).fetchall()[0][0]
        print("sum_valuation_profit: " + str(self.sum_valuation_profit))

        # 전재산이라고 보면 된다. 현재 총손익 까지 고려했을 때
        self.total_invest_price = int(self.start_invest_price + self.sum_valuation_profit)

        # 현재 총 투자한 금액 계산
        sql = "select sum(item_total_purchase) from all_item_db where sell_date = '%s'"
        self.total_purchase_price = self.engine_simulator.execute(sql % (0)).fetchall()[0][0]
        if self.total_purchase_price is None:
            self.total_purchase_price = 0

        # 매도를 한 종목들 대상 수익 계산
        sql = "select sum(valuation_profit) from all_item_db where sell_date != '%s'"
        self.total_valuation_profit = self.engine_simulator.execute(sql % (0)).fetchall()[0][0]

        if self.total_valuation_profit is None:
            self.total_valuation_profit = 0

        # 현재 투자 가능한 금액(예수금) = (초기자본 + 매도한 종목의 수익) - 현재 총 투자 금액
        self.d2_deposit = int(self.start_invest_price) + int(self.total_valuation_profit) - int(self.total_purchase_price)

    # 시뮬레이팅 할 날짜를 가져 오는 함수
    # 장이 열렸던 날 들을 self.date_rows 에 담기 위해서 gs글로벌의 date값을 대표적으로 가져온 것
    def get_date_for_simul(self):
        sql = f'''        
                    select date from `btc-doge` where date >= {self.simul_start_date} group by date
                        '''
        self.date_rows = self.engine_daily_craw.execute(sql).fetchall()

    # daily_buy_list에 일자 테이블이 존재하는지 확인하는 함수
    def is_date_exist(self, date):
        print("is_date_exist 함수에 들어왔습니다!", date)
        sql = "select 1 from information_schema.tables where table_schema ='coin_daily_list' and table_name = '%s'"
        rows = self.engine_daily_buy_list.execute(sql % (date)).fetchall()
        if len(rows) == 1:
            return True
        else:
            return False

    # 잔액 체크 함수, 잔고가 있으면 True를 반환, 없으면 False를 반환
    def jango_check(self):
        if int(self.d2_deposit) >= (int(self.limit_money) + int(self.invest_unit)):
            return True
        else:
            print("돈부족해서 invest 불가!!!!!!!!")
            return False

    # 출력 함수
    def print_info(self, min_date):
        print("*&*&*&* self.simul_num :" + str(self.simul_num))
        # all_itme_db 테이블이 생성 되어 있으면 보유한 종목 수를 출력
        if self.is_simul_table_exist(self.db_name, "all_item_db"):
            print("simulating 시간: " + str(min_date))
            print("보유종목 수 !!: " + str(self.get_count_possessed_item()))

    # 특정 종목의 시작가를 가져오는 함수(일별)
    def get_now_open_price_by_date(self, code, date):
        sql = "select open from `" + date + "` where code = '%s' group by code"
        open = self.engine_daily_buy_list.execute(sql % (code)).fetchall()
        if len(open) == 1:
            return open[0][0]
        else:
            print("daily_buy_list db의 " + str(date) + " 테이블에 " + str(code) + " 가 존재하지 않는다!")
            return False
        # 테이블의 존재 여부를 파악하는 함수

    # daily_craw 데이터 베이스에서 특정 종목이 존재하는 여부를 파악하는 함수
    def is_daily_craw_table_exist(self, code_name):
        sql = "select 1 from information_schema.tables where table_schema = 'coin_daily_craw' and table_name = '%s'"
        rows = self.engine_daily_craw.execute(sql % (code_name)).fetchall()
        if len(rows) == 1:
            return True
        else:
            print("daily_craw db 에 " + str(code_name) + " 테이블이 존재하지 않는다. !! ")
            return False

    # min_craw 데이터 베이스에서 특정 종목이 존재하는 여부를 파악하는 함수
    def is_min_craw_table_exist(self, code_name):
        sql = "select 1 from information_schema.tables where table_schema = 'coin_min_craw' and table_name = '%s'"
        rows = self.engine_craw.execute(sql % (code_name)).fetchall()
        if len(rows) == 1:
            return True
        else:
            print("min_craw db 에 " + str(code_name) + " 테이블이 존재하지 않는다. !! ")
            return False

    # 분별 현재 누적 거래량 가져오는 함수
    def get_now_volume_by_min(self, code_name, min_date):
        sql = "select sum_volume from `" + code_name + "` where date = '%s' and open != 0 and volume !=0 order by sum_volume desc limit 1"
        rows = self.engine_craw.execute(sql % (min_date)).fetchall()
        if len(rows) == 1:
            return rows[0][0]
        else:
            return False

    # 분별 현재 종가 가져오는 함수
    # (close가 일별 데이터에서는 일별 종가 이지만, 분별 데이터에서의 close는 각 분별에 대한 종가를 의미
    # 즉, 1분 간격으로 변화하는 시세를 가져오는 함수
    def get_now_close_price_by_min(self, code_name, min_date):
        sql = "select close from `" + code_name + "` where date = '{}' and open != 0 and volume !=0 order by sum_volume desc limit 1"
        rows = self.engine_craw.execute(sql.format(min_date)).fetchall()

        if len(rows) == 1:
            return rows[0][0]
        else:
            return False

    # 특정 종목의 종가를 가져오는 함수
    def get_now_close_price_by_date(self, code, date):
        sql = "select close from `" + date + "` where code = '%s' group by code"
        return_price = self.engine_daily_buy_list.execute(sql % (code)).fetchall()

        if len(return_price) == 1:
            return return_price[0][0]
        else:
            return False

    # 특정 종목의 어제 종가를 가져오는 함수
    def get_yes_close_price_by_date(self, code, date):
        sql = "select close from `" + date + "` where code = '%s' group by code"
        return_price = self.engine_daily_buy_list.execute(sql % (code)).fetchall()

        if len(return_price) == 1:

            return return_price[0][0]
        else:
            return False

    # 종목의 현재 일자에 대한 주가 정보를 가져 오는 함수
    def get_now_price_by_date(self, code_name, date):
        # date='20211001'
        sql = "select d1_diff_rate, close, open, high, low, volume, clo5, clo10, clo20, clo40, clo60, clo3, clo100, clo120 from `" + date + "` where code_name = '%s' group by code"
        rows = self.engine_daily_buy_list.execute(sql % (code_name)).fetchall()

        if len(rows) == 1:
            return rows
        else:
            return False

    # all_item_db의 보유한 종목에 현재가를 실시간으로 반영하는 함수
    def db_to_all_item_present_price_update_by_min(self, code_name, now_close_price):
        sql = "update all_item_db set present_price = '%s' where code_name = '%s' and sell_date = 0"
        self.engine_simulator.execute(sql % (now_close_price, code_name))

    # 분 마다 보유한 종목의 시세를 업데이트 하는 함수
    def update_all_db_by_min(self, min_date):
        # 매분마다 possess db 가져와야한다
        possessed_code_name = self.get_data_from_possessed_item()
        for j in range(len(possessed_code_name)):
            # 현재 시간의 close 값을 가져온다.
            if self.get_min_table_use:
                now_close_price = self.get_now_close_price_by_get_min_table(possessed_code_name[j][0], min_date)
            else:
                now_close_price = self.get_now_close_price_by_min(possessed_code_name[j][0], min_date)
            # print("possessed_code_name: ", possessed_code_name[j][0], "now_close_price: ", now_close_price, "min_date", min_date)
            if now_close_price:
                self.db_to_all_item_present_price_update_by_min(possessed_code_name[j][0], now_close_price)
            else:
                # print(min_date + " / " + str(possessed_code_name[j][0]) + " 의 open_price 가 존재하지 않는다")
                continue

    # 보유 중인 종목들의 주가를 일별로 업데이트 하는 함수
    # all_item_db에서 업데이트를 한다.  option = 'ALL' 의미는 인자값을 date 하나만 줬을 때 option에는 기본값으로 ALL을 준다는 의미
    def update_all_db_by_date(self, date, option='ALL'):
        print("update_all_db_by_date 함수에 들어왔다!")
        # 현재 보유 중인 종목 들의 code_name 리스트
        possessed_code_name_list = self.get_data_from_possessed_item()
        if len(possessed_code_name_list) == 0:
            print("현재 보유 중인 종목이 없다 !!!!!")
        for j in range(len(possessed_code_name_list)):
            # 현재 주가를 가져오는 함수
            code_name = possessed_code_name_list[j][0]
            rows = self.get_now_price_by_date(code_name, date)
            if rows == False:
                continue
            #np.nan_to_num(th_cci, copy=False)
            d1_diff_rate = rows[0][0]
            close = rows[0][1]
            open = rows[0][2]
            high = rows[0][3]
            low = rows[0][4]
            volume = rows[0][5]
            clo5 = rows[0][6]
            clo10 = rows[0][7]
            clo20 = rows[0][8]
            clo40 = rows[0][9]
            clo60 = rows[0][10]
            clo3 = rows[0][11]
            clo100 = rows[0][12]
            clo120 = rows[0][13]

            if d1_diff_rate is None:
                d1_diff_rate=0
            if close is None:
                dlose=0
            if open is None:
                open=0
            if high is None:
                high=0
            if low is None:
                low=0
            if volume is None:
                volume=0
            if clo5 is None:
                clo5=0
            if clo10 is None:
                clo10=0
            if clo20 is None:
                clo20=0
            if clo40 is None:
                clo40=0
            if clo60 is None:
                clo60=0
            if clo3 is None:
                clo3=0
            if clo100 is None:
                clo100=0
            if clo120 is None:
                clo120=0

            test=0
            # 만약에 open가에 어떤 값이 있으면(True) 현재 주가를 all_item_db에 반영 하기 위해 아래 함수를 들어간다.
            if open:
                self.db_to_all_item_present_price_update(code_name, d1_diff_rate, close, open, high, low, volume, clo5,
                                                         clo10, clo20,
                                                         clo40, clo60, clo3, clo100, clo120, option)
            else:
                continue

    # 보유 중인 종목들의 주가 이외의 기타 정보들을 업데이트 하는 함수
    def update_all_db_etc(self):
        # valuation_price 업데이트
        sql = f"update all_item_db set valuation_price = round((present_price  * holding_amount) - item_total_purchase * {self.fees_rate} - present_price*holding_amount*{self.fees_rate + self.tax_rate}) where sell_date = '%s'"
        self.engine_simulator.execute(sql % (0))

        # valuation_profit, rate 업데이트 @# 수정함
        sql = "update all_item_db set rate= round((valuation_price - item_total_purchase)/item_total_purchase*100,2), valuation_profit =  valuation_price - item_total_purchase where sell_date = '%s'"
        self.engine_simulator.execute(sql % (0))

    # 언제 종목을 팔지(익절, 손절) 결정 하는 알고리즘.
    # !@##############################################################################################################################
    def get_sell_list(self, i):
        print("get_sell_list!!!")
        # 단순히 현재 보유 종목의 수익률이
        # 익절 기준 수익률(self.sell_point) 이 넘거나,
        # 손절 기준 수익률(self.losscut_point) 보다 떨어지면 파는 알고리즘
        if self.sell_list_num == 1:
            # select 할 컬럼은 항상 코드명, 수익률, 매도할 종목의 현재가, 수익(손실)금액
            # sql 첫 번째 라인은 항상 고정
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and (rate>='%s' or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(sql % (0, self.sell_point, self.losscut_point)).fetchall()

        # 5 / 20 이동 평균선 데드크로스 이거나, losscut_point(손절 기준 수익률) 이하로 떨어지면 손절하는 알고리즘
        elif self.sell_list_num == 2:
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and ((clo5 < clo20) or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(sql % (0, self.losscut_point)).fetchall()


        # 5 / 40 이동 평균선 데드크로스 이거나, losscut_point(손절 기준 수익률) 이하로 떨어지면 손절하는 알고리즘
        elif self.sell_list_num == 3:
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and ((clo5 < clo40) or rate <= '%s') group by code"

            sell_list = self.engine_simulator.execute(sql % (0, self.losscut_point)).fetchall()

        # 절대 모멘텀 전략 (특정일 전 보다 n% 이하로 떨어지면 매도) / code 버전
        elif self.sell_list_num == 4:
            sell_list = []
            sql = "SELECT code, rate, present_price, valuation_profit FROM all_item_db WHERE sell_date = 0 " \
                  "group by code"
            # realtime_daily_buy_list_temp 로 일단 위 조건의 종목을을받는다.
            sell_list_temp = self.engine_simulator.execute(sql).fetchall()
            for row in sell_list_temp:
                code = row[0]
                present_price = row[2]
                # date_rows_yesterday 가 self.date_rows[i-1] 값이다.
                # date_rows_today 가 self.date_rows[i]
                # 오늘 기준 n일 전 날짜
                date_before = self.date_rows[i - self.day_before][0]
                # 오늘 기준 n일 전 종가
                date_before_close = self.get_now_close_price_by_date(code, date_before)
                if date_before_close != 0 and date_before_close != False:
                    diff_point_calc = (present_price - date_before_close) / date_before_close * 100
                    # 현재가(present_price)가 self.day_before 일 전 종가 보다 self.diff_point(0도 가능) 만큼 떨어 지면 매도
                    if diff_point_calc < self.diff_point * (-1):
                        sell_list.append(row)

        # 절대 모멘텀 전략 (특정일 전 보다 n% 이하로 떨어지면 매도) / query 버전
        elif self.sell_list_num == 5:
            date_before = self.date_rows[i - self.day_before][0]
            sql = "SELECT ALLDB.code, ALLDB.rate, ALLDB.present_price, ALLDB.valuation_profit " \
                  "FROM all_item_db ALLDB, daily_buy_list.`" + date_before + "` BEFORE_DAY " \
                                                                             "WHERE ALLDB.code = BEFORE_DAY.code " \
                                                                             "AND ALLDB.sell_date = 0 " \
                                                                             "AND (ALLDB.present_price - BEFORE_DAY.close) / BEFORE_DAY.close * 100 < '%s' "
            sell_list = self.engine_simulator.execute(sql % (self.diff_point * (-1))).fetchall()

        # 절대 모멘텀 전략 + losscut_point 추가 (특정일 전 보다 n% 이하로 떨어지면 매도) / query 버전
        elif self.sell_list_num == 6:
            date_before = self.date_rows[i - self.day_before][0]
            sql = "SELECT ALLDB.code, ALLDB.rate, ALLDB.present_price, ALLDB.valuation_profit " \
                  "FROM all_item_db ALLDB, daily_buy_list.`" + date_before + "` BEFORE_DAY " \
                                                                             "WHERE ALLDB.code = BEFORE_DAY.code " \
                                                                             "AND ALLDB.sell_date = 0 " \
                                                                             "AND ((ALLDB.present_price - BEFORE_DAY.close) / BEFORE_DAY.close * 100 < '%s' " \
                                                                             "OR ALLDB.rate <= '%s')"
            sell_list = self.engine_simulator.execute(sql % (self.diff_point * (-1), self.losscut_point)).fetchall()

        elif self.sell_list_num == 7:
            date_yesterday = self.date_rows[i - 1][0]
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and (datediff('%s',left(buy_date,8))>180 or rate>='%s' or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(
                sql % (0, date_yesterday, self.sell_point, self.losscut_point)).fetchall()

        elif self.sell_list_num == 8:
            date_yesterday = self.date_rows[i - 1][0]
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and (datediff('%s',left(buy_date,8))>90 or rate>='%s' or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(
                sql % (0, date_yesterday, self.sell_point, self.losscut_point)).fetchall()

        elif self.sell_list_num == 9:
            date_yesterday = self.date_rows[i - 1][0]
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and (datediff('%s',left(buy_date,8))>120 or rate>='%s' or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(
                sql % (0, date_yesterday, self.sell_point, self.losscut_point)).fetchall()

        elif self.sell_list_num == 10:
            date_yesterday = self.date_rows[i - 1][0]
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and (datediff('%s',left(buy_date,8))>150 or rate>='%s' or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(
                sql % (0, date_yesterday, self.sell_point, self.losscut_point)).fetchall()

        elif self.sell_list_num == 11:
            date_yesterday = self.date_rows[i - 1][0]
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and (datediff('%s',left(buy_date,8))>270 or rate>='%s' or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(
                sql % (0, date_yesterday, self.sell_point, self.losscut_point)).fetchall()

        elif self.sell_list_num == 12:
            date_yesterday = self.date_rows[i - 1][0]
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and (datediff('%s',left(buy_date,8))>365 or rate>='%s' or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(
                sql % (0, date_yesterday, self.sell_point, self.losscut_point)).fetchall()

        elif self.sell_list_num == 13:
            date_yesterday = self.date_rows[i - 1][0]
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and (datediff('%s',left(buy_date,8))>30 or rate>='%s' or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(
                sql % (0, date_yesterday, self.sell_point, self.losscut_point)).fetchall()

        elif self.sell_list_num == 14:
            date_yesterday = self.date_rows[i - 1][0]
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and (datediff('%s',left(buy_date,8))>60 or rate>='%s' or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(
                sql % (0, date_yesterday, self.sell_point, self.losscut_point)).fetchall()

        elif self.sell_list_num == 15:
            date_yesterday = self.date_rows[i - 1][0]
            onedaybefore = self.date_rows[i - 2][0]
            sql = f'''
                    SELECT db.code, db.rate, db.present_price,db.valuation_profit,yes_sub.standard_line,onedaybefore_sub.standard_line 
                    FROM all_item_db db,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_buy_list.subindex where date='{date_yesterday}') yes_sub,
                    (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_buy_list.subindex where date='{onedaybefore}') onedaybefore_sub
                    WHERE (db.sell_date = '0')
                    and db.code=yes_sub.code
                    and db.code=onedaybefore_sub.code
                    and (
                        datediff('{date_yesterday}',left(db.buy_date,8))>60 
                        or db.rate>='{self.sell_point}' 
                        or db.rate <= '{self.losscut_point}'
                        or (onedaybefore_sub.standard_line > yes_sub.standard_line)
                        ) 
                    group by db.code
               '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 16:
            date_yesterday = self.date_rows[i - 1][0]
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and ((datediff('%s',left(buy_date,8))>14 and rate > -5) or rate>='%s' or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(
                sql % (0, date_yesterday, self.sell_point, self.losscut_point)).fetchall()

        elif self.sell_list_num == 17:
            if i < 2:
                sell_list = []
                pass
            else:
                date_yesterday = self.date_rows[i - 1][0]
                onedaybefore = self.date_rows[i - 2][0]
                sql = f'''
                        SELECT db.code, db.rate, db.present_price,db.valuation_profit,
                        ma1.code,ma1.ma19,ma1.ma20,ma2.code,ma2.ma19,ma2.ma20
                        FROM all_item_db db,
                        (select code,ma19,ma20 from daily_buy_list.subindex where date ='{date_yesterday}') ma1,
                        (select code,ma19,ma20 from daily_buy_list.subindex where date ='{onedaybefore}') ma2
                        WHERE (db.sell_date = '0')
                        and db.code=ma1.code
                        and db.code=ma2.code
                        and (
                            datediff('{date_yesterday}',left(db.buy_date,8))>60 
                            or db.rate>='{self.sell_point}' 
                            or db.rate <= '{self.losscut_point}'
                            or ( ma1.ma19 < ma1.ma20 and ma2.ma19 > ma2.ma20)
                            ) 
                        group by db.code
                   '''
                sell_list = self.engine_simulator.execute(sql).fetchall()



        elif self.sell_list_num == 301:
            date_yesterday = self.date_rows[i - 1][0]
            sell_list = []

            if len(self.sell_list_condition) > 0:
                for code in self.sell_list_condition:
                    sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                          "and (datediff('%s',left(buy_date,8))>90 or rate>='%s' or rate <= '%s' or code='%s') group by code"
                    sell_list_temp = self.engine_simulator.execute(
                        sql % (0, date_yesterday, self.sell_point, self.losscut_point, code)).fetchall()
                    sell_list.append(sell_list_temp)
                    # 이 아래는 2차원 list를 1차원으로 바꾸는 방법이다.

            else:
                sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                      "and (datediff('%s',left(buy_date,8))>90 or rate>='%s' or rate <= '%s') group by code"
                sell_list_temp = self.engine_simulator.execute(
                    sql % (0, date_yesterday, self.sell_point, self.losscut_point)).fetchall()
                sell_list.append(sell_list_temp)

            sell_list = sum(sell_list, [])

        elif self.sell_list_num == 18:
            date_yesterday = self.date_rows[i - 1][0]
            sql = "SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db, " \
                  "(select code,switch_line,standard_line,backspan,prespan1,prespan2 from subindex where date='{backspan_day}') ichimoku" \
                  "WHERE db.code=ichimoku.code" \
                  "and (db.sell_date = '%s') " \
                  "and (datediff('%s',left(db.buy_date,8))>90 or db.rate>='%s' or db.rate <= '%s' or" \
                  "(datediff('%s',left(db.buy_date,8))>30 and ichimoku.prespan1<db.close)) group by code"
            sell_list = self.engine_simulator.execute(
                sql % (0, date_yesterday, self.sell_point, self.losscut_point)).fetchall()

        elif self.sell_list_num == 19:  # 한달뒤에 선스1 아래로 가면 손절함. 30일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>30 and db.close<ichimoku.prespan1)) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 20:  # 한달뒤에 선스1 아래로 가면 손절함. 10일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>10 and db.close<ichimoku.prespan1)) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 21:  # 한달뒤에 선스1 아래로 가면 손절함. 20일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>20 and db.close<ichimoku.prespan1)) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 22:  # 한달뒤에 선스1 아래로 가면 손절함. 40일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>40 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>40 and db.close<ichimoku.prespan1)) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 23:  # 한달뒤에 선스1 아래로 가면 손절함. 50일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>50 and db.close<ichimoku.prespan1)) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 24:  # 한달뒤에 선스1 아래로 가면 손절함. 60일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>60 and db.close<ichimoku.prespan1)) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()


        elif self.sell_list_num == 25:  # 한달뒤에 선스1 아래로 가면 손절함. 70일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>70 and db.close<ichimoku.prespan1)) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 26:  # 한달뒤에 선스1 아래로 가면 손절함. 80일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>80 and db.close<ichimoku.prespan1)) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()


        elif self.sell_list_num == 27:  # 한달뒤에 선스1의 5% 아래로 가면 손절함. 10일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>10 and db.close<(ichimoku.prespan1*0.95))) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 28:  # 한달뒤에 선스1의 5% 아래로 가면 손절함. 20일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>20 and db.close<(ichimoku.prespan1*0.95))) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 29:  # 한달뒤에 선스1의 5% 아래로 가면 손절함. 30일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>30 and db.close<(ichimoku.prespan1*0.95))) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 30:  # 한달뒤에 선스1의 5% 아래로 가면 손절함. 40일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>40 and db.close<(ichimoku.prespan1*0.95))) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 31:  # 한달뒤에 선스1의 5% 아래로 가면 손절함. 50일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>50 and db.close<(ichimoku.prespan1*0.95))) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 32:  # 한달뒤에 선스1의 5% 아래로 가면 손절함. 60일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>60 and db.close<(ichimoku.prespan1*0.95))) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 33:  # 한달뒤에 선스1의 5% 아래로 가면 손절함. 70일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>70 and db.close<(ichimoku.prespan1*0.95))) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 34:  # 한달뒤에 선스1의 5% 아래로 가면 손절함. 80일
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db,
                  (select code,switch_line,standard_line,backspan,prespan1,prespan2 from daily_subindex.`{date_yesterday}`) ichimoku
                  WHERE db.code=ichimoku.code
                  and (db.sell_date = 0) 
                  and (datediff('{date_yesterday}',left(db.buy_date,8))>90 or db.rate>={self.sell_point} or db.rate <= {self.losscut_point} or
                  (datediff('{date_yesterday}',left(db.buy_date,8))>80 and db.close<(ichimoku.prespan1*0.95))) group by code'''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 35:  # PER, PBR, PCR, PSR관련 알고리즘.
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit FROM all_item_db db
                  WHERE (db.sell_date = 0) and EXISTS(select * from etc.`date` where date = '{date_yesterday}')                  
                '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 36:  # PER, PBR, PCR, PSR관련 알고리즘.
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price FROM all_item_db db
                  WHERE (db.sell_date = 0) and (EXISTS(select * from etc.`date` where date = '{date_yesterday}')
                    or db.rate <= {self.losscut_point})                  
                '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 37:  # PER, PBR, PCR, PSR관련 알고리즘.
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price FROM all_item_db db
                  WHERE (db.sell_date = 0) and (EXISTS(select * from etc.`date` where date = '{date_yesterday}')
                    or db.rate <= {self.losscut_point} or db.rate>={self.sell_point})                  
                '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 921:  # PER, PBR, PCR, PSR관련 알고리즘.
            date_today = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price FROM all_item_db db
                  WHERE (db.sell_date = 0) and (datediff('{date_today}',left(db.buy_date,8))>=29
                    or db.rate <= {self.losscut_point})                  
                '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 1509:  # PER, PBR, PCR, PSR관련 알고리즘.
            date_today = self.date_rows[i - 1][0]

            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price FROM all_item_db db
                  WHERE (db.sell_date = 0) and (
                    datediff('{date_today}',left(db.buy_date,8))>=29
                    or {self.mdd_value}>={self.final_mdd_losscut}
                    or db.rate <= {self.losscut_point})                  
                '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

            if self.mdd_value >= self.final_mdd_losscut:
                self.mdd_sell_check = 1



        elif self.sell_list_num == 1604:  # PER, PBR, PCR, PSR관련 알고리즘.
            date_today = self.date_rows[i - 1][0]
            # self.today

            try:
                sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price FROM all_item_db db
                      WHERE (db.sell_date = 0) and (
                        (datediff('{date_today}',left(db.buy_date,8))>=29                    
                         and db.code not in (select code from rdb_overlap_list)   )
                        or db.rate <= {self.losscut_point}       
                        )       
                    '''
                sell_list = self.engine_simulator.execute(sql).fetchall()
            except:
                sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price FROM all_item_db db
                                      WHERE (db.sell_date = 0) and (
                                        (datediff('{date_today}',left(db.buy_date,8))>=30                    
                                           )
                                        or db.rate <= {self.losscut_point}       
                                        )       
                                    '''
                sell_list = self.engine_simulator.execute(sql).fetchall()

            if self.mdd_value >= self.final_mdd_losscut:
                self.mdd_sell_check = 1


        elif self.sell_list_num == 1644:  # PER, PBR, PCR, PSR관련 알고리즘.
            date_today = self.date_rows[i - 1][0]
            # self.today

            try:
                reval_sql = f'''SELECT reval_date 
                                                      FROM setting_data
                                                      limit 1
                                                '''
                reval_date = self.engine_simulator.execute(reval_sql).fetchall()[0][0]
                sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price 
                          FROM all_item_db db
                          WHERE (db.sell_date = 0) and (
                            (datediff('{self.today}',left(db.buy_date,8))>0
                             and {reval_date} = {self.today}           
                             and db.code not in (select code from rdb_overlap_list)   )
                            or db.rate <= {self.losscut_point}       
                            )       
                    '''
                sell_list = self.engine_simulator.execute(sql).fetchall()
            except:

                sell_list = []

        elif self.sell_list_num == 1802:  # 중첩 대상 팔지 않기.
            date_yesterday = self.date_rows[i - 1][0]
            realtime_daily_len_sql = f'''
                    select count(*) from realtime_daily_buy_list
            '''
            realtime_daily_len_rows = self.engine_simulator.execute(realtime_daily_len_sql).fetchall()[0][0]

            # self.etc_method(self.total_invest_price)
            test = 0

            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price 
                        FROM all_item_db db
                  WHERE (db.sell_date = 0) and (
                    (
                    (
                    (datediff({self.today},left(db.buy_date,8))>0 and db.rate >{self.sell_point})
                    or db.rate <= {self.losscut_point} or
                    datediff({self.today},left(db.buy_date,8))>{self.revalancing_date}
                    )
                    and db.code not in (select code from rdb_overlap_list)
                    )
                    or {realtime_daily_len_rows} <= {self.realtime_daily_buy_list_count}
                    )

                '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 1899:  # 중첩 대상 팔지 않기.
            date_yesterday = self.date_rows[i - 1][0]
            realtime_daily_len_sql = f'''
                    select count(*) from realtime_daily_buy_list
            '''
            realtime_daily_len_rows = self.engine_simulator.execute(realtime_daily_len_sql).fetchall()[0][0]

            # self.etc_method(self.total_invest_price)
            test = 0

            try:
                reval_sql = f'''SELECT reval_date 
                                                      FROM jackbot1668.setting_data
                                                      limit 1
                                                '''
                reval_date = self.engine_simulator.execute(reval_sql).fetchall()[0][0]

                lisk_sensing_sql = f'''
                                                    select count(*) from all_item_db
                                                    where sell_date=0 and rate < {self.lisk_sensing_rate}
                                            '''
                lisk_sensing = self.engine_simulator.execute(lisk_sensing_sql).fetchall()[0][0]

                sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price 
                          FROM all_item_db db
                          WHERE (db.sell_date = 0) and (
                            (datediff('{self.today}',left(db.buy_date,8))>0
                             and {reval_date} = {self.today}           
                             and db.code not in (select code from rdb_overlap_list)   )
                            or db.rate <= {self.losscut_point}  
                            or {lisk_sensing} > {self.lisk_sensing_count}      
                            )       
                    '''
                sell_list = self.engine_simulator.execute(sql).fetchall()
            except:

                sell_list = []

        elif self.sell_list_num == 2073:  # PER, PBR, PCR, PSR관련 알고리즘.
            # date_today = self.date_rows[i - 1][0]
            # self.today
            try:
                realtime_daily_len_sql = f'''
                                    select realtime_daily_buy_list_length from setting_data
                            '''
                self.realtime_daily_buy_list_length = \
                self.engine_simulator.execute(realtime_daily_len_sql).fetchall()[0][0]

            except:
                self.realtime_daily_buy_list_length = 100

            test = 0
            try:

                reval_sql = f'''select count(date) from daily_craw.`gs글로벌` 
                                where left(date,6)=left('{self.today}',6)
                                                '''
                reval_date = self.engine_simulator.execute(reval_sql).fetchall()[0][0]

                sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price 
                          FROM all_item_db db
                          WHERE (db.sell_date = 0) and (
                            (datediff('{self.today}',left(db.buy_date,8))>0
                             and {reval_date}=0         
                             and db.code not in (select code from rdb_overlap_list)   )
                            or db.rate <= {self.losscut_point}   
                            or {self.realtime_daily_buy_list_length} <= {self.buy_list_length_limit}    
                            )       
                    '''
                sell_list = self.engine_simulator.execute(sql).fetchall()
            except:
                sell_list = []

        elif self.sell_list_num == 2091:  # 중첩 대상 팔지 않기.
            date_yesterday = self.date_rows[i - 1][0]
            try:
                realtime_daily_len_sql = f'''
                        select realtime_daily_buy_list_length from setting_data
                '''
                realtime_daily_len_rows = self.engine_simulator.execute(realtime_daily_len_sql).fetchall()[0][0]
            except:
                realtime_daily_len_rows = 100
            # self.etc_method(self.total_invest_price)
            test = 0
            try:
                sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price 
                            FROM all_item_db db
                      WHERE (db.sell_date = 0) and (
                        (
                        (
                        (datediff({self.today},left(db.buy_date,8))>0 and db.rate >{self.sell_point})
                        or db.rate <= {self.losscut_point} or
                        datediff({self.today},left(db.buy_date,8))>{self.revalancing_date}
                        )
                        and db.code not in (select code from rdb_overlap_list)
                        )
                        or {realtime_daily_len_rows} <= {self.realtime_daily_buy_list_count}
                        )

                    '''
                sell_list = self.engine_simulator.execute(sql).fetchall()
            except:
                sell_list = []

        elif self.sell_list_num == 2224:  # 중첩 대상 팔지 않기.
            date_yesterday = self.date_rows[i - 1][0]
            try:
                realtime_daily_len_sql = f'''
                        select realtime_daily_buy_list_length from setting_data
                '''
                realtime_daily_len_rows = self.engine_simulator.execute(realtime_daily_len_sql).fetchall()[0][0]
            except:
                realtime_daily_len_rows = 100
            # self.etc_method(self.total_invest_price)
            test = 0
            try:
                sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price 
                            FROM all_item_db db
                      WHERE (db.sell_date = 0) and (
                        (
                        (
                        (datediff({self.today},left(db.buy_date,8))>0 and db.rate >{self.sell_point})
                        or db.rate <= {self.losscut_point} or
                        datediff({self.today},left(db.buy_date,8))>{self.revalancing_date}
                        )
                        and db.code not in (select code from rdb_overlap_list)
                        )
                        or {realtime_daily_len_rows} <= {self.realtime_daily_buy_list_count}
                        )

                    '''
                sell_list = self.engine_simulator.execute(sql).fetchall()
            except:
                sell_list = []

        elif self.sell_list_num == 2248:  # 중첩 대상 팔지 않기.
            date_yesterday = self.date_rows[i - 1][0]
            try:
                realtime_daily_len_sql = f'''
                        select realtime_daily_buy_list_length from setting_data
                '''
                realtime_daily_len_rows = self.engine_simulator.execute(realtime_daily_len_sql).fetchall()[0][0]
            except:
                realtime_daily_len_rows = 100
            # self.etc_method(self.total_invest_price)
            test = 0
            try:
                sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price 
                            FROM all_item_db db
                      WHERE (db.sell_date = 0) and (
                        (
                        (
                        (datediff({self.today},left(db.buy_date,8))>0 and db.rate >{self.sell_point})
                        or db.rate <= {self.losscut_point} or
                        datediff({self.today},left(db.buy_date,8))>{self.revalancing_date}
                        )
                        and db.code not in (select code from rdb_overlap_list)
                        )
                        or {realtime_daily_len_rows} <= {self.realtime_daily_buy_list_count}
                        )

                    '''
                sell_list = self.engine_simulator.execute(sql).fetchall()
            except:
                sell_list = []

        elif self.sell_list_num == 5001:

            sql = f'''SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = 0) 
                  and (rate>={self.sell_point} or rate <= {self.losscut_point}) group by code'''
            test=0
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 5002:  # 중첩 대상 팔지 않기.

            try:
                sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price 
                            FROM all_item_db db
                      WHERE (db.sell_date = 0) and 
                        (
                        (
                        (datediff({self.today},left(db.buy_date,8))>0 and db.rate >{self.sell_point})
                        or db.rate <= {self.losscut_point} or
                        datediff({self.today},left(db.buy_date,8))>{self.revalancing_date}
                        )
                        and db.code not in (select code from rdb_overlap_list)
                        ) 
                        

                    '''
                sell_list = self.engine_simulator.execute(sql).fetchall()
            except:
                sell_list = []

        elif self.sell_list_num == 5004:  # 중첩 대상 팔지 않기.

            try:
                sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price 
                            FROM all_item_db db
                      WHERE (db.sell_date = 0) and 
                        (right({self.min_date_for_rarry},4)='0900'
                        or right({self.min_date_for_rarry},4)='0901'
                        or right({self.min_date_for_rarry},4)='0902'
                        or right({self.min_date_for_rarry},4)='0903'
                        or right({self.min_date_for_rarry},4)='0904'
                            )


                    '''
                sell_list = self.engine_simulator.execute(sql).fetchall()
            except:
                sell_list = []

        elif self.sell_list_num == 5011:  # 중첩 대상 팔지 않기.

            try:
                sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price 
                            FROM all_item_db db
                      WHERE (db.sell_date = 0) and (
                        (right({self.min_date_for_rarry},4)='0900'
                        or right({self.min_date_for_rarry},4)='0901'
                        or right({self.min_date_for_rarry},4)='0902'
                        or right({self.min_date_for_rarry},4)='0903'
                        or right({self.min_date_for_rarry},4)='0904'
                            ) 
                        )
                    '''
                sell_list = self.engine_simulator.execute(sql).fetchall()
            except:
                sell_list = []

            # or (db.rate >= {self.sell_point}
            #     or db.rate <= {self.losscut_point})


        elif self.sell_list_num == 53:  # 중첩 대상 팔지 않기.
            date_yesterday = self.date_rows[i - 1][0]

            # self.etc_method(self.total_invest_price)

            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit,db.purchase_price 
                        FROM all_item_db db
                  WHERE (db.sell_date = 0) and (
                    (
                    (EXISTS(select * from etc.`date` where date = '{date_yesterday}')
                    or db.rate <= {self.losscut_point})
                    and db.code not in (select code from rdb_overlap_list)
                    )  or {self.mdd_value}>={self.final_mdd_losscut}
                    )

                '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

            test = 0

            if self.mdd_value >= self.final_mdd_losscut:
                self.mdd_sell_check = 1






        ##################################################################################################################################################################################################################
        else:
            print(
                f"{self.simul_num}번 알고리즘에 대한 self.sell_list_num 설정이 비었습니다. variable_setting 함수에서 self.sell_list_num을 확인해주세요.")
            sys.exit(1)

        return sell_list

    # 실제로 매도를 하는 함수 (매도 한 결과를 all_item_db에 반영)
    def sell_send_order(self, min_date, sell_price, sell_rate, code):
        # print("sell send order")
        sql = "UPDATE all_item_db SET sell_date= '%s', sell_price ='%s' ,sell_rate ='%s' WHERE code='%s' and sell_date = '%s' " \
              "ORDER BY buy_date desc LIMIT 1"
        self.engine_simulator.execute(sql % (min_date, sell_price, sell_rate, code, 0))
        # 매도 후 정산
        self.check_balance()

    # 매도를 하기 위한 함수
    def auto_trade_sell_stock(self, date, _i):
        # 매도 할 리스트를 가져오는 함수
        sell_list = self.get_sell_list(_i)
        for i in range(len(sell_list)):
            # 코드명
            get_sell_code = sell_list[i][0]
            # 수익률
            get_sell_rate = sell_list[i][1]
            # 종목의 현재 주가
            get_present_price = sell_list[i][2]
            # 수익(손실) 금액 (종목의 순수익, 순손실 금액)
            valuation_profit = sell_list[i][3]

            print("get_sell_rate : ", get_sell_rate)

            if get_sell_rate < 0:
                print("손절 매도!!!!$$$$$$$$$$$ 수익: " + str(valuation_profit) + " / 수익률 : " + str(
                    get_sell_rate) + " / 종목코드: " + str(get_sell_code) + " $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

            else:
                print("익절 매도!!!!$$$$$$$$$$$ 수익: " + str(valuation_profit) + " / 수익률 : " + str(
                    get_sell_rate) + " / 종목코드: " + str(get_sell_code) + " $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

            # 실제로 매도를 하는 함수 (매도 한 결과를 all_item_db에 반영)
            self.sell_send_order(date, get_present_price, get_sell_rate, get_sell_code)

    # 몇개의 주를 살지 계산해주는 함수
    def buy_num_count(self, invest_unit, present_price):
        # jackbot("******************* buy_num_count!!!")
        return int(int(invest_unit) / int(present_price))

    # 금일 수익 계산 함수
    def get_today_profit(self, date):
        # jackbot("******************* get_today_profit!!!")
        sql = "SELECT sum(valuation_profit) from all_item_db where sell_date like '%s'"
        return self.engine_simulator.execute(sql % ("%%" + date + "%%")).fetchall()[0][0]

    # 총 매입금액 계산 함수
    def get_sum_item_total_purchase(self):

        # jackbot("******************* get_sum_item_total_purchase!!!")
        sql = "SELECT sum(item_total_purchase) from all_item_db where sell_date = '%s'"
        rows = self.engine_simulator.execute(sql % (0)).fetchall()[0][0]
        if rows is not None:
            return rows
        else:
            return 0

    # 총평가금액 계산 함수
    def get_sum_valuation_price(self):
        sql = "SELECT sum(valuation_price) from all_item_db where sell_date = '%s'"
        rows = self.engine_simulator.execute(sql % (0)).fetchall()[0][0]
        if rows is not None:
            return rows
        else:
            return 0

    # 오늘 일자 익절 종목 수
    def get_today_profitcut_count(self, date):
        sql = "SELECT count(code) from all_item_db where sell_date like '%s' and sell_rate>='%s'"
        return self.engine_simulator.execute(sql % ("%%" + date + "%%", 0)).fetchall()[0][0]

    # 오늘 일자 손절 종목 수
    def get_today_losscut_count(self, date):
        sql = "SELECT count(code) from all_item_db where sell_date like '%s' and sell_rate<'%s'"
        return self.engine_simulator.execute(sql % ("%%" + date + "%%", 0)).fetchall()[0][0]

    # 오늘 일자 매도금액
    def get_sum_today_sell_price(self, date):
        sql = "SELECT sum(valuation_price) from all_item_db where sell_date like '%s'"
        return self.engine_simulator.execute(sql % ("%%" + date + "%%")).fetchall()[0][0]

    # 오늘 일자 익절 종목 대상 수익
    def get_sum_today_profitcut(self, date):
        sql = "SELECT sum(valuation_profit) from all_item_db where sell_date like '%s' and valuation_profit >= '%s' "
        return self.engine_simulator.execute(sql % ("%%" + date + "%%", 0)).fetchall()[0][0]

    # 오늘 일자 손절 종목 대상 손실 금액
    def get_sum_today_losscut(self, date):
        sql = "SELECT sum(valuation_profit) from all_item_db where sell_date like '%s' and valuation_profit < '%s' "
        return self.engine_simulator.execute(sql % ("%%" + date + "%%", 0)).fetchall()[0][0]

    # 총 익절 종목 대상 수익
    def get_sum_total_profitcut(self):
        sql = "SELECT sum(valuation_profit) from all_item_db where sell_date != 0 and valuation_profit >= '%s' "
        return self.engine_simulator.execute(sql % (0)).fetchall()[0][0]

    # 총 손절 종목 대상 손실 금액
    def get_sum_total_losscut(self):
        sql = "SELECT sum(valuation_profit) from all_item_db where sell_date != 0 and valuation_profit < '%s' "
        return self.engine_simulator.execute(sql % (0)).fetchall()[0][0]

    # 전체 일자 익절한 종목 수
    def get_sum_total_profitcut_count(self):
        # jackbot("******************* get_sum_total_profitcut_count!!!")
        sql = "select count(code) from all_item_db where sell_date != 0 and valuation_profit >= '%s'"
        return self.engine_simulator.execute(sql % (0)).fetchall()[0][0]

    # 전체 일자 손절한 종목 수
    def get_sum_total_losscut_count(self):
        # jackbot("******************* get_sum_total_losscut_count!!!")
        sql = "select count(code) from all_item_db where sell_date != 0 and valuation_profit < '%s' "
        return self.engine_simulator.execute(sql % (0)).fetchall()[0][0]

    # jango_data의 저장 된 일자 반환 함수
    def get_len_jango_data_date(self):

        sql = "select date from jango_data"
        rows = self.engine_simulator.execute(sql).fetchall()

        return len(rows)

    # 총 보유한 종목 수
    def get_total_possess_count(self):
        # jackbot("******************* get_total_possess_count!!!")
        sql = "select count(code) from all_item_db where sell_date = '%s'"
        return self.engine_simulator.execute(sql % (0)).fetchall()[0][0]

    # jango_data 테이블을 만드는 함수
    def db_to_jango(self, date_rows_today, date_rows_yesterday):
        #원래는 다 date_rows_today였음!!!!!!!!!!!!!!!!
        # 정산 함수
        self.check_balance()
        if self.is_simul_table_exist(self.db_name, "all_item_db") == False:
            return

        self.jango.loc[0, 'date'] =date_rows_today # date_rows_yesterday # date_rows_today

        # self.jango.loc[0, 'total_asset'] = self.total_invest_price - self.loan_money
        self.jango.loc[0, 'today_profit'] = self.get_today_profit(date_rows_today)
        self.jango.loc[0, 'sum_valuation_profit'] = self.sum_valuation_profit
        self.jango.loc[0, 'total_profit'] = self.total_valuation_profit

        self.jango.loc[0, 'total_invest'] = self.total_invest_price
        self.jango.loc[0, 'd2_deposit'] = self.d2_deposit
        # 총매입금액
        self.jango.loc[0, 'sum_item_total_purchase'] = self.get_sum_item_total_purchase()

        # 총평가금액
        self.jango.loc[0, 'total_evaluation'] = self.get_sum_valuation_price()
        self.jango.loc[0, 'today_profitcut_count'] = self.get_today_profitcut_count(date_rows_today)
        self.jango.loc[0, 'today_losscut_count'] = self.get_today_losscut_count(date_rows_today)

        self.jango.loc[0, 'today_invest_price'] = float(self.today_invest_price)

        # self.jango.loc[0, 'today_reinvest_price'] = self.today_reinvest_price
        self.jango.loc[0, 'today_sell_price'] = self.get_sum_today_sell_price(date_rows_today)

        # 오늘 기준 수익률 (키움 잔고 상단에 뜨는 수익률) -0.33 (수수료, 세금)
        try:
            self.jango.loc[0, 'today_rate'] = round(
                (float(self.jango.loc[0, 'total_evaluation']) - float(
                    self.jango.loc[0, 'sum_item_total_purchase'])) / float(
                    self.jango.loc[0, 'sum_item_total_purchase']) * 100 - 0.33, 2)
        except ZeroDivisionError as e:
            pass

        # self.jango.loc[0, 'volume_limit'] = self.volume_limit

        # self.jango.loc[0, 'reinvest_point'] = self.reinvest_point
        self.jango.loc[0, 'sell_point'] = self.sell_point
        # self.jango.loc[0, 'max_reinvest_count'] = self.max_reinvest_count
        self.jango.loc[0, 'invest_limit_rate'] = self.invest_limit_rate
        self.jango.loc[0, 'invest_unit'] = self.invest_unit

        self.jango.loc[0, 'limit_money'] = self.limit_money
        self.jango.loc[0, 'total_possess_count'] = self.get_total_possess_count()
        self.jango.loc[0, 'today_buy_list_count'] = self.len_df_realtime_daily_buy_list
        # self.jango.loc[0, 'today_reinvest_count'] = self.get_today_reinvest_count(date_rows_today)
        # self.jango.loc[0, 'today_cant_reinvest_count'] = self.get_today_cant_reinvest_count()

        # 오늘 익절한 금액
        self.jango.loc[0, 'today_profitcut'] = self.get_sum_today_profitcut(date_rows_today)
        # 오늘 손절한 금액
        self.jango.loc[0, 'today_losscut'] = self.get_sum_today_losscut(date_rows_today)

        # 지금까지 총 익절한 금액
        self.jango.loc[0, 'total_profitcut'] = self.get_sum_total_profitcut()

        # 지금까지 총 손절한 금액
        self.jango.loc[0, 'total_losscut'] = self.get_sum_total_losscut()

        # 지금까지 총 익절한놈들
        self.jango.loc[0, 'total_profitcut_count'] = self.get_sum_total_profitcut_count()

        # 지금까지 총 손절한 놈들

        self.jango.loc[0, 'total_losscut_count'] = self.get_sum_total_losscut_count()

        self.jango.loc[0, 'today_buy_count'] = 0
        self.jango.loc[0, 'today_buy_total_sell_count'] = 0
        self.jango.loc[0, 'today_buy_total_possess_count'] = 0

        self.jango.loc[0, 'today_buy_today_profitcut_count'] = 0

        self.jango.loc[0, 'today_buy_today_losscut_count'] = 0
        self.jango.loc[0, 'today_buy_total_profitcut_count'] = 0

        self.jango.loc[0, 'today_buy_total_losscut_count'] = 0
        # self.jango.loc[0, 'today_buy_reinvest_count0_sell_count'] = 0
        #
        # self.jango.loc[0, 'today_buy_reinvest_count1_sell_count'] = 0
        # self.jango.loc[0, 'today_buy_reinvest_count2_sell_count'] = 0
        # self.jango.loc[0, 'today_buy_reinvest_count3_sell_count'] = 0
        # self.jango.loc[0, 'today_buy_reinvest_count4_sell_count'] = 0
        #
        # self.jango.loc[0, 'today_buy_reinvest_count4_sell_profitcut_count'] = 0
        # self.jango.loc[0, 'today_buy_reinvest_count4_sell_losscut_count'] = 0
        #
        # self.jango.loc[0, 'today_buy_reinvest_count5_sell_count'] = 0
        # self.jango.loc[0, 'today_buy_reinvest_count5_sell_profitcut_count'] = 0
        # self.jango.loc[0, 'today_buy_reinvest_count5_sell_losscut_count'] = 0
        #
        # self.jango.loc[0, 'today_buy_reinvest_count0_remain_count'] = 0
        #
        # self.jango.loc[0, 'today_buy_reinvest_count1_remain_count'] = 0
        # self.jango.loc[0, 'today_buy_reinvest_count2_remain_count'] = 0
        # self.jango.loc[0, 'today_buy_reinvest_count3_remain_count'] = 0
        # self.jango.loc[0, 'today_buy_reinvest_count4_remain_count'] = 0
        # self.jango.loc[0, 'today_buy_reinvest_count4_remain_count'] = 0
        # self.jango.loc[0, 'today_buy_reinvest_count5_remain_count'] = 0

        # # 데이터베이스에 테이블이 존재할 때 수행 동작을 지정한다.
        # 'fail', 'replace', 'append' 중 하나를 사용할 수 있는데 기본값은 'fail'이다.
        # 'fail'은 데이터베이스에 테이블이 있다면 아무 동작도 수행하지 않는다.
        # 'replace'는 테이블이 존재하면 기존 테이블을 삭제하고 새로 테이블을 생성한 후 데이터를 삽입한다.
        # 'append'는 테이블이 존재하면 데이터만을 추가한다.
        self.jango.to_sql('jango_data', self.engine_simulator, if_exists='append')

        #     # today_earning_rate
        sql = "update jango_data set today_earning_rate =round(today_profit / total_invest * '%s',2) WHERE date='%s'"
        # rows[i][0] 하는 이유는 rows[i]는 튜플( )로 나온다 그 튜플의 원소를 꺼내기 위해 rows[i]에 [0]을 추가
        self.engine_simulator.execute(sql % (100, date_rows_today))

    # 시뮬레이션이 다 끝났을 때 마지막 jango_data 정리
    def arrange_jango_data(self):
        if self.engine_simulator.dialect.has_table(self.engine_simulator, 'jango_data'):
            len_date = self.get_len_jango_data_date()
            sql = "select date from jango_data"
            rows = self.engine_simulator.execute(sql).fetchall()

            print('jango_data 최종 정산 중...')
            # 위에 전체
            for i in range(len_date):
                # today_buy_count
                sql = "UPDATE jango_data SET today_buy_count=(select count(*) from (select code from all_item_db where buy_date like '%s') b) WHERE date='%s'"
                # date 하는 이유는 rows[i]는 튜플로 나온다 그 튜플의 원소를 꺼내기 위해 [0]을 추가
                self.engine_simulator.execute(sql % ("%%" + str(rows[i][0]) + "%%", rows[i][0]))

                # today_buy_total_sell_count ( 익절, 손절 포함)
                sql = "UPDATE jango_data SET today_buy_total_sell_count=(select count(*) from (select code from all_item_db a where buy_date like '%s' and (a.sell_date != 0) group by code ) b) WHERE date='%s'"
                self.engine_simulator.execute(sql % ("%%" + rows[i][0] + "%%", rows[i][0]))

                # today_buy_total_possess_count 오늘 사고 계속 가지고 있는것들
                sql = "UPDATE jango_data SET today_buy_total_possess_count=(select count(*) from (select code from all_item_db a where buy_date like '%s' and a.sell_date = '%s' group by code ) b) WHERE date='%s'"
                self.engine_simulator.execute(sql % ("%%" + rows[i][0] + "%%", 0, rows[i][0]))

                sql = "UPDATE jango_data SET today_buy_today_profitcut_count=(select count(*) from (select code from all_item_db where buy_date like '%s' and sell_date like '%s' and (sell_rate >= '%s' ) group by code ) b) WHERE date='%s'"
                self.engine_simulator.execute(sql % ("%%" + rows[i][0] + "%%", "%%" + rows[i][0] + "%%", 0, rows[i][0]))

                sql = "UPDATE jango_data SET today_buy_today_profitcut_rate= round(today_buy_today_profitcut_count /today_buy_count *100,2) WHERE date = '%s'"
                self.engine_simulator.execute(sql % (rows[i][0]))

                sql = "UPDATE jango_data SET today_buy_today_losscut_count=(select count(*) from (select code from all_item_db where buy_date like '%s' and sell_date like '%s' and sell_rate < '%s'  group by code ) b) WHERE date='%s'"
                self.engine_simulator.execute(sql % ("%%" + rows[i][0] + "%%", "%%" + rows[i][0] + "%%", 0, rows[i][0]))

                sql = "UPDATE jango_data SET today_buy_today_losscut_rate=round(today_buy_today_losscut_count /today_buy_count *100,2) WHERE date = '%s'"
                self.engine_simulator.execute(sql % (rows[i][0]))

                sql = "UPDATE jango_data SET today_buy_total_profitcut_count=(select count(*) from (select code from all_item_db where buy_date like '%s' and sell_rate >= '%s'  group by code ) b) WHERE date='%s'"
                self.engine_simulator.execute(sql % ("%%" + rows[i][0] + "%%", 0, rows[i][0]))

                sql = "UPDATE jango_data SET today_buy_total_profitcut_rate=round(today_buy_total_profitcut_count /today_buy_count *100,2) WHERE date = '%s'"
                self.engine_simulator.execute(sql % (rows[i][0]))

                sql = "UPDATE jango_data SET today_buy_total_losscut_count=(select count(*) from (select code from all_item_db where buy_date like '%s' and sell_rate < '%s'  group by code ) b) WHERE date='%s'"
                self.engine_simulator.execute(sql % ("%%" + rows[i][0] + "%%", 0, rows[i][0]))

                sql = "UPDATE jango_data SET today_buy_total_losscut_rate=round(today_buy_total_losscut_count/today_buy_count*100,2) WHERE date = '%s'"
                self.engine_simulator.execute(sql % (rows[i][0]))
        print('jango_data 최종 정산 완료')

    # 분 데이터를 가져오는 함수
    def get_date_min_for_simul(self, simul_start_date):
        simul_start_date_min = simul_start_date + self.start_min
        simul_end_date_min = simul_start_date + "1530"

        sql = "select date from `krw-btc` where date >= '%s' and date <='%s' and open != 0 group by date"
        self.min_date_rows = self.engine_craw.execute(sql % (simul_start_date_min, simul_end_date_min)).fetchall()

    # 분별 시뮬레이팅 함수
    # 새로운 종목 매수 및 보유한 종목의 데이터를 업데이트 하는 함수, 매도 함수도 포함
    def trading_by_min(self, date_rows_today, date_rows_yesterday, i):
        self.print_info(date_rows_today)

        # all_item_db가 존재하고, 현재 보유 중인 종목이 있다면 아래 로직을 들어간다.
        if self.is_simul_table_exist(self.db_name, "all_item_db") and len(self.get_data_from_possessed_item()) != 0:
            # 보유 중인 종목들의 주가를 일별로 업데이트 하는 함수(option 이 OPEN 이면 OPEN가만 업데이트)
            self.update_all_db_by_date(date_rows_today, option='OPEN')

        # 분별 시간 데이터를 가져온다.
        if self.get_min_table_use:
            #date_rows_today로 해버리면 내일의 날짜이기 떄문에 오늘의 날짜로만 해야된다.!!!!!!!!!!!!!!
            simul_start_min = date_rows_today + self.start_min
            simul_end_min = self.date_rows_after_1day + "0800"


            self.get_realtime_daily_buy_list()

            #self.min_date_rows = df_aht.date
            coin_cnt=0
            for coin in self.rdb_code_list_for_min_trading:
                coin=coin.lower()
                globals()['total_result_{}'.format(coin[4:])] = globals()['df_{}'.format(coin[4:])][globals()['df_{}'.format(coin[4:])].date >= simul_start_min]
                globals()['total_result_{}'.format(coin[4:])] = globals()['total_result_{}'.format(coin[4:])][globals()['total_result_{}'.format(coin[4:])].date <= simul_end_min]

                #
                # self.btc_result = self.df_btc_sql[self.df_btc_sql.date >= simul_start_min]
                # self.btc_result = self.btc_result[self.btc_result.date <= simul_end_min]
                #
                # self.eth_result = self.df_eth_sql[self.df_eth_sql.date >= simul_start_min]
                # self.eth_result = self.eth_result[self.eth_result.date <= simul_end_min]
                #
                # coin_cnt+=1
                # if coin_cnt ==len(self.rdb_code_list_for_min_trading):
                #     self.min_date_rows = globals()['total_result_{}'.format(coin[4:])].date
            globals()['total_result_{}'.format('btc')] = globals()['df_{}'.format('btc')][
                globals()['df_{}'.format('btc')].date >= simul_start_min]
            globals()['total_result_{}'.format('btc')] = globals()['total_result_{}'.format('btc')][
                globals()['total_result_{}'.format('btc')].date <= simul_end_min]

            self.min_date_rows=globals()['total_result_{}'.format('btc')].date

            if len(self.rdb_code_list_for_min_trading)==0 and self.is_simul_table_exist(self.db_name, "all_item_db")==False:
                self.min_date_rows=[]
        else:
            self.get_date_min_for_simul(date_rows_today)

        if len(self.min_date_rows) != 0:
            # 분 단위로 for문을 돈다
            for min in self.min_date_rows:

                #min = self.min_date_rows[t][0]
                self.min_date_for_rarry = min
                logger.debug(f'''{min}입니다.''')

                if self.trading_time_control_for_min_simul:
                    if (int(min[8:]) >= int(self.trading_buy_start_time) and int(min[8:]) <=int(self.trading_buy_end_time))\
                                    or (int(min[8:]) >= int(self.trading_sell_start_time) and int(min[8:]) <=int(self.trading_sell_end_time)):
                        pass
                    else:
                        continue

                # all_item_db가 존재하고 현재 보유 중인 종목이 있는 경우
                if self.is_simul_table_exist(self.db_name, "all_item_db") and len(
                        self.get_data_from_possessed_item()) != 0:
                    self.print_info(min)
                    self.update_all_db_by_min(min)
                    self.update_all_db_etc()
                    # 매도 함수
                    self.auto_trade_sell_stock(min, i)
                    # self.buy_stop 이 False 이고, 보유 자산이 있으면 실제 매수를 한다.
                    if not self.buy_stop and self.jango_check():
                        # 매수 할 종목을 가져온다
                        # self.get_realtime_daily_buy_list()

                        if self.len_df_realtime_daily_buy_list > 0:
                            if str(min[-4:])=='0900' or str(min[-4:])=='0901' or str(min[-4:])=='0902' or str(min[-4:])=='0903' or str(min[-4:])=='0904':
                                test=0
                                logger.debug(f'''buy pass 시간입니다.''')
                                continue
                            else:
                                self.auto_trade_stock_realtime(min, date_rows_today, date_rows_yesterday)
                        else:
                            print("realtime_daily_buy_list에 금일 매수 대상 종목이 0개 이다.  ")


                #  여긴 가장 초반에 all_itme_db를 만들어야 할때이거나 매수한 종목이 없을 때 들어가는 로직
                else:
                    if not self.buy_stop and self.jango_check():
                        test=0
                        if str(min[-4:]) == '0900' or str(min[-4:]) == '0901' or str(min[-4:]) == '0902' or str(
                                min[-4:]) == '0903' or str(min[-4:]) == '0904':
                            test = 0
                            logger.debug(f'''buy pass 시간입니다.''')
                            continue
                        else:
                            test=1
                            self.auto_trade_stock_realtime(min, date_rows_today, date_rows_yesterday)

                # 9시에만 매수를 하는 경우는 한번만 9시에 매수 하고 self.buy_stop을 true로 변경하여 이후로 매수하지 않도록 설정
                if not self.buy_stop and self.only_nine_buy:
                    print("9시 매수 끝!!!!!!!!!!")
                    self.buy_stop = True


        else:
            print("min_craw db의 종목 테이블에 " + str(
                date_rows_today) + " 데이터가 존재 하지 않는다! self.simul_start_date 날짜를 변경 하세요! (분별 데이터는 콜렉터에서 최근 1년 데이터만 가져옵니다! ")

    # 새로운 종목 매수 및 보유한 종목의 데이터를 업데이트 하는 함수, 매도 함수도 포함
    def trading_by_date(self, date_rows_today, date_rows_yesterday, i):
        self.print_info(date_rows_today)

        # all_item_db가 존재하고, 현재 보유 중인 종목이 있다면 아래 로직을 들어간다.
        if self.is_simul_table_exist(self.db_name, "all_item_db") and len(self.get_data_from_possessed_item()) != 0:
            # 보유 중인 종목들의 주가를 일별로 업데이트 하는 함수
            self.update_all_db_by_date(date_rows_today, option='OPEN')
            # 보유 중인 종목들의 주가 이외의 기타 정보들을 업데이트 하는 함수
            self.update_all_db_etc()
            # 매도 함수
            self.auto_trade_sell_stock(date_rows_today, i)
            # MDD 확인 알고리즘.

            # 보유 자산이 있다면, 실제 매수를 한다.
            if self.jango_check():
                # 돈있으면 매수 시작
                self.auto_trade_stock_realtime(str(date_rows_today) + "0900", date_rows_today, date_rows_yesterday)

        #  여긴 가장 초반에 all_itme_db를 만들어야 할때이거나 매수한 종목이 없을 때 들어가는 로직
        else:
            if self.jango_check():
                self.auto_trade_stock_realtime(str(date_rows_today) + "0900", date_rows_today, date_rows_yesterday)

    # 매일 시뮬레이팅 돌기 전 초기화 세팅
    def daily_variable_setting(self):
        self.buy_stop = False
        self.today_invest_price = 0
        # 아래부분 추가하였슴
        self.invest_sort_simul_or_real = 0  # 0이 시뮬
        #self.etc_method(self.total_invest_price)

        test=0
        # 아래부분 추가하였슴 !@
        self.invest_unit = int(float(self.total_invest_price) * (1 / self.divide_invest_unit) * self.avg_momentum_rate * self.divide_rate_using_MDD * self.shannon_rate)  # !#

        # 섀넌의 균형 포트폴리오 적용
        if self.shannon_rate_on == 1:
            self.limit_money = (float(self.total_invest_price) - self.invest_unit * self.divide_invest_unit)

    #        if self.avg_momentum_apply_inbus_on ==1:
    # @#
    def etc_method(self, total_price):
        self.MDD_Algorithm(total_price)
        if self.avg_momentum_on == 1:
            self.avg_momentum(total_price)
        else:
            self.limit_money = int(float(total_price) * (1 - self.divide_rate_using_MDD))

    ################################## MDD Line▼▼▼▼▼▼▼▼▼▼ ##################################

    def MDD_Algorithm(self, total_price):

        MDD = self.MDD(total_price)

        if self.mdd_losscut_on == 1:
            self.mdd_value = MDD

            # MDD 숫자로 손절치는 알고리즘임.
            if self.mdd_sell_check == 1:
                self.mdd_sell_count = ((self.mdd_value - self.mdd_losscut) // self.mdd_sell_next_step) + 1
                self.mdd_sell_check = 0

            if self.mdd_sell_count > 0:
                self.final_mdd_losscut = self.mdd_losscut + self.mdd_sell_count * self.mdd_sell_next_step
                if self.mdd_value < self.mdd_losscut:
                    self.mdd_sell_count = 0
                    self.final_mdd_losscut = self.mdd_losscut
            elif self.mdd_sell_count == 0:
                self.final_mdd_losscut = self.mdd_losscut

        if self.MDD_on == 1:

            MDD_Privious = 0

            if self.invest_sort_simul_or_real == 0:
                MDD_privious_max_in = self.MDD_privious_max
                MDD_privious_min_in = self.MDD_privious_min
                MDD_privious_in = self.MDD_privious
            elif self.invest_sort_simul_or_real == 1:
                [MDD_Money_Max, MDD_Money_Min, MDD_privious_max_in, MDD_privious_min_in,
                 MDD_privious_in] = self.MDD_Read()
                MDD_privious_max_in = float(MDD_privious_max_in)
                MDD_privious_min_in = float(MDD_privious_min_in)
                MDD_privious_in = float(MDD_privious_in)

            if self.MDD_variable_on == 1 and MDD > MDD_privious_max_in:
                MDD_privious_max_in = MDD
            # 이전 MDD 낮은 대상
            if self.MDD_variable_on == 1 and MDD < MDD_privious_max_in - self.mdd_compensation_rate:
                if MDD_privious_in > MDD:
                    pass
                else:
                    MDD_privious_max_in = MDD  # 이것만 원래 한 것임.

            # if self.MDD_variable_on==1 and MDD<self.MDD_privious and MDD<self.MDD_privious_min:
            #     self.MDD_privious_min=MDD
            #
            # if self.MDD_variable_on==1 and MDD-self.MDD_privious_min > 3 and MDD>self.MDD_privious:
            #     self.MDD_privious_max=MDD

            # print(f"============================================ MDD : {MDD} ============================================")
            # print("============================================ 공격적인 알고리즘 ============================================")
            if MDD <= self.MDD_rate or (self.MDD_variable_on == 1 and MDD <= (
                    float(MDD_privious_max_in) * float(self.MDD_variable_rate))):
                if self.MDD_condition_changing_on == 1:  # 조건 변경 사용할지 말지
                    self.avg_momentum_month = self.mdd_attack_avg_momentum_month
                    self.divide_invest_unit = self.mdd_attack_divide_invest_unit
                    self.avg_momentum_each_stock_on = self.mdd_attack_avg_momentum_each_stock_on
                if self.divide_rate_using_MDD_on == 1:  # MDD 비율에 따라 금액할당 사용할지 말지
                    self.divide_rate_using_MDD = self.divide_rate_using_MDD_setting
                    if self.MDD_zero_check == 1 and MDD != 0:
                        self.divide_rate_using_MDD = 1
                    if self.MDD_zero_check == 1 and MDD == 0:
                        self.MDD_zero_check = 0
                if MDD == 0:
                    # 이전 MDD Max Min 둘다 초기화
                    MDD_privious_max_in = 0
                    self.MDD_privious_min = 0
            # print("============================================ 보수적인 알고리즘 ============================================")
            elif MDD > self.MDD_rate and not (self.MDD_variable_on == 1 and MDD <= (
                    float(MDD_privious_max_in) * float(self.MDD_variable_rate))):
                # not (self.MDD_variable_on == 1 and MDD <= self.MDD_privious_max * self.MDD_variable_rate)
                # 이 부분은 variable설정 시 안 들어가게 하고 만약에 variable보다 높아지면 들어오게 설정함.
                if self.MDD_condition_changing_on == 1:  # 조건 변경 사용할지 말지
                    self.avg_momentum_month = self.mdd_defence_avg_momentum_month
                    self.divide_invest_unit = self.mdd_defence_divide_invest_unit
                    self.avg_momentum_each_stock_on = self.mdd_defence_avg_momentum_each_stock_on
                if self.divide_rate_using_MDD_on == 1:  # MDD 비율에 따라 금액할당 사용할지 말지
                    self.divide_rate_using_MDD = 1
                    self.MDD_zero_check = 1

            MDD_privious_in = MDD

            if self.invest_sort_simul_or_real == 0:
                self.MDD_privious_max = MDD_privious_max_in
                self.MDD_privious_min = MDD_privious_min_in
                self.MDD_privious = MDD_privious_in
            elif self.invest_sort_simul_or_real == 1:
                self.MDD_Write(MDD_Money_Max, MDD_Money_Min, MDD_privious_max_in, MDD_privious_min_in, MDD_privious_in)

    def MDD(self, total_price):
        MDD = 0
        MDD_Max = 0
        MDD_Min = 0
        MDD_yes = 0
        if self.invest_sort_simul_or_real == 0:
            MDD_Max = self.MDD_Max
            MDD_Min = self.MDD_Min
        elif self.invest_sort_simul_or_real == 1:
            [MDD_Money_Max, MDD_Money_Min, temp1, temp2, temp3] = self.MDD_Read()
            MDD_Max = int(MDD_Money_Max)
            MDD_Min = int(MDD_Money_Min)
        # 여기에 MDD Read

        if total_price > MDD_Max:
            MDD_Max = total_price
            # print("MDD_Max :",self.MDD_Max)
            MDD = 0
        elif total_price < MDD_Max:
            MDD_Min = total_price
            MDD = ((1 - (MDD_Min / MDD_Max)) * 100)

        if self.invest_sort_simul_or_real == 0:
            self.MDD_Max = MDD_Max
            self.MDD_Min = MDD_Min
        elif self.invest_sort_simul_or_real == 1:
            self.MDD_Write(MDD_Max, MDD_Min, temp1, temp2, temp3)

        return MDD

    def MDD_Read(self):
        sql = f'''
            select MDD_Money_Max,MDD_Money_Min,MDD_Max,MDD_Min,MDD_yes from setting_data
        '''
        rows = self.engine_simulator.execute(sql).fetchall()
        MDD_Money_Max = rows[0][0]
        MDD_Money_Min = rows[0][1]
        MDD_Max = rows[0][2]
        MDD_Min = rows[0][3]
        MDD_yes = rows[0][4]

        return [MDD_Money_Max, MDD_Money_Min, MDD_Max, MDD_Min, MDD_yes]

    def MDD_Write(self, MDD_Money_Max, MDD_Money_Min, MDD_Max, MDD_Min, MDD_yes):
        sql = f'''
            update setting_data set MDD_Money_Max={MDD_Money_Max},MDD_Money_Min={MDD_Money_Min},MDD_Max={MDD_Max},MDD_Min={MDD_Min},MDD_yes={MDD_yes} limit 1
        '''
        self.engine_simulator.execute(sql)

    ################################## MDD Line ▲▲▲▲▲▲▲▲▲▲ ##################################

    ################################## 종목별 모멘텀 ▼▼▼▼▼▼▼▼▼▼ ##################################
    # 종목별 모멘텀 적용시에 code별 rate 다르게 해주는 것
    def avg_momentum_each_stock_rate_code(self, code):
        sql = f'''
                        select avg_momentum_plus_12month 
                        from daily_subindex.`{self.date_for_avg_momentum_yes}` 
                        where code='{code}'
                    '''
        code_avg_momentum_12month = self.engine_daily_craw.execute(sql).fetchall()[0][0]
        self.avg_momentum_each_stock_rate = code_avg_momentum_12month / self.sum_avg_momentum

    def rarry_setting_invest_unit(self,code):
        sql = f'''
                                select clo3,clo5,clo10,clo20,close 
                                from coin_daily_list.`{self.date_for_rarry}` 
                                where code='{code}'
                            '''
        avg_result = self.engine_daily_buy_list.execute(sql).fetchall()

        clo3 = avg_result[0][0]
        clo5 = avg_result[0][1]
        clo10 = avg_result[0][2]
        clo20 = avg_result[0][3]
        current_price = avg_result[0][4]

        avg_score=0

        if current_price>=clo3:
            avg_score+=1

        if current_price >= clo5:
            avg_score += 1

        if current_price>=clo10:
            avg_score+=1

        if current_price>=clo20:
            avg_score+=1

        return avg_score

    # 종목별 모멘텀 적용시에 금일 사는 대상의 모멘텀의 전체 합을 구하는 함수
    def avg_momentum_each_stock_list(self, realtime_daily_buy_list):
        # 여기에 코드로 찾는 것?! subindex에서 불러오는 것을 만들자.
        # 데일리 바이리스트 대상들을 다 가져와야 함.
        # 하나씩 가져와서 리스트에 추가를 한다.
        # realtime_daily_buy_list이 코드리스트임
        avg_momentum_each_stock_list = []

        for code in realtime_daily_buy_list:
            sql = f'''
                select code,avg_momentum_plus_12month 
                from daily_subindex.`{self.date_for_avg_momentum_yes}` 
                where code='{code}'
            '''
            momentum_sql = self.engine_daily_craw.execute(sql).fetchall()
            avg_momentum_each_stock_list.append(momentum_sql[0][1])

        self.sum_avg_momentum = sum(avg_momentum_each_stock_list)

        ################################## 종목별 모멘텀▲▲▲▲▲▲▲▲ ##################################

        #############################모멘텀 함수#############################

    def avg_momentum(self, total_price):

        sql = f'''
            select date,close from `t_sector` where date <= {self.date_for_avg_momentum_yes} and code='001'
        '''
        rows = self.engine_daily_craw.execute(sql).fetchall()
        # self.avg_momentum_day 몇일로 자를 것인지, 예를들어 20은 한달 단위이다.
        # self.avg_momentum_period 몇달로 할 것인지(12개월간 볼것인지 6개월간으로 볼것인지)
        rows_slicing = rows[-1 * (20 * self.avg_momentum_period):]  # 24/20
        momentum_rows = []
        count = 1
        for i in range(len(rows_slicing)):
            # self.avg_momentum_day는 기본설정이 한달(20)로 되어있다. 설정된 기간(period를 잘게 쪼갤것인지 결정함, 낮을수록 잘개 쪼개짐)
            if count % self.avg_momentum_day == 0:
                momentum_rows.append(rows_slicing[-1 * (i + 1)])
            elif count == 1:
                momentum_rows.append(rows_slicing[-1 * (i + 1)])
            count = count + 1
        # rows_slicing에는 현재부터 +12개월의 data가 담겨있음.
        # avg_momentum은 현재순간부터 지난 12개월까지 전달과 비교해서 코스피가 올랐으면 1, 아니면 0을 반영한다.
        avg_momentum = []
        for i in range(len(momentum_rows) - 1):
            if (momentum_rows[i][1] / momentum_rows[i + 1][1]) - 1 < 0:
                avg_momentum.append(0)
            else:
                avg_momentum.append(1)

        # self.avg_momentum_rate=sum(avg_momentum)/12
        self.avg_momentum_rate = sum(avg_momentum) / (self.avg_momentum_period * (20 / self.avg_momentum_day))
        self.limit_money = int(float(total_price) *
                               (1 - (sum(avg_momentum) / (self.avg_momentum_period * (20 / self.avg_momentum_day))))
                               + float(total_price) * (
                                       1 - self.divide_rate_using_MDD))  # MDD 비율별 금액 적용시에 limit money도 설정해줘야함.

        if self.avg_momentum_apply_month == 1:
            if sum(avg_momentum) <= self.avg_momentum_month:
                self.avg_momentum_rate = sum(avg_momentum) / (self.avg_momentum_period * (20 / self.avg_momentum_day))
                self.limit_money = int(float(total_price) *
                                       (1 - (sum(avg_momentum) / (
                                               self.avg_momentum_period * (20 / self.avg_momentum_day))))
                                       + float(total_price) * (1 - self.divide_rate_using_MDD))
                self.avg_momentum_apply_inbus_real_on = 1
                if self.avg_momentum_apply_inbus_on == 1:
                    # inbus 투자금액, rate별로 limit_money에서 비율을 가져간다.
                    self.avg_momentum_apply_inbus_invest = int(self.limit_money * self.avg_momentum_apply_inbus_rate)
                    # 금액 넣은 만큼 limit_money에서 빼줌. @#적용할지 고민하자.
                    # self.limit_money = self.limit_money-self.avg_momentum_apply_inbus_invest

                # 특정 개월수 이하에서 투자를 아예 안하게 만들어 버리는 것. 생각보다 좋진 않음, 인버스 알고리즘과 추후에 시뮬레이션 필요함.
                if self.avg_momentum_invest_zero == 1:
                    self.avg_momentum_rate = 0
                    self.limit_money = total_price
            else:
                self.avg_momentum_rate = 1
                # 1로 설정했을때는 limit_money가 0이 되어야 제대로 반영된다.
                self.limit_money = 0 + float(total_price) * (1 - self.divide_rate_using_MDD)
                self.avg_momentum_apply_inbus_real_on = 0

        #############################모멘텀 함수#############################

    # 분별 시뮬레이팅
    def simul_by_min(self, date_rows_today, date_rows_yesterday, i):
        print("**************************   date: " + date_rows_today)

        # 일별 시뮬레이팅 하며 변수 초기화(분별시뮬레이터의 경우도 하루 단위로 초기화)
        self.daily_variable_setting()
        # daily_buy_list에 시뮬레이팅 할 날짜에 해당하는 테이블과 전 날 테이블이 존재하는지 확인
        if self.is_date_exist(date_rows_today) and self.is_date_exist(date_rows_yesterday):
            # 우선 매수리스트를 가져온다.
            self.db_to_realtime_daily_buy_list(date_rows_today, date_rows_yesterday, i)
            # 분별 시뮬레이팅 시작한다.
            self.trading_by_min(date_rows_today, date_rows_yesterday, i)
            self.db_to_jango(date_rows_today, date_rows_yesterday)

            # [추가 코드]all_item_db가 존재하고, 현재 보유 중인 종목이 있다면 아래 로직을 들어간다.
            if self.is_simul_table_exist(self.db_name, "all_item_db") and len(self.get_data_from_possessed_item()) != 0:
                # 보유 중인 종목들의 주가를 일별로 업데이트 하는 함수(분별 종가 업데이트 이외에 clo5, clo20등등의 값을 업데이트)
                self.update_all_db_by_date(date_rows_today, option='ALL')

        else:
            print(date_rows_today + "테이블은 존재하지 않는다!!!")

    # 일별 시뮬레이팅
    def simul_by_date(self, date_rows_today, date_rows_yesterday, i):
        print("**************************   date: " + date_rows_today)
        # 일별 시뮬레이팅 하며 변수 초기화
        self.daily_variable_setting()
        # daily_buy_list에 시뮬레이팅 할 날짜에 해당하는 테이블과 전 날 테이블이 존재하는지 확인
        if self.is_date_exist(date_rows_today) and self.is_date_exist(date_rows_yesterday):
            # 당일 매수 할 종목들을 realtime_daily_buy_list 테이블에 세팅
            self.db_to_realtime_daily_buy_list(date_rows_today, date_rows_yesterday, i)
            # 트레이딩(매수, 매도) 함수 + 보유 종목의 현재가 업데이트 함수
            self.trading_by_date(date_rows_today, date_rows_yesterday, i)

            # [추가 코드]all_item_db가 존재하고, 현재 보유 중인 종목이 있다면 아래 로직을 들어간다.
            if self.is_simul_table_exist(self.db_name, "all_item_db") and len(self.get_data_from_possessed_item()) != 0:
                # 보유 중인 종목들의 주가를 일별로 업데이트 하는 함수(분별 종가 업데이트 이외에 clo5, clo20등등의 값을 업데이트)
                self.update_all_db_by_date(date_rows_today, option='ALL')

            # 일별 정산
            self.db_to_jango(date_rows_today,date_rows_yesterday)

        else:
            print(date_rows_today + "테이블은 존재하지 않는다!!!")

    # 날짜 별 로테이팅 함수
    def rotate_date(self):
        if self.use_min and self.get_min_table_use:
            # 새롭게 만드는 분별 시뮬레이션(처음에 분별 테이블을 모조리 가져온다.)
            #self.get_min_table()
            self.get_min_table_all_coin()

        for i in range(1, len(self.date_rows)):
            if i == (len(self.date_rows)-1):
                continue
            # print("self.date_rows!!" ,self.date_rows)
            # 시뮬레이팅 할 일자
            #date_rows_today = self.date_rows[i+1][0]
            self.date_rows_after_1day = self.date_rows[i+1][0]
            date_rows_today = self.date_rows[i][0]
            # 시뮬레이팅 하기 전의 일자
            #date_rows_yesterday = self.date_rows[i][0]
            date_rows_yesterday = self.date_rows[i - 1][0]
            self.date_for_avg_momentum_yes = date_rows_yesterday
            self.date_for_rarry=date_rows_yesterday

            # self.simul_reset 이 False, 즉 시뮬레이터를 멈춘 지점 부터 실행하기 위한 조건
            if not self.simul_reset and not self.simul_reset_lock:
                if int(date_rows_today) <= int(self.last_simul_date):
                    print("**************************   date: " + date_rows_today + "simul jango date exist pass ! ")
                    continue
                else:
                    self.simul_reset_lock = True

            # 분별 시뮬레이팅
            if self.use_min:
                self.simul_by_min(date_rows_today, date_rows_yesterday, i)
            # 일별 시뮬레이팅
            else:
                self.simul_by_date(date_rows_today, date_rows_yesterday, i)

        # 마지막 jango_data 정리
        self.arrange_jango_data()

    def update_realtime_daily_buy_list_length(self, length):
        sql = f'''update setting_data set realtime_daily_buy_list_length={length} limit 1'''
        self.engine_simulator.execute(sql)

    def get_min_table(self):
        btc_sql = "select * from `krw-btc`"
        self.min_btc_rows = self.engine_craw.execute(btc_sql).fetchall()

        eth_sql = "select * from `krw-eth`"
        self.min_eth_rows = self.engine_craw.execute(eth_sql).fetchall()

        self.df_btc_sql=pd.DataFrame(self.min_btc_rows,
                            columns=['index','date', 'check_item', 'code', 'code_name', 'd1_diff_rate', 'close', 'open', 'high',
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
        self.min_btc_rows=0

        self.df_eth_sql = pd.DataFrame(self.min_eth_rows,
                                  columns=['index', 'date', 'check_item', 'code', 'code_name', 'd1_diff_rate', 'close',
                                           'open', 'high',
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
        self.min_eth_rows = 0

    def get_min_table_all_coin(self):
        all_coin_sql = f'''select TABLE_NAME 
                             FROM information_schema.tables 
                             WHERE table_schema = 'coin_daily_craw'
                             and left(table_name, 3) = 'KRW'
                             and (table_name='krw-btc' or
                                table_name='krw-eth' or
                                table_name='krw-ltc' or
                                table_name='krw-xrp' or
                                table_name='krw-ada') 
                        '''
        # left(table_name, 3) = 'KRW'

        all_coin = self.engine_daily_craw.execute(all_coin_sql).fetchall()
        #all_coin=[]
        logger.debug("All coin을 불러왔습니다.")

        for i in range(len(all_coin)):
            coin_name=all_coin[i][0][4:]
            coin = all_coin[i][0]
            globals()['df_{}'.format(coin_name)]=0
            #globals()['self.df_{}'.format(coin)]=[]
            #globals()['df_{}'.format(i)] = "hi~ df_{}".format(i)

            coin_min_sql = f'''select * from `{coin}`'''
            self.min_coin_rows = self.engine_craw.execute(coin_min_sql).fetchall()

            globals()['df_{}'.format(coin_name)] = pd.DataFrame(self.min_coin_rows,
                                columns=['index','date', 'check_item', 'code', 'code_name', 'd1_diff_rate', 'close', 'open', 'high',
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
            logger.debug(f'''{coin}을 DF에 넣는중입니다. {(i+1)}/{len(all_coin)}''')
            self.min_coin_rows=0



    # 분별 현재 종가 가져오는 함수
    # (close가 일별 데이터에서는 일별 종가 이지만, 분별 데이터에서의 close는 각 분별에 대한 종가를 의미
    # 즉, 1분 간격으로 변화하는 시세를 가져오는 함수
    # !@
    def get_now_close_price_by_get_min_table(self, code_name, min_date):
        code_name = code_name.lower()

        #close = self.btc_result[self.btc_result.date == min_date].open.tolist()[0]
        try:
            close = globals()['total_result_{}'.format(code_name[4:])][globals()['total_result_{}'.format(code_name[4:])].date == min_date].open.tolist()[0]
        except:
            close=0
        # globals()['self.{}_result'.format(coin)]


        # if code_name=='krw-btc':
        #     try:
        #         close=self.btc_result[self.btc_result.date == min_date].open.tolist()[0]
        #     except:
        #         close=0
        # elif code_name=='krw-eth':
        #     try:
        #         close=self.eth_result[self.eth_result.date == min_date].open.tolist()[0]
        #     except:
        #         close=0
        # sql = "select close from `" + code_name + "` where date = '{}' and open != 0 and volume !=0 order by sum_volume desc limit 1"
        # rows = self.engine_craw.execute(sql.format(min_date)).fetchall()

        if close is None:
            return False
        else:
            return close

    # 분별 현재 누적 거래량 가져오는 함수
    # !@
    def get_now_volume_by_get_min_table(self, code_name, min_date):
        code_name=code_name.lower()
        volume=0

        try:
            volume = sum(globals()['df_{}'.format(code_name[4:])][globals()['df_{}'.format(code_name[4:])].date <= min_date].volume)
        except:
            volume = 0
        # if code_name == 'krw-btc':
        #     volume = sum(self.btc_result[self.btc_result.date <= min_date].volume)
        # elif code_name == 'krw-eth':
        #     volume = sum(self.eth_result[self.eth_result.date <= min_date].volume)
        # sql = "select close from `" + code_name + "` where date = '{}' and open != 0 and volume !=0 order by sum_volume desc limit 1"
        # rows = self.engine_craw.execute(sql.format(min_date)).fetchall()
        test=0

        if volume is None:
            return False
        else:
            return volume

    # 분별 현재 종가 가져오는 함수
    # (close가 일별 데이터에서는 일별 종가 이지만, 분별 데이터에서의 close는 각 분별에 대한 종가를 의미
    # 즉, 1분 간격으로 변화하는 시세를 가져오는 함수


if __name__ == '__main__':
    logger.error('simulator.py로 실행해 주시기 바랍니다.')
    sys.exit(1)
