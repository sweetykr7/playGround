ver = "#version 1.3.9"
print(f"simulator_func_mysql Version: {ver}")
import sys
is_64bits = sys.maxsize > 2**32
if is_64bits:
    print('64bit 환경입니다.')
else:
    print('32bit 환경입니다.')

from sqlalchemy import event
from sqlalchemy.exc import ProgrammingError

from library.daily_crawler import *
import pymysql.cursors
# import numpy as np
from datetime import timedelta
from library.logging_pack import *
from library import cf
from pandas import DataFrame


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

    # 시뮬레이션 옵션 설정 함수
    def variable_setting(self):
        # 아래 if문으로 들어가기 전까지의 변수들은 모든 알고리즘에 공통적으로 적용 되는 설정
        # 오늘 날짜를 설정
        self.date_setting()
        # 시뮬레이팅이 끝나는 날짜.
        self.simul_end_date = self.today
        self.start_min = "0900"

        # 아래 3개는 분별시뮬레이션 옵션
        # (use_min, only_nine_buy 변수만 각각의 알고리즘에 붙여 넣기 해서 사용)
        # 분별 시뮬레이션을 사용하고 싶을 경우 아래 옵션을 True로 변경하여 사용
        self.use_min = False
        # 아침 9시에만 매수를 하고 싶은 경우 True, 9시가 아니어도 매수를 하고 싶은 경우 False(분별 시뮬레이션 적용 가능 / 일별 시뮬레이션은 9시에만 매수, 매도)
        self.only_nine_buy = True
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

        # 섀넌의 포트폴리오
        self.shannon_rate = 1
        self.shannon_rate_on = 0  # on/off 조정


        # 실시간 조건 list용
        self.condition_trade=False

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
        self.prespan2_rate = 0.95

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

        #MDD 전역 변수 설정
        self.mdd_value=0
        self.mdd_losscut=15

        self.mdd_losscut_on=0

        self.mdd_sell_count=0 #mdd로 팔은 횟수 카운트
        self.mdd_sell_next_step=5 #mdd로 팔고나서 다음 스텝에서 얼마되면 손절할건지
        self.mdd_sell_check=0
        self.final_mdd_losscut=self.mdd_losscut


        self.pbr_rate = 1
        self.pbr_value = 0.2 # pbr값


        #per value
        self.per_value = 2

        # noise rate을 rarryk에 적용하기

        self.noise_rate_on = 0

        ##########################################평균 모멘텀 스코어 적용##########################################


        self.rarry_k=0.5
        self.noise_rate_on=0
        self.divide_invest_unit = 20

        #실시간 일목균형표 적용위해
        self.backspan_day=0

        #분별 시뮬레이션 당일 rate control 하기
        self.min_rate_control_on = False
        self.min_rate_control = 1

        #중첩 데이터 삭제 안하기
        self.overlap_data_delete_on=True #True가 지우는 것임

        #중첩 데이터를 삭제안하고 따로 테이블을 만들려고 생성함.
        self.r_d_b_overlap_save_on_off=False #False가 보관 안하는 것임

        #볼밴 상단에서 비율 벗어나면 매도하는 계수
        self.bband_u_rate=1

        self.bband_u_buy_rate=1
        self.bband_u_sell_rate=1

        #볼밴 상단 돌파한 기간 얼마인지의 변수
        self.bband_1month_period =1

        #pbr rate, gpa적용하려고 넣어둠.
        self.pbr_rate = 1
        self.for_gpa_pbr_order_limit=100
        self.for_gpa_gpa_order_limit=50
        self.for_gpa_gpa_rate_order_limit = 50
        self.for_gpa_qoq_rate_order_limit=50

        #prespan으로 손절하는 알고리즘 변수
        self.prespan1_losscut=1
        self.prespan2_losscut=1


        #시가 총액 제한
        self.market_cap=100000000000
        self.marketcap_rate=10
        #거래대금 제한, 기본 천만원으로 해둠.
        self.trading_money=10000000


        #월 특정 구간 투자용
        self.monthly_invest_time_divide_unit=6

        #realtime daily buylist 중첩 처리하는 곳 리밋설정
        self.r_d_b_limit_control=False

        #분할매도 on/off False가 off

        self.sell_division_on=False

        #분할 매수 on/off False가 off
        self.buy_division_on=False




        print("self.simul_num!!! ", self.simul_num)

        ###!@####################################################################################################################
        # 아래 부터는 알고리즘 별로 별도의 설정을 해주는 부분

        if self.simul_num in (1, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18,

                              658,
                              #2201 # 분별로 다시 준비
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
            self.start_invest_price = 10000000

            # 매수 금액
            self.invest_unit = 1000000

            # 자산 중 최소로 남겨 둘 금액
            self.limit_money = 3000000

            # 익절 수익률 기준치
            self.sell_point = 10

            # 손절 수익률 기준치
            self.losscut_point = -2

            # 실전/모의 봇 돌릴 때 매수하는 순간 종목의 최신 종가 보다 1% 이상 오른 경우 사지 않도록 하는 설정(변경 가능)
            self.invest_limit_rate = 1.03 #원래 1.01
            # 실전/모의 봇 돌릴 때 매수하는 순간 종목의 최신 종가 보다 -2% 이하로 떨어진 경우 사지 않도록 하는 설정(변경 가능)
            self.invest_min_limit_rate = 0.97 #원래 0.98

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
                self.use_min= True
                self.only_nine_buy = False
                self.simul_start_date = "20190701"

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

            elif self.simul_num == 1401:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 36
                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정
                self.simul_start_date = "20031229"
                #self.simul_start_date = "20031229"
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
                #평균 모멘텀 스코어 켜기
                self.avg_momentum_on=1
                self.avg_momentum_apply_month=1 #전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month=3 #공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on=1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100-5)/100
                self.prespan1_rate=0.978

            elif self.simul_num == 1402:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

            elif self.simul_num == 1403:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 19
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False


            elif self.simul_num == 1404:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 20
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False


            elif self.simul_num == 1405:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 21
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

            elif self.simul_num == 1406:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 22
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False


            elif self.simul_num == 1407:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 23
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

            elif self.simul_num == 1408:
                self.db_to_realtime_daily_buy_list_num = 933
                self.sell_list_num = 24
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.bband_u_rate=0.8

            elif self.simul_num == 1409:
                self.db_to_realtime_daily_buy_list_num = 933
                self.sell_list_num = 24
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.bband_u_rate=0.75

            elif self.simul_num == 1411:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 25
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.bband_u_buy_rate=0.85
                self.bband_u_sell_rate=0.80


            elif self.simul_num == 1412:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 25
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.bband_u_buy_rate=0.85
                self.bband_u_sell_rate=0.85

            elif self.simul_num == 1413:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 25
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.bband_u_buy_rate=0.85
                self.bband_u_sell_rate=0.92

            elif self.simul_num == 1414:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 25
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.bband_u_buy_rate=0.85
                self.bband_u_sell_rate=0.95

            elif self.simul_num == 1415:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 36
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
                self.divide_invest_unit = 20
                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

            elif self.simul_num == 1416:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 26
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.bband_u_buy_rate=0.85
                self.bband_u_sell_rate=0.85

            elif self.simul_num == 1417:
                self.db_to_realtime_daily_buy_list_num = 935
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.bband_u_buy_rate=0.85
                self.bband_u_sell_rate=0.85

            elif self.simul_num == 1417:
                self.db_to_realtime_daily_buy_list_num = 935
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.bband_u_buy_rate=0.85
                self.bband_u_sell_rate=0.85

            elif self.simul_num == 1418:
                self.db_to_realtime_daily_buy_list_num = 935
                self.sell_list_num = 25
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.bband_u_buy_rate=0.85
                self.bband_u_sell_rate=0.85

            elif self.simul_num == 1419:
                self.db_to_realtime_daily_buy_list_num = 941
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

            elif self.simul_num == 1420:
                self.db_to_realtime_daily_buy_list_num = 936
                self.sell_list_num = 36
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
                self.losscut_point = -20
                # 몇 분할 투자할지
                self.divide_invest_unit = 30
                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

            elif self.simul_num == 1421:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 36
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
                # Shannon rate 적용
                self.shannon_rate_on = 1
                self.shannon_rate = 0.5
                self.shannon_change_rate=1

                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False





            elif self.simul_num == 1422:
                self.db_to_realtime_daily_buy_list_num = 937
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.shannon_rate_on=1
                self.shannon_rate=0.5

            elif self.simul_num == 1423:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.shannon_rate_on=1
                self.shannon_rate=0.5


            elif self.simul_num == 1424:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False


                self.shennon_rate_on=1
                self.shennon_rate=0.5

            elif self.simul_num == 1425:
                self.db_to_realtime_daily_buy_list_num = 932
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on = False

                self.shennon_rate_on = 1
                self.shennon_rate = 0.8

            elif self.simul_num == 1426:
                self.db_to_realtime_daily_buy_list_num = 975
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1

            elif self.simul_num == 1427:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100

            elif self.simul_num == 1428:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 200

            elif self.simul_num == 1429:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 300

            elif self.simul_num == 1430:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 400

            elif self.simul_num == 1431:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 500

            elif self.simul_num == 1432:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 30

            elif self.simul_num == 1433:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 40

            elif self.simul_num == 1434:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 50

            elif self.simul_num == 1435:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 60


            elif self.simul_num == 1436:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 70

            elif self.simul_num == 1437:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 80


            elif self.simul_num == 1438:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 90

            elif self.simul_num == 1441:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 38
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100

            elif self.simul_num == 1442:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 39
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100

                self.prespan1_losscut=0.96
                self.prespan2_losscut=0.98

            elif self.simul_num == 1443:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 39
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100

                self.prespan1_losscut=0.96
                self.prespan2_losscut=1

            elif self.simul_num == 1451:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 40
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100

                self.prespan1_losscut=0.95
                self.prespan2_losscut=0.95


                self.bband_u_sell_rate=0.85


            elif self.simul_num == 1452:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 41
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100

                self.prespan1_losscut=0.95
                self.prespan2_losscut=0.95


                self.bband_u_sell_rate=0.85

            elif self.simul_num == 1453:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 41
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100

                self.prespan1_losscut=0.95
                self.prespan2_losscut=0.95


                self.bband_u_sell_rate=0.85

            elif self.simul_num == 1461:
                self.db_to_realtime_daily_buy_list_num = 981
                self.sell_list_num = 36
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

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100



            elif self.simul_num == 1471:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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
                self.divide_invest_unit = 20
                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100


            elif self.simul_num == 1472:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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
                self.divide_invest_unit = 15
                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100

            elif self.simul_num == 1473:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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
                self.divide_invest_unit = 10
                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100


            elif self.simul_num == 1474:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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
                self.divide_invest_unit = 20
                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 50


            elif self.simul_num == 1475:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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
                self.divide_invest_unit = 20
                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 150

            elif self.simul_num == 1476:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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
                self.divide_invest_unit = 20
                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 200

            elif self.simul_num == 1477:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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
                self.divide_invest_unit = 20
                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100

                self.shannon_rate = 0.7
                self.shannon_rate_on = 1  # on/off 조정

            elif self.simul_num == 1478:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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
                self.divide_invest_unit = 20
                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100

                self.shannon_rate = 0.8
                self.shannon_rate_on = 1  # on/off 조정


            elif self.simul_num == 1479:
                self.db_to_realtime_daily_buy_list_num = 976
                self.sell_list_num = 36
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
                self.divide_invest_unit = 20
                # 평균 모멘텀 스코어 켜기
                self.avg_momentum_on = 1
                self.avg_momentum_apply_month = 1  # 전부 적용하는 것이 아니라 특정 점수 이하만 적용하는 알고리즘, 적용할지(1),안할지(0)

                self.avg_momentum_month = 3  # 공격(특정 점수 이하만 적용함)

                self.avg_momentum_each_stock_on = 0
                self.MDD_on = 1
                self.MDD_rate = 3
                self.MDD_condition_changing_on = 1

                self.MDD_variable_on = 1
                self.MDD_variable_rate = (100 - 5) / 100
                self.prespan1_rate = 0.978
                self.overlap_data_delete_on=False

                self.pbr_rate=1
                self.for_gpa_pbr_order_limit = 100






            elif self.simul_num == 1101:
                self.db_to_realtime_daily_buy_list_num = 1101
                self.sell_list_num = 1101
                self.simul_start_date = "20190701"

                self.use_min = True
                self.only_nine_buy = False
                self.trade_check_num = 3

                self.volume_up = 5
                self.prespan1_rate = 0.98

                self.noise_rate_on = 1

                # stock_finance에 데이터가 쌓인 시점의 다음날로 self.simul_start_date 를 설정

                self.start_invest_price = 10000000
                self.limit_money = 0
                self.sell_point = 20
                self.losscut_point = -5
                self.divide_invest_unit = 10



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
            logger.error(f"입력 하신 {self.simul_num}번 알고리즘에 대한 설정이 없습니다. simulator_func_mysql.py 파일의 variable_setting함수에 알고리즘을 설정해주세요. ")
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
            self.tax_rate = 0.0025
            self.fees_rate = 0.00015

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
        self.today = datetime.datetime.today().strftime("%Y%m%d")
        self.today_detail = datetime.datetime.today().strftime("%Y%m%d%H%M")
        self.today_date_form = datetime.datetime.strptime(self.today, "%Y%m%d").date()

    # DB 이름 세팅 함수
    def db_name_setting(self):
        if self.op == "real":
            self.engine_simulator = create_engine(
                "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/" + str(
                    self.db_name),
                encoding='utf-8')

        else:
            # db_name을 setting 한다.
            self.db_name = "simulator" + str(self.simul_num)
            self.engine_simulator = create_engine(
                "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/" + str(
                    self.db_name), encoding='utf-8')

        self.engine_daily_craw = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_craw",
            encoding='utf-8')

        self.engine_craw = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/min_craw",
            encoding='utf-8')
        self.engine_daily_buy_list = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_buy_list",
            encoding='utf-8')

        from library.open_api import escape_percentage
        event.listen(self.engine_simulator, 'before_execute', escape_percentage, retval=True)
        event.listen(self.engine_daily_craw, 'before_execute', escape_percentage, retval=True)
        event.listen(self.engine_craw, 'before_execute', escape_percentage, retval=True)
        event.listen(self.engine_daily_buy_list, 'before_execute', escape_percentage, retval=True)

        # 특정 데이터 베이스가 아닌, mysql 에 접속하는 객체
        self.db_conn = pymysql.connect(host=cf.db_ip, port=int(cf.db_port), user=cf.db_id, password=cf.db_passwd,
                                       charset='utf8')

    # 매수 함수
    def invest_send_order(self, date, code, code_name, price, yes_close, j):
        # print("invest_send_order!!!")
        # 시작가가 투자하려는 금액 보다 작아야 매수가 가능하기 때문에 아래 조건


        if price < self.invest_unit:
            if self.min_rate_control_on and (price/yes_close) > self.min_rate_control:
                pass
            else:
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
        print("auto_trade_stock_realtime 함수에 들어왔다!!--",min_date)
        # self.df_realtime_daily_buy_list 에 있는 모든 종목들을 매수한다
        #test=0
        for j in range(self.len_df_realtime_daily_buy_list):
            # try: #!@ 매수하려는 종목수 이상으로 매수 못하게 해버리는 것.
            #     if self.get_count_possessed_item() >= self.divide_invest_unit:
            #         break
            # except:
            #     pass

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
                    price = self.get_now_close_price_by_min(code_name, min_date)

                # 어제 종가를 가져온다.
                yes_close = self.get_yes_close_price_by_date(code, date_rows_yesterday)

                # False는 데이터가 없는것
                if code_name == False or price == 0 or price == False:
                    continue

                # 촬영 후 아래 if 문 추가 (향후 실시간 조건 매수 시 사용) ###################
                if self.use_min and not self.only_nine_buy and self.trade_check_num :

                    prespan1 = self._get_yes_prespan1(code_name,self.backspan_day)
                    # 시작가
                    open = self.get_now_open_price_by_date(code, date_rows_today)
                    # 당일 누적 거래량
                    sum_volume = self.get_now_volume_by_min(code_name, min_date)


                    #
                    # if self.noise_rate_on==1:
                    #     self.rarry_k=float(self.noise_rate_list[j][0])

                    # open, sum_volume 값이 존재 할 경우
                    if open and sum_volume:
                        # 매수 할 종목에 대한 dataframe row와, 시작가, 현재가, 분별 누적 거래량 정보를 전달
                        if not self.trade_check(self.df_realtime_daily_buy_list.loc[j], open, price, sum_volume, prespan1):
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
                                               'volume',
                                               'clo5', 'clo10', 'clo20', 'clo40', 'clo60', 'clo80',
                                               'clo100', 'clo120',
                                               "clo5_diff_rate", "clo10_diff_rate", "clo20_diff_rate",
                                               "clo40_diff_rate", "clo60_diff_rate",
                                               "clo80_diff_rate", "clo100_diff_rate",
                                               "clo120_diff_rate",
                                               'yes_clo5', 'yes_clo10', 'yes_clo20', 'yes_clo40',
                                               'yes_clo60',
                                               'yes_clo80',
                                               'yes_clo100', 'yes_clo120',
                                               'vol5', 'vol10', 'vol20', 'vol40', 'vol60', 'vol80',
                                               'vol100', 'vol120'])
        return df_daily_buy_list

    # realtime_daily_buy_list 테이블의 매수 리스트를 가져오는 함수
    def get_realtime_daily_buy_list(self):
        print("get_realtime_daily_buy_list 함수에 들어왔습니다!")

        # 이 부분은 촬영 후 코드를 간소화 했습니다. 조건문 모두 없앴습니다.
        # check_item = 매수 했을 시 날짜가 찍혀 있다. 매수 하지 않았을 때는 0
        sql = "select * from realtime_daily_buy_list where check_item = '%s' group by code"

        realtime_daily_buy_list = self.engine_simulator.execute(sql % (0)).fetchall()

        self.df_realtime_daily_buy_list = DataFrame(realtime_daily_buy_list,
                                                    columns=['index', 'index2', 'index3', 'date', 'check_item',
                                                             'code', 'code_name', 'd1_diff_rate', 'close', 'open',
                                                             'high', 'low',
                                                             'volume',
                                                             'clo5', 'clo10', 'clo20', 'clo40', 'clo60', 'clo80',
                                                             'clo100', 'clo120',
                                                             "clo5_diff_rate", "clo10_diff_rate", "clo20_diff_rate",
                                                             "clo40_diff_rate", "clo60_diff_rate",
                                                             "clo80_diff_rate", "clo100_diff_rate",
                                                             "clo120_diff_rate",
                                                             'yes_clo5', 'yes_clo10', 'yes_clo20', 'yes_clo40',
                                                             'yes_clo60',
                                                             'yes_clo80',
                                                             'yes_clo100', 'yes_clo120',
                                                             'vol5', 'vol10', 'vol20', 'vol40', 'vol60', 'vol80',
                                                             'vol100', 'vol120'])

        self.len_df_realtime_daily_buy_list = len(self.df_realtime_daily_buy_list)

    # 가장 최근의 daily_buy_list에 담겨 있는 날짜 테이블 이름을 가져오는 함수
    def get_recent_daily_buy_list_date(self):
        sql = "select TABLE_NAME from information_schema.tables where table_schema = 'daily_buy_list' and TABLE_NAME like '%s' order by table_name desc limit 1"
        row = self.engine_daily_buy_list.execute(sql % ("20%%")).fetchall()

        if len(row) == 0:
            return False
        return row[0][0]

    # 실시간 주가 분석 알고리즘 함수 (느낌표 골뱅이 추가하면 검색 시 편합니다) (고급클래스에서 소개)
    def trade_check(self, df_row, open_price, current_price, current_sum_volume, prespan1):
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
            yes_total_tr_price = yes_close * yes_volume
            # 현재 거래 대금
            current_total_tr_price = current_price * current_sum_volume

            _range = yes_high - yes_low
           # if current_price > yes_close and current_total_tr_price > yes_total_tr_price * self.volume_up:

            if open_price + _range * self.rarry_k < current_price:
                return True
            else:
                return False

        elif self.trade_check_num ==5 :
            if current_sum_volume > yes_volume*5 and current_price > prespan1:
                return True
            else:
                return False

        elif self.trade_check_num ==6 :
            if current_sum_volume > yes_volume*self.volume_up and current_price < yes_close*0.985:
                return True
            else:
                return False



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
                    "and NOT exists (select null from stock_invest_caution e where a.code=e.code and DATE_SUB('%s', INTERVAL '%s' MONTH ) < e.post_date and e.post_date < Date('%s') and e.type != '투자경고 지정해제' group by e.code)"\
                    "and NOT exists (select null from stock_invest_warning f where a.code=f.code and f.post_date <= DATE('%s') and (f.cleared_date > DATE('%s') or f.cleared_date is null) group by f.code)"\
                    "and NOT exists (select null from stock_invest_danger g where a.code=g.code and g.post_date <= DATE('%s') and (g.cleared_date > DATE('%s') or g.cleared_date is null) group by g.code)"\
                    "and a.close < '%s'"

            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql % (date_rows_yesterday, self.interval_month, date_rows_yesterday,date_rows_yesterday ,date_rows_yesterday,date_rows_yesterday,date_rows_yesterday, self.invest_unit)).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 5:
            sql = "select * from `" + date_rows_yesterday + "` a " \
                    "where yes_clo20 > yes_clo5 and clo5 > clo20 " \
                    "and volume * close > '%s' " \
                    "and vol20 * '%s' < volume " \
                    "and d1_diff_rate > '%s' " \
                    "and NOT exists (select null from stock_konex b where a.code=b.code)" \
                    "and NOT exists (select null from stock_managing c where a.code=c.code and c.code_name != '' group by c.code) " \
                    "and NOT exists (select null from stock_insincerity d where a.code=d.code and d.code_name !='' group by d.code) " \
                    "and NOT exists (select null from stock_invest_caution e where a.code=e.code and DATE_SUB('%s', INTERVAL '%s' MONTH ) < e.post_date and e.post_date < Date('%s') and e.type != '투자경고 지정해제' group by e.code)"\
                    "and NOT exists (select null from stock_invest_warning f where a.code=f.code and f.post_date <= DATE('%s') and (f.cleared_date > DATE('%s') or f.cleared_date is null) group by f.code)"\
                    "and NOT exists (select null from stock_invest_danger g where a.code=g.code and g.post_date <= DATE('%s') and (g.cleared_date > DATE('%s') or g.cleared_date is null) group by g.code)"\
                    "and a.close < '%s'" \
                    "order by volume * close desc"
            realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql % (self.total_transaction_price,self.vol_mul, self.d1_diff , date_rows_yesterday, self.interval_month, date_rows_yesterday,date_rows_yesterday ,date_rows_yesterday,date_rows_yesterday,date_rows_yesterday, self.invest_unit)).fetchall()

        # 절대 모멘텀 전략 : 특정일 전의 종가 보다 n% 이상 상승한 종목 매수 (code version)
        elif self.db_to_realtime_daily_buy_list_num == 7:
            # 아래에서 필터링 된 매수종목을 append 해주기 위해 비어있는 리스트를 만들어준다.
            realtime_daily_buy_list = []
            if i < self.day_before + 1:
                pass
            else:
                sql = "SELECT * FROM `" + date_rows_yesterday +"` a " \
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
                    date_before = self.date_rows[i-1-self.day_before][0]
                    # 어제 일자 기준 n 일전 종가
                    date_before_close = self.get_now_close_price_by_date(code, date_before)
                    if date_before_close != 0 and date_before_close != False :
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
                      "FROM `"+date_before+"` BEFORE_DAY, `" + date_rows_yesterday +"` YES_DAY "\
                        "WHERE BEFORE_DAY.code = YES_DAY.code "\
                        "AND (YES_DAY.close - BEFORE_DAY.close) / BEFORE_DAY.close * 100 > '%s' " \
                        "AND NOT exists (SELECT null FROM stock_konex b WHERE YES_DAY.code=b.code)" \
                        "AND YES_DAY.close < '%s'"

                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql % (self.diff_point, self.invest_unit)).fetchall()

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

        # elif self.db_to_realtime_daily_buy_list_num == 1101:
        #
        #     where_sql = f'''
        #                             FROM `{date_rows_yesterday}` day, daily_subindex.`{date_rows_yesterday}` sub,
        #                             stock_info INF
        #                             WHERE day.code=sub.code
        #                             and day.code=INF.code
        #                             and day.close>day.clo5
        #                             and day.close>day.clo10
        #                             and day.close>day.clo20
        #                             AND INF.stock_market in ('거래소', '코스닥')
        #                             order by sub.avg_noise
        #                             limit 20
        #                         '''
        #     sql = f'''
        #                             SELECT day.*
        #                         ''' + where_sql
        #     realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()
        #
        #     noise_rate_sql = f'''
        #                                         SELECT sub.avg_noise
        #                                     ''' + where_sql
        #     self.noise_rate_list = self.engine_daily_buy_list.execute(noise_rate_sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 285:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1
                    where DAY.code = subindex1.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and subindex1.backspan>subindex1.prespan1
                    and subindex1.backspan<subindex1.prespan2
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 286:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1,                  
                    daily_subindex.`{date_rows_yesterday}` subindex2
                    where DAY.code = subindex1.code
                    and DAY.code=subindex2.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and subindex1.backspan>subindex1.prespan1
                    and subindex1.backspan<subindex1.prespan2
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by subindex2.avg_noise desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 288:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1
                    where DAY.code = subindex1.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and subindex1.backspan>subindex1.prespan1
                    and subindex1.backspan<subindex1.prespan2
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and (1-abs((DAY.open-DAY.close)/(DAY.high-DAY.low)))<0.7
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 289:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1,
                    daily_subindex.`{date_rows_yesterday}` subindex2
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and subindex1.backspan>subindex1.prespan1
                    and subindex1.backspan<subindex1.prespan2
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and (DAY.close>subindex2.prespan1 and DAY.close > subindex2.prespan2)
                    and (1-abs((DAY.open-DAY.close)/(DAY.high-DAY.low)))<0.60
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 290:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1,
                    daily_subindex.`{date_rows_yesterday}` subindex2
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and (subindex1.backspan>subindex1.prespan1
                    or subindex1.backspan<subindex1.prespan2)
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and (DAY.close>subindex2.prespan1 and DAY.close > subindex2.prespan2)
                    and (1-abs((DAY.open-DAY.close)/(DAY.high-DAY.low)))<0.6
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()




        elif self.db_to_realtime_daily_buy_list_num == 291:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{date_rows_yesterday}` subindex1
                    where DAY.code = subindex1.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and (DAY.close>subindex1.prespan1
                    and DAY.close>subindex1.prespan2)
                    and DAY.close>subindex1.switch_line
                    and DAY.close>subindex1.standard_line
                    and (DAY.close/subindex1.bband_U)>=0.90
                    and (1-abs((DAY.open-DAY.close)/(DAY.high-DAY.low)))<0.7
                    and DAY.volume > ONEDAYBEFORE.volume*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.volume !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 292:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1,
                    daily_subindex.`{date_rows_yesterday}` subindex2
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and subindex1.backspan>subindex1.prespan1
                    and subindex1.backspan<subindex1.prespan2
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and (subindex1.prespan2-subindex1.prespan1)/subindex1.prespan2<0.1
                    and (DAY.close>subindex2.prespan1 and DAY.close > subindex2.prespan2)
                    and (1-abs((DAY.open-DAY.close)/(DAY.high-DAY.low)))<0.60
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 293:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1,
                    daily_subindex.`{date_rows_yesterday}` subindex2
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and subindex1.backspan>subindex1.prespan1
                    and subindex1.backspan<subindex1.prespan2
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and DAY.close>DAY.open
                    and (DAY.close>subindex2.prespan1 and DAY.close > subindex2.prespan2)
                    and (1-abs((DAY.open-DAY.close)/(DAY.high-DAY.low)))<0.60
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 294:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1,
                    daily_subindex.`{date_rows_yesterday}` subindex2
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and (
                    (subindex1.backspan>subindex1.prespan1 
                    and subindex1.backspan<subindex1.prespan2*1.02)
                    or (subindex1.backspan>subindex1.prespan2
                    and subindex1.backspan<subindex1.prespan1*1.02)
                    )
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and DAY.close/DAY.high > 0.94
                    and DAY.close>DAY.open*0.95
                    and (DAY.close>subindex2.prespan1 and DAY.close > subindex2.prespan2)
                    and (1-abs((DAY.open-DAY.close)/(DAY.high-DAY.low)))<0.60
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 295:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1,
                    daily_subindex.`{date_rows_yesterday}` subindex2
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and subindex1.backspan>subindex1.prespan1
                    and subindex1.backspan<subindex1.prespan2
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and DAY.close > DAY.open*0.95
                    and (DAY.close>subindex2.prespan1 and DAY.close > subindex2.prespan2)
                    and (1-abs((DAY.open-DAY.close)/(DAY.high-DAY.low)))<0.60
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 296:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1,
                    daily_subindex.`{date_rows_yesterday}` subindex2
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and subindex1.backspan>subindex1.prespan1
                    and subindex1.backspan<subindex1.prespan2
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and DAY.close > DAY.open*1.03
                    and (DAY.close>subindex2.prespan1 and DAY.close > subindex2.prespan2)
                    and (1-abs((DAY.open-DAY.close)/(DAY.high-DAY.low)))<0.60
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()




        elif self.db_to_realtime_daily_buy_list_num == 1201:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY,  stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1,
                    daily_subindex.`{date_rows_yesterday}` subindex2,
                    daily_subindex.`{onedaybefore}` subindex3
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = subindex3.code
                    and DAY.code = INF.code
                    and 1-(subindex2.BBand_L/subindex2.BBand_U)<0.12
                    and (1-(subindex1.BBand_L/subindex1.BBand_U))>(1-(subindex2.BBand_L/subindex2.BBand_U))*2
                    and subindex2.close>subindex2.switch_line
                    and subindex2.close>subindex2.standard_line
                    and subindex3.ma19<subindex3.ma20
                    and subindex2.ma19>subindex2.ma20
                    and INF.stock_market in ('거래소', '코스닥')
                    order by subindex2.avg_noise
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 331:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1,
                    daily_subindex.`{date_rows_yesterday}` subindex2
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and subindex1.backspan>subindex1.prespan1
                    and subindex1.backspan<subindex1.prespan2
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and DAY.close > DAY.open*1.03
                    and DAY.code_name not like '%홀딩스%'
                    and (DAY.close>subindex2.prespan1 and DAY.close > subindex2.prespan2)
                    and (1-abs((DAY.open-DAY.close)/(DAY.high-DAY.low)))<0.60
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 332:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1,
                    daily_subindex.`{date_rows_yesterday}` subindex2
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and subindex1.backspan>subindex1.prespan1
                    and subindex1.backspan<subindex1.prespan2
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and DAY.code_name not like '%홀딩스%'
                    and (DAY.close>subindex2.prespan1 and DAY.close > subindex2.prespan2)
                    and (1-abs((DAY.open-DAY.close)/(DAY.high-DAY.low)))<0.60
                    and DAY.volume > ONEDAYBEFORE.vol20*5
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 333:

            setting_day=25
            if i < setting_day + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day = self.date_rows[i-1-setting_day][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY, `{onedaybefore}` ONEDAYBEFORE, stock_info INF,
                    daily_subindex.`{backspan_day}` subindex1,
                    daily_subindex.`{date_rows_yesterday}` subindex2
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = ONEDAYBEFORE.code
                    and DAY.code = INF.code
                    and (subindex1.backspan>subindex1.prespan1
                    or subindex1.backspan>subindex1.prespan2)
                    and subindex1.backspan>subindex1.switch_line
                    and subindex1.backspan>subindex1.standard_line
                    and DAY.code_name not like '%홀딩스%'
                    and (DAY.close>subindex2.prespan1 and DAY.close > subindex2.prespan2)
                    and (1-abs((DAY.open-DAY.close)/(DAY.high-DAY.low)))<0.40
                    and DAY.volume > ONEDAYBEFORE.vol20*10
                    and INF.stock_market in ('거래소', '코스닥')
                    and ONEDAYBEFORE.vol20 !=0
                    order by (DAY.close * DAY.volume) desc
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 1202:

            setting_day1=20
            setting_day2=40
            setting_day3=60
            if i < setting_day2 + 1:
                realtime_daily_buy_list=[]
                pass
            else:
                backspan_day1 = self.date_rows[i-1-setting_day1][0]
                backspan_day2 = self.date_rows[i - 1 - setting_day2][0]
                #backspan_day3 = self.date_rows[i - 1 - setting_day3][0]
                onedaybefore = self.date_rows[i-2][0]

                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY,  stock_info INF,
                    daily_subindex.`{backspan_day1}` subindex1,
                    daily_subindex.`{backspan_day2}` subindex2,
                    daily_subindex.`{date_rows_yesterday}` subindex4,
                    daily_subindex.`{onedaybefore}` subindex5
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = subindex4.code
                    and DAY.code = subindex5.code
                    and DAY.code = INF.code
                    and 1-(subindex2.BBand_L/subindex2.BBand_U)<0.12
                    and (1-(subindex1.BBand_L/subindex1.BBand_U))>(1-(subindex4.BBand_L/subindex4.BBand_U))*2
                    and (1-(subindex2.BBand_L/subindex2.BBand_U))>(1-(subindex4.BBand_L/subindex4.BBand_U))*2
                    and subindex4.close>subindex4.switch_line
                    and subindex4.close>subindex4.standard_line
                    and subindex5.ma19<subindex5.ma20
                    and subindex4.ma19>subindex4.ma20
                    and INF.stock_market in ('거래소', '코스닥')
                    order by subindex4.avg_noise
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()


        elif self.db_to_realtime_daily_buy_list_num == 1301:


            if i < 3 + 1:
                realtime_daily_buy_list = []
                pass
            else:
                onedaybefore = self.date_rows[i - 2][0]
                twodaybefore = self.date_rows[i - 3][0]
                threedaybefore = self.date_rows[i - 4][0]


                sql = f'''
                    SELECT DAY.*
                    FROM `{date_rows_yesterday}` DAY,  stock_info INF,
                    daily_subindex.`{date_rows_yesterday}` subindex1,
                    daily_subindex.`{onedaybefore}` subindex2,
                    daily_subindex.`{twodaybefore}` subindex3
                    where DAY.code = subindex1.code
                    and DAY.code = subindex2.code
                    and DAY.code = subindex3.code
                    and DAY.code = INF.code
                    and DAY.code_name not like '%스팩'
                    and (subindex1.BBand_U > subindex1.close and subindex1.close > subindex1.BBand_U*0.97)
                    and (subindex2.BBand_U > subindex2.close and subindex2.close > subindex2.BBand_U*0.97)
                    and (subindex3.BBand_U > subindex3.close and subindex3.close > subindex3.BBand_U*0.97)
                    and (subindex1.close>subindex1.prespan1
                    and subindex1.close>subindex1.prespan2)
                    and DAY.vol20*DAY.close > 100000000
                    and INF.stock_market in ('거래소', '코스닥')
                    and subindex1.best_52!=0
                    and DAY.close>subindex1.best_52*0.95
                    order by subindex1.avg_noise
                '''


                realtime_daily_buy_list = self.engine_daily_buy_list.execute(sql).fetchall()

        elif self.db_to_realtime_daily_buy_list_num == 301:


            realtime_daily_buy_list=[]


            if len(self.realtime_daily_buy_list_condition)>0:
                for code in self.realtime_daily_buy_list_condition:
                    sql = f'''
                                SELECT DAY.*
                                FROM `{date_rows_yesterday}` DAY                    
                                where DAY.code = '{code}'
                            '''

                    realtime_daily_buy_list_temp = self.engine_daily_buy_list.execute(sql).fetchall()
                    realtime_daily_buy_list.append(realtime_daily_buy_list_temp)
                #이 아래는 2차원 list를 1차원으로 바꾸는 방법이다.
                realtime_daily_buy_list=sum(realtime_daily_buy_list,[])









        ######################################################################################################################################################################################
        else:
            print(f"{self.simul_num}번 알고리즘에 대한 self.db_to_realtime_daily_buy_list_num 설정이 비었습니다. variable_setting 함수에서 self.db_to_realtime_daily_buy_list_num 을 확인해주세요.")
            sys.exit(1)
        # realtime_daily_buy_list 에 종목이 하나라도 있다면, 즉 매수할 종목이 하나라도 있다면 아래 로직을 들어간다.
        if len(realtime_daily_buy_list) > 0:
            # realtime_daily_buy_list 라는 리스트를 df_realtime_daily_buy_list 라는 데이터프레임으로 변환하는 과정
            # 차이점은 리스트는 컬럼에 대한 개념이 없는데, 데이터프레임은 컬럼이 있다.

            df_realtime_daily_buy_list = DataFrame(realtime_daily_buy_list,
                                                   columns=['index', 'index2', 'date', 'check_item', 'code',
                                                            'code_name', 'd1_diff_rate', 'close', 'open', 'high',
                                                            'low', 'volume',
                                                            'clo5', 'clo10', 'clo20', 'clo40', 'clo60', 'clo80',
                                                            'clo100', 'clo120',
                                                            "clo5_diff_rate", "clo10_diff_rate", "clo20_diff_rate",
                                                            "clo40_diff_rate", "clo60_diff_rate", "clo80_diff_rate",
                                                            "clo100_diff_rate", "clo120_diff_rate",
                                                            'yes_clo5', 'yes_clo10', 'yes_clo20', 'yes_clo40',
                                                            'yes_clo60',
                                                            'yes_clo80',
                                                            'yes_clo100', 'yes_clo120',
                                                            'vol5', 'vol10', 'vol20', 'vol40', 'vol60', 'vol80',
                                                            'vol100', 'vol120'])

            # lamda는 익명 함수이다. 여기서 int로 param을 보내야 6d ( 정수) 에서 안걸린다.
            df_realtime_daily_buy_list['code'] = df_realtime_daily_buy_list['code'].apply(
                lambda x: "{:0>6d}".format(int(x)))

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

                r_d_b_overlap_list=[]
                if self.r_d_b_limit_control:
                    r_d_b_unit_limit=self.r_d_b_unit_limit
                else:
                    r_d_b_unit_limit=self.divide_invest_unit

                try:
                    if self.r_d_b_overlap_save_on_off :
                        rbd_overlap_list_sql=f'''
                            select db.code,db.code_name,db.buy_date,db.sell_date,db.rate
                            from (select * from realtime_daily_buy_list limit {r_d_b_unit_limit}) rdb
                            , all_item_db db
                            where rdb.code=db.code
                            and db.sell_date=0
                            
                        '''

                        r_d_b_overlap_list = self.engine_simulator.execute(rbd_overlap_list_sql).fetchall()
                    else :
                        r_d_b_overlap_list=[]
                except:
                    r_d_b_overlap_list=[]
                if len(r_d_b_overlap_list)>0:
                    df_rdb_overlap_list = DataFrame(r_d_b_overlap_list,
                                                           columns=['code', 'code_name',
                                                                    'buy_date', 'sell_date','rate'])
                else:
                    df_rdb_overlap_list=pd.DataFrame(index=range(0,0),
                                                            columns=['code', 'code_name',
                                                                    'buy_date', 'sell_date','rate'])
                df_rdb_overlap_list.to_sql('rdb_overlap_list', self.engine_simulator, if_exists='replace')

                test=0

                # 현재 보유 중인 종목은 매수 리스트(realtime_daily_buy_list) 에서 제거 하는 로직 !@
                if self.is_simul_table_exist(self.db_name, "all_item_db") and self.overlap_data_delete_on and not self.buy_division_on :
                    test=0
                    sql = "delete from realtime_daily_buy_list where code in (select code from all_item_db where sell_date = '%s' or buy_date = '%s' or sell_date = '%s')"
                    # delete는 리턴 값이 없기 때문에 fetchall 쓰지 않는다.
                    self.engine_simulator.execute(sql % (0, date_rows_today, date_rows_today))
                elif self.is_simul_table_exist(self.db_name, "all_item_db") and self.overlap_data_delete_on and self.buy_division_on :
                    sql = f'''
                        delete from realtime_daily_buy_list 
                            where code in (select code 
                                        from all_item_db 
                                        where sell_date = 0 
                                        or buy_date = {date_rows_today} or sell_date = {date_rows_today}
                                        )
                            '''
                    # delete는 리턴 값이 없기 때문에 fetchall 쓰지 않는다.
                    self.engine_simulator.execute(sql)

                # 영상 촬영 후 추가 된 코드입니다. AI챕터에서 다룰 예정입니다.
                if self.use_ai:
                    from ai_filter import ai_filter
                    ai_filter(self.ai_filter_num, engine=self.engine_simulator, until=date_rows_yesterday)

                # 최종적으로 realtime_daily_buy_list 테이블에 저장 된 종목들을 가져온다.
                self.get_realtime_daily_buy_list()

            # 모의, 실전 투자 봇 의 경우
            else:
                # check_item 컬럼에 0 으로 setting
                df_realtime_daily_buy_list['check_item'] = int(0)
                df_realtime_daily_buy_list.to_sql('realtime_daily_buy_list', self.engine_simulator, if_exists='replace')

                # 현재 보유 중인 종목들은 삭제
                sql = "delete from realtime_daily_buy_list where code in (select code from possessed_item)"
                self.engine_simulator.execute(sql)


        # 매수할 종목이 없으면, df_realtime_daily_buy_list라는 데이터프레임의 길이를 저장하는
        # len_df_realtime_daily_buy_list에 다가 0을 넣는다.
        else:
            self.len_df_realtime_daily_buy_list = 0
            #강의 촬영 후 추가 코드 (매수 조건에 맞는 종목이 하나도 없을 경우 realtime_daily_buy_list 를 비워준다)
            if self.engine_simulator.dialect.has_table(self.engine_simulator, "realtime_daily_buy_list"):
                self.engine_simulator.execute("""
                    DELETE FROM realtime_daily_buy_list 
                """)

    # 현재의 주가를 all_item_db에 있는 보유한 종목들에 대해서 반영 한다.
    def db_to_all_item_present_price_update(self, code_name, d1_diff_rate, close, open, high, low, volume, clo5, clo10, clo20,
                                                         clo40, clo60, clo80, clo100, clo120, option='ALL'):
        # 영상 촬영 후 아래 내용 업데이트 하였습니다.
        if self.op == 'real': # 콜렉터에서 업데이트 할 때는 현재가를 종가로 업데이트(trader에서 실시간으로 present_price 업데이트함)
            present_price = close
        else:
            present_price = open # 시뮬레이터에서는 open가를 현재가로 업데이트

        # option이 ALL이면 모든 데이터 업데이트
        if option == "ALL":
            sql = f"update all_item_db set d1_diff_rate = {d1_diff_rate}, close = {close}, open = {open}, high = {high}, " \
                  f"low = {low}, volume = {volume}, present_price = {present_price}, clo5 = {clo5}, clo10 = {clo10}, clo20 = {clo20}, " \
                  f"clo40 = {clo40}, clo60 = {clo60}, clo80 = {clo80}, clo100 = {clo100}, clo120 = {clo120} " \
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
                                              'volume', 'clo5', 'clo10', 'clo20', 'clo40', 'clo60', 'clo80',
                                              'clo100', 'clo120', "clo5_diff_rate", "clo10_diff_rate",
                                              "clo20_diff_rate", "clo40_diff_rate", "clo60_diff_rate",
                                              "clo80_diff_rate", "clo100_diff_rate", "clo120_diff_rate"])

    # 가장 초기에 매수 했을 때 all_item_db 에 추가하는 함수
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
        self.df_all_item.loc[0, 'holding_amount'] = int(self.invest_unit / purchase_price)
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
        try:
            self.df_all_item.loc[0, 'd1_diff_rate'] = float(df.loc[index, 'd1_diff_rate'])
        except:
            self.df_all_item.loc[0, 'd1_diff_rate'] = 0
        self.df_all_item.loc[0, 'clo5'] = df.loc[index, 'clo5']
        self.df_all_item.loc[0, 'clo10'] = df.loc[index, 'clo10']
        self.df_all_item.loc[0, 'clo20'] = df.loc[index, 'clo20']
        self.df_all_item.loc[0, 'clo40'] = df.loc[index, 'clo40']
        self.df_all_item.loc[0, 'clo60'] = df.loc[index, 'clo60']
        self.df_all_item.loc[0, 'clo80'] = df.loc[index, 'clo80']
        self.df_all_item.loc[0, 'clo100'] = df.loc[index, 'clo100']
        self.df_all_item.loc[0, 'clo120'] = df.loc[index, 'clo120']

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
        if df.loc[index, 'clo80_diff_rate'] is not None:
            self.df_all_item.loc[0, 'clo80_diff_rate'] = float(df.loc[index, 'clo80_diff_rate'])
        if df.loc[index, 'clo100_diff_rate'] is not None:
            self.df_all_item.loc[0, 'clo100_diff_rate'] = float(df.loc[index, 'clo100_diff_rate'])
        if df.loc[index, 'clo120_diff_rate'] is not None:
            self.df_all_item.loc[0, 'clo120_diff_rate'] = float(df.loc[index, 'clo120_diff_rate'])

        self.df_all_item.loc[0, 'valuation_profit'] = int(0)

        # 컬럼 중에 nan 값이 있는 경우 0으로 변경 -> 이렇게 안하면 아래 데이터베이스에 넣을 때
        # AttributeError: 'numpy.int64' object has no attribute 'translate' 에러 발생
        self.df_all_item = self.df_all_item.fillna(0)

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

        try:
            if self.sell_division_on:
                sql = f'''select (SELECT sum(valuation_profit) from all_item_db)
                            +(select sum(valuation_profit) from division_sell_list)'''
                self.sum_valuation_profit = int(self.engine_simulator.execute(sql).fetchall()[0][0])
                print("sum_valuation_profit: " + str(self.sum_valuation_profit))
            else:
                sql = "SELECT sum(valuation_profit) from all_item_db"
                self.sum_valuation_profit = self.engine_simulator.execute(sql).fetchall()[0][0]
                print("sum_valuation_profit: " + str(self.sum_valuation_profit))
        except:
            sql = "SELECT sum(valuation_profit) from all_item_db"
            self.sum_valuation_profit = self.engine_simulator.execute(sql).fetchall()[0][0]
            print("sum_valuation_profit: " + str(self.sum_valuation_profit))

        # 전재산이라고 보면 된다. 현재 총손익 까지 고려했을 때
        self.total_invest_price = self.start_invest_price + self.sum_valuation_profit
        test=0
        # 현재 총 투자한 금액 계산
        sql = "select sum(item_total_purchase) from all_item_db where sell_date = '%s'"
        self.total_purchase_price = self.engine_simulator.execute(sql % (0)).fetchall()[0][0]
        if self.total_purchase_price is None:
            self.total_purchase_price = 0

        # 매도를 한 종목들 대상 수익 계산
        try:
            if self.sell_division_on:
                sql = f'''select (select sum(valuation_profit) from (select * from all_item_db where sell_date !=0) a)+
                    (select sum(valuation_profit) from division_sell_list)
                        '''
                self.total_valuation_profit = int(self.engine_simulator.execute(sql).fetchall()[0][0])
            else:
                sql = "select sum(valuation_profit) from all_item_db where sell_date != '%s'"
                self.total_valuation_profit = self.engine_simulator.execute(sql % (0)).fetchall()[0][0]
        except:
            sql = "select sum(valuation_profit) from all_item_db where sell_date != '%s'"
            self.total_valuation_profit = self.engine_simulator.execute(sql % (0)).fetchall()[0][0]

        test=0
        if self.total_valuation_profit is None:
            self.total_valuation_profit = 0

        # 현재 투자 가능한 금액(예수금) = (초기자본 + 매도한 종목의 수익) - 현재 총 투자 금액
        self.d2_deposit = self.start_invest_price + self.total_valuation_profit - self.total_purchase_price

    # 시뮬레이팅 할 날짜를 가져 오는 함수
    # 장이 열렸던 날 들을 self.date_rows 에 담기 위해서 gs글로벌의 date값을 대표적으로 가져온 것
    def get_date_for_simul(self):
        sql = "select date from `gs글로벌` where date >= '%s' and date <= '%s' group by date"
        self.date_rows = self.engine_daily_craw.execute(sql % (self.simul_start_date, self.simul_end_date)).fetchall()

    # daily_buy_list에 일자 테이블이 존재하는지 확인하는 함수
    def is_date_exist(self, date):
        print("is_date_exist 함수에 들어왔습니다!", date)
        sql = "select 1 from information_schema.tables where table_schema ='daily_buy_list' and table_name = '%s'"
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
        sql = "select 1 from information_schema.tables where table_schema = 'daily_craw' and table_name = '%s'"
        rows = self.engine_daily_craw.execute(sql % (code_name)).fetchall()
        if len(rows) == 1:
            return True
        else:
            print("daily_craw db 에 " + str(code_name) + " 테이블이 존재하지 않는다. !! ")
            return False

    # min_craw 데이터 베이스에서 특정 종목이 존재하는 여부를 파악하는 함수
    def is_min_craw_table_exist(self, code_name):
        sql = "select 1 from information_schema.tables where table_schema = 'min_craw' and table_name = '%s'"
        rows = self.engine_craw.execute(sql % (code_name)).fetchall()
        if len(rows) == 1:
            return True
        else:
            print("min_craw db 에 " + str(code_name) + " 테이블이 존재하지 않는다. !! ")
            return False

    # 분별 현재 누적 거래량 가져오는 함수
    def get_now_volume_by_min(self, code_name, min_date):
        sql = f'''select sum_volume from `{code_name}` 
                        where date = {min_date} 
                            and open != 0 
                            and volume !=0 
                            order by sum_volume desc 
                            limit 1
                            '''

        rows = self.engine_craw.execute(sql).fetchall()


        if len(rows) == 1:
            return rows[0][0]
        else:
            return False

    def _get_yes_prespan1(self, code_name,backspan_day):

        if backspan_day !=0:
            sql='select prespan1 from daily_subindex.`%s` where code_name="%s"'
            rows = self.engine_craw.execute(sql % (backspan_day,code_name)).fetchall()
        else:
            rows=[]

        if len(rows) == 1:
            return rows[0][0]
        else:
            return False


    # 분별 현재 종가 가져오는 함수
    # (close가 일별 데이터에서는 일별 종가 이지만, 분별 데이터에서의 close는 각 분별에 대한 종가를 의미
    # 즉, 1분 간격으로 변화하는 시세를 가져오는 함수
    def get_now_close_price_by_min(self, code_name, min_date):
        sql = f'''select close from `{code_name}` 
                    where date = {min_date} 
                        and open != 0 
                        and volume !=0 
                        order by sum_volume desc 
                        limit 1'''
        rows = self.engine_craw.execute(sql).fetchall()

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
        sql = "select d1_diff_rate, close, open, high, low, volume, clo5, clo10, clo20, clo40, clo60, clo80, clo100, clo120 from `" + date + "` where code_name = '%s' group by code"
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
            clo80 = rows[0][11]
            clo100 = rows[0][12]
            clo120 = rows[0][13]


            # 만약에 open가에 어떤 값이 있으면(True) 현재 주가를 all_item_db에 반영 하기 위해 아래 함수를 들어간다.
            if open:
                self.db_to_all_item_present_price_update(code_name, d1_diff_rate, close, open, high, low, volume, clo5, clo10, clo20,
                                                         clo40, clo60, clo80, clo100, clo120, option)
            else:
                continue

    # 보유 중인 종목들의 주가 이외의 기타 정보들을 업데이트 하는 함수
    def update_all_db_etc(self):
        # valuation_price 업데이트
        sql = f"update all_item_db set valuation_price = round((present_price  * holding_amount) - item_total_purchase * {self.fees_rate} - present_price*holding_amount*{self.fees_rate + self.tax_rate}) where sell_date = '%s'"
        self.engine_simulator.execute(sql % (0))

        # valuation_profit, rate 업데이트
        sql = "update all_item_db set rate= round((valuation_price - item_total_purchase)/item_total_purchase*100,2), valuation_profit =  valuation_price - item_total_purchase where sell_date = '%s';"
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
                 "FROM all_item_db ALLDB, daily_buy_list.`" + date_before + "` BEFORE_DAY "\
                   "WHERE ALLDB.code = BEFORE_DAY.code " \
                   "AND ALLDB.sell_date = 0 "\
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

        # elif self.sell_list_num == 1101:
        #     date_end=self.date_rows[i][0]+'1519'
        #     buy_end=self.date_rows[i][0]+'1100'
        #
        #     if self.min_now==date_end:
        #         sql=f'''
        #                 SELECT code, rate, present_price,valuation_profit
        #                 FROM all_item_db
        #                 WHERE (sell_date = '0') and rate>5
        #            '''
        #         sell_list = self.engine_simulator.execute(sql).fetchall()
        #         self.buy_stop = True
        #     elif self.min_now>date_end:
        #         self.buy_stop=True
        #         sell_list = []
        #     elif self.min_now>buy_end:
        #         self.buy_stop = True
        #         sql = f'''
        #                                     SELECT code, rate, present_price,valuation_profit
        #                                     FROM all_item_db
        #                                     WHERE (sell_date = '0') and (rate<{self.losscut_point} or
        #                                         rate>{self.sell_point})
        #                                            '''
        #         sell_list = self.engine_simulator.execute(sql).fetchall()
        #     else:
        #         sql = f'''
        #                     SELECT code, rate, present_price,valuation_profit
        #                     FROM all_item_db
        #                     WHERE (sell_date = '0') and (rate<{self.losscut_point} or
        #                         rate>{self.sell_point})
        #                            '''
        #         sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 8:
            date_yesterday = self.date_rows[i-1][0]
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and (datediff('%s',left(buy_date,8))>90 or rate>='%s' or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(sql % (0,date_yesterday, self.sell_point, self.losscut_point)).fetchall()
        elif self.sell_list_num == 9:
            date_yesterday = self.date_rows[i - 1][0]
            sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                  "and ((datediff('%s',left(buy_date,8))>=0 and rate>=0) or rate>='%s' or rate <= '%s') group by code"
            sell_list = self.engine_simulator.execute(
                sql % (0, date_yesterday, self.sell_point, self.losscut_point)).fetchall()
        elif self.sell_list_num == 10:
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit 
                      FROM all_item_db db, daily_subindex.`{date_yesterday}` subindex
                      WHERE db.code=subindex.code 
                      and (db.sell_date = '0') 
                      and ((db.present_price/subindex.bband_U)<0.85
                      or rate > {self.sell_point})
                      group by code
                        '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 11:
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit 
                      FROM all_item_db db, daily_subindex.`{date_yesterday}` subindex
                      WHERE db.code=subindex.code 
                      and (db.sell_date = '0') 
                      and ((db.present_price/subindex.bband_U)<0.90
                      or rate > {self.sell_point})
                      group by code
                        '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 12:
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit 
                      FROM all_item_db db, daily_subindex.`{date_yesterday}` subindex
                      WHERE db.code=subindex.code 
                      and (db.sell_date = '0') 
                      and ((db.present_price/subindex.bband_U)<0.90
                      or rate > {self.sell_point} 
                      or (datediff('{date_yesterday}',left(buy_date,8))>30 and rate>0)
                        )
                      group by code
                        '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 13:
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit 
                      FROM all_item_db db, daily_subindex.`{date_yesterday}` subindex
                      WHERE db.code=subindex.code 
                      and (db.sell_date = '0') 
                      and (rate > {self.sell_point} 
                      or (datediff('{date_yesterday}',left(buy_date,8))>30 
                        and db.present_price<subindex.prespan1
                        and db.present_price<subindex.prespan2
                        )
                      or datediff('{date_yesterday}',left(buy_date,8))>90 
                      or rate < {self.losscut_point}
                        )
                      group by code
                        '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 14:
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit 
                      FROM all_item_db db, daily_subindex.`{date_yesterday}` subindex
                      WHERE db.code=subindex.code 
                      and (db.sell_date = '0') 
                      and (rate > {self.sell_point} 
                      or (db.present_price<subindex.prespan1*0.98
                        and db.present_price<subindex.prespan2*0.98
                        )
                      or datediff('{date_yesterday}',left(buy_date,8))>90 
                      or rate < {self.losscut_point}
                        )
                      group by code
                        '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 15:
            date_yesterday = self.date_rows[i - 1][0]
            one_day_before = self.date_rows[i - 2][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit 
                      FROM all_item_db db, daily_subindex.`{date_yesterday}` subindex1
                      WHERE db.code=subindex1.code
                      and (db.sell_date = '0') 
                      and (db.rate > {self.sell_point} 
                      or (subindex1.ma19 < subindex1.ma20 
                        and db.present_price < subindex1.standard_line
                        and db.present_price < subindex1.switch_line
                        and (db.present_price/subindex1.bband_U)<0.90
                        )
                      or datediff('{date_yesterday}',left(db.buy_date,8))>90 
                      or db.rate < {self.losscut_point}
                        )
                      group by code
                        '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 16:
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit 
                      FROM all_item_db db, daily_subindex.`{date_yesterday}` subindex
                      WHERE db.code=subindex.code 
                      and (db.sell_date = '0') 
                      and (rate > {self.sell_point} 
                      or (datediff('{date_yesterday}',left(buy_date,8))>30 
                        and db.present_price<subindex.prespan1
                        and db.present_price<subindex.prespan2
                        and ((subindex.BBand_U/subindex.close-1)+(1-subindex.BBand_L/subindex.close) > 0.1)
                        )
                      or datediff('{date_yesterday}',left(buy_date,8))>90 
                      or rate < {self.losscut_point}
                        )
                      group by code
                        '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 17:
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit 
                      FROM all_item_db db, daily_subindex.`{date_yesterday}` subindex
                      WHERE db.code=subindex.code 
                      and (db.sell_date = '0') 
                      and ((db.present_price/subindex.bband_U)<0.95
                      or rate > {self.sell_point} 
                      or (datediff('{date_yesterday}',left(buy_date,8))>30 and rate>0)
                        )
                      group by code
                        '''
            sell_list = self.engine_simulator.execute(sql).fetchall()

        elif self.sell_list_num == 18:
            date_yesterday = self.date_rows[i - 1][0]
            sql = f'''SELECT db.code, db.rate, db.present_price,db.valuation_profit 
                      FROM all_item_db db, daily_subindex.`{date_yesterday}` subindex
                      WHERE db.code=subindex.code 
                      and (db.sell_date = '0') 
                      and ((db.present_price/subindex.bband_U)<0.90
                      or rate > {self.sell_point} 
                      or (datediff('{date_yesterday}',left(buy_date,8))>30 and rate>0)
                        )
                      group by code
                        '''
            sell_list = self.engine_simulator.execute(sql).fetchall()




        elif self.sell_list_num == 301:
            date_yesterday = self.date_rows[i-1][0]
            sell_list=[]

            if len(self.sell_list_condition)>0:
                for code in self.sell_list_condition:
                    sql = "SELECT code, rate, present_price,valuation_profit FROM all_item_db WHERE (sell_date = '%s') " \
                          "and (datediff('%s',left(buy_date,8))>90 or rate>='%s' or rate <= '%s' or code='%s') group by code"
                    sell_list_temp = self.engine_simulator.execute(
                        sql % (0, date_yesterday, self.sell_point, self.losscut_point, code)).fetchall()
                    sell_list.append(sell_list_temp)
                    # 이 아래는 2차원 list를 1차원으로 바꾸는 방법이다.

        ##################################################################################################################################################################################################################
        else:
            print(f"{self.simul_num}번 알고리즘에 대한 self.sell_list_num 설정이 비었습니다. variable_setting 함수에서 self.sell_list_num을 확인해주세요.")
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

    # 분할매수용!!!!! 실제로 매도를 하는 함수 (매도 한 결과를 all_item_db에 반영)
    def sell_send_order_division(self, min_date, sell_price, sell_rate, code):
        # print("sell send order")
        test=0
        if sell_rate > self.sell_division_rate:
            test=0
            division_sell_sql = f'''
                    select code,code_name,purchase_price,holding_amount,
                    present_price as sell_price,
                    rate,
                    (holding_amount-round(holding_amount/2,0)) sell_amount,
                    buy_date,
                    {min_date} sell_date,
                    round((present_price  * (holding_amount-round(holding_amount/2,0))) - (purchase_price  * (holding_amount-round(holding_amount/2,0))) * {self.fees_rate} - present_price*(holding_amount-round(holding_amount/2,0))*{self.fees_rate + self.tax_rate}) valuation_price,
                    round((present_price  * (holding_amount-round(holding_amount/2,0))) - (purchase_price  * (holding_amount-round(holding_amount/2,0))) * {self.fees_rate} - present_price*(holding_amount-round(holding_amount/2,0))*{self.fees_rate + self.tax_rate}) - ((holding_amount-round(holding_amount/2,0))*purchase_price) valuation_profit
                    from all_item_db
                    where code={code} and sell_date=0
                '''
            division_sell_sql_result = self.engine_simulator.execute(division_sell_sql).fetchall()

            division_sell_list = DataFrame(division_sell_sql_result,
                       columns=['code', 'code_name', 'purhcase_price', 'holding_amount', 'sell_price','rate'
                                ,'sell_amount','buy_date','sell_date','valuation_price','valuation_profit'])

            division_sell_list.to_sql('division_sell_list', self.engine_simulator, if_exists='append')

            update_sql = f'''
                                update all_item_db
                                set holding_amount = round(holding_amount/2,0),
                                item_total_purchase=holding_amount*purchase_price
                                where code = {code}
                                and sell_date=0
                        '''
            self.engine_simulator.execute(update_sql)

            test=0
            self.update_all_db_etc()
            test=0



        else:
            sql = "UPDATE all_item_db SET sell_date= '%s', sell_price ='%s' ,sell_rate ='%s' WHERE code='%s' and sell_date = '%s' " \
                  "ORDER BY buy_date desc LIMIT 1"
            self.engine_simulator.execute(sql % (min_date, sell_price, sell_rate, code, 0))
            # 매도 후 정산


        self.check_balance()

        #test=0
            # all_item_db의 holding_amount,item_total_purchase를 바꿔야함
            # valuation_price, valuation_profit 업데이트 해주는 함수임(현재가 X 보유수량으로 함)

            #
            # test=0
            # sql=f'''
            #         select holding_amount,purchase_price,item_total_purchase from all_item_db
            #         where code = {code}
            #         and sell_date=0
            #     '''
            # sql_result=self.engine_simulator.execute(sql).fetchall()
            # holding_amount=sql_result[0][0]
            # holding_amount_half=round(holding_amount/2,0)








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
            # 분할매도 할지 말지
            try:
                division_sell_list_step = sell_list[i][5]
            except:
                division_sell_list_step=0

            if self.sell_division_on and division_sell_list_step==1:
                if get_sell_rate < 0:
                    print("손절 매도(분할매도)!!!!$$$$$$$$$$$ 수익: " + str(valuation_profit/2) + " / 수익률 : " + str(
                        get_sell_rate) + " / 종목코드: " + str(get_sell_code) + " $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
                else:
                    print("익절 매도(분할매도)!!!!$$$$$$$$$$$ 수익: " + str(valuation_profit/2) + " / 수익률 : " + str(
                        get_sell_rate) + " / 종목코드: " + str(get_sell_code) + " $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
            else:
                if get_sell_rate < 0:
                    print("손절 매도!!!!$$$$$$$$$$$ 수익: " + str(valuation_profit) + " / 수익률 : " + str(
                        get_sell_rate) + " / 종목코드: " + str(get_sell_code) + " $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
                else:
                    print("익절 매도!!!!$$$$$$$$$$$ 수익: " + str(valuation_profit) + " / 수익률 : " + str(
                        get_sell_rate) + " / 종목코드: " + str(get_sell_code) + " $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")


            # 실제로 매도를 하는 함수 (매도 한 결과를 all_item_db에 반영)
            test=0
            if self.sell_division_on and division_sell_list_step==1 :
                self.sell_send_order_division(date, get_present_price, get_sell_rate, get_sell_code)
            else:
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
    def db_to_jango(self, date_rows_today):
        # 정산 함수
        self.check_balance()
        if self.is_simul_table_exist(self.db_name, "all_item_db") == False:
            return

        self.jango.loc[0, 'date'] = date_rows_today

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
        # 촬영 후 업데이트 되었습니다
        dt_format = '%Y%m%d%H%M'
        simul_time = datetime.datetime.strptime(simul_start_date + "0900", dt_format)
        min_delta = datetime.timedelta(minutes=1)

        times = []
        while simul_time.hour != 15 or simul_time.minute != 31:
            times.append((datetime.datetime.strftime(simul_time, dt_format),))
            simul_time += min_delta

        self.min_date_rows = times
    # 분별 시뮬레이팅 함수
    # 새로운 종목 매수 및 보유한 종목의 데이터를 업데이트 하는 함수, 매도 함수도 포함
    def trading_by_min(self, date_rows_today, date_rows_yesterday, i):
        self.print_info(date_rows_today)

        # all_item_db가 존재하고, 현재 보유 중인 종목이 있다면 아래 로직을 들어간다.
        if self.is_simul_table_exist(self.db_name, "all_item_db") and len(self.get_data_from_possessed_item()) != 0:
            # 보유 중인 종목들의 주가를 일별로 업데이트 하는 함수(option 이 OPEN 이면 OPEN가만 업데이트)
            self.update_all_db_by_date(date_rows_today, option='OPEN')

        # 분별 시간 데이터를 가져온다.
        self.get_date_min_for_simul(date_rows_today)
        if len(self.min_date_rows) != 0:
            # 분 단위로 for문을 돈다
            for t in range(len(self.min_date_rows)):



                min = self.min_date_rows[t][0]
                self.min_now = min #실시간 하려고 내가 추가함


                #9시 매도, 종가 매수를 위한 알고리즘 추가해봄봄 !@
                # if self.trade_check_num==5 and(min[-4:] != '0900') and (min[-4:] != '1530') \
                #         and(min[-4:] != '0901') and(min[-4:] != '0902') and(min[-4:] != '0903')\
                #         and(min[-4:] != '0905'):
                #     continue


                #실시간 일목균형표 매매를 위해 날짜 계산 추가함.
                if self.trade_check_num == 5:

                    setting_day = 25
                    if i < setting_day + 1:
                        self.backspan_day = 0
                        continue

                    else:
                        self.backspan_day = self.date_rows[i - setting_day][0]


                else:
                    self.backspan_day = 0

                #print("date_rows_today 확인용",date_rows_today)

                # all_item_db가 존재하고 현재 보유 중인 종목이 있는 경우
                if self.is_simul_table_exist(self.db_name,"all_item_db") and len(self.get_data_from_possessed_item()) != 0:
                    self.print_info(min)
                    self.update_all_db_by_min(min)
                    self.update_all_db_etc()
                    # 매도 함수


                    self.auto_trade_sell_stock(min, i)
                    #3시 30분에만 매매함.
                    if self.trade_check_num == 5 and (min[-4:] != '1530'):
                        continue
                    # self.buy_stop 이 False 이고, 보유 자산이 있으면 실제 매수를 한다.
                    if not self.buy_stop and self.jango_check():
                        # 매수 할 종목을 가져온다
                        self.get_realtime_daily_buy_list()


                        #일목균형표 실시간 매매를 위해서 날짜 넣어주려고 추가시켜줌 backspan_day가 26일 전 날짜임.
                        if self.len_df_realtime_daily_buy_list > 0:

                            self.auto_trade_stock_realtime(min, date_rows_today, date_rows_yesterday)
                        else:
                            print("realtime_daily_buy_list에 금일 매수 대상 종목이 0개 이다.  ")


                #  여긴 가장 초반에 all_itme_db를 만들어야 할때이거나 매수한 종목이 없을 때 들어가는 로직
                else:
                    if not self.buy_stop and self.jango_check():
                        #3시 30분에만 매도하게 함
                        if self.trade_check_num == 5 and (min[-4:] != '1530'):
                            continue
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
            self.update_all_db_by_date(date_rows_today, option = 'OPEN')
            # 보유 중인 종목들의 주가 이외의 기타 정보들을 업데이트 하는 함수
            self.update_all_db_etc()
            # 매도 함수
            self.auto_trade_sell_stock(date_rows_today, i)

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

        #아래부분 추가하였슴 !@
        self.invest_sort_simul_or_real = 0 # 0이 시뮬
        #self.etc_method(self.total_invest_price)

        # self.invest_unit = int(float(self.total_invest_price) * ( 1 / self.divide_invest_unit)  * self.shannon_rate)
        #
        # # 섀넌의 균형 포트폴리오 적용
        # if self.shannon_rate_on == 1:
        #     self.limit_money = (float(self.total_invest_price) - self.invest_unit * self.divide_invest_unit)

        # self.invest_unit = int( float(self.total_invest_price) * (1 / self.divide_invest_unit))  # !#
        # 아래부분 추가하였슴 !@


        # 아래부분 추가하였슴 !@
        self.invest_unit = int(
            float(self.total_invest_price) * (
                        1 / self.divide_invest_unit) * self.avg_momentum_rate * self.divide_rate_using_MDD * self.shannon_rate)  # !#

        # 섀넌의 균형 포트폴리오 적용
        if self.shannon_rate_on == 1:
            self.limit_money = (float(self.total_invest_price) - self.invest_unit * self.divide_invest_unit)

            #######주의할 것은 실제 투자할때 전체 limit money + 한개의 투자금액보다 예수금이 적은지 확인해서 한다(시뮬레이션에서)
            ######그래서 아래와 같이 만들어 주었음.(limit money는 전체금액으로 셋팅해준다)




        # if self.shannon_rate_on == 1:
        #     self.limit_money = self.limit_money + self.invest_unit * self.divide_invest_unit * (1 - self.shannon_rate)

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

        if self.mdd_losscut_on==1:
            self.mdd_value=MDD

            #MDD 숫자로 손절치는 알고리즘임.
            if self.mdd_sell_check==1 :
                self.mdd_sell_count = ((self.mdd_value-self.mdd_losscut) // self.mdd_sell_next_step) + 1
                self.mdd_sell_check=0



            if self.mdd_sell_count > 0:
                self.final_mdd_losscut = self.mdd_losscut + self.mdd_sell_count * self.mdd_sell_next_step
                if self.mdd_value < self.mdd_losscut:
                    self.mdd_sell_count = 0
                    self.final_mdd_losscut = self.mdd_losscut
            elif self.mdd_sell_count==0:
                self.final_mdd_losscut=self.mdd_losscut




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
            self.db_to_jango(date_rows_today)

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
            self.db_to_jango(date_rows_today)

        else:
            print(date_rows_today + "테이블은 존재하지 않는다!!!")

    # 날짜 별 로테이팅 함수
    def rotate_date(self):
        for i in range(1, len(self.date_rows)):
            # print("self.date_rows!!" ,self.date_rows)
            # 시뮬레이팅 할 일자
            date_rows_today = self.date_rows[i][0]

            # 시뮬레이팅 하기 전의 일자
            date_rows_yesterday = self.date_rows[i - 1][0]
            self.yes_day_for_BBand_U = date_rows_yesterday
            self.date_for_avg_momentum_yes = date_rows_yesterday


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

    def shannon_control(self):
        # 아래부분 추가하였슴 !@
        self.invest_unit = int(
            float(self.total_invest_price) * (
                        1 / self.divide_invest_unit) * self.avg_momentum_rate * self.divide_rate_using_MDD * self.shannon_rate)  # !#

        # 섀넌의 균형 포트폴리오 적용
        if self.shannon_rate_on == 1:
            self.limit_money = (float(self.total_invest_price) - self.invest_unit * self.divide_invest_unit)

    def monthly_invest_time(self,today_date):

        monthly_invest_time_after_divide = 30 // (self.monthly_invest_time_divide_unit)
        #30에서 6나누면 5
        range_start=(self.monthly_invest_time_divide_section-1)*monthly_invest_time_after_divide +1
        range_end = self.monthly_invest_time_divide_section*monthly_invest_time_after_divide

        only_date=int(today_date[6:])
        try :
            revalencing_over_date_check_sql=f'''
                    select db.* from all_item_db db, setting_data setting_data
                    where date_diff({today_date},setting_data.final_buy_date)>30
                '''

            revalencing_over_date_check = self.engine_simulator.execute(revalencing_over_date_check_sql).fetchall()
        except:
            revalencing_over_date_check=[]

        if len(revalencing_over_date_check) >0:
            return True



        if only_date >= range_start and only_date <= range_end :
            return True
        else :
            return False




if __name__ == '__main__':
    logger.error('simulator.py로 실행해 주시기 바랍니다.')
    sys.exit(1)
