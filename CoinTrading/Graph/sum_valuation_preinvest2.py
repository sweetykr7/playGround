ver = "#version 1.3.0 (by zilad)"
print(f"get_multi_profit_graph.py Version: {ver}")

import pymysql
from sqlalchemy import create_engine
from library import coin_cf as cf
import pandas as pd
from matplotlib import font_manager, rc, ticker, pyplot as plt
from library.logging_pack import *
import random
import time
from datetime import datetime


def getCAGR(first, last, years):
    return (last / first) ** (1 / years) - 1


def get_factors(s_date, s_name):
    # MySQL의 'daily_craw' DB에 연결하기
    pymysql.install_as_MySQLdb()
    engine = create_engine(
        "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port
        + "/" + 'daily_craw', encoding='utf-8')

    # MySQL 테이블에서 필요한 데이터(rows)를 가져와서 가공하기
    if type(s_name) == str:
        if s_name in ('KOSPI', 'KOSDAQ'):
            ######################################## KOSPI, KOSDAQ ##########################################
            sql = f'select date, close from t_sector where code_name = "종합({s_name})" and date >= {s_date} order by date'
            rows = engine.execute(sql).fetchall()

        else:
            sql = f'select date, close from `{s_name}` where date >= {s_date} order by date'
            rows = engine.execute(sql).fetchall()

        sec = pd.DataFrame(rows, columns=['date', 'close'])
        sec_dpc = (sec['close'] / sec['close'].shift(1) - 1) * 100  # (오늘 종가 / 어제 종가 - 1) X 100
        # sec_diff = (sec['close']/ sec['close'][0] - 1) * 100  # (오늘 종가 / 시작날 종가 - 1) X 100
        sec_dpc.iloc[0] = 0  # 일간 변동률(daily percent change)의 첫번째 값인 NaN을 0으로 변경한다.
        sec_dpc_cs = sec_dpc.cumsum()  # 일간 변동률의 누적 합을 구한다.

        return [sec, sec_dpc_cs]
        # return [sec, sec_diff]

    elif type(s_name) == int:
        ######################################## simulator ##############################################
        # 이전 코드
        # sql = f'''
        #                 select a.date, cast(b.sum_valuation_profit as signed), cast(b.total_invest as signed)
        #                 from (select date from daily_craw.t_sector where code = 001 and date >= {s_date}) a
        #                     left outer join (select date, sum_valuation_profit, total_invest from simulator{s_name}.jango_data) b
        #                     on a.date = b.date
        #                 order by a.date;
        #               '''
        # rows = engine.execute(sql).fetchall()

        sql = f'''
                select b.date, cast(b.sum_valuation_profit as signed), cast(b.total_invest as signed)
                from (select date, sum_valuation_profit, total_invest from coin_simulator{s_name}.jango_data) b
                order by b.date;
              '''
        rows = engine.execute(sql).fetchall()

        simul = pd.DataFrame(rows, columns=['date', 'sum_valuation_profit', 'total_invest'])
        pre_invest = simul.iloc[0][2]
        simul_dpc_cs = simul['sum_valuation_profit'] / simul['total_invest'] * 100  # (총수익금 / 총투자금) X 100
        simul_dpc_cs = simul_dpc_cs.fillna(0)  # 시뮬레이션의 결측값을 0으로 대체
        simul_profit_rate = simul['sum_valuation_profit'] / pre_invest * 100
        simul_profit_rate = simul_profit_rate.fillna(0)

        firstdate = datetime.strptime(simul.iloc[0][0], "%Y%m%d")
        lastdate = datetime.strptime(rows[-1][0], "%Y%m%d")

        date_diff = lastdate - firstdate

        cagr = getCAGR(pre_invest, (simul.iloc[-1][1] + pre_invest), (date_diff.days / 365))
        print(
            f"{s_name}, cagr : {cagr}, days : {date_diff.days}, years : {date_diff.days / 365}, total : {simul.iloc[-1][1] + pre_invest}")

        return [simul, simul_dpc_cs, simul_profit_rate, simul['sum_valuation_profit'], cagr]

    else:
        logger.debug(f"주식이나 시뮬레이션 이름 '{s_name}'이 잘못 되었습니다.")


def get_all_simul_nums():
    # MySQL의 'information_schema' DB에 연결하기
    pymysql.install_as_MySQLdb()
    engine = create_engine(
        "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port
        + "/" + str('information_schema'), encoding='utf-8')

    # 시뮬레이션이 실행되어 MySQL에 저장된 '시뮬레이터 DB명'을 알아내서 simul_names 리스트에 담는다.
    sql = "select table_schema from tables where table_schema like 'simulator%%' group by table_schema"
    simul_names = engine.execute(sql).fetchall()

    # simul_names에 담긴 시뮬레이터 DB명에서 앞 부분 'similator'를 제외한 나머지 숫자들만 simul_nums에 담는다.
    simul_nums = []
    for k in range(len(simul_names)):
        s = simul_names[k - 1][0][9:]
        simul_nums.append(int(s))

    return simul_nums


##########################################################################################
class PofitGraph:

    def __init__(self):
        ## 시뮬레이션 그래프의 시작날짜(start_date)와 시뮬레이션 넘버(simul_num) 설정하기
        start_date = '20050101'  # '20191001'

        ## 시뮬레이션 넘버(simul_num)에 '정수 0'을 넣으면 모든 시물레이터의 누적수익률 그래프를 그려준다.
        ## 특정 simul_num 번호만을 넣어 주면 그에 해당하는 그래프만 보여준다.
        ## 시뮬레이션 넘버(simul_num)에 'KOSPI', 'KOSDAQ'이나 '주식명'을 넣으면 주가지수 일간 변동률의 누적 합 그래프를 그려준다.
        # !@
        # 차례대로 9(683),10(693,694),11(703,704),8(713,714)
        simul_num = [5017,5016] #976
        #1964,1943,1944,1945,1946,1947,1948,1949,1950,1951,1952,1953,1954,1955,1956,1957,1958,1959,1960,1961,1962,1963,1942
        #1527,1530,1542
        #  1104,1123,1930,1931,1932,1933,1934,1935,1936,1937,1938,1939,1929
        # 893, 932, 933, 934, 935, 936, 937, 938, 939
        # 분별 시뮬
        # simul_num = [2, 4, 201, 202, 203]
        # simul_num = ['KOSPI', 'KOSDAQ', '삼성전자', 0]
        # simul_num = ['KOSPI', 'KOSDAQ', '삼성전자', 2, 3, 4, 201]

        # 모든 그래프를 얻어 저장하기 - 제대로된 그래프를 얻으려면 사전에 모든 시뮬레이션을 오늘 날짜까지 전부 시행해야 한다.
        self.get_all_profit_graphs(start_date, simul_num)

        logger.debug(f"모든 수익률 그래프를 얻어서 'c:/stock_graph/' 폴더에 'multi_profit_rate_graph.png' 그림파일로 저장했습니다.")

    #################################### 그래프 그리기 ##############################################
    def get_graph(self, tname, tname_dcp_cs, tlabel, simul_profit_rate, sum_valuation_profit, cagr):
        # 여러 종목들과 시뮬레이션들마다 서로 다른 색깔과 모양을 가진 그래프를 그린다.
        if tlabel == 'KOSPI':
            slabel = '코스피'
            tcolor = [0.8, 0, 0]
            lst = '--'
        elif tlabel == 'KOSDAQ':
            slabel = '코스닥'
            tcolor = [0, 0, 0.8]
            lst = '--'
        else:
            if type(tlabel) == str:
                slabel = tlabel
            else:
                slabel = 'simulator' + str(tlabel)

            tcolor = [random.random() * 0.6, random.random() * 0.6, random.random() * 0.6]
            lst = '-'

        # self.ax.plot(tname['date'], tname_dcp_cs, color=tcolor, linestyle=lst, label=slabel)  # 수익율 그래프 그리기
        self.ax.plot(tname['date'], simul_profit_rate, color=tcolor, linestyle=lst, label=slabel)  # 수익율 그래프 그리기 !#
        # self.ax.set_ylabel('수익률 %[총 수익금 / 총 투자금(초기투자금+수익금)]')
        self.ax.set_ylabel('수익률 %[총 금액(초기투자금+수익금)/초기투자금]')
        self.ax.set_xlabel('날 짜')  # 241,236,246,238,250,240
        self.ax.set_title('★★★Graph비교★★★\n'
                          '\n'
                          '\n'
                          ''
                          )  # None

        # 238 20%, 240 30%
        # '893 Ref(공30 12/3, 방30 12/5, 선행스팬 0.95%),\n'
        # '선행스팬 라인 split 932:0.92, 933:0.93, 934:0.94, 935:0.96, 936:0.97, 937:0.98, 938:0.99, 939:1.00'
        self.ax.xaxis.set_major_locator(ticker.MaxNLocator(30))  # x-축에 보일 ticker 개수
        self.ax.xaxis.grid(True)
        self.ax.yaxis.grid(True)
        #self.ax.set_yscale('log', basey=10)
        #self.ax.set_ylim([10, simul_profit_rate.iloc[-1] * 1.2])
        self.ax.legend(loc='upper left')
        plt.axvline(len(tname['date']), color='r', linewidth=0.5, linestyle='--')  # 끝에 선 귿기

        # plt.axvline(len(tname['date']), color='r', linestyle='--', linewidth=3)  # 끝에 선 귿기

        # 그래프 선 오른쪽에 주식명 (str형)이나 simul_num (int형)을 적어서 여러 그래프들을 구별하기 용이하게 만든다.
        if type(tlabel) == str:
            # self.ax.text(len(tname['date']), simul_profit_rate[-1:], tlabel, color=tcolor)  # 그래프 선 오른쪽에 이름쓰기 !#
            self.ax.text(len(tname['date']), tname_dcp_cs[-1:], tlabel,
                         color=tcolor)  # 그래프 선 오른쪽에 이름쓰기 tname_dcp_cs[-1:]
        else:
            n = int(str(tlabel)[-1])  # tlabel(simul_num 숫자) 중에 일의 자릿수만 골라내어 int형으로 변환하기
            # self.ax.text(len(tname['date']) + n * 2, simul_profit_rate[-1:], tlabel, color=tcolor)  # 그래프 선 오른쪽에 엇갈리게 번호쓰기

            tlabel2 = '총 수익금 : ' + str(sum_valuation_profit) + '원'
            self.ax.text(len(tname['date']) + n * 2, simul_profit_rate[-1:], tlabel,
                         color=tcolor)  # 그래프 선 오른쪽에 엇갈리게 번호쓰기
            self.ax.text(len(tname['date']) + 180 + n * 2, simul_profit_rate[-1:], tlabel2,
                         color=tcolor)  # 그래프 선 오른쪽에 엇갈리게 번호쓰기

    #################################### 반복해서 모든 그래프 얻기 ##############################################
    def get_all_profit_graphs(self, s_date, s_num):
        # 한글 폰트 지정
        font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/gulim.ttc").get_name()
        rc('font', family=font_name, size=12)

        # 그래프 공간 생성
        fig, self.ax = plt.subplots(figsize=(20, 10))

        # s_num이 0일 때는 모든 simultor 번호로 바꾸어 준다.
        if 0 in s_num:
            del s_num[s_num.index(0)]  # simul_num 리스트에서 0 지우고,
            s_num += get_all_simul_nums()  # simul_num에 모든 simultor 번호를 더한다.

        # simul_num을 반복적으로 넣어주면서 그래프를 그린다.
        for s_name in s_num:
            [tname, tname_dcp_cs, simul_profit_rate, sum_valuation_profit, cagr] = get_factors(s_date, s_name)
            self.get_graph(tname, tname_dcp_cs, s_name, simul_profit_rate, int(sum_valuation_profit[-1:]), cagr)

        plt.xticks(rotation=25)  # x-축 글씨 45도 회전

        # 저장할 폴더 생성
        if not os.path.isdir('c:/stock_graph/'):
            os.mkdir('c:/stock_graph/')

        # 파일명에 '년월일시분초' 정보를 붙여서 반복 저장하더라도 서로 다른 이름이 되도록 한다.
        t_time = time.strftime('%y-%m-%d %H-%M-%S')
        fig.savefig(fname=f'c:/stock_graph/multi_profit_rate_graph ({t_time}).png')

        plt.show()
        plt.close()


if __name__ == "__main__":
    PofitGraph()
