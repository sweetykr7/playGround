ver = "#version 1.3.0 (by zilad)"
print(f"get_multi_profit_graph.py Version: {ver}")

import pymysql
from sqlalchemy import create_engine
from library import cf
import pandas as pd
from matplotlib import font_manager, rc, ticker, pyplot as plt
from library.logging_pack import *
import random
import time


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
                from (select date, sum_valuation_profit, total_invest from simulator{s_name}.jango_data) b
                order by b.date;
              '''
        rows = engine.execute(sql).fetchall()

        simul = pd.DataFrame(rows, columns=['date', 'sum_valuation_profit', 'total_invest'])
        pre_invest = simul.iloc[0][2]
        simul_dpc_cs = simul['sum_valuation_profit'] / simul['total_invest'] * 100  # (총수익금 / 총투자금) X 100
        simul_dpc_cs = simul_dpc_cs.fillna(0)  # 시뮬레이션의 결측값을 0으로 대체
        simul_profit_rate=simul['sum_valuation_profit'] / pre_invest * 100
        simul_profit_rate=simul_profit_rate.fillna(0)

        return [simul, simul_dpc_cs, simul_profit_rate,simul['sum_valuation_profit']]

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
        start_date = '20191001'

        ## 시뮬레이션 넘버(simul_num)에 '정수 0'을 넣으면 모든 시물레이터의 누적수익률 그래프를 그려준다.
        ## 특정 simul_num 번호만을 넣어 주면 그에 해당하는 그래프만 보여준다.
        ## 시뮬레이션 넘버(simul_num)에 'KOSPI', 'KOSDAQ'이나 '주식명'을 넣으면 주가지수 일간 변동률의 누적 합 그래프를 그려준다.
        #!@
        simul_num = [266,267,254,268,269,256,270,271,258]
        #241,246,247,248,238,249
        #241,250,251,252,240,253
        # simul_num = [2, 4, 201, 202, 203]
        # simul_num = ['KOSPI', 'KOSDAQ', '삼성전자', 0]
        # simul_num = ['KOSPI', 'KOSDAQ', '삼성전자', 2, 3, 4, 201]

        # 모든 그래프를 얻어 저장하기 - 제대로된 그래프를 얻으려면 사전에 모든 시뮬레이션을 오늘 날짜까지 전부 시행해야 한다.
        self.get_all_profit_graphs(start_date, simul_num)

        logger.debug(f"모든 수익률 그래프를 얻어서 'c:/stock_graph/' 폴더에 'multi_profit_rate_graph.png' 그림파일로 저장했습니다.")

    #################################### 그래프 그리기 ##############################################
    def get_graph(self, tname, tname_dcp_cs, tlabel, simul_profit_rate, sum_valuation_profit):
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

        self.ax.plot(tname['date'], tname_dcp_cs, color=tcolor, linestyle=lst, label=slabel)  # 수익율 그래프 그리기
        self.ax.plot(tname['date'], simul_profit_rate, color=tcolor, linestyle=lst, label=slabel)  # 수익율 그래프 그리기 !#
        self.ax.set_ylabel('수익률 %[총 수익금 / 총 투자금(초기투자금+수익금)]')
        self.ax.set_xlabel('날 짜') #241,236,246,238,250,240
        self.ax.set_title('★★★일목균형표로 매수후 /거래대금별 매수/리밸런싱(1,2개월)/익절10,20,30%/★★★\n'
                          '일목균형표/초기투자금 1000만원/손절(-40%)/\n'
                          '20분할 매수/후행스팬이 선행스팬1,2사이에 있고 거래량이 전날대비 10배이상일시 매수/\n'
                          '266:익절10%/리밸런싱1개월, 267:익절10%/리밸런싱2개월, 254:익절10%/리밸런싱3개월,'
                          '268:익절20%/리밸런싱1개월, 269:익절20%/리밸런싱2개월, 256:익절20%/리밸런싱3개월,\n'
                          '270:익절30%/리밸런싱1개월, 271:익절30%/리밸런싱2개월, 258:익절30%/리밸런싱3개월') #None
        #238 20%, 240 30%
        self.ax.xaxis.set_major_locator(ticker.MaxNLocator(10))  # x-축에 보일 ticker 개수
        self.ax.xaxis.grid(True)
        self.ax.yaxis.grid(True)
        self.ax.legend(loc='upper left')

        # 그래프 선 오른쪽에 주식명 (str형)이나 simul_num (int형)을 적어서 여러 그래프들을 구별하기 용이하게 만든다.
        if type(tlabel) == str:
            self.ax.text(len(tname['date']), simul_profit_rate[-1:], tlabel, color=tcolor)  # 그래프 선 오른쪽에 이름쓰기 !#
            #self.ax.text(len(tname['date']), tname_dcp_cs[-1:], tlabel, color=tcolor)  # 그래프 선 오른쪽에 이름쓰기 tname_dcp_cs[-1:]
        else:
            n = int(str(tlabel)[-1])  # tlabel(simul_num 숫자) 중에 일의 자릿수만 골라내어 int형으로 변환하기
            # self.ax.text(len(tname['date']) + n * 2, simul_profit_rate[-1:], tlabel, color=tcolor)  # 그래프 선 오른쪽에 엇갈리게 번호쓰기

            tlabel2='총 수익금 : '+str(sum_valuation_profit)+'원'
            self.ax.text(len(tname['date']) + n * 2, tname_dcp_cs[-1:], tlabel, color=tcolor)  # 그래프 선 오른쪽에 엇갈리게 번호쓰기
            self.ax.text(len(tname['date']) +50+ n * 2, tname_dcp_cs[-1:], tlabel2, color=tcolor)  # 그래프 선 오른쪽에 엇갈리게 번호쓰기

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
            [tname, tname_dcp_cs, simul_profit_rate, sum_valuation_profit] = get_factors(s_date, s_name)
            self.get_graph(tname, tname_dcp_cs, s_name, simul_profit_rate, int(sum_valuation_profit[-1:]))

        plt.xticks(rotation=25)  # x-축 글씨 45도 회전

        # 저장할 폴더 생성
        if not os.path.isdir('c:/stock_graph/'):
            os.mkdir('c:/stock_graph/')

        # 파일명에 '년월일시분초' 정보를 붙여서 반복 저장하더라도 서로 다른 이름이 되도록 한다.
        t_time = time.strftime('%y-%m-%d %H-%M-%S')
        fig.savefig(fname=f'c:/stock_graph/multi_profit_rate_graph ({t_time}).png')

        # plt.show()
        plt.close()


if __name__ == "__main__":
    PofitGraph()
