import numpy as np
import datetime
import pymysql
pymysql.install_as_MySQLdb()

from library import coin_cf as cf

from sqlalchemy import create_engine, event, Text, Float
from sqlalchemy.pool import Pool

import pandas as pd
import talib.abstract as ta
import pymysql.cursors

from library.logging_pack import *

#데이터 변환
class subindex:

    def __init__(self):
        logger.debug("subindex 함수로 들어왔다!!")
        logger.debug("subindex시작!!!!")
        self.variable_setting()

        self.stand_date = cf.start_date

    def variable_setting(self):
        db_name = cf.real_db_name
        self.db_setting_etc(db_name)
        self.today = datetime.datetime.now().strftime('%Y%m%d')


    def db_setting_etc(self, db_name):
        self.coin_daily_craw_engine = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_daily_craw",
            encoding='utf-8')
        self.coin_daily_list_engine = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/coin_daily_list",
            encoding='utf-8')


    def delete_subindex(self):
        try:
            del_sql = f'DROP TABLE subindex'
            self.coin_daily_list_engine.execute(del_sql)
            print("subindex 테이블 삭제")
        except :
            print("delete subindex table pass")
            pass

    def collecting(self):
        #print("pass1")


        co_sql = f'''select TABLE_NAME 
                     FROM information_schema.tables 
                     WHERE table_schema = 'coin_daily_craw'
                '''

        target_code = self.coin_daily_craw_engine.execute(co_sql).fetchall()

        num = len(target_code)
        latest_index = self.get_latest_index(target_code)

        for i in range(num-latest_index):
            code = target_code[i+latest_index][0]
            self.db_name=code
            self.collect_db()
            print(f'''[{code}] 을 가져온다!-,{i+latest_index}''')

    def get_latest_index(self,target_code):

        try:
            latest_code_sql = f'''
                        SELECT code FROM daily_buy_list.subindex
                        WHERE date= {self.today}
                        ORDER BY code DESC LIMIT 1
                            '''

            latest_code=self.coin_daily_list_engine.execute(latest_code_sql).fetchall()[0][0]

            test=0

            for i, (scode, _) in enumerate(target_code):
                if scode == latest_code:
                    latest_index = i+1
        except :  # 아직 한번도 데이터를 넣지 않아 테이블이 존재하지 않을 시
            latest_index = 0
            self.delete_subindex()
        return latest_index



    def collect_db(self):
        # 데이터 불러오기
        sql = f'''select date,code,open,close,low,high,volume,(1-abs(open-close)/abs(high-low)) noise from coin_daily_craw.`{self.db_name}` where Date >= {self.stand_date} order by Date '''
        test=0
        rows = self.coin_daily_craw_engine.execute(sql).fetchall() #self.db_name
        three_s = pd.DataFrame(rows, columns=['date', 'code', 'open', 'close', 'low', 'high', 'volume','noise'])
        three_s = three_s.fillna(0)

        th_noise = list(np.asarray(three_s['noise'].tolist()))

        th_date = list(np.asarray(three_s['date'].tolist()))
        th_date_np = np.array(th_date, dtype='f8')
        th_close = list(np.asarray(three_s['close'].tolist()))
        th_close_np = np.array(th_close, dtype='f8')
        th_high = list(np.asarray(three_s['high'].tolist()))
        th_high_np = np.array(th_high, dtype='f8')
        th_low = list(np.asarray(three_s['low'].tolist()))
        th_low_np = np.array(th_low, dtype='f8')
        th_volume = list(np.asarray(three_s['volume'].tolist()))
        th_volume_np = np.array(th_volume, dtype='f8')

        # 보조지표 계산
        th_cci = ta._ta_lib.CCI(th_high_np, th_low_np, th_close_np, 20)
        th_rsi = ta._ta_lib.RSI(th_close_np, 14)
        th_OBV = ta._ta_lib.OBV(th_close_np, th_volume_np)
        th_macd, th_macd_signal, th_macd_hist = ta._ta_lib.MACD(th_close_np, fastperiod=12, slowperiod=26,
                                                                signalperiod=9)
        th_stoch_slowk, th_stoch_slowd = ta._ta_lib.STOCH(th_high_np, th_low_np, th_close_np,
                                                          fastk_period=5, slowk_period=3, slowk_matype=0,
                                                          slowd_period=3, slowd_matype=0)
        th_BBAND_U, th_BBAND_M, th_BBAND_L = ta._ta_lib.BBANDS(th_close_np, timeperiod=5, nbdevup=2, nbdevdn=2,
                                                               matype=0)

        for_bol_close=pd.DataFrame(th_close)
        new_bband_U, new_bband_M, new_bband_L = self.Bollinger(for_bol_close)

        bband_overcome_within_1month = self.bband_overcome(20, th_close, new_bband_U)

        #atr = ta._ta_lib.stream_ATR()
        #ta._ta_lib.ATR()




        switch_line=self.ichimoku_switch_line(th_close,th_high,th_low)
        standard_line=self.ichimoku_standard_line(th_close,th_high,th_low)
        backspan=self.ichimoku_backspan(th_close)
        prespan1_26=self.ichimoku_prespan1(th_close,th_high,th_low)
        prespan2_26=self.ichimoku_prespan2(th_close,th_high,th_low)
        ma19 = three_s['close'].ewm(span=19, min_periods=8, adjust=False).mean()
        ma20 = three_s['close'].ewm(span=20, min_periods=8, adjust=False).mean()

        #첫번째 파라미터 개월수, 두번째 일수
        avg_momentum_20day=self.avg_momentum(12,20,th_close)
        avg_momentum_plus_12month=self.avg_momentum_plus(12,20,avg_momentum_20day,th_close)
        #여기까지 테스트함.

        avg_noise = self.avg_noise(th_noise)

        best_52 = self.best_52(th_high)
        high_60days, low_60days = self.high_low_60days(th_high, th_low)


        # nan을 모두 0으로 전환
        np.nan_to_num(th_cci, copy=False)
        np.nan_to_num(th_rsi, copy=False)
        np.nan_to_num(th_macd, copy=False)
        np.nan_to_num(th_macd_signal, copy=False)
        np.nan_to_num(th_macd_hist, copy=False)
        np.nan_to_num(th_stoch_slowk, copy=False)
        np.nan_to_num(th_stoch_slowd, copy=False)
        np.nan_to_num(th_BBAND_L, copy=False)
        np.nan_to_num(th_BBAND_M, copy=False)
        np.nan_to_num(th_BBAND_U, copy=False)
        np.nan_to_num(th_OBV, copy=False)
        np.nan_to_num(ma19,copy=False)
        np.nan_to_num(ma20, copy=False)
        ma19 = list(np.asarray(ma19.tolist()))
        ma20 = list(np.asarray(ma20.tolist()))

        np.nan_to_num(avg_noise, copy=False)
        np.nan_to_num(best_52, copy=False)
        np.nan_to_num(high_60days, copy=False)
        np.nan_to_num(low_60days, copy=False)

        # np.nan_to_num(new_bband_L, copy=False)
        # np.nan_to_num(new_bband_U, copy=False)
        # np.nan_to_num(new_bband_M, copy=False)


        #
        # if len(new_bband_U)>0:
        #     new_bband_U.columns = ['new_bband_U']
        #     new_bband_M.columns = ['new_bband_M']
        #     new_bband_L.columns = ['new_bband_L']


        df_switch_line = pd.DataFrame(switch_line, columns=['switch_line'])
        df_standard_line = pd.DataFrame(standard_line, columns=['standard_line'])
        df_backspan = pd.DataFrame(backspan, columns=['backspan'])
        df_prespan1_26 = pd.DataFrame(prespan1_26, columns=['prespan1'])
        df_prespan2_26 = pd.DataFrame(prespan2_26, columns=['prespan2'])
        df_ma19=pd.DataFrame(ma19,columns=['ma19'])
        df_ma20=pd.DataFrame(ma20, columns=['ma20'])
        df_avg_momentum_plus_12month=pd.DataFrame(avg_momentum_plus_12month, columns=['avg_momentum_plus_12month'])
        df_avg_momentum_20day=pd.DataFrame(avg_momentum_20day,columns=['avg_momentum_20day'])

        df_bband_overcome_within_1month = pd.DataFrame(bband_overcome_within_1month, columns=['bband_1month'])

        df_th_cci = pd.DataFrame(th_cci, columns=['th_cci'])
        df_th_rsi = pd.DataFrame(th_rsi, columns=['th_rsi'])
        df_th_macd = pd.DataFrame(th_macd, columns=['th_macd'])
        df_th_macd_signal = pd.DataFrame(th_macd_signal, columns=['th_macd_signal'])
        df_th_macd_hist = pd.DataFrame(th_macd_hist, columns=['th_macd_hist'])
        df_th_stoch_slowk = pd.DataFrame(th_stoch_slowk, columns=['th_stoch_slowk'])
        df_th_stoch_slowd = pd.DataFrame(th_stoch_slowd, columns=['th_stoch_slowd'])
        df_th_BBAND_L = pd.DataFrame(th_BBAND_L, columns=['th_BBAND_L'])
        df_th_BBAND_M = pd.DataFrame(th_BBAND_M, columns=['th_BBAND_M'])
        df_th_BBAND_U = pd.DataFrame(th_BBAND_U, columns=['th_BBAND_U'])
        df_th_OBV = pd.DataFrame(th_OBV, columns=['th_OBV'])
        df_ma19 = pd.DataFrame(ma19, columns=['ma19'])
        df_ma20 = pd.DataFrame(ma20, columns=['ma20'])

        df_avg_noise=pd.DataFrame(avg_noise, columns=['avg_noise'])
        df_best_52=pd.DataFrame(best_52, columns=['best_52'])
        df_high_60days=pd.DataFrame(high_60days, columns=['high_60days'])
        df_low_60days=pd.DataFrame(low_60days, columns=['low_60days'])


        subindex = pd.concat(
            [three_s,df_switch_line ,df_standard_line ,
             df_backspan ,df_prespan1_26 ,df_prespan2_26 ,
             df_ma19,df_ma20,
             df_avg_momentum_plus_12month,df_avg_momentum_20day,
             df_bband_overcome_within_1month ,
             df_th_cci ,df_th_rsi ,
             df_th_macd ,df_th_macd_signal ,df_th_macd_hist ,
             df_th_stoch_slowk ,df_th_stoch_slowd ,
             df_th_BBAND_L ,df_th_BBAND_M ,df_th_BBAND_U ,
             df_th_OBV ,df_ma19 ,df_ma20,
             df_avg_noise,df_best_52,df_high_60days,df_low_60days
                ], axis=1)


        #subindex=subindex.tail(n=1)

        try:
            subindex.to_sql(name='subindex', con=self.coin_daily_list_engine, if_exists='append')
        except:
            pass


    def avg_momentum_plus(self,avg_momentum_period,avg_momentum_day,avg_momentum,th_close):
        avg_momentum_period = 12  # 몇달치 할지, Ref.12
        avg_momentum_day = 20  # 몇일치로 나눌 건지 Ref.20
        sep_1year=[]



        for i in range(len(avg_momentum)):
            temp_sum_list=[]
            if i<219:
                sep_1year.append(0)
            else:
                #avg_momentum에 20일 분량이 들어 있어서 20일을 빼준다.
                temp_avg_momentum=avg_momentum[i-219:i+(avg_momentum_period*avg_momentum_day)-20-219]
                count = 1
                for j in range(len(temp_avg_momentum)):
                    if count % avg_momentum_day == 0:
                        temp_sum_list.append(temp_avg_momentum[-1 * count])
                        #test3=temp_avg_momentum[-1 * count]
                        #print("count : ",count,"temp_sum_list", temp_sum_list)
                    elif count == 1:
                        temp_sum_list.append(temp_avg_momentum[-1 * count])
                        #test4=temp_avg_momentum[-1 * count]
                        #print("count : ", count, "temp_sum_list", temp_sum_list)
                    count = count + 1

                sep_1year.append(sum(temp_sum_list))

        return sep_1year




    def avg_momentum(self,avg_momentum_period,avg_momentum_day,th_close):

        avg_momentum_period=12 # 몇달치 할지, Ref.12
        avg_momentum_day =20 # 몇일치로 나눌 건지 Ref.20

        sep_20day_rows=[]
        sep_20day=[]

        for i in range(len(th_close)):
            if i<19:
                sep_20day.append(0)
            else:
                test1=th_close[i]
                test2=th_close[i-avg_momentum_day+1]
                if ((th_close[i]/th_close[i-avg_momentum_day+1])-1) > 0 :
                    sep_20day.append(1)
                else:
                    sep_20day.append(0)

        return sep_20day












    #추가 함수 만든곳
    #일목균형표 >> ichimoku_
    def ichimoku_switch_line(self, th_close,th_high,th_low):
        switch_line=[]
        for i in range(len(th_close)):
            if i<8:
                switch_line.append(0)
            else:
                maxp=max(th_high[i-8:i+1])
                minp=min(th_low[i-8:i+1])
                sump=(maxp+minp)/2
                switch_line.append(sump)
        return switch_line

    def ichimoku_standard_line(self, th_close,th_high,th_low):
        standard_line=[]
        for i in range(len(th_close)):
            if i<25:
                standard_line.append(0)
            else:
                maxp = max(th_high[i - 25:i + 1])
                minp = min(th_low[i - 25:i + 1])
                sump = (maxp + minp) / 2
                standard_line.append(sump)

        return standard_line

    def ichimoku_backspan(self,th_close):
        backspan=[]
        for i in range(len(th_close)):
            if i > len(th_close)-26:
                backspan.append(0)
            else:
                backspan.append(th_close[i+25])

        return backspan

    def ichimoku_prespan1(self, th_close,th_high,th_low):
        prespan1=[]
        switch_line=self.ichimoku_switch_line(th_close,th_high,th_low)
        standard_line=self.ichimoku_standard_line(th_close,th_high,th_low)
        for i in range(len(th_close)):
            if i <25:
                prespan1.append(0)
            else:
                prespan1.append((switch_line[i]+standard_line[i])/2)

        prespan1_26=[]
        for i in range(len(prespan1)):
            if i<51:
                prespan1_26.append(0)
            else:
                prespan1_26.append(prespan1[i-25])


        return prespan1_26

    def ichimoku_prespan2(self, th_close,th_high,th_low):
        prespan2=[]
        for i in range(len(th_close)):
            if i < 51:
                prespan2.append(0)
            else:
                maxp = max(th_high[i - 51:i + 1])
                minp = min(th_low[i - 51:i + 1])
                sump = (maxp + minp) / 2
                prespan2.append(sump)

        prespan2_26=[]
        for i in range(len(prespan2)):
            if i<77:
                prespan2_26.append(0)
            else:
                prespan2_26.append(prespan2[i - 26])

        return prespan2_26

    def bband_overcome(self, period, th_close, th_BBAND_U):
        bband_overcome_list = []
        confirm_overcome=0
        bband_overcome_gap=0
        if len(th_BBAND_U)==0:
            bband_overcome_list=[]
            return bband_overcome_list
        else :
            th_BBAND_U=th_BBAND_U.iloc[:,0]
        for i in range(len(th_close)):
            if i < period-1:
                bband_overcome_list.append(0)
            else:
                for j in range(period):
                    if th_close[i-period+1+j] > th_BBAND_U[i-period+1+j]:
                        confirm_overcome=1
                        bband_overcome_gap=period-j
                if confirm_overcome==1:
                    bband_overcome_list.append(bband_overcome_gap)
                else:
                    bband_overcome_list.append(0)

            confirm_overcome=0


        return bband_overcome_list

    def Bollinger(self,close):
        bol_mean_list=[]
        bol_upper_list=[]
        bol_down_list=[]

        ma20 = close.rolling(window=20).mean()

        bol_upper_list=ma20+2*close.rolling(window=20).std()

        bol_down_list=ma20-2*close.rolling(window=20).std()

        return bol_upper_list, ma20, bol_down_list

    def avg_noise(self,th_noise):


        avg_noise_list=[]

        avg_cal_noise_list=[]

        for i in range(len(th_noise)):
            if i<19:
                avg_noise_list.append(0)
            else:
                noise_temp = th_noise[i - 19:i + 1]
                noise_rate = sum(noise_temp)/len(noise_temp)

                avg_noise_list.append(noise_rate)




        return avg_noise_list

    def best_52(self,th_high):


        best_52=[]

        for i in range(len(th_high)):
            if i<239:
                best_52.append(0)
            else:
                best_52.append(max(th_high[i -239:i+1]))

        return best_52

    def high_low_60days(self,th_high,th_low):

        best_60days=[]
        low_60days=[]

        for i in range(len(th_high)):
            if i<59:
                best_60days.append(0)
                low_60days.append(0)
            else:
                best_60days.append(max(th_high[i - 59 : i+1]))
                low_60days.append(min(th_high[i - 59 : i+1]))

        return best_60days,low_60days

    def keltner(self):
        test=0





if __name__ == "__main__":
    subindex = subindex()
    subindex.collecting()
    logger.debug("모든 subindex 수집끝!!!!")

#모든 종목 데이터 한바퀴씩















