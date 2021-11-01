import pandas as pd
from sqlalchemy import create_engine
from library import cf
import talib.abstract as ta
import pymysql.cursors
import numpy as np
from library.logging_pack import *
from library.open_api import *
from pandas import DataFrame as df
import datetime



pymysql.install_as_MySQLdb()

daily_craw_engine=create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_craw",
            encoding='utf-8')
daily_buy_list_engine = create_engine(
    "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_buy_list" ,
    encoding='utf-8')
simul_engine=create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/simulator11",
            encoding='utf-8')
min_craw_engine = create_engine("mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/min_craw",
            encoding='utf-8')
fnguide_engine = create_engine("mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/fnguide",
            encoding='utf-8')




#stand_date = '20070903'


#완료한 date를 불러오는 곳
# stand_date_sql = f'''
# select complete_date from subindex_setting_data
# '''
# stand_date = daily_buy_list_engine.execute(stand_date_sql).fetchall()[0][0]
#print("stand_date",stand_date)

#완료한 date 하루를 삭제하는 곳
#완료되지 않은 상태에서 또 진행하면 중복되기 때문에 마지막날만 삭제를 해주고 진행한다.

#delete_date_sql=f'''
#delete from subindex_test where date='{stand_date}'
#'''
#daily_buy_list_engine.execute(delete_date_sql).fetchall()




#데이터 변환
class subindex:

    def __init__(self):
        logger.debug("subindex 함수로 들어왔다!!")
        logger.debug("subindex시작!!!!")
        #self.open_api = open_api()
        #self.stand_date = self.open_api.today
        self.stand_date = '20060101'
        self.start_date = '20060101'
        try:
            del_sql = f'DROP TABLE subindex'
            daily_buy_list_engine.execute(del_sql)
            print("subindex 테이블 삭제")
        except :
            print("delete subindex table pass")
            pass

        self.engine_daily_craw = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_craw",
            encoding='utf-8')
        self.engine_daily_buy_list = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_buy_list",
            encoding='utf-8')

        self.engine_daily_subindex=create_engine(
        "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_subindex",
        encoding='utf-8')

    def collecting_list_check(self):
        co_sql = f'''select tb.TABLE_NAME FROM information_schema.tables tb
                                WHERE tb.table_schema = "daily_craw"
                            '''

        return daily_craw_engine.execute(co_sql).fetchall()

    def collecting(self):

        target_code=self.collecting_list_check()


        try:
            del_sql = f'DROP TABLE subindex'
            daily_buy_list_engine.execute(del_sql)
            print("subindex 테이블 삭제")
        except:
            print("delete subindex table pass")
            pass

        # if len(target_code)>=2000:
        #     for_num=2000
        # else:
        #     for_num=len(target_code)

        #for_num = len(target_code)
        #df_index=0
        # df_index = 100
        for i in range(len(target_code)):
            self.db_name = target_code[i][0]
            self.db_name = self.db_name.replace("%", "%%")
            self.collect_db()
            print(self.db_name , "을 가져온다!-",i)
            #df_index=i+1

            #self.daily_subindex()

            #daily_subindex_check_df=pd.DataFrame(target_code[0:df_index], columns=['code_name'])
            #daily_subindex_check_df.to_sql(name='daily_subindex_check', con=self.engine_daily_subindex, if_exists='append')

            target_code = self.collecting_list_check()

    def date_rows_setting(self):
        print("date_rows_setting!!")
        sql = "select date from `gs글로벌` where date >= '%s' group by date"
        self.date_rows = self.engine_daily_craw.execute(sql % self.start_date).fetchall()

    def collect_db(self):
        # 데이터 불러오기
        # self.db_name='서진오토모티브'
        # self.db_name='비케이탑스'

        sql = "select date,code,code_name,open,close,low,high,volume,(1-abs(open-close)/abs(high-low)) noise,vol20  from daily_craw.`%s` where Date >= %s order by Date "
        rows = daily_craw_engine.execute(sql%(self.db_name,self.stand_date)).fetchall() #self.db_name
        three_s = pd.DataFrame(rows, columns=['date', 'code', 'code_name', 'open','close', 'low', 'high', 'volume','noise','vol20'])
        three_s = three_s.fillna(0)
        fnguide_code=three_s.iloc[0][1]
        #fnguide에서 pbr가져오는 함수
        #fnguide_pbr,fnguide_gpa=self.fnguide(fnguide_code,three_s)

        # 데이터 변환
        th_code = list(np.asarray(three_s['code'].tolist()))
        th_code_name = list(np.asarray(three_s['code'].tolist()))

        test=0
        th_date = list(np.asarray(three_s['date'].tolist()))
        th_date_np = np.array(th_date, dtype='f8')
        th_noise = list(np.asarray(three_s['noise'].tolist()))
        th_close = list(np.asarray(three_s['close'].tolist()))
        th_close_np = np.array(th_close, dtype='f8')
        th_high = list(np.asarray(three_s['high'].tolist()))
        th_high_np = np.array(th_high, dtype='f8')
        th_low = list(np.asarray(three_s['low'].tolist()))
        th_low_np = np.array(th_low, dtype='f8')
        th_volume = list(np.asarray(three_s['volume'].tolist()))
        th_volume_np = np.array(th_volume, dtype='f8')
        th_vol20 = list(np.asarray(three_s['vol20'].tolist()))
        th_open = list(np.asarray(three_s['open'].tolist()))

        # 보조지표 계산
        th_cci = ta._ta_lib.CCI(th_high_np, th_low_np, th_close_np, 20)
        th_rsi = ta._ta_lib.RSI(th_close_np, 14)
        th_OBV = ta._ta_lib.OBV(th_close_np, th_volume_np)
        th_macd, th_macd_signal, th_macd_hist = ta._ta_lib.MACD(th_close_np, fastperiod=12, slowperiod=26,
                                                                signalperiod=9)
        th_stoch_slowk, th_stoch_slowd = ta._ta_lib.STOCH(th_high_np, th_low_np, th_close_np,
                                                          fastk_period=5, slowk_period=3, slowk_matype=0,
                                                          slowd_period=3, slowd_matype=0)
        th_BBAND_U, th_BBAND_M, th_BBAND_L = ta._ta_lib.BBANDS(th_close_np, timeperiod=20, nbdevup=2, nbdevdn=2,
                                                               matype=0)
        #for_bol_close=pd.DataFrame(th_close)

        #new_bband_U, new_bband_M, new_bband_L = self.Bollinger(for_bol_close)

        switch_line=self.ichimoku_switch_line(th_close,th_high,th_low)
        standard_line=self.ichimoku_standard_line(th_close,th_high,th_low)
        backspan=self.ichimoku_backspan(th_close)
        prespan1_26=self.ichimoku_prespan1(th_close,th_high,th_low)
        prespan2_26=self.ichimoku_prespan2(th_close,th_high,th_low)
        ma19 = three_s['close'].ewm(span=19, min_periods=8, adjust=False).mean()
        ma20 = three_s['close'].ewm(span=20, min_periods=8, adjust=False).mean()

        realtime_ichimoku=self.realtime_ichimoku(th_close,th_high,th_volume,prespan1_26)

        avg_noise=self.avg_noise(th_noise)

        best_52=self.best_52(th_high)

        #첫번째 파라미터 개월수, 두번째 일수
        avg_momentum_20day=self.avg_momentum(12,20,th_close)
        avg_momentum_plus_12month=self.avg_momentum_plus(12,20,avg_momentum_20day,th_close)

        #
        bband_overcome_within_1month=self.bband_overcome(20,th_close,th_BBAND_U)

        high_60days,low_60days=self.high_low_60days(th_high,th_low)

        #bband_overcome_within_1month_high_5p = self.bband_overcome_high(20, th_high, th_open, th_close, th_BBAND_U, 1.05)

        #bband_overcome_within_1month_high_10p = self.bband_overcome_high(20, th_high, th_open, th_close, th_BBAND_U, 1.1)

        #bband_overcome_within_2month = self.bband_overcome(40, th_close, th_BBAND_U)

        #realtime_vol20=self.realtime_20vol(th_volume,th_vol20)


        #next_day_trade_money=self.next_day_trade_money(th_open,th_volume)
        #여기까지 테스트함.




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


        #
        # np.nan_to_num(new_bband_L,copy=False)
        # np.nan_to_num(new_bband_U,copy=False)
        # np.nan_to_num(new_bband_M,copy=False)
        #
        # new_bband_U.columns=['new_bband_U']
        # new_bband_M.columns = ['new_bband_M']
        # new_bband_L.columns = ['new_bband_L']


        test=0

        # DataFrame 화 하기
        df_code=pd.DataFrame(th_code,columns=['code'])
        df_cci = pd.DataFrame(th_cci, columns=['cci'])
        df_rsi = pd.DataFrame(th_rsi, columns=['rsi'])
        df_macd = pd.DataFrame(th_macd, columns=['macd'])
        df_macd_signal = pd.DataFrame(th_macd_signal, columns=['macd_signal'])
        df_macd_hist = pd.DataFrame(th_macd_hist, columns=['macd_hist'])
        df_stoch_slowk = pd.DataFrame(th_stoch_slowk, columns=['stoch_slowk'])
        df_stoch_slowd = pd.DataFrame(th_stoch_slowd, columns=['stoch_slowd'])
        df_BBand_U = pd.DataFrame(th_BBAND_U, columns=['BBand_U'])
        df_BBand_M = pd.DataFrame(th_BBAND_M, columns=['BBand_M'])
        df_BBand_L = pd.DataFrame(th_BBAND_L, columns=['BBand_L'])
        df_OBV = pd.DataFrame(th_OBV, columns=['OBV'])
        df_switch_line = pd.DataFrame(switch_line, columns=['switch_line'])
        df_standard_line = pd.DataFrame(standard_line, columns=['standard_line'])
        df_backspan = pd.DataFrame(backspan, columns=['backspan'])
        df_prespan1_26 = pd.DataFrame(prespan1_26, columns=['prespan1'])
        df_prespan2_26 = pd.DataFrame(prespan2_26, columns=['prespan2'])
        df_ma19=pd.DataFrame(ma19,columns=['ma19'])
        df_ma20=pd.DataFrame(ma20, columns=['ma20'])
        df_avg_momentum_plus_12month=pd.DataFrame(avg_momentum_plus_12month, columns=['avg_momentum_plus_12month'])
        df_avg_momentum_20day=pd.DataFrame(avg_momentum_20day,columns=['avg_momentum_20day'])

        df_avg_noise=pd.DataFrame(avg_noise,columns=['avg_noise'])

        df_real_ichimoku=pd.DataFrame(realtime_ichimoku,columns=['real_ichimoku'])

        df_best_52=pd.DataFrame(best_52,columns=['best_52'])

        df_bband_overcome_within_1month=pd.DataFrame(bband_overcome_within_1month,columns=['bband_1month'])

        df_high_60days=pd.DataFrame(high_60days,columns=['high_60days'])
        df_low_60days = pd.DataFrame(low_60days, columns=['low_60days'])

        #df_bband_overcome_within_1month_high_5p = pd.DataFrame(bband_overcome_within_1month_high_5p, columns=['bband_1month_high_5p'])
        #df_bband_overcome_within_1month_high_10p = pd.DataFrame(bband_overcome_within_1month_high_10p, columns=['bband_1month_high_10p'])
        #df_bband_overcome_within_2month=pd.DataFrame(bband_overcome_within_2month,columns=['bband_2month'])

        #df_realtime_vol20=pd.DataFrame(realtime_vol20,columns=['realtime_vol20'])

        #df_next_day_trade_money=pd.DataFrame(next_day_trade_money,columns=['next_day_trade_money'])
        #pbr data 추가
        #df_fnguide_pbr=pd.DataFrame(fnguide_pbr,columns=['fnguide_pbr'])
        #df_fnguide_gpa=pd.DataFrame(fnguide_gpa,columns=['fnguide_gpa'])


        # import datetime
        # start_now=datetime.datetime.now()
        # day_before_1year=start_now - datetime.timedelta(365)
        # day_before_1year.strftime('%Y%m%d')



        # 모든 보조지표 합치기
        subindex = pd.concat(
            [three_s,
             round(df_cci,2), round(df_rsi,2), round(df_OBV,2), round(df_macd,2), round(df_macd_signal,2), round(df_macd_hist,2),
             round(df_BBand_U,2), round(df_BBand_M,2),round(df_BBand_L,2), round(df_stoch_slowk,2), round(df_stoch_slowd,2),
             df_switch_line, df_standard_line, df_backspan, df_prespan1_26, df_prespan2_26, round(df_ma19,3), round(df_ma20,3),
             df_avg_momentum_plus_12month,df_avg_momentum_20day,round(df_avg_noise,2),round(df_real_ichimoku,2),
             df_best_52,df_bband_overcome_within_1month,df_high_60days,df_low_60days
             ], axis=1)

        # ,
        # new_bband_L, new_bband_U, new_bband_M

        test=0
        # subindex = pd.concat(
        #     [three_s, df_cci, df_rsi, df_OBV, df_macd, df_macd_signal, df_macd_hist, df_BBand_U, df_BBand_M,
        #      df_BBand_L, df_stoch_slowk, df_stoch_slowd,
        #      df_switch_line, df_standard_line, df_backspan, df_prespan1_26, df_prespan2_26
        #      ], axis=1)

        # mysql 에 테이블 생성
        subindex.to_sql(name='subindex', con=daily_buy_list_engine, if_exists='append')

    def date_rows_setting(self):


        print("date_rows_setting!!")

        sql = "select date from `gs글로벌` where date >= '%s' group by date"
        self.date_rows = self.engine_daily_craw.execute(sql % self.start_date).fetchall()

    # def daily_subindex(self):
    #     print("daily_buy_list!!!")
    #     self.date_rows_setting()
    #
    #     for k in range(len(self.date_rows)):
    #
    #
    #         # print("self.date_rows !!!!", self.date_rows)
    #         print(str(k) + " 번째 : " +self.date_rows[k][0] )
    #         # daily 테이블 존재하는지 확인
    #         #datetime.datetime.today().strftime(" ******* %H : %M : %S *******")
    #
    #
    #         # if self.is_table_exist_daily_subindex_previous_price(self.date_rows[k][0]) == True:
    #         #     # continue
    #         #     print(self.date_rows[k][0] + "테이블은 존재한다 !! continue!! ")
    #         #     continue
    #         # else:
    #         #     print(self.date_rows[k][0] + "테이블은 존재하지 않는다 !!!!!!!!!!! table create !! ")
    #
    #         sql = f'''
    #                 select *
    #                 from `subindex`
    #                 where date = '{self.date_rows[k][0]}'
    #                 '''
    #
    #
    #         # daily craw에서 subindex 만들기 위한 날짜 data를 가져옴.
    #         rows = self.engine_daily_buy_list.execute(sql).fetchall()
    #
    #
    #
    #         if len(rows) != 0:
    #             df_temp = DataFrame(rows,
    #                                 columns=['temp','date', 'code', 'code_name','open', 'close', 'low', 'high', 'volume',
    #                                          'noise','vol20','cci','rsi','OBV','macd','macd_signal','macd_hist',
    #                                          'BBand_U','BBand_M','BBand_L','stoch_slowk','stoch_slowd',
    #                                          'switch_line','standard_line','backspan','prespan1','prespan2',
    #                                          'ma19','ma20','avg_momentum_plus_12month','avg_momentum_20day',
    #                                          'avg_noise','real_ichimoku','best_52','bband_1month'])
    #             df_temp.drop(['temp'],axis=1,inplace=True) #위에 리스트 형태 변환하기 싫어서 그냥 이렇게 함.df_temp.drop(['temp'],axis=1,inplace=True)
    #             df_temp.drop(['noise'], axis=1, inplace=True)
    #             df_temp.to_sql(name=self.date_rows[k][0], con=self.engine_daily_subindex, if_exists='append')
    #
    #             # delete_sql = f'''
    #             #                         delete
    #             #                         from `subindex_new`
    #             #                         where date = '{self.date_rows[k][0]}'
    #             #                         '''
    #             #
    #             # # daily craw에서 subindex 만들기 위한 날짜 data를 가져옴.
    #             # delete_rows = self.engine_daily_buy_list.execute(delete_sql)
    #         elif len(rows) ==0:
    #             continue


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


    def fnguide(self,code,three_s):
        fnguide_sql=f'''
                    select code,code_name,date,yymm,fs_year,fs_month,fs_qtr,m314000,m121200,m111000 
                    from fnguide_bps_qq_5
                    where code={code}
                    order by yymm
        '''
        rows = fnguide_engine.execute(fnguide_sql).fetchall()
        rows_df=pd.DataFrame(rows, columns=['code','code_name','date','yymm','fs_year','fs_month','fs_qtr','bps','gp','asset']
                             )


        fnguide_pbr_list=[]
        fnguide_gpa_list=[]
        find_point = 0
        final_bps=0
        final_gpa=0
        for i in range(len(three_s)):
            close=three_s.iloc[i][3]
            date=three_s.iloc[i][0]
            day_before_1year = str(int(date) - 10000)[:6]
            count_date=0
            sum_gp=0
            pbr=0
            gpa=0
            for k in range(find_point,len(rows_df)):
                if day_before_1year <= rows_df.iloc[k][3] and rows_df.iloc[k][3]<date[:6] : #date임
                    count_date=count_date+1
                    try :
                        sum_gp=sum_gp+rows_df.iloc[k][8]
                    except :
                        pass
                    bps = rows_df.iloc[k][7]
                    asset=rows_df.iloc[k][9]
                    if count_date==4:
                        try:
                            if bps is not None:
                                final_bps=bps
                            if bps is None:
                                bps=final_bps
                            if bps == 0 :
                                continue
                            pbr = close / bps
                        except:
                            pbr=0

                        try:
                            gpa = sum_gp / asset
                            final_gpa=gpa
                        except:
                            gpa=0

                        find_point = k - 3
                        break
                    else:
                        pbr = 0
                        gpa = 0

                    if rows_df.iloc[-1][3] <= date[:6]:


                        try:
                            bps = final_bps #rows_df.iloc[-1][7]
                            if bps == 0 :
                                continue
                            pbr = close / bps
                        except:
                            pbr=0


                        try:
                            gpa = final_gpa
                        except:
                            gpa = 0


                elif rows_df.iloc[0][3]>date[:6]:
                    pbr=0
                    gpa=0
                    break
                else :
                    pbr = 0
                    gpa = 0


            fnguide_pbr_list.append(pbr)
            fnguide_gpa_list.append(gpa)
            #print(f''''{three_s.iloc[i][2]}의 {date}를 update중입니다.''')
        test=0
        return fnguide_pbr_list,fnguide_gpa_list

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

    def realtime_ichimoku(self,th_close,th_high,th_volume,prespan1_26):
        realtime_ichimoku = []
        for i in range(len(th_close)):
            if i < 25:
                realtime_ichimoku.append(0)
            elif i > len(th_close)-26:
                realtime_ichimoku.append(0)
            else:
                if th_close[i-1]<prespan1_26[i-25] and prespan1_26[i-25] < th_high[i] \
                        and th_volume[i] > (th_volume[i-1]*3) and th_volume[i-1] != 0:
                    realtime_ichimoku.append(1)
                else:
                    realtime_ichimoku.append(0)

        return realtime_ichimoku

    def bband_overcome(self, period, th_close, th_BBAND_U):
        bband_overcome_list = []
        confirm_overcome=0
        bband_overcome_gap=0
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

    def bband_overcome_high(self, period, th_high, th_open, th_close, th_BBAND_U, over_percent):
        bband_overcome_list = []
        confirm_overcome=0
        bband_overcome_gap=0
        for i in range(len(th_high)):
            if i < period-1:
                bband_overcome_list.append(0)
            else:
                for j in range(period):
                    if th_high[i-period+1+j] > th_BBAND_U[i-period+1+j]*over_percent and th_open[i-period+1+j] > th_close[i-period+1+j]:
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


    def realtime_20vol(self,vol,vol_20):
        realtime_20vol_list=[]
        for i in range(len(vol)):
            if i < 19:
                realtime_20vol_list.append(0)
            else:
                if  vol[i] > vol_20[i]*3:
                    realtime_20vol_list.append(1)
                else :
                    realtime_20vol_list.append(0)
        return realtime_20vol_list

    def next_day_trade_money(self,open,vol):
        next_day_trade_money_list=[]
        for i in range(len(open)):
            if i < 19:
                next_day_trade_money_list.append(0)
            elif i==len(open)-1:
                next_day_trade_money_list.append(0)
            else:
                next_day_trade_money_list.append(open[i+1]*vol[i+1])

        return next_day_trade_money_list


if __name__ == "__main__":
    # 모든 종목 데이터 한바퀴씩
    subindex = subindex()
    subindex.collecting()

    logger.debug("모든 subindex 수집끝!!!!")






