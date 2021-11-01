ver = "#version 1.0.1"
print(f"daily_subindex: {ver}")



from library.open_api import *
from sqlalchemy import event
from library.daily_crawler import *



class subindex():
    def __init__(self):
        self.variable_setting()

    def variable_setting(self):
        self.today = datetime.datetime.today().strftime("%Y%m%d")
        self.today_detail = datetime.datetime.today().strftime("%Y%m%d%H%M")
        self.start_date = '20060601'
            #cf.start_daily_buy_list
        self.engine_daily_craw = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_craw",
            encoding='utf-8')
        self.engine_daily_craw_previous_price = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_craw_previous_price",
            encoding='utf-8')
        self.engine_daily_subindex = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_subindex",
            encoding='utf-8')

        self.engine_daily_subindex_new = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_subindex_new",
            encoding='utf-8')
        self.engine_daily_buy_list = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_buy_list",
            encoding='utf-8')

        event.listen(self.engine_daily_craw, 'before_execute', escape_percentage, retval=True)
        event.listen(self.engine_daily_buy_list, 'before_execute', escape_percentage, retval=True)
        event.listen(self.engine_daily_subindex, 'before_execute', escape_percentage, retval=True)

    def date_rows_setting(self):
        #
        # date_temp = datetime.datetime.strptime(self.start_date, '%Y%m%d').date() - datetime.timedelta(days=78)
        # sub_date = date_temp.strftime("%Y%m%d")

        print("date_rows_setting!!")
        # 날짜 지정
        sql = "select date from `gs글로벌` where date >= '%s' group by date"
        self.date_rows = self.engine_daily_craw.execute(sql % self.start_date).fetchall()

    def is_table_exist_daily_subindex(self, date):
        sql = "select 1 from information_schema.tables where table_schema ='daily_subindex' and table_name = '%s'"
        rows = self.engine_daily_subindex.execute(sql % (date)).fetchall()

        if len(rows) == 1:
            return True
        elif len(rows) == 0:
            return False

    def is_table_exist_daily_subindex_previous_price(self, date):
        sql = "select 1 from information_schema.tables where table_schema ='daily_subindex' and table_name = '%s'"
        rows = self.engine_daily_subindex.execute(sql % (date)).fetchall()

        if len(rows) == 1:
            return True
        elif len(rows) == 0:
            return False

    def daily_subindex(self):
        print("daily_buy_list!!!")
        self.date_rows_setting()
        self.get_stock_item_all()

        for k in range(len(self.date_rows)):


            # print("self.date_rows !!!!", self.date_rows)
            print(str(k) + " 번째 : " + datetime.datetime.today().strftime(" ******* %H : %M : %S *******"))
            # daily 테이블 존재하는지 확인


            if self.is_table_exist_daily_subindex_previous_price(self.date_rows[k][0]) == True:
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
                rows = self.engine_daily_buy_list.execute(sql).fetchall()



                if len(rows) != 0:
                    df_temp = DataFrame(rows,
                                        columns=['temp','date', 'code', 'code_name','open', 'close', 'low', 'high', 'volume',
                                                 'noise','vol20','cci','rsi','OBV','macd','macd_signal','macd_hist',
                                                 'BBand_U','BBand_M','BBand_L','stoch_slowk','stoch_slowd',
                                                 'switch_line','standard_line','backspan','prespan1','prespan2',
                                                 'ma19','ma20','avg_momentum_plus_12month','avg_momentum_20day',
                                                 'avg_noise','real_ichimoku','best_52','bband_1month',
                                                 'high_60days','low_60days'])
                    df_temp.drop(['temp'],axis=1,inplace=True) #위에 리스트 형태 변환하기 싫어서 그냥 이렇게 함.df_temp.drop(['temp'],axis=1,inplace=True)
                    df_temp.drop(['noise'], axis=1, inplace=True)
                    df_temp.to_sql(name=self.date_rows[k][0], con=self.engine_daily_subindex, if_exists='replace')

                    # delete_sql = f'''
                    #                         delete
                    #                         from `subindex_new`
                    #                         where date = '{self.date_rows[k][0]}'
                    #                         '''
                    #
                    # # daily craw에서 subindex 만들기 위한 날짜 data를 가져옴.
                    # delete_rows = self.engine_daily_buy_list.execute(delete_sql)
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
    subindex = subindex()
    subindex.daily_subindex()

