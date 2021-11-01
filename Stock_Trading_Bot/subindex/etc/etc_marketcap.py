ver = "#version 1.0.1"
print(f"etc_info: {ver}")



from library.open_api import *
from sqlalchemy import event
from library.daily_crawler import *


#marketcap_2021=pd.read_csv('marketcap/marketcap_20210703.csv', header=None)
#marketcap_2021=pd.read_excel('marketcap/marketcap_20210703.xlsx', header=None)

#marketcap_2021=marketcap_2021.iloc[1:,0]


#date_row_2021 = [y.replace('-','') for y in date_2021]


class etc_info():
    def __init__(self):
        self.variable_setting()

    def variable_setting(self):
        self.today = datetime.datetime.today().strftime("%Y%m%d")
        self.today_detail = datetime.datetime.today().strftime("%Y%m%d%H%M")
        #self.start_date = cf.start_daily_buy_list
        self.start_date = '20210412'
            #'20031230'

        self.engine_etc_marketcap = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/etc_marketcap",
            encoding='utf-8')

        event.listen(self.engine_etc_marketcap, 'before_execute', escape_percentage, retval=True)




    def is_table_exist_etc_info(self, date):
        sql = "select 1 from information_schema.tables where table_schema ='etc_marketcap' and table_name = '%s'"
        rows = self.engine_etc_marketcap.execute(sql % (date)).fetchall()

        if len(rows) == 1:
            return True
        elif len(rows) == 0:
            return False


    def etc_marketcap(self):

        self.date_rows_setting()

        test=0

        for i in range(len(self.date_rows)):
            if self.is_table_exist_etc_info(self.date_rows[i][0]):
                print(f'''{self.date_rows[i][0]}은 존재한다.''')
                continue
            else :
                print(f'''{self.date_rows[i][0]}은 존재하지 않는다.''')


                test=0
                marketcap_sql=f'''
                        select date,code,name,close,stocks,marcap from etc_marketcap.`marketcap2021`
                        where date={self.date_rows[i][0]}
                '''



                marketcap = self.engine_etc_marketcap.execute(marketcap_sql).fetchall()

                test = 0
                df_marketcap=pd.DataFrame(marketcap,columns=['date','code','code_name','close','stocks','marketcap'])

                test=0

                df_marketcap.to_sql(self.date_rows[i][0], con=self.engine_etc_marketcap, if_exists='replace')

        # for k in range(len(self.date_rows)):

        # header_list=['date','day','Algo_1','Algo_2','Algo_3','Algo_4','Algo_5','Algo_6','Algo_7','Algo_8','Algo_9','Algo_10']
        # df_mdays=df_mdays.reindex(columns=header_list)
        # df_mdays=df_mdays.fillna('0')
        #
        # df_mdays.to_sql('etc_info_date', con=self.engine_etc_info, if_exists='replace')
        #
        # print('etc_info_date! Complete!')



    def get_stock_item_all(self):
        print("get_stock_item_all!!!!!!")
        sql = "select code_name,code from stock_item_all"
        self.stock_item_all = self.engine_daily_buy_list.execute(sql).fetchall()

    def is_table_exist_daily_craw(self, code, code_name):
        sql = "select 1 from information_schema.tables where table_schema ='etc_marketcap' and table_name = '%s'"
        rows = self.engine_daily_craw.execute(sql % (code_name)).fetchall()

        if len(rows) == 1:
            # print(code + " " + code_name + " 테이블 존재한다!!!")
            return True
        elif len(rows) == 0:
            # print("####################" + code + " " + code_name + " no such table!!!")
            # self.create_new_table(self.cc.code_df.iloc[i][0])
            return False

    def date_rows_setting(self):
        #
        # date_temp = datetime.datetime.strptime(self.start_date, '%Y%m%d').date() - datetime.timedelta(days=78)
        # sub_date = date_temp.strftime("%Y%m%d")

        print("date_rows_setting!!")
        # 날짜 지정
        sql = "select date from daily_craw.`gs글로벌` where date >= '%s' group by date"
        self.date_rows = self.engine_etc_marketcap.execute(sql % self.start_date).fetchall()

    def run(self):

        self.transaction_info()

        # print("run end")
        return 0





if __name__ == "__main__":
    subindex = etc_info()
    subindex.etc_marketcap()

