# version 1.3.2
# *단위: 억원, %, 배, 주 * 분기: 순액기준
# 총 크롤링한 종목의 수 : select count(*) from (select * from naver group by code) a
# 재무제표의 가장 윗쪽 테이블

import re
import datetime

import pandas as pd
import pymysql
import requests
from sqlalchemy.exc import ProgrammingError

pymysql.install_as_MySQLdb()

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from library import cf


class NoEncDataException(Exception):
    pass


class GPACrawl:
    def __init__(self):
        db_url = URL(
            drivername="mysql+mysqldb",
            username=cf.db_id,
            password=cf.db_passwd,
            host=cf.db_ip,
            port=cf.db_port,
            database='fnguide'
        )
        self.db_engine = create_engine(db_url)

        self.daily_buy_list_engine = create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_buy_list",
            encoding='utf-8')

        # 모든 종목 코드 가져오기
        self.get_stock_item_all()

    def get_stock_item_all(self):
        sql = """
        SELECT code, code_name
        FROM stock_item_all
        WHERE code not in (
            SELECT code FROM stock_konex
        )
        ORDER BY code
        """  # Konex 제외 (기업현황 데이터 미제공)
        self.stock_item_all = self.daily_buy_list_engine.execute(sql).fetchall()


    # 크롤링 함수
    def get_data_info(self, cmp_code):


        get_url = 'http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}'.format(cmp_code)



        res = requests.get(get_url)

        try:
            return pd.read_html(res.text)[0],pd.read_html(res.text)[2]

        except Exception as e:
                return []

    def get_latest_index(self):
        """
         naver 테이블에서 가장 최근에 받은 종목의 index를 반환

         :return: latest_index
         """
        latest_year = datetime.datetime.now().year-1

        try:
            latest_code = self.db_engine.execute("""
                        SELECT code FROM total_gpa 
                        WHERE year = {}
                        ORDER BY code DESC LIMIT 1
                    """.format(latest_year)).first()[0]

            for i, (scode, _) in enumerate(self.stock_item_all):
                if scode == latest_code:
                    latest_index = i
        except ProgrammingError:  # 아직 한번도 데이터를 넣지 않아 테이블이 존재하지 않을 시
            latest_index = 0
        return latest_index

    def crawl(self):

        num = len(self.stock_item_all)  # print 용 변수
        latest_index = self.get_latest_index()
        count = latest_index # print 용 변수
        ####################################################

        # self.stock_item_all[latest_index:] : 가장 최근 받은 종목 이후로 시작
        for stock_code in self.stock_item_all[latest_index:]:
            count += 1
            code = stock_code[0]
            code_name = stock_code[1]
            print("++++++++++++++ {} ++++++++++++++ {} / {}".format(code_name, count, num))

            try:
                df,df2 = self.get_data_info(code)
            except:
                continue
            #print("================df확인================")
            #print(df)

            # 추가 내용 (비어있는지 체크, len(df)가 0이면 False-> not len(df) 는 True -> continue
            if not len(df):
                continue

            #print("df check1=====================")
            #print(df)

            test=0
            # 데이터 프레임에 code, code_name 컬럼을 추가하고 각각의 값을 넣어준다
            df['code'] = code
            df['code_name'] = code_name

            #print("df check2=====================")
           # print(df)

            # year_df 에서 사용할 컬럼만을 짜른다.
            year_df = df[df.columns[1:-4]]
            # print(df)
            # print(year_df)


            # List comprehension
            years=[int(y.split('/')[0]) for y in year_df.columns if not y.startswith('Unnamed:')]
            last_year = years[-1]
            last_qq=year_df.columns[-1].replace('/','')


            new_df = pd.DataFrame(columns=['code', 'code_name', 'year','year_qq', '매출총이익', '자산총계'])

            target_line=0
            target_line2=0

            for i in range(len(df)):
                if df.iloc[i,0]=="매출총이익":
                    target_line=i


            for i in range(len(df2)):
                if df2.iloc[i,0]=="자산":
                    target_line2=i

            for i in range(len(years)):
                if years[i]==last_year:
                    year_target_line=i


            if target_line==0:
                continue



            new_df.loc[len(new_df)] = (code, code_name, last_year,last_qq, df.iloc[target_line,year_target_line+1],df2.iloc[target_line2,year_target_line+1])  # loc
            print("입력 중.. {}".format(new_df.iloc[-1:].values))

            new_df.to_sql('total_gpa', self.db_engine, if_exists='append', index=False)

        # index False


if __name__ == "__main__":
    gpacraw = GPACrawl()
    gpacraw.crawl()
    print("fnguide 크롤링을 성공적으로 마쳤습니다.")

