ver = "#version 1.0.0"
print(f"krx_crawling: {ver}")

# 강의에서 패러럴즈 관련 내용은 패치하여 문제없이 작동하게 만들었으니 무시하셔도 괜찮습니다.
# 엑셀 파일의 저장 위치는 기존 다운로드 폴더에서 bot 프로젝트 폴더안의 KIND_xls로 변경 되었습니다.
# 크롬드라이버설치 위치 C:chromedriver/chromedrive.exe => 자동으로 크롬드라이버가 설치 되도록 업데이트 되었습니다. 따로 C드라이브에 설치 하지 않으셔도 됩니다.

import datetime
import os
import sys
import pathlib
from datetime import timedelta
from time import sleep

import pandas as pd
import pymysql
#  selenium 이란 여러 언어에서 웹드라이버를 통해
#  웹 자동화 테스트 혹은 웹 자동화를 도와주는 라이브러리
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from sqlalchemy import create_engine, VARCHAR, DATE
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import InternalError, OperationalError

from library import cf

pymysql.install_as_MySQLdb()


BACKSPACE = '\ue003'
ENTER = '\ue007'
TAB = '\ue004'
END = '\ue010'


class krx_Crawler:
    def __init__(self, snapshot_dir_name='krx_snapshots', download_dir_name='krx_xlsx'):
        db_url = URL(
            drivername="mysql+mysqldb",
            username=cf.db_id,
            password=cf.db_passwd,
            host=cf.db_ip,
            port=cf.db_port,
            database='crawling_krx_daily'
        )
        self.db_engine = create_engine(db_url)
        self.variable_setting()
        self.snapshot_path = pathlib.Path(__file__).parent.absolute() / snapshot_dir_name
        self.download_path = pathlib.Path(__file__).parent.absolute() / download_dir_name

    def check_version(self):
        br_ver = self.driver.capabilities['browserVersion']
        dr_ver = self.driver.capabilities['chrome']['chromedriverVersion'].split(' ')[0]
        print(f'Browser Version: {br_ver}\nChrome Driver Version: {dr_ver}')

        if br_ver[:2] != dr_ver[:2]:
            print(
                '브라우저 버전과 크롬 드라이버의 버전이 다릅니다.\n',
                f'https://chromedriver.chromium.org/downloads 에서 {br_ver[:2]}으로 시작하는 버전을 선택하시고\n',
                'chromedriver_win32.zip 파일을 다운 받아주세요. 그 후 받으신 압축 파일안에있는 chromedriver.exe 파일을\n',
                'C:\chromedriver 폴더에 덮어 씌워주시기 바랍니다.'
                '\n\n'
                '자세한 내용은 위키(https://wikidocs.net/87166#_1)를 참고하여 주시기 바랍니다.'
            )
            sys.exit(1)

    def variable_setting(self):
        # ??는 두글자로 아무것이나 와도 된다는 소리임, * 는 글자수 제한이 없음
        self.today = datetime.date.today()
        #self.FNAME_PATTERN = '투자??종목*.xlsx'
        self.FNAME_PATTERN = f'''data_*_{self.today.strftime('%Y%m%d')}*.csv'''

        # 크롤링 시작일
        self.DEFAULT_START_DATE = datetime.date(2003, 1, 1)
        # 엑셀에서 5000개만 담을 수 있어서 100일 단위로 조회하여 데이터를 불러옴
        #self.rotate_period = 100

        # 촬영 후 self.download_path 를 init으로 옮겼습니다.

        # 아래 두 줄을 촬영 후 craw 함수로 옮겼습니다.
        # options = webdriver.ChromeOptions()
        # self.driver = webdriver.Chrome("C:\chromedriver\chromedriver", options=options)



    # 현재 다운로드 폴더 안에 있는 엑셀파일을 삭제
    def clean_excel(self):
        for fname in self.download_path.glob(self.FNAME_PATTERN):
            os.remove(fname)

    # kind 사이트에 달력에 날짜를 설정하는 함수
    def date_select(self, date):


        selected_tag_a = self.driver.find_element_by_css_selector('input#trdDd')
        selected_tag_a.click()

        test2=0
        # 칸에서 가장 끝으로 이동
        selected_tag_a.send_keys(Keys.END)

        # kind 사이트의 날짜를 하나씩 지우는 로직
        for i in range(1,12):
            # Keys 선언으로 가면 관련 코드 다 나와있다 (ctrl + keys 클릭)
            selected_tag_a.send_keys(Keys.BACKSPACE)
        selected_tag_a.send_keys(date)

        test3=0

        # selected_tag_a = self.driver.find_element_by_css_selector('input#endDate')
        # selected_tag_a.click()
        #
        # selected_tag_a.send_keys(Keys.END)
        #
        # for i in range(1, 12):
        #     selected_tag_a.send_keys(Keys.BACKSPACE)

        #selected_tag_a.send_keys(end.strftime('%Y%m%d'))

    def is_simul_table_exist(self, table_name):
        sql = "select 1 from information_schema.tables where table_schema = '%s' and table_name = '%s'"
        rows = self.db_engine.execute(sql % ("crawling_krx_daily", table_name)).fetchall()
        if len(rows) == 1:
            return True
        else:
            return False

    # 엑셀파일을 다운 받아서 db에 넣는 함수
    def insert_to(self, file_name, date):

        # kind 검색(윈도우 사이즈
        #element = self.driver.find_element_by_xpath('//*[@id="searchForm"]/section/div/div[3]/a[1]')
        element = self.driver.find_element_by_xpath('//*[@id="jsSearchButton"]')

        test4=0

        #self.driver.execute_script('arguments[0].scrollIntoView(true);', element) # 사이트 포지션 이동
        #self.driver.execute_script('window.scrollBy(100, 0)') # 사이트 포지션 이동
        #self.take_snapshot('before_search.png') # 디버깅용 snapshot (bot / kind_snapshots 폴더에 저장)
        element.send_keys((Keys.ENTER))
        self.dialog_block_wait() #로딩이 끝나는 순간까지 대기

        element = self.driver.find_element_by_xpath('//*[@id="MDCSTAT035_FORM"]/div[2]/div/p[2]/button[2]')
        element.send_keys((Keys.ENTER))
        self.dialog_block_wait()  # 로딩이 끝나는 순간까지 대기

        test4=0

        # kind 엑셀다운로드 (촬영 후 Enter를 click으로 변경 했습니다.)
        # element = self.driver.find_element_by_xpath('//*[@id="ui-id-1"]/div/div[1]')
        # element.click()
        # self.driver.execute_script('arguments[0].scrollIntoView(true);', element) # 사이트 포지션 이동
        # self.driver.execute_script('window.scrollBy(0, 100)') # 사이트 포지션 이동

        # selected_tag_a = self.driver.find_element_by_css_selector('input#trdDd')
        # selected_tag_a.click()

        element = self.driver.find_element_by_xpath('/html/body/div[2]/section[2]/section/section/div/div/form/div[2]/div[2]/div[2]/div/div[2]')
        element.click()




        #//*[@id="con_tab1"]/div[1]/button
        #self.driver.execute_script('arguments[0].scrollIntoView(true);', element)
        #self.driver.execute_script('window.scrollBy(100, 0)')
        #self.take_snapshot('before_click.png')



        test5=0

        # try:
        #     element = self.driver.find_element_by_xpath('//*[@id="popSys"]/div[3]/button')
        #     element.click()
        # except:
        #     pass

        #Excel download 감안해서 1초 슬립
        sleep(1)

        file_name_list=os.listdir(self.download_path)
        file_name = [file for file in file_name_list if file.endswith(".csv")][0]

        # 파일이 다 다운될 때 까지 대기(촬영 후 아래 while문 추가 하였습니다.)
        # while not list(self.download_path.glob(self.FNAME_PATTERN)):
        #     sleep(1)

        # 엑셀 데이터를 가져온다.
        # df = pd.read_html(
        #     str(self.download_path / file_name),
        #     header=0,
        #     converters={'종목코드': str}
        # )[0]

        test7=0
        try:
            krx_table = pd.read_csv(str(self.download_path / file_name),encoding='euc-kr')


            krx_table=krx_table.rename(columns={
                            '종목코드':'code',
                            '종목명':'code_name',
                            '종가':'close',
                            '대비':'yes_today_gap',
                            '등락률':'rate',
                            '주당배당금':'DPS',
                            '배당수익률':'DPS_rate'
                                            }
                                             )



            print(f'''{date}를 db에 추가 하고있습니다.''')
            krx_table.to_sql(
                date,
                self.db_engine, if_exists='replace'
            )
        except:
            pass


        # 촬영 후 코드가 수정 되었지만 영상 후반에 설명이 나옵니다~
        # 엑셀 파일이 비어 있는 경우 code 컬럼에 '결과값이 없습니다' 라는 내용이 들어가 있다. 아래는 이러한 경우를 제외하는 로직
        #df = df[df.종목코드 != '결과값이 없습니다.']


        # 만약에 df(데이터프레임)에 '해제일'이라는 컬럼이 있는 경우(투자경고, 투자위험 종목)
        # if '해제일' in df.columns:
        #     # 해제일이 아직 지정되지 않은 경우는 '-' 값이 들어있다. 이럴 때는 해제일 컬럼을 None으로 변경
        #     df.loc[df.해제일 == '-', '해제일'] = None

        # if len(spread_table):
        #     #del spread_table['번호']
        #     spread_table = spread_table.rename(columns={
        #         '종목코드': 'code',
        #         '종목명': 'code_name',
        #         '공시일': 'post_date',
        #         '지정일': 'fix_date',
        #         '유형': 'type',
        #         '해제일': 'cleared_date'
        #     })
        #
        #     spread_table.to_sql(
        #         table_name,
        #         self.db_engine, if_exists='append',
        #         dtype={
        #             'code': VARCHAR(length=6),
        #             'post_date': DATE,
        #             'fix_date': DATE,
        #             'cleared_date': DATE
        #         }
        #     )

        self.clean_excel()

    def get_last_date_from(self, table_name):
        date = self.DEFAULT_START_DATE

        if self.is_simul_table_exist(table_name):
            sql = "select date from {} order by date desc limit 1".format(table_name)
            try:
                import datetime
                result = self.db_engine.execute(sql).fetchall()[0][0]
                #result=datetime.datetime.strptime(result,'%Y%m%d')

                if result:
                    date = result
            except IndexError:
                pass

        return date

    def get_date(self):
        sql = f'''
                select date from daily_craw.`gs글로벌`
                where date>'20030101'
                    order by date
            '''
        result = self.db_engine.execute(sql).fetchall()

        return result

    def is_table_exists(self, table):
        sql = f'''
                        select 1 from information_schema.tables 
                        where table_schema ='crawling_krx_daily' and table_name = {table}
                    '''
        result = self.db_engine.execute(sql).fetchall()

        if len(result)>0:
            return True
        else:
            return False

    # crawling하고, db에 넣는 함수
    def crawl_and_insert(self, file_name, table_name):
        # 달력이 종목 탭을 가려서 탭(투자위험종목 등) 클릭을 못하는 경우를 방지
        #search_bar = self.driver.find_element_by_css_selector('#AKCKwd')
        #self.driver.execute_script('arguments[0].scrollIntoView(true);', search_bar) #추가 코드. 클릭 할 위치로 scroll
        #search_bar.click() #클릭

        #아래 selected_tab 에 a[title="{}"]는 a link를 찾아주라는 소리임. title file_name에 있는 것으로
        # selected_tab = self.driver.find_element_by_css_selector('a[title="{}"]'.format(file_name.split('.')[0]))
        # self.actions.move_to_element(search_bar) # 추가 코드. 클릭 할 위치로 scroll
        # self.driver.execute_script('window.scrollBy(100, 0)') # 추가 코드. 클릭 할 위치로 scroll
        # selected_tab.click() # 클릭
        # self.dialog_block_wait() # 대기
        # self.take_snapshot("b-dateinput.png") #캡처(디버깅용)
        test=0
        date_rows = self.get_date()

        # 마지막 post날짜 가져와서 1일을 더해준다.
        #start_date = self.get_last_date_from(table_name)+timedelta(1)
        #end_date = start_date + timedelta(self.rotate_period)
        test=0

        for i in range(len(date_rows)):
            date=date_rows[i][0]
            if self.is_table_exists(date):
                print(f'{date}는 존재한다.')
                continue
            else:
                print(f'{date}는 존재하지 않는다.')
                self.date_select(date)
                self.dialog_block_wait()
                self.insert_to(file_name, date)

                #start_date = start_date + timedelta(1)



            # while start_date < self.today:
            #     self.date_select(start_date)
            #     self.dialog_block_wait()
            #     self.insert_to(file_name, table_name)
            #
            #     start_date=start_date + timedelta(1)
                #start_date = end_date + timedelta(1)
                #end_date = start_date + timedelta(self.rotate_period)




    # 크롤링 시작하는 함수
    def craw(self):
        # 시작전에 디렉토리 한번 정리
        self.clean_excel()

        # ---------------------------------영상 촬영 후 추가 된 코드---------------------------------
        # 셀레니움의 상태 확인용 스냅샷 디렉토리 확인 및 생성(촬영 후 추가된 코드입니다.)
        pathlib.Path(self.snapshot_path).mkdir(exist_ok=True)
        # 아래 라인들은 촬영 후 variable_setting 함수에 있던 것을 옮겨왔습니다.
        options = webdriver.ChromeOptions()
        # Selenium이 띄운 크롬창의 다운로드 폴더 경로를 지정 (bot 프로젝트 폴더안의 KIND_xls 폴더)
        options.add_experimental_option("prefs", {"download.default_directory": str(self.download_path)})

        path = self.chrome_driver_update() # 크롬 드라이버를 자동으로 path 위치에 설치합니다

        '''자동으로 크롬드라이버가 설치 되도록 업데이트 되었습니다. 따로 C드라이브에 크롬드라이버를 설치 하지 않으셔도 됩니다.'''
        self.driver = webdriver.Chrome(path, options=options)
        self.driver.implicitly_wait(2)  # get(url)로 요청한 페이지 내용들이 모두 로딩이 완료될 때까지 int(초) 만큼 암묵적으로 기다린다
        self.driver.set_window_position(0, 0)
        self.driver.set_window_size(1300, 3000)
        self.actions = ActionChains(self.driver)  # 스크롤 이동을 위한 ActionChains 객체
        self.check_version()  # 버전 체크


        # stock_invest_warning(투자경고종목), stock_invest_danger(투자위험종목)
        # 의 경우는 항상 테이블을 삭제해준다.
        # 이유는 투자주의 종목과 다르게
        # 투자경고, 투자위험 종목은 엑셀파일에 '해제일' 컬럼이 있다. 따라서 매번 '해제일'을 업데이트 해줘야 하기 때문에
        # db를 삭제하고 다시 받아온다.
        # try, except 의 경우 혹시라도
        # stock_invest_warning, stock_invest_danger 테이블이 없을 경우 sql문을 실행하면 에러가 발생하기 때문에
        # 그럴 때는 그냥 에러로 인식 하지말고 pass 하라는 의미
        # try:
        #     self.clean_database()
        # except (InternalError, OperationalError):
        #     pass
        # # ----------------------------------------------------------------------------------------

        # 한국신용평가 등급별금리스프레드 사이트로 접속

        self.driver.get('http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020502')
        self.dialog_block_wait() # 대기
        self.take_snapshot("b-candi.png") # 디버깅용 캡처처

       # 투자주의 / 투자경고 / 투자위험종목 리스트
        # 순서 : 투자주의종목-> 투자경고종목 -> 투자위험종목
        # ( ) 안에는 각각의 엑셀이름과 테이블 이름을 명시
        file_name='data_*.csv'
        insert_table_names = [(file_name, 'krx_crawling')]
        for names in insert_table_names:
            # # 영상 촬영 후 추가 된 코드
            # # 투자경고종목, 투자위험 종목의 경우 kind에 데이터가 많지 않아서 6000일 단위로 조회해도 무리가 없음
            # # for문의 처음 돌때 names에는 ('투자주의종목.xls', 'stock_invest_caution') 값이 들어가 있다.
            # # 이 때 names[0]에는 '투자주의종목.xls', names[1]에는 'stock_invest_caution' 값이 들어가 있다.
            # if names[0] != '투자주의종목.xls':
            #     self.rotate_period = 6000
            # # 투자주의종목의 경우는 데이터가 많아서 100일 단위로 끊어서 조회
            # else:
            #     self.rotate_period = 100

            self.crawl_and_insert(*names)

        # chrome 브라우저 닫기
        self.driver.close()

    # 로딩이 끝나는 순간까지 대기
    def dialog_block_wait(self):
        try:
            wait = WebDriverWait(self.driver, 1.5)
            self.take_snapshot("dialog_block_wait_before.png")
            wait.until(EC.visibility_of_element_located((By.CLASS_NAME, 'ui-dialog')))
            self.take_snapshot("dialog_block_wait_appear.png")
            wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, 'ui-dialog')))
            self.take_snapshot("dialog_block_wait_disappear.png")
        except TimeoutException:
            pass

    def clean_database(self):
        self.db_engine.execute('DROP TABLE IF EXISTS stock_invest_warning')

    # 스냅샷 찍는 함수 (단순히 에러가 발생 하는 순간의 화면을 담아 놓기 위해 설정한 기능입니다. )
    # bot/kind_snapshots 폴더에 저장(자동 생성)
    def take_snapshot(self, filename):
        self.driver.save_screenshot(str(self.snapshot_path / filename))

    def chrome_driver_update(self):
        print("chrome_driver_update..")
        update = True
        # pip install check-chromedriver
        package_name = 'chromedriver-autoinstaller'
        pip_show_list = os.popen(f"pip show {package_name}").read().strip().split('\n')
        for pip_show_str in pip_show_list:
            if package_name not in pip_show_str:
                continue
            else:
                update = False
                break
        if update:
            os.system(f'pip install {package_name}==0.2.2')
            print(f"성공적으로 {package_name} 패키지를 설치 했습니다")

        import chromedriver_autoinstaller
        path = chromedriver_autoinstaller.install()  # Check if the current version of chromedriver exists
                                              # and if it doesn't exist, download it automatically,
                                              # then add chromedriver to path


        print("chrome_driver_update 완료!")
        return path
if __name__ == "__main__":
    client = krx_Crawler()
    client.craw()
