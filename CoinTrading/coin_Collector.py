# version 1.0.0
print("Coin_collector 프로그램이 시작 되었습니다!")

from library.coin_collector_api import *

class Collector:
    print("Coin_collector 클래스에 들어왔습니다.")

    def __init__(self):
        print("__init__ 함수에 들어왔습니다.")
        self.collector_api = get_Coin_Data()

    def collecting(self):
        self.collector_api.coin_update()


if __name__ == "__main__":
    print("__main__에 들어왔습니다.")

    c = Collector()
    # 데이터 수집 시작 -> 주식 종목, 종목별 금융 데이터 모두 데이터베이스에 저장.
    c.collecting()
