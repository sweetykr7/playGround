from library import coin_cf as cf

import pandas as pd

import pyupbit
access_key = cf.access_key
secret_key = cf.secret_key

upbit = pyupbit.Upbit(access_key, secret_key)




#현재가 조회
coin_name="KRW-BTC"

price = pyupbit.get_current_price(coin_name)
print(price)
#
# price = pyupbit.get_current_volume(coin_name)
# print(price)
# #10호가 데이터 얻기

# orderbook = pyupbit.get_orderbook("KRW-BTC")
# bids_asks = orderbook[0]['orderbook_units']
#
# for bid_ask in bids_asks:
#     print(bid_ask)

# ask_price는 매도 호가이고 bid_price는 매수 호가입니다. ask_size와 bid_size는 매도 호가 수량과 매수 호가 수량입니다.

#잔고 조회


# balances=upbit.get_balances()
# print(balances)

# 잔고 조회 메서드의 리턴 값은 튜플입니다. 튜플 객체의 0번에는 잔고 데이터 (파이썬 리스트 객체)가
# 1번에는 호출 제한 데이터 (파이썬 딕셔너리 객체)가 있습니다.
# 잔고 데이터를 살펴보면 원화로 약 100만 원이 있는 것을 확인할 수 있습니다.
# 튜플의 1번에 있는 호출 제한 데이터는 업비트 API를 호출할 때 초당/분당 호출이 가능한 요청 수를 의미합니다.
# 예를 들어, 아래의 값은 'default' 그룹에 대해서 1분간 1799개, 1초에 29개의 API 호출이 가능함을 의미합니다.
# 참고로 API마다 그룹이 있는데 그룹 단위로 호출 제한을 판단하므로 과도한 호출을 하는 경우에는 초당/분당 호출 가능 수를 확인하는 것이 필요합니다.



#매수 / 매도
# 지정가 매수 upbit.buy_limit_order("KRW-XRP", 100, 20)
# 시장가 매수 upbit.buy_market_order("KRW-BTC", 5000)
#upbit.buy_market_order("KRW-BTC", 5000)

# 지정가 매도 upbit.sell_limit_order
# 시장가 매도 upbit.sell_market_order("KRW-BTC", 10000)


#{'uuid': '5501bdcd-e867-486a-97f6-f995347f172a', 'side': 'bid', 'ord_type': 'limit',
# 'price': '1000.0', 'state': 'wait', 'market': 'KRW-XRP', 'created_at': '2021-10-12T11:32:49+09:00',
# 'volume': '10.0', 'remaining_volume': '10.0', 'reserved_fee': '5.0', 'remaining_fee': '5.0',
# 'paid_fee': '0.0', 'locked': '10005.0', 'executed_volume': '0.0', 'trades_count': 0}


# ret = upbit.buy_limit_order("KRW-XRP", 1000, 10)
# print(ret)


#주문 취소
# ret = upbit.cancel_order('5501bdcd-e867-486a-97f6-f995347f172a')
# print(ret)


#미체결 정보 조회
# ret1=upbit.buy_limit_order("KRW-BTC", 50000000, 0.001)
# print(ret1)

# ret = upbit.get_order("KRW-BTC")
# uuid=ret[0]['uuid']
# if len(ret)>0:
#     print("아직 매수 대기 상태입니다.")
# print(uuid)
