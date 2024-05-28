import time
import requests
import pandas as pd
import datetime
import os


while(1):
    
    # ---(BTC)---
    book_BTC = {}
    response_BTC = requests.get ('https://api.bithumb.com/public/orderbook/BTC_KRW/?count=5')
    book_BTC = response_BTC.json()

    data_BTC = book_BTC['data']

    bids_BTC = (pd.DataFrame(data_BTC['bids'])).apply(pd.to_numeric,errors='ignore')
    bids_BTC.sort_values('price', ascending=False, inplace=True)
    bids_BTC = bids_BTC.reset_index(); del bids_BTC['index']
    bids_BTC['type'] = 0
    
    asks_BTC = (pd.DataFrame(data_BTC['asks'])).apply(pd.to_numeric,errors='ignore')
    asks_BTC.sort_values('price', ascending=True, inplace=True)
    asks_BTC['type'] = 1


    # ---(ETH)---
    book_ETH = {}
    response_ETH = requests.get ('https://api.bithumb.com/public/orderbook/ETH_KRW/?count=5')
    book_ETH = response_ETH.json()

    data_ETH = book_ETH['data']

    bids_ETH = (pd.DataFrame(data_ETH['bids'])).apply(pd.to_numeric,errors='ignore')
    bids_ETH.sort_values('price', ascending=False, inplace=True)
    bids_ETH = bids_ETH.reset_index(); del bids_ETH['index']
    bids_ETH['type'] = 0
    
    asks_ETH = (pd.DataFrame(data_ETH['asks'])).apply(pd.to_numeric,errors='ignore')
    asks_ETH.sort_values('price', ascending=True, inplace=True)
    asks_ETH['type'] = 1  

    df_BTC = pd.concat([bids_BTC, asks_BTC])
    df_ETH = pd.concat([bids_ETH, asks_ETH])

    timestamp = datetime.datetime.now()
    req_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')

    df_BTC['quantity'] = df_BTC['quantity'].round(decimals=4)
    df_BTC['timestamp'] = req_timestamp

    df_ETH['quantity'] = df_ETH['quantity'].round(decimals=4)
    df_ETH['timestamp'] = req_timestamp


    fn_BTC = f'book-{req_timestamp[:10]}-bithumb-btc.csv'
    fn_ETH = f'book-{req_timestamp[:10]}-bithumb-eth.csv'


    should_write_header_BTC = os.path.exists(fn_BTC)
    should_write_header_ETH = os.path.exists(fn_ETH)
    
    if should_write_header_BTC == False:
        df_BTC.to_csv(fn_BTC, index=False, header=True, mode = 'a')

    elif should_write_header_ETH == False:
        df_ETH.to_csv(fn_ETH, index=False, header=True, mode = 'a')

    else:
        df_BTC.to_csv(fn_BTC, index=False, header=False, mode = 'a')
        df_ETH.to_csv(fn_ETH, index=False, header=False, mode = 'a')
    
    current_time = datetime.datetime.now()
    next_day = current_time + datetime.timedelta(days=1)
    next_day_date = next_day.strftime('%Y-%m-%d')
    
    '''
    if next_day_date == req_timestamp[:10]:
        # print("[]: ", req_timestamp[:10])
        break
    '''
    

    time.sleep(3)