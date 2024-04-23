import time
import requests
import pandas as pd
import datetime
import os

timestamp = datetime.datetime.now()
req_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
current_time = datetime.datetime.now()
next_day = current_time + datetime.timedelta(days=1)
next_day_date = next_day.strftime('%Y-%m-%d')


while(next_day_date != req_timestamp[:10]):

    book = {}
    response = requests.get ('https://api.bithumb.com/public/orderbook/BTC_KRW/?count=5')
    book = response.json()

    data = book['data']

    bids = (pd.DataFrame(data['bids'])).apply(pd.to_numeric,errors='ignore')
    bids.sort_values('price', ascending=False, inplace=True)
    bids = bids.reset_index(); del bids['index']
    bids['type'] = 0
    
    asks = (pd.DataFrame(data['asks'])).apply(pd.to_numeric,errors='ignore')
    asks.sort_values('price', ascending=True, inplace=True)
    asks['type'] = 1 


    #print (bids)
    #print ("\n")
    #print (asks)

    #df = pd.concat([bids, asks])
    #print(df)


    #time.sleep(5)
    #continue;

    df = pd.concat([bids, asks])



    df['quantity'] = df['quantity'].round(decimals=4)
    df['timestamp'] = req_timestamp
    
    #print (df)
    #print ("\n")


    fn = f'book-{req_timestamp[:10]}-exchange-market.csv'
    #df.to_csv(fn, "./2024-00-00-bithumb-orderbook.csv", header=False, mode = 'a')
    #df.to_csv(fn, "price, quantity, type, timestemp", header=False, mode = 'a')

    should_write_header = os.path.exists(fn)
    if should_write_header == False:
        df.to_csv(fn, index=False, header=True, mode = 'a')
    else:
        df.to_csv(fn, index=False, header=False, mode = 'a')
    

    '''
    if next_day_date == req_timestamp[:10]:
        # print("[]: ", req_timestamp[:10])
        break
    '''


    time.sleep(4.9)