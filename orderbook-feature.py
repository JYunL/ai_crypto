import os
import math
import pandas as pd

def cal_mid_price(gr_bid_level, gr_ask_level):
    if len(gr_bid_level) > 0 and len(gr_ask_level) > 0:
        bid_top_price = gr_bid_level.iloc[0].price
        ask_top_price = gr_ask_level.iloc[0].price
        mid_price = (bid_top_price + ask_top_price) * 0.5
        return mid_price
    else:
        print('Error: serious cal_mid_price')
        return -1

# 전체 행 수를 계산
total_rows = sum(1 for _ in open('2024-05-01-upbit-BTC-book.csv')) - 1  # 헤더 행을 제외

# 청크 단위로 데이터 읽기
chunksize = 10000
processed_rows = 0

# trade 데이터
df_trade = pd.read_csv('2024-05-01-upbit-BTC-trade.csv')

fn_mid = '2024-05-01-exchange-market-feature.csv'
should_write_header = not os.path.exists(fn_mid)

# 초기화
var = {
    '_flag': True,
    'prevBidQty': 0,
    'prevAskQty': 0,
    'prevBidTop': 0,
    'prevAskTop': 0,
    'bidSideAdd': 0,
    'bidSideDelete': 0,
    'askSideAdd': 0,
    'askSideDelete': 0,
    'bidSideTrade': 0,
    'askSideTrade': 0,
    'bidSideFlip': 0,
    'askSideFlip': 0,
    'bidSideCount': 0,
    'askSideCount': 0
}

for chunk in pd.read_csv('2024-05-01-upbit-BTC-book.csv', chunksize=chunksize):
    chunk['timestamp'] = pd.to_datetime(chunk['timestamp'])

    # 데이터를 0시부터 3시까지로 필터링
    # start_time = pd.Timestamp('2024-05-01 00:00:00')
    # end_time = pd.Timestamp('2024-05-01 01:00:00')
    # filtered_df = chunk[(chunk['timestamp'] >= start_time) & (chunk['timestamp'] < end_time)]

    # 그룹화
    # groups = filtered_df.groupby('timestamp')
    groups = chunk.groupby('timestamp')
    keys = groups.groups.keys()

    level_1 = 10
    params = [(0.1, level_1, 1)]
    for param in params:
        ratio = param[0]
        level = param[1]
        interval = param[2]

    for i in keys:
        gr_o = groups.get_group(i)
        
        gr_bid_level = gr_o[(gr_o['type'] == 0)]
        gr_ask_level = gr_o[(gr_o['type'] == 1)]

        # --mid price--
        mid_price = cal_mid_price(gr_bid_level, gr_ask_level)
        #print(mid_price)

        # --imbalance(0.2, 5, 1)--
        quant_v_bid = gr_bid_level.quantity**ratio
        price_v_bid = gr_bid_level.price * quant_v_bid

        quant_v_ask = gr_ask_level.quantity**ratio
        price_v_ask = gr_ask_level.price * quant_v_ask

        askQty = quant_v_ask.values.sum()
        bidPx = price_v_bid.values.sum()
        bidQty = quant_v_bid.values.sum()
        askPx = price_v_ask.values.sum()
        bid_ask_spread = interval

        if bidQty > 0 and askQty > 0:
            book_price = (((askQty * bidPx) / bidQty) + ((bidQty * askPx) / askQty)) / (bidQty + askQty)

        indicator_value = (book_price - mid_price) / bid_ask_spread
        
        # --delta_v1(0.2, 5, 1)--
        decay = math.exp(-1.0/interval)
    
        _flag = var['_flag']
        prevBidQty = var['prevBidQty']
        prevAskQty = var['prevAskQty']
        prevBidTop = var['prevBidTop']
        prevAskTop = var['prevAskTop']
        bidSideAdd = var['bidSideAdd']
        bidSideDelete = var['bidSideDelete']
        askSideAdd = var['askSideAdd']
        askSideDelete = var['askSideDelete']
        bidSideTrade = var['bidSideTrade']
        askSideTrade = var['askSideTrade']
        bidSideFlip = var['bidSideFlip']
        askSideFlip = var['askSideFlip']
        bidSideCount = var['bidSideCount']
        askSideCount = var['askSideCount'] 
  
        curBidQty = gr_bid_level['quantity'].sum()
        curAskQty = gr_ask_level['quantity'].sum()
        if not gr_bid_level.empty:
            curBidTop = gr_bid_level.iloc[0].price
        if not gr_ask_level.empty:
            curAskTop = gr_ask_level.iloc[0].price


        if _flag:
            var['prevBidQty'] = curBidQty
            var['prevAskQty'] = curAskQty
            var['prevBidTop'] = curBidTop
            var['prevAskTop'] = curAskTop
            var['_flag'] = False
            
        
        if curBidQty > prevBidQty:
            bidSideAdd += 1
            bidSideCount += 1
        if curBidQty < prevBidQty:
            bidSideDelete += 1
            bidSideCount += 1
        if curAskQty > prevAskQty:
            askSideAdd += 1
            askSideCount += 1
        if curAskQty < prevAskQty:
            askSideDelete += 1
            askSideCount += 1
        
        if curBidTop < prevBidTop:
            bidSideFlip += 1
            bidSideCount += 1
        if curAskTop > prevAskTop:
            askSideFlip += 1
            askSideCount += 1


        _count_1 = (df_trade[(df_trade['type']==1)])['count'].reset_index(drop=True).get(0,0)
        _count_0 = (df_trade[(df_trade['type']==0)])['count'].reset_index(drop=True).get(0,0)
    
        bidSideTrade += _count_1
        bidSideCount += _count_1
    
        askSideTrade += _count_0
        askSideCount += _count_0
    

        if bidSideCount == 0:
            bidSideCount = 1
        if askSideCount == 0:
            askSideCount = 1

        bidBookV = (-bidSideDelete + bidSideAdd - bidSideFlip) / (bidSideCount**ratio)
        askBookV = (askSideDelete - askSideAdd + askSideFlip ) / (askSideCount**ratio)
        tradeV = (askSideTrade/askSideCount**ratio) - (bidSideTrade / bidSideCount**ratio)
        bookDIndicator = askBookV + bidBookV + tradeV
        
       
        var['bidSideCount'] = bidSideCount * decay #exponential decay
        var['askSideCount'] = askSideCount * decay
        var['bidSideAdd'] = bidSideAdd * decay
        var['bidSideDelete'] = bidSideDelete * decay
        var['askSideAdd'] = askSideAdd * decay
        var['askSideDelete'] = askSideDelete * decay
        var['bidSideTrade'] = bidSideTrade * decay
        var['askSideTrade'] = askSideTrade * decay
        var['bidSideFlip'] = bidSideFlip * decay
        var['askSideFlip'] = askSideFlip * decay

        var['prevBidQty'] = curBidQty
        var['prevAskQty'] = curAskQty
        var['prevBidTop'] = curBidTop
        var['prevAskTop'] = curAskTop
        #var['df1'] = df1
        

        if mid_price != -1:
            df_mid = pd.DataFrame([[bookDIndicator, indicator_value, mid_price, i]], columns=['book-delta-v1-0.2-5-1', 'book-imbalance-0.2-5-1', 'mid_price', 'timestamp'])
            df_mid.to_csv(fn_mid, index=False, header=should_write_header, mode='a')
            should_write_header = False  # 첫 번째 쓰기 이후로는 헤더를 쓰지 않도록 설정
    
    processed_rows += len(chunk)
    progress = (processed_rows / total_rows) * 100
    print(f'Progress: {progress:.2f}%')

print("File processing complete.")
