import datetime
from datetime import timezone
import time
import requests
import pandas as pd
import calendar
from calendar import weekday, monthrange
import numpy as np
from bs4 import BeautifulSoup
import warnings

dic = [
    "date", 
    "time",
    "open", 
    "high",
    "low",
    "close",
    "volume",
]

def conversion_table(flag, month): #flag = 1為call, flag = 0為put
    #期交所規定call put月份代號
    
    #call
    call_table = { 1:"A", 2:"B", 3:"C", 4:"D", 5:"E", 6:"F", 7:"G", 8:"H", 9:"I", 10:"J", 11:"K", 12:"L" }
    #put
    put_table = { 1:"M", 2:"N", 3:"O", 4:"P", 5:"Q", 6:"R", 7:"S", 8:"T", 9:"U", 10:"V", 11:"W", 12:"S" }
    
    if flag == 1:
        return call_table[month]
    else:
        return put_table[month] 
    
def Date_to_Timestamp(front_days): #把日期轉為時間戳
    dt = datetime.datetime.now()
    dt = dt - datetime.timedelta(front_days)
    timestamp = time.mktime(dt.timetuple())
    
    return int(timestamp)

def Timestamp_to_Date(time_stamp): #把時間戳轉為日期
    timestamp = datetime.datetime.fromtimestamp(time_stamp)
    Date = timestamp.strftime('%Y%m%d')
    Time = timestamp.strftime('%H%M%S')
    
    return Date, Time
    
def API_Value_Set(symbol, resolution, start, end): #接上期天api並調整參數
    start_stamp = Date_to_Timestamp(start) #把日期轉為時間戳
    end_stamp = Date_to_Timestamp(end) #把日期轉為時間戳
    symbol = symbol #商品名稱
    resolution = resolution #幾分K
    
    url = "https://api.e01.futures-op.com/api/ohlcv?"
    url = url + "symbol=" + symbol + "&resolution=" + str(resolution)
    url = url + "&from=" + str(start_stamp) + "&to=" + str(end_stamp)
    
    return url #取得api網址

def Get_Origin_Data(url): #取得原始時間,低高開收及量六部分
    r = requests.get(url, verify=False)
    content = r.text
    soup = BeautifulSoup(content, 'html.parser')
    six_part = str(soup).split(']')
    
    return six_part

def Form_to_DataFrame(six_part, find_price_flat_flag, only_today_night_flag , dt1): #把data轉成dataframe
    global dic
    df = pd.DataFrame(columns=dic) 
    
    count = 0 #紀錄已到原六種data中的哪一個
    bar_information_list = [] #紀錄每條bar資訊
    Date_list = [] #紀錄Date(年月日)
    Time_list = [] #紀錄時間段(小時分秒)
    L_list = [] #低
    H_list = [] #高
    O_list = [] #開
    C_list = [] #收
    V_list = [] #量
    for i in six_part: #六部分執行六次
        count = count + 1 
        part_tmp = i.split('[')
        if len(part_tmp) > 1:
            bar_tmp = part_tmp[1].split(',') #此部分的所有元素
            if len(bar_tmp) <= 1:
                return df #甚麼都沒有就直接return
            for j in bar_tmp: 
                if count == 1:
                    value_tmp = int(j) #j在這時timestamp
                    Date, Time = Timestamp_to_Date(value_tmp) #把時間戳轉為日期
                    
                    Date_list.append(Date)
                    Time_list.append(Time)
                else:
                    value_tmp = float(j) 
                    if count == 2:
                        L_list.append(value_tmp)
                    elif count == 3:
                        H_list.append(value_tmp)
                    elif count == 4:
                        O_list.append(value_tmp)
                    elif count == 5:
                        C_list.append(value_tmp)
                    elif count == 6:
                        V_list.append(value_tmp)
                    else:
                        a = 1
                        
    for i in range(len(Date_list)):
        bar_information_list = [ Date_list[i], Time_list[i], O_list[i], H_list[i], L_list[i], C_list[i], V_list[i] ]
        #print(bar_information_list)
        if find_price_flat_flag == 0: #因確認價平只需用第一列判斷即可,節省時間
            if only_today_night_flag == 1 or int(Time_list[i]) == 84600:
                #只有夜盤則直接抓第一筆        否則抓當日開盤
                a_series = pd.Series(bar_information_list, index = df.columns)
                df = df.append(a_series, ignore_index=True) #把每一列依序存進df
                break;
        else: #盼頓日夜盤寫好
            if str(Date_list[i]) == str(dt1) and int(Time_list[i]) >= 150000 and int(Time_list[i]) <= 50000: #可抓夜盤
                a = 1
            else:
                a_series = pd.Series(bar_information_list, index = df.columns)
                df = df.append(a_series, ignore_index=True) #把每一列依序存進df
        
    return df
 
def Find_week_number(symbol):
    next_month_flag = 0
    dt = datetime.date.today()
    
    wed_list = []
    s, e = monthrange(dt.year, dt.month)
    for i in range( s, e+1):
        if weekday(dt.year, dt.month, i) == 2: #找尋月份裡的每個禮拜三
            wed_list.append(i)
            
    if dt.day <= wed_list[0]:
        symbol = symbol + str(1)
        w = str(1)
        
    elif dt.day <= wed_list[1]:
        symbol = symbol + str(2)
        w = str(2)
        
    elif dt.day <= wed_list[2]:
        symbol = symbol + str('O')
        w = str('O')
        
    elif dt.day <= wed_list[3]:
        symbol = symbol + str(4)
        w = str(4)
        
    elif len(wed_list) == 5 and dt.day <= wed_list[4]: #該月有第五周的狀況
        symbol = symbol + str(5)  
        w = str(5)
        
    else : #已經要換月的剩餘天數
        symbol = symbol + str(1)
        next_month_flag = 1
        w = str(1)
        
    return symbol, next_month_flag, w

def Find_price_flat(df1, df2, price, mcall, mput, mpirce):
    #print(np.unique(df['date']))
    
    #####從第一筆資料開始找尋價平履約價#####
    min_call = mcall
    min_put = mput
    min_strike_price = mprice

    new_call = df1['close'][0]
    new_put = df2['close'][0]
    if int(new_call) == 0 or int(new_put) == 0: #若有一方為0代表已經價外
        return min_call, min_put, min_strike_price
    new_strike_price = str(price) 
    #print(new_call,new_put,new_strike_price)
    min_tmp = min_call + min_put
    new_tmp = new_call + new_put
    if int(new_tmp) != 0: #若等於0代表沒有交易已經是價外
        if int(min_tmp) == 0: 
            min_call = new_call
            min_put = new_put
            min_strike_price = new_strike_price
        else:
            if new_tmp < min_tmp:
                min_call = new_call
                min_put = new_put
                min_strike_price = new_strike_price
    #min_call:價平之call min_put:價平之put min_strike_price:價平履約價
    #print(min_call,min_put,min_strike_price)
    
    return min_call, min_put, min_strike_price
    
def main_area( dt, symbol_tmp, next_month_flag, call_put_flag, find_price_flat_flag ,which_day_start, which_day_end ,dt1): 
            #現在日期,商品名前半, 1為需要換月,   , 1為找尋call    0為未確認價平和       從幾天前開始跑
    only_today_night_flag = 0  
    if next_month_flag == 1: #換月
        symbol = symbol_tmp + str(conversion_table(call_put_flag, dt.month+1)) + str(0)
    else:
        symbol = symbol_tmp + str(conversion_table(call_put_flag, dt.month)) + str(0)
    #完整商品名為 TX + 結算日 + 履約價 + callput月份代號 + 0
    
    url = API_Value_Set(symbol, 1, which_day_start, which_day_end ) #接上期天api並調整參數
                #完整商品名稱, 幾分K, 回朔天數,       結束天數 
        
    six_part = Get_Origin_Data(url) #取得原始時間,低高開收及量六部分
    
    if which_day_start == which_day_end:
        only_today_night_flag = 1
    df = Form_to_DataFrame(six_part, find_price_flat_flag, only_today_night_flag, dt1) #把data轉成dataframe
                                    #0為未確認價平           1為只有今日夜盤
    
    return df

def Output_df_to_csv(df, df1, call_put_flag, my_folder_path, past_flag):
                #主要df, 新加入df, 1為輸出到call, 輸出位置,    1為回補資料
    if df.empty:
        df = df1
    else:
        if past_flag == 1: #回補則加入整天
            frames = [df, df1]
        else: #即時則加入最新一筆
            frames = [df, df1.tail(1)]

        df = pd.concat(frames) 
        df.drop_duplicates(['date','time'], 'first', inplace = True) #刪掉指定條件重複列
        
    if call_put_flag == 1: #df輸出到call
        file_address = my_folder_path + "/" + "price_flat_call" + ".csv"
    elif call_put_flag == 0: #df輸出到put的csv
        file_address = my_folder_path + "/" + "price_flat_put" + ".csv"
    else: #df輸出到價平履約價的csv
        file_address = my_folder_path + "/" + "price_flat_price" + ".csv"
    df.to_csv(file_address, index=False)
    
    return df

if __name__ == "__main__":
    
    warnings.filterwarnings("ignore")
    #####設定輸出位置#####
    my_folder_path = "C:/Users/a0985/OneDrive/Desktop/期貨/資料/op_data" #到價平和資料夾
    
    w_new = 'k'
    mcall, mput, mprice = 3000, 3000, 100
    next_day_flag = 1
    df_call = pd.DataFrame(columns=dic)
    df_put = pd.DataFrame(columns=dic) 
    df_price = pd.DataFrame(columns=dic)
    df3 = pd.DataFrame(columns=dic)
    '''
    for i in reversed(range(3)): #跑過去n天
        mcall, mput, mprice = 3000, 3000, 100
        if i - 1 < 0:
            break
        today = datetime.datetime.now() - datetime.timedelta(i)
        today = today.day
        for price in range(11800, 13200, 50):
            symbol_tmp = symbol + str(price)

            df1 = main_area(datetime.date.today(), symbol_tmp, next_month_flag, 1,       0,           i, i-1) 
                            #現在日期,             商品名前半, 是否換月flag, call_flag, 未確認價平和flag 從幾天前跑

            df2 = main_area(datetime.date.today(), symbol_tmp, next_month_flag, 0,       0,           i, i-1) 
                            #現在日期,            商品名前半, 是否換月flag,  put_flag, 未確認價平和flag 從幾天前跑
            ###df1為該履約價call, df2為該履約價put

            mcall, mput, mprice = Find_price_flat(df1, df2, price, mcall, mput, mprice) #找尋價平

            df1 = df1.iloc[0:0] #清空df
            df2 = df2.iloc[0:0] #清空df

        symbol_tmp = symbol + str(mprice)

        df1 = main_area(datetime.date.today(), symbol_tmp, next_month_flag, 1, 1, i, i-1) #價平call
        df2 = main_area(datetime.date.today(), symbol_tmp, next_month_flag, 0, 1, i, i-1) #價平put
        
        df_call = Output_df_to_csv(df_call, df1, 1, my_folder_path, 1)
        print(df_call)
        df_put = Output_df_to_csv(df_put, df2, 0, my_folder_path, 1)
        print(mprice, "Ok")
    '''
    while(True): #從今天開始跑
        symbol = 'TX' #商品名稱
        symbol, next_month_flag, w = Find_week_number(symbol) #確認今日為Week幾
        if  w_new != w: #初次執行或換結算日則csv清空重來
            df = pd.DataFrame()
            df = Output_df_to_csv(df, df, 1, my_folder_path, 0) #csv清空
            df = Output_df_to_csv(df, df, 0, my_folder_path, 0) #csv清空
            df = Output_df_to_csv(df, df, 2, my_folder_path, 0) #csv清空
        w_new = w
        now = datetime.datetime.now() 
        if next_day_flag == 1: #初次執行或已換天要重找價平履約價
            today = now.day #紀錄現在為幾號
            for price in range(11800, 13200, 50): #不同檔履約價
                symbol_tmp = symbol + str(price)  

                df1 = main_area(datetime.date.today(), symbol_tmp, next_month_flag, 1,       0,           0, 0    ,now.strftime('%Y%m%d')) 
                                #現在日期,             商品名前半, 是否換月flag, call_flag, 未確認價平和flag 從幾天前跑
                    
                df2 = main_area(datetime.date.today(), symbol_tmp, next_month_flag, 0,       0,           0, 0     ,now.strftime('%Y%m%d')) 
                                #現在日期,            商品名前半, 是否換月flag,  put_flag, 未確認價平和flag 從幾天前跑
                ###df1為該履約價call, df2為該履約價put
                
                print(price)
                print(df1)
                print(df2)
                if df1.empty or df2.empty: #若有call put其中一方沒值，代表已經價外
                    continue
                else:
                    mcall, mput, mprice = Find_price_flat(df1, df2, price, mcall, mput, mprice) #找尋價平
                                            #目前履約價call, put,履約價, 當前最小call,put及該履約價
                
                print(mprice)
                print('##########')
                df1 = df1.iloc[0:0] #清空df
                df2 = df2.iloc[0:0] #清空df
            next_day_flag = 0
        
        
        if now.day != today and now.hour >= 8 and now.minute >= 45: #若目前時間相較today已隔天開盤
            next_day_flag = 1
            mcall, mput, mprice = 3000, 3000, 100
            continue #重新找新日價平
        
        symbol_tmp = symbol + str(mprice) #價平履約價商品名
        
        df1 = main_area(datetime.date.today(), symbol_tmp, next_month_flag, 1, 1, 0, 0, now.strftime('%Y%m%d')) #價平call
        df2 = main_area(datetime.date.today(), symbol_tmp, next_month_flag, 0, 1, 0, 0, now.strftime('%Y%m%d')) #價平put
        df3['date'] = df1['date']
        df3['time'] = df1['time']
        df3['high'] = df3['close'] = mprice
        df3['open'] = df3['low'] = df3['volume'] = 0
        df3 = df3[['date', 'time', 'open', 'high', 'low', 'close', 'volume']]
        
        df_call = Output_df_to_csv(df_call, df1, 1, my_folder_path, 0) #輸出call的csv
                                                #call
        df_put = Output_df_to_csv(df_put, df2, 0, my_folder_path, 0) #輸出put的csv
                                             #put
        df_price = Output_df_to_csv(df_price, df3, 2, my_folder_path, 0) #輸出價平履約價的csv
                                                  #another
        print(mprice)
        print(df_call.tail(3))
        print(df_put.tail(3))
        
        time.sleep(30)
            
            
    print("ok")

    