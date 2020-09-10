import pandas as pd
import datetime
import os
import numpy as np
import urllib 
import urllib.request
import zipfile

whether_data_is_null_flag = 0 #若查無原始檔則掛起1
min_strike_price = 0
night_df = pd.DataFrame() #處理尋找價平和的df

def get_transaction_second_index(transaction_second_index, start_time, break_time) : 
    transaction_second = start_time
    transaction_second_index = list()
    for i in range(0,200000):
        transaction_second = transaction_second + 1
        tmp_sec = transaction_second % 100 #紀錄秒用
        tmp_min = transaction_second / 100 #紀錄分用
        tmp_min = tmp_min % 100

        if (tmp_sec > 59) or (tmp_min >= 60):
            continue;
        else:
            transaction_second_index.append(transaction_second)
        if transaction_second == break_time:
            break;
    
    return transaction_second_index;
            
def erase_redundant_space_and_value(origin_df) :
    origin_df.rename(columns={' 成交日期':'成交日期','          商品代號':'商品代號','        履約價格':'履約價格'}, inplace = True)
    origin_df.rename(columns={'                                                      到期月份(週別)':'到期月份(週別)'}, inplace=True)
    origin_df.rename(columns={'        買賣權別':'買賣權別','      成交時間':'成交時間','          成交價格':'成交價格'}, inplace=True)
    origin_df.rename(columns={'         成交數量(B or S)':'成交數量(B or S)','     開盤集合競價 ':'開盤集合競價'}, inplace=True)
    
    origin_df.drop_duplicates(inplace=True) #刪除完全重複列
    origin_df = origin_df.dropna().reset_index(drop=True) #把有nan的列delete
    origin_df.drop(labels=['開盤集合競價'], axis = 'columns', inplace = True)#delete多餘欄位
    origin_df['商品代號'] = origin_df['商品代號'].str.replace(' ','') #消除空白
    origin_df = origin_df[ origin_df['商品代號'] == 'TXO' ] #篩選TXO出來
    origin_df['到期月份(週別)'] = origin_df['到期月份(週別)'].astype(str).str.replace(' ','') #消除空白
    origin_df = origin_df.reset_index(drop = True) #目錄重置
    
    return origin_df;

def get_transaction_second_df_to_price_name(price_name, transaction_second_index) : 
    price_name = price_name.fillna(0) #把NaN補成0

    price_name['call'] = price_name['call'].astype(float)
    price_name['put'] = price_name['put'].astype(float)
    price_name['成交時間'] = price_name['成交時間'].astype(int)
    
    aggregation_functions = {'call': 'sum', 'put': 'sum'} #依照相同時間段合併row
    
    #如此每一時間皆有put&call val
    price_name = price_name.groupby('成交時間', as_index=True).aggregate(aggregation_functions)
    price_name = price_name.replace(0.0,np.nan) #把沒成交價的地方改成NaN，方便後續處理
    price_name = price_name.reindex(transaction_second_index) #把缺失的index補足
    price_name = price_name.fillna(method = 'ffill') #有NaN之處補上前筆交易的成交價格
    ###補充1 : 若開盤一段時間內沒有交易資料則捨棄，直到有交易資料###
    ###補充2 : 如果前秒有值下秒沒有的話就要遵照前一秒的值###
    
    return price_name;

def find_near_month(deadline_list, weekofday, day_flag):
    #print(deadline_list)
    week_flag = 0 #判斷週選則掛起1
    for i in deadline_list: #判斷要抓周選還月選
        if 'W4' in str(i) and weekofday == 3 and day_flag == 1: #第三周的禮拜三日盤要看月選
            week_flag = 0
            break;
        if 'W' in str(i) : #有周選擇抓週選
            week_flag = 1
            break;
            
    if week_flag == 0 : #月選直接抓最近月，第二個禮拜三
        deadline_in_month = list()
        tmp_flag = 0
        for i in deadline_list : #因第三周的禮拜三日盤要看月選,所以這裡主要是要把該天的W4濾掉
            if 'W' not in i:
                deadline_in_month.append(i)
            else:
                tmp_flag = 1
        near_month = min(deadline_in_month)
        if tmp_flag == 1 or (day_flag == 0 and weekofday == 2) or (day_flag == 2 and weekofday == 3):
            near_month = int(near_month)
        else:
            near_month = float(near_month) #不寫這個會報錯
        near_month = str(near_month)
        
    else : #平常只會有一倉，週三會有兩個，要抓最近
        deadline_list = deadline_list[ deadline_list.str.contains("W") ] #篩選有W的項出來
        deadline_in_week = list()
        
        for i in deadline_list.str.split("W"):
            a = i[0] + i[1]
            deadline_in_week.append( int(a) )
            
        if len(deadline_in_week) == 1:
            near = deadline_in_week[0]
        else:
            near = min( deadline_in_week )
        near = str(near)
        near_month = near[0:-1] + "W" + near[-1]
    
    print(near_month)
    return near_month;


def get_today_rpt(rpt_name) :
    print("downloading with urllib")
    url = 'https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownload/' + rpt_name + '.zip'
    urllib.request.urlretrieve(url, rpt_name)
    
    zip_name = rpt_name #壓縮文件名

    file_dir = 'C:/Users/a0985/OneDrive/Desktop/op_rpt/2020unzip' #解壓後的文件放在該目錄下
    with zipfile.ZipFile(zip_name, 'r') as myzip:
        for file in myzip.namelist():
            myzip.extract(file,file_dir)

    delete_daily_zip_address =  os.path.abspath('.') + "\\" + rpt_name
    os.remove(delete_daily_zip_address)

def read_origin_data(time_format_in_origin_file_name) :
    global whether_data_is_null_flag
    fname = 'C:/Users/a0985/OneDrive/Desktop/op_rpt/2020unzip/OptionsDaily_' + str(time_format_in_origin_file_name) + '.rpt'
    if os.path.isfile(fname) : #有此檔案
        origin_df = pd.read_csv(fname, encoding = 'big5') #rpt 的編碼方式真D特殊
        whether_data_is_null_flag = 0
    else:                      #查無此檔
        whether_data_is_null_flag = 1
        return;
    return origin_df;


def preprocess(origin_df, weekofday, day_flag) :
    if ' 交易日期' in origin_df.columns: #把原始檔中不同column name統一成成交日期
        origin_df[' 成交日期'] = origin_df[' 交易日期']
        origin_df.drop(labels=[' 交易日期'], axis = 'columns', inplace = True)#delete多餘欄位
    
    ######只留下商品為TXO的rows#####
    #function
    txo_origin_df = erase_redundant_space_and_value(origin_df) #消除column空白處及多餘value
    
    if day_flag == 1: #日盤
        #####篩選日盤交易逐筆資料(當天)並刪掉指定條件重複列#####
        k1 = (txo_origin_df['成交時間'] >= 84500) & (txo_origin_df['成交時間'] <= 134500)
        txo_origin_df = txo_origin_df[ k1 ] 
    elif day_flag == 0 : #前天下午盤(夜盤)
        #####篩選夜盤交易逐筆資料(前天下午三點至今日上午五點)並刪掉指定條件重複列#####
        k1 = (txo_origin_df['成交時間'] >= 150000) & (txo_origin_df['成交時間'] <= 240000)
        txo_origin_df = txo_origin_df[ k1 ] 
    else : #今日凌晨盤(夜盤)
        #####篩選夜盤交易逐筆資料(前天下午三點至今日上午五點)並刪掉指定條件重複列#####
        k1 = (txo_origin_df['成交時間'] >= 0) & (txo_origin_df['成交時間'] <= 50000)
        txo_origin_df = txo_origin_df[ k1 ]
        
    #####找到要抓的最近到期月份(周別)#####
    deadline_list = txo_origin_df['到期月份(週別)'] #抓所有到期時間，要找之中離目前最近
    deadline_list.drop_duplicates(inplace = True)
    #function
    near_month = find_near_month(deadline_list, weekofday, day_flag) #得到最近到期月份(周別)
    
    #####df篩選出符合最近到期月份(周別)#####
    txo_origin_df = txo_origin_df.reset_index(drop = True) #目錄重置
    txo_df = txo_origin_df[ txo_origin_df['到期月份(週別)'] == near_month ] 
    txo_df = txo_df.reset_index(drop = True) #目錄重置
    #print(near_month)
    #print(txo_df)
    
    #####轉為int格式，消掉data多餘空白#####
    txo_df['成交日期'] = txo_df['成交日期'].astype(int)
    txo_df['成交時間'] = txo_df['成交時間'].astype(int)
    txo_df['履約價格'] = txo_df['履約價格'].astype(int)
    
    txo_df.drop_duplicates(['成交日期','履約價格','買賣權別','成交時間'], 'first', inplace = True) #刪掉指定條件重複列
    
    #####用時間段('排序依據')排序data#####
    txo_df['成交時間'] = txo_df['成交時間'].astype(str).str.zfill(6) #成交時間皆在前面補0成6位數
    txo_df['成交日期'] = txo_df['成交日期'].astype(str)
    txo_df['排序依據'] = txo_df['成交日期'] + txo_df['成交時間']
    
    txo_df = txo_df.sort_values(['排序依據'],ascending = True) #根據時間由遠到近sort
    txo_df = txo_df.reset_index(drop = True) #sort後目錄重置
    
    #####接著再使用履約價為最優先排序#####
    txo_df['履約價格'] = txo_df['履約價格'].astype(str)
    txo_df['排序依據'] = txo_df['履約價格'] + txo_df['排序依據']
    txo_df = txo_df.sort_values(['排序依據'],ascending = True) #根據時間由遠到近sort
    txo_df = txo_df.reset_index(drop = True) #sort後目錄重置
    
    return txo_df;

def group_by_strike_price(txo_df, start_date, transaction_second_index):
    
    price_flat_sum_df_exist_flag = 0 #price_flat_sum專門用來找價平履約價,此為用來記錄此df是否存在
    price_flat_sum_df_exist_flag_for_night = 0
    get_import_form_flag = 0 
    #日盤如果84500遍歷後沒找到價平,則會重跑loop,此flag為避免重複call function
    #夜盤則為1345400收盤時
    
    for i in range(18000) :
        price_flat_sum_df_exist_flag = 0
        
        #找出所有履約價
        for strike_price_start_value in np.unique(txo_df['履約價格'].astype(int)):
            
            #####對每個履約價處理每日日盤逐筆交易#####
            #function
            price_name = process_to_import_form(txo_df, strike_price_start_value, transaction_second_index, start_date)
            
            #####完整的當天個別履約價逐筆資料df#####
            price_name = price_name[['成交日期','成交時間','call','put']]
            
            #print(strike_price_start_value)
            #print(price_name)
            #print('################')
            
            '''
            if price_name.empty: #df為空則代表當天無此履約價資料 
                a = 1
            else:
                a = 1
                #####整理成QM讀取CSV的格式#####
                #function
                if get_import_form_flag == 0:
                    get_import_form(price_name, strike_price_start_value) #生成每個履約價的逐筆成交點數
            '''
            #####從第一筆資料開始找尋價平履約價#####
            if price_flat_sum_df_exist_flag == 0:
                min_call = price_name['call'][i]
                min_put = price_name['put'][i]
                min_strike_price = str(strike_price_start_value) 
                price_flat_sum_df_exist_flag = 1
            else :
                new_call = price_name['call'][i]
                new_put = price_name['put'][i]
                new_strike_price = str(strike_price_start_value) 

                min_tmp = min_call + min_put
                new_tmp = new_call + new_put
                if not np.isnan(new_tmp):
                    if np.isnan(min_tmp):
                        min_call = new_call
                        min_put = new_put
                        min_strike_price = new_strike_price
                    else:
                        if new_tmp < min_tmp:
                            min_call = new_call
                            min_put = new_put
                            min_strike_price = new_strike_price
            #min_call:價平之call min_put:價平之put min_strike_price:價平履約價
                

        #print(min_call + min_put,"    ",i)
        #get_import_form_flag = 1 
        if not np.isnan(min_call + min_put) : #假如日盤第一筆資料(84500)沒找到價平和,則繼續往下一筆找,否則break
            break;                            #沒找到也就是說call + put結果皆是nan
    
    return min_strike_price;

def find_price_flat_sum(txo_df, min_strike_price, transaction_second_index, start_date):
    price_flat_sum = pd.DataFrame() #處理尋找價平和的df
    #####把價平履約價的格式準備好#####
    #function
    price_flat_sum = process_to_import_form(txo_df, min_strike_price, transaction_second_index, start_date)

    #完整的當天個別履約價逐筆資料df
    price_flat_sum = price_flat_sum[['成交日期','成交時間','call','put']]
    price_flat_sum['min_strike_price'] = str(min_strike_price)
    price_flat_sum = price_flat_sum.reset_index(drop = True)
    
    price_flat_sum = price_flat_sum[['min_strike_price','call','put', '成交日期', '成交時間']]
    
    return price_flat_sum;

def process_to_import_form(txo_df, strike_price_value, transaction_second_index, start_date):
    
    price_name = txo_df[ txo_df['履約價格'] == str(strike_price_value) ] #依照不同履約價分類df
    price_name = price_name.reset_index(drop = True)

    price_name['買賣權別'] = price_name['買賣權別'].str.replace(' ','') #消除空白
    price_name = price_name.reset_index(drop=True) 

    #多call & put欄位存放個別成交價格
    price_name['call'] = price_name['成交價格'][ price_name['買賣權別'] == 'C' ]
    price_name['put'] = price_name['成交價格'][ price_name['買賣權別'] == 'P' ]

    #濾掉多餘欄位
    price_name = price_name[['成交時間','call','put']]  

    #建立時間逐筆資料
    #function
    price_name = get_transaction_second_df_to_price_name(price_name, transaction_second_index)

    #得到成交價格欄位並補上成交日期
    time_format_in_origin_file_name = start_date.strftime('%Y%m%d')
    price_name['成交日期'] = time_format_in_origin_file_name 
    price_name.reset_index(level=0, inplace=True) #成交時間賦歸欄位
    
    return price_name;

def get_import_form(price_name, strike_price_start_value): #整理成QM讀取CSV的格式
    
    #####輸出csv的名字依照履約價命名#####
    Filename:str = str(strike_price_start_value)
    
    #####'Date', 'Time', 'Price','Volume'為QM的import格式#####
    price_name.rename(columns={'成交時間':'Time','成交日期':'Date'}, inplace = True)
    
    if 'min_strike_price' in price_name.columns : #若此df為記錄價平和，則volume存放當筆之價平和履約價
        price_name['Price'] = price_name['min_strike_price']
    else :
        price_name['Price'] = int( strike_price_start_value )
        
    price_name['Volume'] = 0    
    #price_name['Open'] = price_name['Low'] = 0
    #price_name['High'] = price_name['Close'] = price_name['成交價格']
    
    #####各個履約價自己的call & put#####
    price_name_call = pd.DataFrame() #處理尋找價平和的df
    price_name_call['Price'] = price_name['call'] 
    price_name_call['Volume'] = 0
    price_name_call['Date'] = price_name['Date'] 
    price_name_call['Time'] = price_name['Time']
    
    complete_strike_price_call_data = price_name_call[['Date', 'Time', 'Price', 'Volume']]
    complete_strike_price_call_data['Time'] = complete_strike_price_call_data['Time'].astype('int')
    complete_strike_price_call_data = complete_strike_price_call_data.dropna().reset_index(drop = True) #把有nan的列delete
    complete_strike_pric_call_data = complete_strike_price_call_data.reset_index(drop=True) #成交時間賦歸欄位
    
    #####依照履約價個別輸出成csv#####
    #function
    output_to_csv_by_strike_price(complete_strike_price_call_data, Filename + '_call') #各履約價的tick call
    
    ###
    price_name_put = pd.DataFrame() #處理尋找價平和的df
    price_name_put['Price'] = price_name['put'] 
    price_name_put['Volume'] = 0
    price_name_put['Date'] = price_name['Date']
    price_name_put['Time'] = price_name['Time']
    
    complete_strike_price_put_data = price_name_put[['Date', 'Time', 'Price', 'Volume']]
    complete_strike_price_put_data['Time'] = complete_strike_price_put_data['Time'].astype('int')
    complete_strike_price_put_data = complete_strike_price_put_data.dropna().reset_index(drop = True) #把有nan的列delete
    complete_strike_pric_put_data = complete_strike_price_put_data.reset_index(drop=True) #成交時間賦歸欄位
    
    #####依照履約價個別輸出成csv#####
    #function
    output_to_csv_by_strike_price(complete_strike_price_put_data, Filename + '_put') #各履約價的tick call
    
    ###
    complete_strike_price_data = price_name[['Date', 'Time', 'Price', 'Volume']]
    complete_strike_price_data['Time'] = complete_strike_price_data['Time'].astype('int')
    complete_strike_price_data = complete_strike_price_data.dropna().reset_index(drop = True) #把有nan的列delete
    complete_strike_price_data = complete_strike_price_data.reset_index(drop=True) #成交時間賦歸欄位
    
    #####依照履約價個別輸出成csv#####
    #function
    output_to_csv_by_strike_price(complete_strike_price_data, Filename) #各履約價的價格 + 價平和
    ###
    
    
def output_to_csv_by_strike_price(complete_strike_price_data, Filename) :
    
    #####設定輸出位置#####
    my_folder_path = "C:/Users/a0985/OneDrive/Desktop/期貨/資料/op_data" #到價平和資料夾
    file_address = my_folder_path + "/" + "_op_price_" + Filename + ".csv"
    
    if os.path.isfile(file_address) : 
        
        previous_data = pd.read_csv(file_address)
        #if not previous_data.empty:
        #    previous_data = previous_data[ previous_data['Date'] != np.unique(previous_data['Date'])[0] ]
        previous_data = previous_data.append(complete_strike_price_data)
        previous_data = previous_data.reset_index(drop = True) #目錄重置
        previous_data.to_csv(file_address, index = False)
        
    else:
        complete_strike_price_data.to_csv(file_address, index = False)
        
    #print(Filename + 'ok')
    
    return; #end


def process_by_time_gap(origin_df, transaction_second_index, start_date, day_flag):
    global min_strike_price
    global night_df
    #####先整理好資料#####
    #function
    #第一個變數為原始檔資料,第二個為該天為星期幾,做為最近到期判斷用,第三個為當天日期
    txo_df = preprocess(origin_df, start_date.isoweekday(), day_flag )
    
    if day_flag == 1 or day_flag == 0:
        #####開始依照履約價格分類，依照日期順序,且找出日盤時開盤及夜盤開盤的價平履約價#####
        min_strike_price = group_by_strike_price(txo_df, start_date, transaction_second_index)
    
    #####找完價平履約價後,接著就生成該履約價的df#####
    #function
    price_name = find_price_flat_sum(txo_df, min_strike_price, transaction_second_index, start_date)
    
    if day_flag == 0: #下午盤先加入夜盤df中
        night_df = price_name
    elif day_flag == 2: #凌晨盤跟下午盤合併成夜盤
        night_df = night_df.append(price_name)
        night_df = night_df.fillna(method = 'ffill') #有NaN之處補上前筆交易的成交價格
        
        #####整理成QM讀取CSV的格式#####
        #function
        get_import_form(night_df, 'price_flat_sum')
        
        night_df.drop(night_df.index, inplace=True)
    else: #單純日盤
        #####整理成QM讀取CSV的格式#####
        #function
        get_import_form(price_name, 'price_flat_sum')
        

        
        
        
if __name__ == "__main__":
    #start_date = datetime.datetime(2020, 9, 3) #代表資料從何時開始
    start_date = datetime.date.today() #代表資料從何時開始
    #start_date = start_date - datetime.timedelta(days=1)
    end_date = datetime.date.today()
    end_date = end_date + datetime.timedelta(days=1)
    end_date = end_date.strftime('%Y_%m_%d')
    
    time_format_in_origin_file_name = start_date.strftime('%Y_%m_%d') #原始檔名稱用YY_MM_DD表示    
    while time_format_in_origin_file_name != end_date : #匯入資料直到指定停止日期
        
        #####上期交所抓原始zip檔並解壓到特定資料夾#####
        #function
        if start_date.isoweekday() == 6 or start_date.isoweekday() == 7:
            a = 1
        else:
            rpt_name = 'OptionsDaily_' + str(time_format_in_origin_file_name) 
            get_today_rpt(rpt_name)
        
        #####讀取原始資料#####
        #function
        origin_df = read_origin_data(time_format_in_origin_file_name)
        print(time_format_in_origin_file_name,whether_data_is_null_flag)
        
        if(whether_data_is_null_flag == 0):
            
            #####前日下午盤(夜盤)時段逐秒list#####
            transaction_second_index = list() #交易時段為15:00:00 ~ 23:59:59
            #function
            start_time = 149999
            break_time = 235959
            transaction_second_index = get_transaction_second_index(transaction_second_index, start_time, break_time)
            day_flag = 0 #夜盤flag
            #####依照前天下午盤+當天凌晨盤,及當天日盤處理#####
            if start_date.isoweekday() == 1: #禮拜一的原始檔,下午盤的時間在上禮拜五
                process_by_time_gap(origin_df, transaction_second_index, start_date - datetime.timedelta(days=3), day_flag)
            else: #一般狀況
                process_by_time_gap(origin_df, transaction_second_index, start_date - datetime.timedelta(days=1), day_flag)

            #####今日凌晨盤(夜盤)時段逐秒list#####
            origin_df = read_origin_data(time_format_in_origin_file_name)
            transaction_second_index = list() #交易時段為0:00:00 ~ 5:00:00
            #function
            start_time = -1
            break_time = 50000
            transaction_second_index = get_transaction_second_index(transaction_second_index, start_time, break_time)
            day_flag = 2 #夜盤flag
            #####依照前天下午盤+當天凌晨盤,及當天日盤處理#####
            if start_date.isoweekday() == 1: #禮拜一的原始檔,下午盤的時間在上禮拜五
                process_by_time_gap(origin_df, transaction_second_index, start_date - datetime.timedelta(days=2), day_flag)
            else: #一般狀況
                process_by_time_gap(origin_df, transaction_second_index, start_date, day_flag)
            
            
            #####日盤時段逐秒list#####
            origin_df = read_origin_data(time_format_in_origin_file_name)
            transaction_second_index = list() #交易時段為8:45:00 ~ 13:45:00
            #function
            start_time = 84499
            break_time = 134500
            transaction_second_index = get_transaction_second_index(transaction_second_index, start_time, break_time)
            day_flag = 1 #日盤flag
            #####依照前天下午盤+當天凌晨盤,及當天日盤處理#####
            process_by_time_gap(origin_df, transaction_second_index, start_date, day_flag)
        
        print("OK")
        start_date = start_date + datetime.timedelta(days=1)
        time_format_in_origin_file_name = start_date.strftime('%Y_%m_%d')
