import pandas as pd
import datetime
import os
import numpy as np
import urllib
import urllib.request
import zipfile
import requests
from dateutil.parser import parse

whether_data_is_null_flag = 0 #若查無原始檔則掛起1
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
    origin_df = origin_df[['成交日期', '商品代號', '到期月份(週別)', '成交時間', '成交價格', '成交數量(B+S)']]
    origin_df.drop_duplicates(inplace=True) #刪除完全重複列
    origin_df = origin_df.dropna().reset_index(drop=True) #把有nan的列delete
    origin_df['商品代號'] = origin_df['商品代號'].str.replace(' ','') #消除空白
    origin_df = origin_df[ origin_df['商品代號'] == 'MTX' ] #篩選小台出來
    origin_df['到期月份(週別)'] = origin_df['到期月份(週別)'].astype(str).str.replace(' ','') #消除空白
    origin_df = origin_df.reset_index(drop = True) #目錄重置
    
    return origin_df;


def find_near_month(deadline_list, weekofday, day_flag):
    deadline_list = deadline_list.values.tolist()
    tmp = list()
    for i in deadline_list: 
        if "/" not in i:
            tmp.append(i)
    
    deadline_list = tmp
    week_flag = 0 #判斷週選則掛起1
    for i in deadline_list: #判斷要抓周選還月選
        if 'W4' in str(i) and weekofday == 3 and day_flag == 1: #第三周的禮拜三日盤要看月選
            week_flag = 0
            break;
        if 'W' in str(i) :
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
            near_month = int(near_month)
        near_month = str(near_month)
        
    else : #平常只會有一倉，週三會有兩個，要抓最近
        tmp = list()
        for i in deadline_list:
            if "W" in i:
                tmp.append(i) #篩選有W的項出來
            else:
                a = 1
        deadline_in_week = list()
        
        for i in tmp:
            j = i.split("W")
            a = j[0] + j[1]
            deadline_in_week.append( int(a) )
         
        if len(deadline_in_week) == 1:
            near = deadline_in_week[0]
        else:
            near = min( deadline_in_week )
        near = str(near)
        near_month = near[0:-1] + "W" + near[-1]
    
    #print(near_month)
    return near_month;


def get_transaction_second_df_to_price_name(price_name, transaction_second_index) : 
     
    price_name = price_name.fillna(0) #把NaN補成0
    
    price_name['成交時間'] = price_name['成交時間'].astype(int)
    price_name.drop_duplicates(['成交時間'], 'first', inplace = True) #刪掉指定條件重複列
    
    #如此每一時間皆有put&call val
    price_name = price_name.set_index('成交時間')
    price_name = price_name.reindex(transaction_second_index) #把缺失的index補足，084500~134500
    price_name = price_name.fillna(method = 'ffill') #有NaN之處補上前筆交易的成交價格
    
    ###補充1 : 若開盤一段時間內沒有交易資料則捨棄，直到有交易資料###
    ###補充2 : 如果前秒有值下秒沒有的話就要遵照前一秒的值###
    
    return price_name;





def get_today_rpt(rpt_name) :
    print("downloading with urllib")
    url = 'https://www.taifex.com.tw/file/taifex/Dailydownload/Dailydownload/' + rpt_name + '.zip'
    html = requests.head(url) # 用head方法去請求資源頭
    re=html.status_code
    if re != 200:
        return
    else:
        urllib.request.urlretrieve(url, rpt_name)
    
        zip_name = rpt_name #壓縮文件名

        file_dir = 'C:/Users/a0985/OneDrive/Desktop/txf_rpt/2020unzip' #解壓後的文件放在該目錄下
        with zipfile.ZipFile(zip_name, 'r') as myzip:
            for file in myzip.namelist():
                myzip.extract(file,file_dir)

        delete_daily_zip_address =  os.path.abspath('.') + "\\" + rpt_name
        os.remove(delete_daily_zip_address)


def read_origin_data(time_format_in_origin_file_name) :
    global whether_data_is_null_flag
    fname = r'C:/Users/a0985/OneDrive/Desktop/txf_rpt/2020unzip/Daily_' + str(time_format_in_origin_file_name) + '.rpt'
    if os.path.isfile(fname) : #有此檔案
        origin_df = pd.read_csv(fname, encoding = 'big5') #rpt 的編碼方式真D特殊
        whether_data_is_null_flag = 0
    else:                      #查無此檔
        whether_data_is_null_flag = 1
        return;
    return origin_df;


def preprocess(origin_df, day_flag) :
    if ' 交易日期' in origin_df.columns: #把原始檔中不同column name統一成成交日期
        origin_df[' 成交日期'] = origin_df[' 交易日期']
        origin_df.drop(labels=[' 交易日期'], axis = 'columns', inplace = True)#delete多餘欄位
    
    ######只留下商品為mtx的rows#####
    #function
    mtx_origin_df = erase_redundant_space_and_value(origin_df) #消除column空白處及多餘value
    
    
    if day_flag == 1: #日盤
        #####篩選日盤交易逐筆資料(當天)並刪掉指定條件重複列#####
        k1 = (mtx_origin_df['成交時間'] >= 84500) & (mtx_origin_df['成交時間'] <= 134500)
        mtx_origin_df = mtx_origin_df[ k1 ] 
        date_tmp = str( mtx_origin_df['成交日期'].iloc[0] )
        date_tmp = parse(date_tmp)
        date_tmp_week = date_tmp.isoweekday() #此時是星期幾，做為最近到期判斷用
        
    elif day_flag == 0 : #前天下午盤(夜盤)
        #####篩選夜盤交易逐筆資料(前天下午三點至今日上午五點)並刪掉指定條件重複列#####
        k1 = (mtx_origin_df['成交時間'] >= 150000) & (mtx_origin_df['成交時間'] <= 240000)
        mtx_origin_df = mtx_origin_df[ k1 ] 
        date_tmp = str( mtx_origin_df['成交日期'].iloc[0] )
        date_tmp = parse(date_tmp)
        date_tmp_week = date_tmp.isoweekday() #此時是星期幾，做為最近到期判斷用
        
    else : #今日凌晨盤(夜盤)
        #####篩選夜盤交易逐筆資料(前天下午三點至今日上午五點)並刪掉指定條件重複列#####
        k1 = (mtx_origin_df['成交時間'] >= 0) & (mtx_origin_df['成交時間'] <= 50000)
        mtx_origin_df = mtx_origin_df[ k1 ]
        date_tmp = str( mtx_origin_df['成交日期'].iloc[0] )
        date_tmp = parse(date_tmp)
        date_tmp_week = date_tmp.isoweekday() #此時是星期幾，做為最近到期判斷用
        
    #####找到要抓的最近到期月份(周別)#####
    deadline_list = mtx_origin_df['到期月份(週別)'] #抓所有到期時間，要找之中離目前最近
    deadline_list.drop_duplicates(inplace = True)
    #function
    near_month = find_near_month(deadline_list, date_tmp_week, day_flag) #得到最近到期月份(周別)
    print(near_month)
    
    #####df篩選出符合最近到期月份(周別)#####
    mtx_origin_df = mtx_origin_df.reset_index(drop = True) #目錄重置
    mtx_df = mtx_origin_df[ mtx_origin_df['到期月份(週別)'] == near_month ] 
    mtx_df = mtx_df.reset_index(drop = True) #目錄重置
    #print(mtx_df)
    
    #####轉為int格式，消掉data多餘空白#####
    mtx_df['成交日期'] = mtx_df['成交日期'].astype(int)
    mtx_df['成交時間'] = mtx_df['成交時間'].astype(int)
    mtx_df['成交價格'] = mtx_df['成交價格'].astype(int)
    
    mtx_df.drop_duplicates(['成交日期','成交價格','成交時間'], 'first', inplace = True) #刪掉指定條件重複列
    
    #####用時間段('排序依據')排序data#####
    mtx_df['成交時間'] = mtx_df['成交時間'].astype(str).str.zfill(6) #成交時間皆在前面補0成6位數
    mtx_df['成交日期'] = mtx_df['成交日期'].astype(str)
    mtx_df['排序依據'] = mtx_df['成交日期'] + mtx_df['成交時間']
    
    mtx_df = mtx_df.sort_values(['排序依據'],ascending = True) #根據時間由遠到近sort
    mtx_df = mtx_df.reset_index(drop = True) #sort後目錄重置
    
    return mtx_df, date_tmp;


def get_mtx_df(mtx_df ,date_tmp, transaction_second_index):
    
    #####濾掉多餘欄位#####
    price_name = mtx_df[['成交日期','成交時間','成交價格']]
    

    #####建立時間逐筆資料#####
    #function
    price_name = get_transaction_second_df_to_price_name(price_name, transaction_second_index)
    #####得到成交價格欄位並補上成交日期#####
    
    time_format_in_origin_file_name = date_tmp.strftime('%Y%m%d') 
    price_name.reset_index(level=0, inplace=True) #成交時間賦歸欄位
    
    #####完整的當天周選小台價格df#####
    price_name = price_name[['成交日期','成交時間','成交價格']]  
    
    return price_name;


def process_by_time_gap(origin_df, transaction_second_index, day_flag):
    global night_df
    #####先整理好資料#####
    #function
    #第一個變數為原始檔資料,第二個為該天為星期幾,做為最近到期判斷用,第三個為當天日期
    mtx_df, date_tmp = preprocess(origin_df, day_flag )
    
    #####建立周選小台的df#####
    #function
    price_name = get_mtx_df(mtx_df, date_tmp, transaction_second_index)
    
    if day_flag == 0: #下午盤先加入夜盤df中
        night_df = price_name
    elif day_flag == 2: #凌晨盤跟下午盤合併成夜盤
        night_df = night_df.append(price_name)
        night_df = night_df.fillna(method = 'ffill') #有NaN之處補上前筆交易的成交價格
        
        #####整理成QM讀取CSV的格式#####
        #function
        get_import_form(night_df, '_week_mtx_back_price') #生成周選小台逐筆成交價格(夜盤)
        night_df.drop(night_df.index, inplace=True) 
        
    else: #單純日盤
        #####整理成QM讀取CSV的格式#####
        #function
        get_import_form(price_name, '_week_mtx_back_price')  #生成周選小台逐筆成交價格(日盤)
        

def get_import_form(price_name, strike_price_start_value): #整理成QM讀取CSV的格式
    
    #####輸出csv的名字依照規定命名#####
    Filename:str = str(strike_price_start_value)
    
    #####'Date', 'Time', 'Price','Volume'為QM的import格式#####
    price_name.rename(columns={'成交時間':'Time','成交日期':'Date','成交價格':'Price'}, inplace = True)
    price_name['Date'] = price_name['Date'].astype(str)
    price_name['Volume'] = 0    
    
    ###
    complete_strike_price_data = price_name[['Date', 'Time', 'Price', 'Volume']]
    complete_strike_price_data['Time'] = complete_strike_price_data['Time'].astype('int')
    complete_strike_price_data = complete_strike_price_data.dropna().reset_index(drop = True) #把有nan的列delete
    complete_strike_price_data = complete_strike_price_data.reset_index(drop=True) 
    #print(complete_strike_price_data)
    
    #####輸出周選小台價格成csv#####
    #function
    output_to_csv_by_strike_price(complete_strike_price_data, Filename) #各履約價的價格 + 價平和
    ###


def output_to_csv_by_strike_price(complete_strike_price_data, Filename) :
    
    #####設定輸出位置#####
    my_folder_path = "C:/Users/a0985/OneDrive/Desktop/期貨/資料/mtx_data" #到周選txf資料夾
    file_address = my_folder_path + "/"  + Filename + ".csv"
    
    if os.path.isfile(file_address) : 
        
        previous_data = pd.read_csv(file_address)
        #print(Filename, previous_data)
        if not previous_data.empty:
            previous_data = previous_data[ previous_data['Date'] != np.unique(previous_data['Date'])[0] ]
        previous_data = previous_data.append(complete_strike_price_data)
        previous_data = previous_data.reset_index(drop = True) #目錄重置
        previous_data.to_csv(file_address, index = False)
        
    else:
        complete_strike_price_data.to_csv(file_address, index = False)
        
    #print(Filename + '    ok')
    
    return; #end        
        
        
if __name__ == "__main__":
    #start_date = datetime.datetime(2020, 9, 1) #代表資料從何時開始
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
            rpt_name = 'Daily_' + str(time_format_in_origin_file_name)
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
            process_by_time_gap(origin_df, transaction_second_index, day_flag)

            #####今日凌晨盤(夜盤)時段逐秒list#####
            origin_df = read_origin_data(time_format_in_origin_file_name)
            transaction_second_index = list() #交易時段為0:00:00 ~ 5:00:00
            #function
            start_time = 0
            break_time = 50000
            transaction_second_index = get_transaction_second_index(transaction_second_index, start_time, break_time)
            day_flag = 2 #夜盤flag
            #####依照前天下午盤+當天凌晨盤,及當天日盤處理#####
            process_by_time_gap(origin_df, transaction_second_index, day_flag)
            
            #####日盤時段逐秒list#####
            origin_df = read_origin_data(time_format_in_origin_file_name)
            transaction_second_index = list() #交易時段為8:45:00 ~ 13:45:00
            #function
            start_time = 84499
            break_time = 134500
            transaction_second_index = get_transaction_second_index(transaction_second_index, start_time, break_time)
            day_flag = 1 #日盤flag
            #####依照前天下午盤+當天凌晨盤,及當天日盤處理#####
            process_by_time_gap(origin_df, transaction_second_index, day_flag)
        
        print("OK")
        start_date = start_date + datetime.timedelta(days=1)
        time_format_in_origin_file_name = start_date.strftime('%Y_%m_%d')
