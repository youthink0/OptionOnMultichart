import pandas as pd
import datetime
import os
import numpy as np

whether_data_is_null_flag = 0 #若查無原始檔則掛起1


def get_transaction_second_index(transaction_second_index) : 
    transaction_second = 84499
    for i in range(0,100000):
        transaction_second = transaction_second + 1
        tmp_sec = transaction_second % 100
        tmp_min = transaction_second / 100
        tmp_min = tmp_min % 100
        if transaction_second == 86000 or transaction_second == 96000 :
            continue;
        if transaction_second == 106000 or transaction_second == 116000 or transaction_second == 126000:
            continue;
            
        if (tmp_sec > 59) or (tmp_min > 60):
            continue;
        else:
            transaction_second_index .append(transaction_second)
        
        if transaction_second == 134500:
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


def find_near_month(deadline_list):
    deadline_list = deadline_list.values.tolist()
    tmp = list()
    for i in deadline_list: 
        if "/" not in i:
            tmp.append(i)
    
    deadline_list = tmp
    week_flag = 0 #判斷週選則掛起1
    for i in deadline_list: #判斷要抓周選還月選
        if 'W' in str(i) :
            week_flag = 1
            break;
            
    if week_flag == 0 : #月選直接抓最近月，第二個禮拜三
        near_month = min(deadline_list)
        near_month = float(near_month) #不寫這個會報錯
        #near_month = int(near_month)
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


def preprocess(origin_df) :
    if ' 交易日期' in origin_df.columns: #把原始檔中不同column name統一成成交日期
        origin_df[' 成交日期'] = origin_df[' 交易日期']
        origin_df.drop(labels=[' 交易日期'], axis = 'columns', inplace = True)#delete多餘欄位
    
    ######只留下商品為mtx的rows#####
    #function
    mtx_origin_df = erase_redundant_space_and_value(origin_df) #消除column空白處及多餘value
    
    #####找到要抓的最近到期月份(周別)#####
    deadline_list = mtx_origin_df['到期月份(週別)'] #抓所有到期時間，要找之中離目前最近
    deadline_list.drop_duplicates(inplace = True)
    #function
    near_month = find_near_month(deadline_list) #得到最近到期月份(周別)
    
    
    #####df篩選出符合最近到期月份(周別)#####
    mtx_origin_df = mtx_origin_df.reset_index(drop = True) #目錄重置
    mtx_df = mtx_origin_df[ mtx_origin_df['到期月份(週別)'] == near_month ] 
    mtx_df = mtx_df.reset_index(drop = True) #目錄重置
    
    #####轉為int格式，消掉data多餘空白#####
    mtx_df['成交日期'] = mtx_df['成交日期'].astype(int)
    mtx_df['成交時間'] = mtx_df['成交時間'].astype(int)
    mtx_df['成交價格'] = mtx_df['成交價格'].astype(int)
    
    #####篩選日盤交易逐筆資料並刪掉指定條件重複列#####
    mtx_df = mtx_df[ (mtx_df['成交時間'] >= 84500) & (mtx_df['成交時間'] <= 134500) ] 
    mtx_df = mtx_df.reset_index(drop = True) #目錄重置
    
    mtx_df.drop_duplicates(['成交日期','成交價格','成交時間'], 'first', inplace = True) #刪掉指定條件重複列
    
    #####用時間段('排序依據')排序data#####
    mtx_df['成交時間'] = mtx_df['成交時間'].astype(str).str.zfill(6) #成交時間皆在前面補0成6位數
    mtx_df['成交日期'] = mtx_df['成交日期'].astype(str)
    mtx_df['排序依據'] = mtx_df['成交日期'] + mtx_df['成交時間']
    
    mtx_df = mtx_df.sort_values(['排序依據'],ascending = True) #根據時間由遠到近sort
    mtx_df = mtx_df.reset_index(drop = True) #sort後目錄重置
    
    return mtx_df;


def group_by_strike_price(mtx_df ,time_format_in_origin_file_name, transaction_second_index):
    
    #####濾掉多餘欄位#####
    price_name = mtx_df[['成交日期','成交時間','成交價格']]
    

    #####建立時間逐筆資料#####
    #function
    price_name = get_transaction_second_df_to_price_name(price_name, transaction_second_index)
    #####得到成交價格欄位並補上成交日期#####

    time_format_in_origin_file_name = time_format_in_origin_file_name.replace('_','') 
    price_name.reset_index(level=0, inplace=True) #成交時間賦歸欄位
    
    #####完整的當天周選小台價格df#####
    price_name = price_name[['成交日期','成交時間','成交價格']]


    if price_name.empty: #df為空則代表當天無此履約價資料 
        a = 1
    else:
        #####整理成QM讀取CSV的格式#####
        #function
        get_import_form(price_name, 'week_mtx_price') #生成周選小台逐筆成交點數
    
    
    return mtx_df;


def get_import_form(price_name, strike_price_start_value): #整理成QM讀取CSV的格式
    
    #####輸出csv的名字依照規定命名#####
    Filename:str = str(strike_price_start_value)
    
    #####'Date', 'Time', 'Price','Volume'為QM的import格式#####
    price_name.rename(columns={'成交時間':'Time','成交日期':'Date','成交價格':'Price'}, inplace = True)
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
    
    return;


def output_to_csv_by_strike_price(complete_strike_price_data, Filename) :
    
    #####設定輸出位置#####
    my_folder_path = "C:/Users/a0985/OneDrive/Desktop/期貨/資料/week_mtx_data" #到周選mtx資料夾
    file_address = my_folder_path + "/"  + Filename + ".csv"
    
    if os.path.isfile(file_address) : 
        
        previous_data = pd.read_csv(file_address)
        #print(Filename, previous_data)
        #if not previous_data.empty:
        #    previous_data = previous_data[ previous_data['Date'] != np.unique(previous_data['Date'])[0] ]
        previous_data = previous_data.append(complete_strike_price_data)
        previous_data = previous_data.reset_index(drop = True) #目錄重置
        previous_data.to_csv(file_address, index = False)
        
    else:
        complete_strike_price_data.to_csv(file_address, index = False)
        
    #print(Filename + '    ok')
    
    return; #end

if __name__ == "__main__":
    n = 1
    start_date = datetime.datetime(2020, 8, 1) #代表資料從何時開始
    #start_date = datetime.date.today() #代表資料從何時開始
    #start_date = start_date - datetime.timedelta(days=1)
    end_date = datetime.date.today()
    end_date = end_date.strftime('%Y_%m_%d')
    
    transaction_second_index = list() #交易時段為8:45:00 ~ 13:45:00
    
    #function
    transaction_second_index = get_transaction_second_index(transaction_second_index)
    
    time_format_in_origin_file_name = start_date.strftime('%Y_%m_%d') #原始檔名稱用YY_MM_DD表示  
    
    while time_format_in_origin_file_name != end_date : #匯入資料直到指定停止日期
        start_date = start_date + datetime.timedelta(days=n)
        
        #讀取原始資料l
        origin_df = read_origin_data(time_format_in_origin_file_name)
        
        print(time_format_in_origin_file_name,whether_data_is_null_flag)
        
        if(whether_data_is_null_flag == 0):
            
            #先整理好資料
            mtx_df = preprocess(origin_df);

            #開始依照履約價格分類，依照日期順序
            st = group_by_strike_price(mtx_df, time_format_in_origin_file_name, transaction_second_index);
            print('ok')
            #print(st)
        time_format_in_origin_file_name = start_date.strftime('%Y_%m_%d')