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


def find_near_month(deadline_list):
    
    #print(deadline_list)
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
    
    #print(near_month)
    return near_month;


def get_transaction_second_df_to_price_name(price_name, transaction_second_index) : 
    price_name = price_name.fillna(0) #把NaN補成0

    price_name['call'] = price_name['call'].astype(float)
    price_name['put'] = price_name['put'].astype(float)
    price_name['成交時間'] = price_name['成交時間'].astype(int)
    
    aggregation_functions = {'call': 'sum', 'put': 'sum'} #依照相同時間段合併row
    
    #如此每一時間皆有put&call val
    price_name = price_name.groupby('成交時間', as_index=True).aggregate(aggregation_functions)
    price_name = price_name.replace(0.0,np.nan) #把沒成交價的地方改成NaN，方便後續處理
    price_name = price_name.reindex(transaction_second_index) #把缺失的index補足，084500~134500
    price_name = price_name.fillna(method = 'ffill') #有NaN之處補上前筆交易的成交價格
    
    ###補充1 : 若開盤一段時間內沒有交易資料則捨棄，直到有交易資料###
    ###補充2 : 如果前秒有值下秒沒有的話就要遵照前一秒的值###
    
    return price_name;






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


def preprocess(origin_df) :
    if ' 交易日期' in origin_df.columns: #把原始檔中不同column name統一成成交日期
        origin_df[' 成交日期'] = origin_df[' 交易日期']
        origin_df.drop(labels=[' 交易日期'], axis = 'columns', inplace = True)#delete多餘欄位
    
    ######只留下商品為TXO的rows#####
    #function
    txo_origin_df = erase_redundant_space_and_value(origin_df) #消除column空白處及多餘value
    
    #####找到要抓的最近到期月份(周別)#####
    deadline_list = txo_origin_df['到期月份(週別)'] #抓所有到期時間，要找之中離目前最近
    deadline_list.drop_duplicates(inplace = True)
    #function
    near_month = find_near_month(deadline_list) #得到最近到期月份(周別)
    
    #####df篩選出符合最近到期月份(周別)#####
    txo_origin_df = txo_origin_df.reset_index(drop = True) #目錄重置
    txo_df = txo_origin_df[ txo_origin_df['到期月份(週別)'] == near_month ] 
    txo_df = txo_df.reset_index(drop = True) #目錄重置
    
    #####轉為int格式，消掉data多餘空白#####
    txo_df['成交日期'] = txo_df['成交日期'].astype(int)
    txo_df['成交時間'] = txo_df['成交時間'].astype(int)
    txo_df['履約價格'] = txo_df['履約價格'].astype(int)
    
    #####篩選日盤交易逐筆資料並刪掉指定條件重複列#####
    txo_df = txo_df[ (txo_df['成交時間'] >= 84500) & (txo_df['成交時間'] <= 134500) ] 
    txo_df = txo_df.reset_index(drop = True) #目錄重置
    
    txo_df.drop_duplicates(['成交日期','履約價格','買賣權別','成交時間'], 'first', inplace = True) #刪掉指定條件重複列
    
    #####用時間段('排序依據')排序data#####
    txo_df['成交時間'] = txo_df['成交時間'].astype(str).str.zfill(6) #成交時間皆在前面補0成6位數
    txo_df['成交日期'] = txo_df['成交日期'].astype(str)
    txo_df['排序依據'] = txo_df['成交日期'] + txo_df['成交時間']
    
    txo_df = txo_df.sort_values(['排序依據'],ascending = True) #根據時間由遠到近sort
    txo_df = txo_df.reset_index(drop = True) #sort後目錄重置
    
    '''
    store = store[ store['履約價格'] == 11650 ]
    store = store.reset_index(drop = True) #sort後目錄重置
    store.to_csv('op1.csv', index = False)
    '''
    
    return txo_df;


def group_by_strike_price(txo_df ,time_format_in_origin_file_name, transaction_second_index):
    
    #####接著再使用履約價為最優先排序#####
    txo_df['履約價格'] = txo_df['履約價格'].astype(str)
    txo_df['排序依據'] = txo_df['履約價格'] + txo_df['排序依據']
    
    txo_df = txo_df.sort_values(['排序依據'],ascending = True) #根據時間由遠到近sort
    txo_df = txo_df.reset_index(drop = True) #sort後目錄重置
    
    price_flat_sum = pd.DataFrame() #處理尋找價平和的df
    
    price_flat_sum_df_exist_flag = 0
    #####依照不同履約價處理每日日盤逐筆交易#####
    for strike_price_start_value in np.unique(txo_df['履約價格'].astype(int)):
        price_name = txo_df[ txo_df['履約價格'] == str(strike_price_start_value) ] #依照不同履約價分類df
        price_name = price_name.reset_index(drop = True)
        
        price_name['買賣權別'] = price_name['買賣權別'].str.replace(' ','') #消除空白
        price_name = price_name.reset_index(drop=True) 
        
        #####多call & put欄位存放個別成交價格#####
        price_name['call'] = price_name['成交價格'][ price_name['買賣權別'] == 'C' ]
        price_name['put'] = price_name['成交價格'][ price_name['買賣權別'] == 'P' ]
        
        #####濾掉多餘欄位#####
        price_name = price_name[['成交時間','call','put']]  
        
        #####建立時間逐筆資料#####
        #function
        price_name = get_transaction_second_df_to_price_name(price_name, transaction_second_index)
        
        #####得到成交價格欄位並補上成交日期#####
        price_name['成交價格'] = price_name['call'] + price_name['put'] 
        
        time_format_in_origin_file_name = time_format_in_origin_file_name.replace('_','')
        price_name['成交日期'] = time_format_in_origin_file_name 
        price_name.reset_index(level=0, inplace=True) #成交時間賦歸欄位
        
        #####完整的當天個別履約價逐筆資料df#####
        price_name = price_name[['成交日期','成交時間','call','put']]
        
        #print(strike_price_start_value)
        #print(price_name)
        #print('################')
        
        if price_name.empty: #df為空則代表當天無此履約價資料 
            a = 1
        else:
            a = 1
            
            #####整理成QM讀取CSV的格式#####
            #function
            get_import_form(price_name, strike_price_start_value) #生成每個履約價的逐筆成交點數
        
        
        #####找到逐筆時間的價平和#####
        if price_flat_sum_df_exist_flag == 0:
            price_flat_sum['min_call'] = price_name['call']
            price_flat_sum['min_put'] = price_name['put']
            price_flat_sum['min_strike_price'] = str(strike_price_start_value) #此DF用來尋找價平和
            price_flat_sum_df_exist_flag = 1
        else :
            price_flat_sum['new_call'] = price_name['call']
            price_flat_sum['new_put'] = price_name['put']
            price_flat_sum['new_strike_price'] = str(strike_price_start_value) #此DF用來尋找價平和
            price_flat_sum = price_flat_sum.replace(np.nan,-1) #把沒成交價的地方改成NaN，方便後續處理
            
            condition = price_flat_sum['new_call'] + price_flat_sum['new_put'] < price_flat_sum['min_call'] + price_flat_sum['min_put']
            con1 = price_flat_sum['min_call'] == float(-1) 
            con2 = price_flat_sum['min_put'] == float(-1)
            con3 = price_flat_sum['new_call'] != float(-1)
            con4 = price_flat_sum['new_put'] != float(-1)
            k = (condition | con1 | con2) & con3 & con4
            price_flat_sum['min_strike_price'] = np.where(k, price_flat_sum['new_strike_price'], price_flat_sum['min_strike_price'] )
            price_flat_sum['min_call'] = np.where(k, price_flat_sum['new_call'], price_flat_sum['min_call'] )
            price_flat_sum['min_put'] = np.where(k, price_flat_sum['new_put'], price_flat_sum['min_put'] )
            price_flat_sum['k'] = k
            #min_call:價平之call min_put:價平之put min_strike_price:價平履約價
        #print(price_flat_sum)
        
        
        #price_flat_sum[str(strike_price_start_value)] = price_name['成交價格'] #此DF用來尋找價平和
    price_flat_sum.rename(columns={'min_call':'call','min_put':'put'}, inplace = True)
    price_flat_sum = price_flat_sum.reset_index(drop=True)
    #print(price_flat_sum)
    price_flat_sum['成交時間'] = price_name['Time'] #用'成交時間'會error,因為call了get_import_form後已經改變了欄位名字
    price_flat_sum['成交日期'] = price_name['Date']
    price_flat_sum = price_flat_sum[['min_strike_price','call','put', '成交日期', '成交時間']]
    #price_flat_sum.to_csv('_test_strike_price.csv', index = False)
    
    #function
    get_import_form(price_flat_sum, 'price_flat_sum')
    
    
    return txo_df;


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
    
    return;
    
    
def output_to_csv_by_strike_price(complete_strike_price_data, Filename) :
    
    #####設定輸出位置#####
    my_folder_path = "C:/Users/a0985/OneDrive/Desktop/期貨/資料/op_data_add" #到價平和資料夾
    file_address = my_folder_path + "/" + "_op_price_" + Filename + "_add.csv"
    
    if os.path.isfile(file_address) : 
        
        previous_data = pd.read_csv(file_address)
        #print(Filename, previous_data)
        if not previous_data.empty:
            previous_data = previous_data[ previous_data['Date'] != np.unique(previous_data['Date'])[0] ]
        previous_data = previous_data.append(complete_strike_price_data)
        previous_data = previous_data.reset_index(drop = True) #目錄重置
        #print(previous_data)
        previous_data.to_csv(file_address, index = False)
        
    else:
        #a = 1
        complete_strike_price_data.to_csv(file_address, index = False)
        
    #print(Filename + 'ok')
    
    return; #end



if __name__ == "__main__":
    n = 1
    #start_date = datetime.datetime(2020, 7, 5) #代表資料從何時開始
    start_date = datetime.date.today() #代表資料從何時開始
    start_date = start_date - datetime.timedelta(days=1)
    end_date = datetime.date.today()
    end_date = end_date.strftime('%Y_%m_%d')
    
    transaction_second_index = list() #交易時段為8:45:00 ~ 13:45:00
    
    #function
    transaction_second_index = get_transaction_second_index(transaction_second_index)
    
    time_format_in_origin_file_name = start_date.strftime('%Y_%m_%d') #原始檔名稱用YY_MM_DD表示  
    
    while time_format_in_origin_file_name != end_date : #匯入資料直到指定停止日期
        start_date = start_date + datetime.timedelta(days=n)
        time_format_in_origin_file_name = start_date.strftime('%Y_%m_%d')
        
        #讀取原始資料l
        origin_df = read_origin_data(time_format_in_origin_file_name)
        print(time_format_in_origin_file_name,whether_data_is_null_flag)
        
        if(whether_data_is_null_flag == 0):
            
            #先整理好資料
            txo_df = preprocess(origin_df);

            #開始依照履約價格分類，依照日期順序
            st = group_by_strike_price(txo_df, time_format_in_origin_file_name, transaction_second_index);
            #print(st)
