import os
import enum
import urllib
import zipfile
import datetime
import requests
import numpy as np
import pandas as pd
import urllib.request
from dateutil.parser import parse

min_strike_price = 0
night_df = pd.DataFrame()


class trading_hours(enum.IntEnum):
    am = 0
    pm = 1
    midnight = 2


def get_transaction_second_index(transaction_second_index, start_time, break_time):
    transaction_second = start_time
    transaction_second_index = list()
    for i in range(0, 200000):
        transaction_second = transaction_second + 1
        tmp_sec = transaction_second % 100  # 紀錄秒用
        tmp_min = transaction_second / 100  # 紀錄分用
        tmp_min = tmp_min % 100

        if (tmp_sec > 59) or (tmp_min >= 60):
            continue
        else:
            transaction_second_index.append(transaction_second)
        if transaction_second == break_time:
            break

    return transaction_second_index


def erase_redundant_space_and_value(origin_df):
    # 消除空白
    origin_df = origin_df.rename(columns=lambda x: x.strip())
    origin_df = origin_df.applymap(
        lambda x: x.strip() if isinstance(x, str) else x)
    origin_df.drop(labels=['開盤集合競價'], axis='columns',
                   inplace=True)  # delete多餘欄位
    origin_df = origin_df[origin_df['商品代號'] == 'TXO']  # 篩選TXO出來
    origin_df = origin_df.reset_index(drop=True)  # 目錄重置
    return origin_df


def get_transaction_second_df_to_price_name(price_name, transaction_second_index):
    price_name = price_name.fillna(0)  # 把NaN補成0
    price_name['call'] = price_name['call'].astype(float)
    price_name['put'] = price_name['put'].astype(float)
    price_name['成交時間'] = price_name['成交時間'].astype(int)
    aggregation_functions = {'call': 'last', 'put': 'last',
                             'call_Volume': 'sum', 'put_Volume': 'sum'}  # 依照相同時間段合併row

    #如此每一時間皆有put&call val
    price_name = price_name.groupby('成交時間').aggregate(aggregation_functions)
    price_name[['call', 'put']] = price_name[['call', 'put']
                                             ].replace(0.0, np.nan)  # 把沒成交價的地方改成NaN，方便後續處理
    price_name = price_name.reindex(transaction_second_index)  # 把缺失的index補足
    price_name[['call', 'put']] = price_name[['call', 'put']].fillna(
        method='ffill')  # 有NaN之處補上前筆交易的成交價格
    price_name[['call', 'put']] = price_name[['call', 'put']].fillna(method='bfill')
    price_name[['call_Volume', 'put_Volume']] = price_name[['call_Volume', 'put_Volume']].fillna(0)  # 有NaN之處補上0
    ###補充1 : 若開盤一段時間內沒有交易資料則捨棄，直到有交易資料###
    ###補充2 : 如果前秒有值下秒沒有的話就要遵照前一秒的值###

    return price_name


def find_near_month(deadline_list, weekofday, day_flag):

    week_flag = False  # 判斷週選則為True
    for i in deadline_list:  # 判斷要抓周選還月選
        if 'W4' in str(i) and weekofday == 3 and day_flag == trading_hours.am:  # 第三周的禮拜三日盤要看月選
            week_flag = False
            break
        if 'W' in str(i):  # 有周選擇抓週選
            week_flag = True
            break

    if week_flag is False:  # 月選直接抓最近月
        deadline_in_month = list()
        for i in deadline_list:  # 因第三周的禮拜三日盤要看月選,所以這裡主要是要把該天的W4濾掉
            if 'W' not in str(i):
                deadline_in_month.append(i)
        deadline_in_month = [int(x) for x in deadline_in_month]
        near_month = min(deadline_in_month)
        near_month = str(near_month)

    else:  # 平常只會有一倉，週三會有兩個，要抓最近
        # 篩選有W的項出來

        deadline_list = deadline_list.loc[deadline_list.str.contains(
            "W", na=False)]
        deadline_in_week = list()

        for i in deadline_list.str.split("W"):
            a = i[0] + i[1]
            deadline_in_week.append(int(a))

        near = min(deadline_in_week)
        near = str(near)
        near_month = near[0:-1] + "W" + near[-1]

    print(f"Nearest contract: {near_month}")
    return near_month


def get_today_rpt(rpt_name):
    print("downloading with urllib")
    url = 'https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownload/' + rpt_name + '.zip'
    html = requests.head(url)  # 用head方法去請求資源頭
    re = html.status_code
    if re != 200:
        return
    else:
        urllib.request.urlretrieve(url, rpt_name)

    zip_name = rpt_name  # 壓縮文件名

    file_dir = os.getcwd() + "/origin_data"  # 解壓後的文件放在該目錄下
    with zipfile.ZipFile(zip_name, 'r') as myzip:
        for file in myzip.namelist():
            myzip.extract(file, file_dir)

    delete_daily_zip_address = os.path.abspath('.') + "/" + rpt_name
    os.remove(delete_daily_zip_address)


def read_origin_data(time_format_in_origin_file_name):
    rpt_has_found = False
    current_path = os.getcwd()
    fname = current_path + '/origin_data/OptionsDaily_' + \
        str(time_format_in_origin_file_name) + '.rpt'
    if os.path.isfile(fname):  # 有此檔案
        origin_df = pd.read_csv(fname, encoding='big5')
        rpt_has_found = True
    else:  # 查無此檔
        origin_df = 0
        rpt_has_found = False
    return origin_df, rpt_has_found


def preprocess(origin_df, day_flag):
    ######只留下商品為TXO的rows#####
    #function
    txo_origin_df = erase_redundant_space_and_value(
        origin_df)  # 消除column空白處及多餘value
    txo_origin_df = txo_origin_df.rename(columns={'交易日期': '成交日期'})
    if day_flag == trading_hours.am:  # 日盤
        #####篩選日盤交易逐筆資料(當天)並刪掉指定條件重複列#####
        k1 = (txo_origin_df['成交時間'] >= 84500) & (
            txo_origin_df['成交時間'] <= 134500)

    elif day_flag == trading_hours.pm:  # 前天下午盤(夜盤)
        #####篩選夜盤交易逐筆資料(前天下午三點至今日上午五點)並刪掉指定條件重複列#####
        k1 = (txo_origin_df['成交時間'] >= 150000) & (
            txo_origin_df['成交時間'] <= 240000)

    else:  # 今日凌晨盤(夜盤)
        #####篩選夜盤交易逐筆資料(前天下午三點至今日上午五點)並刪掉指定條件重複列#####
        k1 = (txo_origin_df['成交時間'] >= 0) & (txo_origin_df['成交時間'] <= 50000)

    txo_origin_df = txo_origin_df[k1]
    if txo_origin_df.empty:
        return pd.DataFrame(), 0, 0  # dataframe無值
    date_tmp = str(txo_origin_df['成交日期'].iloc[0])
    date_tmp = parse(date_tmp)
    date_tmp_week = date_tmp.isoweekday()  # 此時是星期幾，做為最近到期判斷用

    #####找到要抓的最近到期月份(周別)#####
    deadline_list = txo_origin_df['到期月份(週別)']  # 抓所有到期時間，要找之中離目前最近
    deadline_list.drop_duplicates(inplace=True)
    #function
    near_month = find_near_month(
        deadline_list, date_tmp_week, day_flag)  # 得到最近到期月份(周別)
    near_month = near_month.strip()
    #####df篩選出符合最近到期月份(周別)#####
    txo_origin_df = txo_origin_df.reset_index(drop=True)  # 目錄重置
    txo_origin_df['到期月份(週別)'] = txo_origin_df['到期月份(週別)'].astype(str).apply(lambda x: x.split(".")[0])
    txo_df = txo_origin_df[txo_origin_df['到期月份(週別)'] == near_month]
    txo_df = txo_df.reset_index(drop=True)  # 目錄重置
    #####轉為int格式，消掉data多餘空白#####
    txo_df['成交日期'] = txo_df['成交日期'].astype(int)
    txo_df['成交時間'] = txo_df['成交時間'].astype(int)
    txo_df['履約價格'] = txo_df['履約價格'].astype(int)
    txo_df['成交數量(B or S)'] = txo_df['成交數量(B or S)'].astype(int)

    #####用時間段('排序依據')排序data#####
    txo_df['成交時間'] = txo_df['成交時間'].astype(str).str.zfill(6)  # 成交時間皆在前面補0成6位數
    txo_df['成交日期'] = txo_df['成交日期'].astype(str)
    txo_df['排序依據'] = txo_df['成交日期'] + txo_df['成交時間']

    txo_df = txo_df.sort_values(['排序依據'], ascending=True)  # 根據時間由遠到近sort
    txo_df = txo_df.reset_index(drop=True)  # sort後目錄重置

    #####接著再使用履約價為最優先排序#####
    txo_df['履約價格'] = txo_df['履約價格'].astype(str)
    txo_df['排序依據'] = txo_df['履約價格'] + txo_df['排序依據']
    txo_df = txo_df.sort_values(['排序依據'], ascending=True)  # 根據時間由遠到近sort
    txo_df = txo_df.reset_index(drop=True)  # sort後目錄重置
    return txo_df, date_tmp, near_month


def group_by_strike_price(txo_df, date_tmp, transaction_second_index):
    #日盤如果84500遍歷後沒找到價平,則會重跑loop,此flag為避免重複call function
    #夜盤則為1345400收盤時
    min_flat_sum = 9999
    for strike_price_start_value in np.unique(txo_df['履約價格'].astype(int)):
        price_name = process_to_import_form(
            txo_df, strike_price_start_value, transaction_second_index, date_tmp)
        price_name = price_name.fillna(method='bfill')
        tmp_call = price_name.loc[0, ['call']].values[0]
        tmp_put = price_name.loc[0, ['put']].values[0]
        tmp_flat_sum = tmp_call + tmp_put
        if tmp_flat_sum < min_flat_sum:
            min_flat_sum = tmp_flat_sum
            min_strike_price = str(strike_price_start_value)
    return min_strike_price


def find_price_flat_sum(txo_df, min_strike_price, transaction_second_index, date_tmp):
    price_flat_sum = pd.DataFrame()  # 處理尋找價平和的df
    #####把價平履約價的格式準備好#####
    #function
    price_flat_sum = process_to_import_form(
        txo_df, min_strike_price, transaction_second_index, date_tmp)

    #完整的當天個別履約價逐筆資料df
    price_flat_sum = price_flat_sum[[
        '成交日期', '成交時間', 'call', 'put', 'call_Volume', 'put_Volume']]
    price_flat_sum['min_strike_price'] = str(min_strike_price)
    price_flat_sum = price_flat_sum.reset_index(drop=True)

    price_flat_sum = price_flat_sum[[
        'min_strike_price', 'call', 'put', 'call_Volume', 'put_Volume', '成交日期', '成交時間']]
    return price_flat_sum


def process_to_import_form(txo_df, tmp_strike_price, transaction_second_index, date_tmp):
    price_name = txo_df[txo_df['履約價格'] == str(tmp_strike_price)]  # 依照不同履約價分類df
    price_name = price_name.reset_index(drop=True)

    price_name['買賣權別'] = price_name['買賣權別'].str.replace(' ', '')  # 消除空白
    price_name = price_name.reset_index(drop=True)

    #多call & put欄位存放個別成交價格
    price_name['call'] = price_name['成交價格'][price_name['買賣權別'] == 'C']
    price_name['put'] = price_name['成交價格'][price_name['買賣權別'] == 'P']
    price_name["call_Volume"] = price_name['成交數量(B or S)'][price_name['買賣權別'] == 'C']
    price_name["put_Volume"] = price_name['成交數量(B or S)'][price_name['買賣權別'] == 'P']
    #濾掉多餘欄位
    price_name = price_name[['成交時間', 'call',
                             'put', 'call_Volume', 'put_Volume']]

    #建立時間逐筆資料
    #function
    price_name = get_transaction_second_df_to_price_name(
        price_name, transaction_second_index)

    #得到成交價格欄位並補上成交日期
    time_format_in_origin_file_name = date_tmp.strftime('%Y%m%d')
    price_name['成交日期'] = time_format_in_origin_file_name
    price_name.reset_index(level=0, inplace=True)  # 成交時間賦歸欄位
    return price_name


# 將tick的data轉換成分k
# 並找出對應的OHLC跟成交量
def tick_to_min(df):
    df['Time'] = df['Time'].apply(lambda x: x // 100)
    row_iter = df.iterrows()
    first_idx, first = next(row_iter)
    O = H = L = C = first['Price']
    V = first['Volume']
    for idx, row in row_iter:
        if row['Time'] == first['Time']:
            if row['Price'] > H:
                H = row['Price']
            if row['Price'] < L:
                L = row['Price']
            C = row['Price']
            V += row['Volume']
        else:
            df.loc[first_idx, 'O'] = O
            df.loc[first_idx, 'H'] = H
            df.loc[first_idx, 'L'] = L
            df.loc[first_idx, 'C'] = C
            df.loc[first_idx, 'Volume'] = V // 2
            first = row
            first_idx = idx
            O = H = L = C = first['Price']
            V = first['Volume']
    # Last row
    df.loc[first_idx, 'O'] = O
    df.loc[first_idx, 'H'] = H
    df.loc[first_idx, 'L'] = L
    df.loc[first_idx, 'C'] = C
    df.loc[first_idx, 'Volume'] = V // 2

    df.drop_duplicates(subset=['Time'], inplace=True)
    df['Time'] = df['Time'].apply(lambda x: x * 100)
    df['Time'] = df['Time'].shift(periods=-1)
    df = df[:-1]
    df['Time'] = df['Time'].astype(np.int64)

    df = df.drop(['Price'], axis=1)
    df = df[['Date', 'Time', 'O', 'H', 'L', 'C', 'Volume']]
    return df


def get_import_form(price_name, strike_price_start_value):  # 整理成QM讀取CSV的格式

    #####輸出csv的名字依照履約價命名#####
    Filename: str = str(strike_price_start_value)
    #####'Date', 'Time', 'Price','Volume'為QM的import格式#####
    price_name.rename(columns={'成交時間': 'Time', '成交日期': 'Date'}, inplace=True)
    if 'min_strike_price' in price_name.columns:  # 若此df為記錄價平和，則volume存放當筆之價平和履約價
        price_name['Price'] = price_name['min_strike_price']
    else:
        price_name['Price'] = int(strike_price_start_value)
    #####各個履約價自己的call & put#####
    price_name_call = pd.DataFrame()  # 處理尋找價平和的df
    price_name_call['Price'] = price_name['call']
    price_name_call['Date'] = price_name['Date']
    price_name_call['Time'] = price_name['Time']
    price_name_call['Volume'] = price_name['call_Volume']
    complete_strike_price_call_data = price_name_call[[
        'Date', 'Time', 'Price', 'Volume']]
    complete_strike_price_call_data['Time'] = complete_strike_price_call_data['Time'].astype(
        'int')
    complete_strike_price_call_data = complete_strike_price_call_data.dropna(
    ).reset_index(drop=True)  # 把有nan的列delete
    complete_strike_price_call_data = complete_strike_price_call_data.reset_index(
        drop=True)  # 成交時間賦歸欄位

    complete_strike_price_call_data = tick_to_min(
        complete_strike_price_call_data)
    #####依照履約價個別輸出成csv#####
    #function
    output_to_csv_by_strike_price(
        complete_strike_price_call_data, Filename + '_call')  # 各履約價的tick call

    ###
    price_name_put = pd.DataFrame()  # 處理尋找價平和的df
    price_name_put['Price'] = price_name['put']
    price_name_put['Date'] = price_name['Date']
    price_name_put['Time'] = price_name['Time']
    price_name_put['Volume'] = price_name['put_Volume']
    complete_strike_price_put_data = price_name_put[[
        'Date', 'Time', 'Price', 'Volume']]
    complete_strike_price_put_data['Time'] = complete_strike_price_put_data['Time'].astype(
        'int')
    complete_strike_price_put_data = complete_strike_price_put_data.dropna(
    ).reset_index(drop=True)  # 把有nan的列delete
    complete_strike_price_put_data = complete_strike_price_put_data.reset_index(
        drop=True)  # 成交時間賦歸欄位

    complete_strike_price_put_data = tick_to_min(
        complete_strike_price_put_data)
    #####依照履約價個別輸出成csv#####
    #function
    output_to_csv_by_strike_price(
        complete_strike_price_put_data, Filename + '_put')  # 各履約價的tick call

    ###
    complete_strike_price_data = price_name[['Date', 'Time', 'Price']]
    complete_strike_price_data['Volume'] = 0
    complete_strike_price_data['Time'] = complete_strike_price_data['Time'].astype(
        'int')
    complete_strike_price_data = complete_strike_price_data.dropna(
    ).reset_index(drop=True)  # 把有nan的列delete
    complete_strike_price_data = complete_strike_price_data.reset_index(
        drop=True)  # 成交時間賦歸欄位

    #####依照履約價個別輸出成csv#####
    #function
    complete_strike_price_data = tick_to_min(complete_strike_price_data)
    output_to_csv_by_strike_price(
        complete_strike_price_data, Filename)  # 各履約價的價格 + 價平和
    ###


def output_to_csv_by_strike_price(complete_strike_price_data, Filename):
    print(Filename)
    #####設定輸出位置#####
    current_path = os.getcwd()
    my_folder_path = current_path + "/" + "output_data"
    if not os.path.exists(my_folder_path):
        os.makedirs(my_folder_path)
    file_address = my_folder_path + "/" + "_option_back_" + Filename + ".csv"
    file_address = "temp.csv"
    if os.path.isfile(file_address):
        previous_data = pd.read_csv(file_address)
        # if not previous_data.empty: #更新今天資訊後，把最早那天的資訊刪除
        #     previous_data = previous_data[ previous_data['Date'] != np.unique(previous_data['Date'])[0]]
        previous_data = previous_data.append(complete_strike_price_data)
        previous_data = previous_data.reset_index(drop=True)  # 目錄重置
        previous_data.to_csv(file_address, index=False)

    else:
        complete_strike_price_data.to_csv(file_address, index=False)

    return  # end


def process_by_time_gap(origin_df, transaction_second_index, day_flag, i):
    global min_strike_price
    global night_df
    #####先整理好資料#####
    #function
    #第一個變數為原始檔資料,第三個為當天日期
    txo_df, date_tmp, near_month = preprocess(origin_df, day_flag)
    if txo_df.empty is True:
        return
    #####判斷是周/月選的履約價
    unit = 0
    if 'W' in str(near_month):
        unit = 50
    else:
        unit = 100

    if day_flag == trading_hours.am or day_flag == trading_hours.pm:
        #####開始依照履約價格分類，依照日期順序,且找出日盤時開盤及夜盤開盤的價平履約價#####
        min_strike_price = group_by_strike_price(
            txo_df, date_tmp, transaction_second_index)

    #####找完價平履約價後,接著就生成該履約價的df#####
    #function
    tmp_strike_price = int(int(min_strike_price) + i * unit)
    price_name = find_price_flat_sum(
        txo_df, tmp_strike_price, transaction_second_index, date_tmp)
    if price_name.empty is True:
        return
    if day_flag == trading_hours.pm:  # 下午盤先加入夜盤df中
        night_df = price_name
    elif day_flag == trading_hours.midnight:  # 凌晨盤跟下午盤合併成夜盤
        night_df = night_df.append(price_name)
        night_df[['call', 'put']] = night_df[['call', 'put']].fillna(
            method='ffill')  # 有NaN之處補上前筆交易的成交價格
        #####整理成QM讀取CSV的格式#####
        #function
        get_import_form(night_df, f"{i}")
        night_df.drop(night_df.index, inplace=True)
    else:  # 單純日盤
        #####整理成QM讀取CSV的格式#####
        #function
        get_import_form(price_name, f"{i}")


if __name__ == "__main__":
    start_date = datetime.datetime(2017, 3, 2)  # 代表資料從何時開始
    # start_date = datetime.date.today()  # 代表資料從何時開始
    # start_date = start_date - datetime.timedelta(days=1)
    # end_date = datetime.date.today()
    end_date = start_date + datetime.timedelta(days=1)
    end_date = end_date.strftime('%Y_%m_%d')

    time_format_in_origin_file_name = start_date.strftime(
        '%Y_%m_%d')  # 原始檔名稱用YY_MM_DD表示
    while time_format_in_origin_file_name != end_date:  # 匯入資料直到指定停止日期
        #####上期交所抓原始zip檔並解壓到特定資料夾#####
        #function
        if start_date.isoweekday() == 6 or start_date.isoweekday() == 7:
            start_date = start_date + datetime.timedelta(days=1)
            time_format_in_origin_file_name = start_date.strftime('%Y_%m_%d')
            continue
        else:
            rpt_name = 'OptionsDaily_' + str(time_format_in_origin_file_name)

            origin_df, rpt_has_found = read_origin_data(
                time_format_in_origin_file_name)

            if rpt_has_found is False:
                get_today_rpt(rpt_name)
                origin_df, has_found = read_origin_data(
                    time_format_in_origin_file_name)
                if has_found is False:
                    start_date = start_date + datetime.timedelta(days=1)
                    time_format_in_origin_file_name = start_date.strftime(
                        '%Y_%m_%d')
                    continue
        #####讀取原始資料#####
        #function
        print(f"Date: {time_format_in_origin_file_name}. Status: {rpt_has_found}")
        for i in range(6):  # 價平到價外五檔
            #####前日下午盤(夜盤)時段逐秒list#####
            night_df = pd.DataFrame()
            transaction_second_index = list()  # 交易時段為15:00:00 ~ 23:59:59
            #function
            start_time = 149999
            break_time = 235959
            transaction_second_index = get_transaction_second_index(
                transaction_second_index, start_time, break_time)
            #####依照昨天下午盤+當天凌晨盤,及當天日盤處理#####
            process_by_time_gap(
                origin_df, transaction_second_index, trading_hours.pm, i)

            #####今日凌晨盤(夜盤)時段逐秒list#####
            transaction_second_index = list()  # 交易時段為0:00:00 ~ 5:00:00
            #function
            start_time = -1
            break_time = 50000
            transaction_second_index = get_transaction_second_index(
                transaction_second_index, start_time, break_time)
            #####依照昨天下午盤+當天凌晨盤,及當天日盤處理#####
            process_by_time_gap(
                origin_df, transaction_second_index, trading_hours.midnight, i)

            #####日盤時段逐秒list#####
            transaction_second_index = list()  # 交易時段為8:45:00 ~ 13:45:00
            #function
            start_time = 84499
            break_time = 134500
            transaction_second_index = get_transaction_second_index(
                transaction_second_index, start_time, break_time)
            #####依照昨天下午盤+當天凌晨盤,及當天日盤處理#####
            process_by_time_gap(
                origin_df, transaction_second_index, trading_hours.am, i)

        print("Processing data complete")
        start_date = start_date + datetime.timedelta(days=1)
        time_format_in_origin_file_name = start_date.strftime('%Y_%m_%d')
