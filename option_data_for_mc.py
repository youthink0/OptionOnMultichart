import pandas as pd
import datetime
import os

def op_in():
    fname = 'C:/Users/a0985/OneDrive/Desktop/op_rpt/2020unzip/OptionsDaily_2020_07_10.rpt'
    store = pd.read_csv(fname, encoding = 'big5')
    return store;

def preprocess(store):
    store.rename(columns={' 成交日期':'成交日期','          商品代號':'商品代號','        履約價格':'履約價格'}, inplace = True)
    store.rename(columns={'                                                      到期月份(週別)':'到期月份(週別)'}, inplace=True)
    store.rename(columns={'        買賣權別':'買賣權別','      成交時間':'成交時間','          成交價格':'成交價格'}, inplace=True)
    store.rename(columns={'         成交數量(B or S)':'成交數量(B or S)','     開盤集合競價 ':'開盤集合競價'}, inplace=True)

    store.drop_duplicates(inplace=True) #刪除重複列
    store = store.dropna().reset_index(drop=True) #把有nan的列delete
    store.drop(labels=['開盤集合競價'], axis = 'columns', inplace = True)#delete多餘欄位
    store['商品代號'] = store['商品代號'].str.replace(' ','') #消除空白
    store = store[ store['商品代號'] == 'TXO' ] #篩選TXO出來

    store['成交日期'] = store['成交日期'].astype(int)#轉為int格式，消掉多餘空白
    store['成交時間'] = store['成交時間'].astype(int)
    store['履約價格'] = store['履約價格'].astype(int)
    
    
    store['成交時間'] = store['成交時間'].astype(str).str.zfill(6) #成交時間皆在前面補0成6位
    store['成交日期'] = store['成交日期'].astype(str)
    store['排序依據'] = store['成交日期'] + store['成交時間']
    
    store.drop_duplicates(['成交日期','履約價格','買賣權別','成交時間'], 'first', inplace = True) #刪掉重複列
    store = store.sort_values(['排序依據'],ascending = True) #根據時間由遠到近sort
    store = store.reset_index(drop = True) #sort後目錄重置
   
    '''
    a = store['成交日期'] == "20200709"
    b = store['履約價格'] == "10000"
    c = store['成交時間'] == "152339"
    
    print(store[ (c) ])
    '''
    
    return store;

def group(store):
    store['履約價格'] = store['履約價格'].astype(str)
    store['排序依據'] = store['履約價格'] + store['排序依據']
    
    store = store.sort_values(['排序依據'],ascending = True) #根據時間由遠到近sort
    store = store.reset_index(drop = True) #sort後目錄重置
    #print(store['排序依據'])
    
    store = store.groupby(['排序依據']).成交價格.sum() #履約價格,成交日期，成交時間相同者為同組
    store = store.reset_index() #把排序依據回復成column
    #print(store.dtypes)
    store['成交時間'] = store['排序依據'].str[-6:-1] + store['排序依據'].str[-1]
    store['成交日期'] = store['排序依據'].str[-14:-6]
    store['成交日期'] = store['成交日期'].astype(int)
    store['履約價'] = store['排序依據'].str[0:-14] #分離各項值
    print(store)
    
    range_start = 6000
    range_end = 16000
    gap = 50
    store1 = store
    while range_start <= range_end:
        #price_name = "df_" + str(range_start)
        price_name = store[ store['履約價'] == str(range_start) ]
        #print(price_name)
        
        if price_name.empty: #df為空    
            a = 1
        else:
            price_name.drop(labels=['履約價'], axis = 'columns', inplace = True)#delete多餘欄位
            
            import_form(price_name, range_start)
        
        range_start = range_start + gap
        store = store1
    
    return store;
    
def import_form(pri, range_start): #整理成QM讀取CSV的格式
    Foldername:str = 'op_data'
    Filename:str = str(range_start)
    if not os.path.exists("{Foldername}".format(Foldername=Foldername)):
        os.makedirs("{Foldername}".format(Foldername=Foldername))
    pri.rename(columns={'成交時間':'Time','成交日期':'Date'}, inplace = True)
    pri['Open'] = pri['Low'] = 0
    pri['Volume'] = 87
    pri['Price'] = pri['High'] = pri['Close'] = pri['成交價格'].astype('float64')
    pri = pri[['Date', 'Time', 'Price','Volume']]
    
    op_out(pri, Foldername, Filename)
    return;
    
def op_out(pri, Foldername, Filename): #輸出到csv
    pri['Time'] = pri['Time'].astype('int')
    #print(pri.dtypes)
    pri = pri.dropna().reset_index(drop=True) #把有nan的列delete
    pri.to_csv(Foldername + "/" + Filename + ".csv", index = False)
    print(Filename + 'ok')
    return;
    
if __name__ == "__main__":
    #讀取原始資料
    st = op_in();
    
    #先整理好資料
    st = preprocess(st);

    #開始依照履約價格分類，依照日期順序
    st = group(st);
    #print(st)
    
    '''
    #整理成QM讀取CSV的格式
    st = import_form(st);
    
    #輸出到csv
    op_out(st);
    '''