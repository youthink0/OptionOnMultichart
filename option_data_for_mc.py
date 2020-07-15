import pandas as pd

def op_in():
    fname = 'C:/Users/a0985/OneDrive/桌面/op_rpt/2020unzip/OptionsDaily_2020_07_10.rpt'
    store = pd.read_csv(fname, encoding = 'big5')
    return store;

def preprocess(store):
    store.rename(columns={' 成交日期':'成交日期','          商品代號':'商品代號','        履約價格':'履約價格'}, inplace = True)
    store.rename(columns={'                                                      到期月份(週別)':'到期月份(週別)'}, inplace=True)
    store.rename(columns={'        買賣權別':'買賣權別','      成交時間':'成交時間','          成交價格':'成交價格'}, inplace=True)
    store.rename(columns={'         成交數量(B or S)':'成交數量(B or S)','     開盤集合競價 ':'開盤集合競價'}, inplace=True)


    store.drop_duplicates(inplace=True) #刪除重複列
    store = store.dropna().reset_index(drop=True) #把有nan的列delete
    store['商品代號'] = store['商品代號'].str.replace(' ','') #消除空白
    store = store[ store['商品代號'] == 'TXO' ] #篩選TXO出來

    store['成交日期'] = store['成交日期'].astype(int)#轉為int格式，消掉多餘空白
    store['成交時間'] = store['成交時間'].astype(int)
    store['履約價格'] = store['履約價格'].astype(int)
    store.drop(labels=['成交數量(B or S)','開盤集合競價'], axis = 'columns', inplace = True)#delete多餘欄位


    store['成交時間'] = store['成交時間'].astype(str).str.zfill(6) #成交時間皆在前面補0成6位
    store['成交日期'] = store['成交日期'].astype(str)
    store['排序依據'] = store['成交日期'] + store['成交時間']
    store = store.sort_values(['排序依據'],ascending = True) #根據時間由遠到近sort
    store = store.reset_index(drop = True) #sort後目錄重置
    store.drop_duplicates(['成交日期','履約價格','買賣權別','成交時間'], 'first', inplace = True)
    return store;


    #store = store.groupby(['成交日期','履約價格','成交時間'], as_index = True).sum()
    #store.head(10000)

def op_out(store):
    store.to_csv('op.csv', index = 0, encoding='UTF-8_sig')
    print('ok')

if __name__ == "__main__":
    #讀取原始資料
    st = op_in();
    
    #先整理好資料
    st = preprocess(st);
    
    #輸出到csv
    op_out(st);