# OptionOnMultichart
>整理並匯出選擇權周選及月選(看最近到期)的每日逐筆call&amp;put資料的csv檔，以及價平履約價csv檔，供Multichart讀取  

#邏輯講解:  
1.每日的call&put資訊由來為抓取逐筆資料中, 有符合最近到期日期者  
	eg. 若到期月份(周別)有 2020012, 202008W2, 202008, 則最近到期為202008W2  

2.碰到禮拜三同時開倉的狀況 :  
	日盤 : 抓該日結算資料  
	夜盤 : 抓新開倉資料  
	eg. 某禮拜三W1結算,W2開倉,則該日日盤抓W1資料,夜盤則抓W2資料  
	  
	若到期月份(周別)中無W,也就是無周選的情況下,則抓近月選  
  
	若某禮拜三碰到月選結算,W4新開倉的狀況,則同樣比照禮拜三狀況 :  
		該日日盤 : 抓月選資料  
		該日夜盤 : 抓W4資料  
		  
3.原始檔方面 :  
	每個原始檔的的日盤及上午0點整至上午5點為該天日期,下午則為前天日期  
		eg. 08_05.rpt的原始檔里, 150000~240000為8/4的資訊,其餘則為8/5當天的資訊  
