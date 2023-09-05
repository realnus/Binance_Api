import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

#NusLib Import
import sys
sys.path.insert(1, 'C:\\MyProjects2\\PythonNusLib\\NusLib')       
import NusLibGeneric

Symbol = ""
print("arg count", str(len(sys.argv)))

if(len(sys.argv) == 2):
    Symbol = str(sys.argv[1])
    print("asdasd",Symbol)
else:
    Symbol = 'APEUSDT'
    #Symbol = 'SANTOSUSDT'
    print("bbbb",Symbol)


#SaveLog True olur ise tum zaman ve priceları generate eder bir log csv olarak manual kontrol etmek için.
SaveLog = False 

StartDateTime = datetime(2022, 4, 2, 15, 00)
EndDateTime = datetime(2022, 4, 2, 15, 5)
"""
StartDateTime = datetime(2022, 4, 1, 00, 00)
EndDateTime = datetime(2022, 5, 1, 00, 00)
"""
df_Log= pd.DataFrame(columns=['CurrentDateTime', 'DateTime_24H_Ago'])
csv_ReadPath = "C:\\MyProjects2\\CC\\Binance_Api\\Db_BulkJobs\\"
csv_SavePath = "C:\\MyProjects2\\CC\\Binance_Api\\Db_BulkJobs\\"

import numpy as np
import pandas as pd

import dask.dataframe as dd
import dask.array as da
import dask.bag as db


#df_main = pd.read_csv(csv_ReadPath + Symbol +"-aggTrades-2022-04.csv",  sep=',')
df_main = dd.read_csv(csv_ReadPath + Symbol +"-aggTrades-2022-04.csv",  sep=',')
df_main.columns = ['TradeId', 'Price', 'Quantity', 'FirstTradeId', 'LastTradeId', 'TradeTime','IsTheBuyerTheMarketMaker', 'Ignore']

print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " df_main Column Conversions..." )
#Add TradeCount to the Dataframe by using (LastTradeId - FirstTradeId)
df_main["TradeCount"] = df_main["LastTradeId"] -  df_main["FirstTradeId"]

#drop FirstTradeId , LastTradeId and Ignore columns
#df_main.drop(['LastTradeId','FirstTradeId', 'Ignore'], inplace=True, axis=1)
df_main = df_main.drop(['LastTradeId','FirstTradeId', 'Ignore'], axis=1)
print(df_main.columns)

#Convert TradeTime Unix to HumanReadible yyyy-MM-dd HH:mm:ss.fff ( unit='ms' 6 digit milisecond cikariyor,datetime64[ms] ile 3'e indiriyoruz. )
df_main['TradeDateTime'] = dd.to_datetime(df_main['TradeTime'], unit='ms').astype('datetime64[ms]')

small = df_main.get_partition(0)

#drop TradeTime (Unix)
df_main = df_main.drop(['TradeTime'], axis=1)

#Convert to 1 and 0 ( 1 for True, 0 for False)
#df_main['IsTheBuyerTheMarketMaker_conv'] = np.where(df_main['IsTheBuyerTheMarketMaker'] == 'TRUE', 1, 0)

###Burda series'e donusuyor. buna bir bak nasıl convert etmeliyiz.
#df_main = df_main['IsTheBuyerTheMarketMaker'].replace('TRUE',1)
#df_main = df_main['IsTheBuyerTheMarketMaker'].replace('FALSE',0)


#Drop IsTheBuyerTheMarketMaker column
#df_main = df_main.drop(['IsTheBuyerTheMarketMaker'], axis=1)

#Rename IsTheBuyerTheMarketMaker_conv to IsTheBuyerTheMarketMaker
#df_main.rename(columns = {'IsTheBuyerTheMarketMaker_conv':'IsTheBuyerTheMarketMaker'}, inplace = True)


TimeDifference_1s = timedelta(seconds = 1)
TimeDifference_2s = timedelta(seconds = 2)
TimeDifference_3s = timedelta(seconds = 3)
TimeDifference_5s = timedelta(seconds = 5)
TimeDifference_10s = timedelta(seconds = 10)
TimeDifference_30s = timedelta(seconds = 30)
TimeDifference_1m = timedelta(seconds = 60)
TimeDifference_5m = timedelta(seconds = 60 * 5)
TimeDifference_15m = timedelta(seconds = 60 * 15)
TimeDifference_30m = timedelta(seconds = 60 * 30)
TimeDifference_1H = timedelta(seconds = 60 * 60)
TimeDifference_4H = timedelta(seconds = 60 * 60 * 4)
TimeDifference_12H = timedelta(seconds = 60 * 60 * 12)
TimeDifference_24H = timedelta(seconds = 60 * 60 * 24)
TimeDifference_48H = timedelta(seconds = 60 * 60 * 48) #bunu sub dataframe icin kullaniyoruz.

CurrentDateTime = StartDateTime
LoopCount = 0
df_ResultsToSave = pd.DataFrame()

#df = df_main

#date1 = (CurrentDateTime - TimeDifference_24H).strftime("%Y-%m-%d %H:%M:%S")
#date2 = (CurrentDateTime + TimeDifference_1H).strftime("%Y-%m-%d %H:%M:%S")

date1 = (CurrentDateTime - TimeDifference_24H)
date2 = (CurrentDateTime + TimeDifference_1H)

#df_main = df_main.to_dataframe()

a =             df_main.loc[(df_main['TradeDateTime'] > date1) & (df_main['TradeDateTime']  < date2)].compute()
#jan_first_day = jan_2021[(jan_2021["date"] >= "2021-01-01 00:00:00") & (jan_2021["date"] <= "2021-01-01 23:59:59")]

#a = df_main.loc[(df_main['TradeTime'] >= CurrentDateTime - TimeDifference_24H) & (df_main['TradeTime']  <= CurrentDateTime + TimeDifference_1H)].compute()
print(a)
print(a)
"""
mask = (df_main['TradeDateTime'] >= CurrentDateTime - TimeDifference_24H) & (df_main['TradeDateTime'] <= CurrentDateTime + TimeDifference_1H)
df = df_main.loc[mask]
print(df.shape)
print(df_main.shape)
"""

#EndDate = max(df['TradeDateTime'])
while(CurrentDateTime < EndDateTime):
    
    CurrentDateTime += timedelta(seconds = 1)
    DateTime_24H_Ago = CurrentDateTime - TimeDifference_24H

    DateTime_1s_Ago = CurrentDateTime - TimeDifference_1s
    DateTime_2s_Ago = CurrentDateTime - TimeDifference_2s
    DateTime_3s_Ago = CurrentDateTime - TimeDifference_3s
    DateTime_5s_Ago = CurrentDateTime - TimeDifference_5s
    DateTime_10s_Ago = CurrentDateTime - TimeDifference_10s
    DateTime_30s_Ago = CurrentDateTime - TimeDifference_30s
    DateTime_1m_Ago = CurrentDateTime - TimeDifference_1m
    DateTime_5m_Ago = CurrentDateTime - TimeDifference_5m
    DateTime_15m_Ago = CurrentDateTime - TimeDifference_15m
    DateTime_30m_Ago = CurrentDateTime - TimeDifference_30m
    DateTime_1H_Ago = CurrentDateTime - TimeDifference_1H
    DateTime_4H_Ago = CurrentDateTime - TimeDifference_4H
    DateTime_12H_Ago = CurrentDateTime - TimeDifference_12H
    DateTime_24H_Ago = CurrentDateTime - TimeDifference_24H


    Current_Price = 0

    _1s_Price = 0
    _2s_Price = 0
    _3s_Price = 0
    _5s_Price = 0
    _10s_Price = 0
    _30s_Price = 0
    _1m_Price = 0
    _5m_Price = 0
    _15m_Price = 0
    _30m_Price = 0
    _1H_Price = 0
    _4H_Price = 0
    _12H_Price = 0
    _24H_Price = 0

    _1s_PercentChange = -999
    _2s_PercentChange = -999
    _3s_PercentChange = -999
    _5s_PercentChange = -999
    _10s_PercentChange = -999
    _30s_PercentChange = -999
    _1m_PercentChange = -999
    _5m_PercentChange = -999
    _15m_PercentChange = -999
    _30m_PercentChange = -999
    _1H_PercentChange = -999
    _4H_PercentChange = -999
    _12H_PercentChange = -999
    _24H_PercentChange = -999

    _1s_Price = 0
    _2s_Price = 0
    _3s_Price = 0
    _5s_Price = 0
    _10s_Price = 0
    _30s_Price = 0
    _1m_Price = 0
    _5m_Price = 0
    _15m_Price = 0
    _30m_Price = 0
    _1H_Price = 0
    _4H_Price = 0
    _12H_Price = 0
    _24H_Price = 0


    #Set Edelim
    df_CurrentPrice = df_main.loc[df_main['TradeDateTime'] <= CurrentDateTime].tail(1)

    df_1s_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_1s_Ago].tail(1)
    df_1s_Price.rename(columns = {'TradeDateTime':'df_1s_TradeDateTime'}, inplace = True)

    df_2s_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_2s_Ago].tail(1)
    df_2s_Price.rename(columns = {'TradeDateTime':'df_2s_TradeDateTime'}, inplace = True)

    df_3s_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_3s_Ago].tail(1)
    df_3s_Price.rename(columns = {'TradeDateTime':'df_3s_TradeDateTime'}, inplace = True)

    df_5s_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_5s_Ago].tail(1)
    df_5s_Price.rename(columns = {'TradeDateTime':'df_5s_TradeDateTime'}, inplace = True)

    df_10s_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_10s_Ago].tail(1)
    df_10s_Price.rename(columns = {'TradeDateTime':'df_10s_TradeDateTime'}, inplace = True)

    df_30s_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_30s_Ago].tail(1)
    df_30s_Price.rename(columns = {'TradeDateTime':'df_30s_TradeDateTime'}, inplace = True)

    df_1m_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_1m_Ago].tail(1)
    df_1m_Price.rename(columns = {'TradeDateTime':'df_1m_TradeDateTime'}, inplace = True)

    df_5m_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_5m_Ago].tail(1)
    df_5m_Price.rename(columns = {'TradeDateTime':'df_5m_TradeDateTime'}, inplace = True)

    df_15m_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_15m_Ago].tail(1)
    df_15m_Price.rename(columns = {'TradeDateTime':'df_15m_TradeDateTime'}, inplace = True)

    df_30m_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_30m_Ago].tail(1)
    df_30m_Price.rename(columns = {'TradeDateTime':'df_30m_TradeDateTime'}, inplace = True)

    df_1H_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_1H_Ago].tail(1)
    df_1H_Price.rename(columns = {'TradeDateTime':'df_1H_TradeDateTime'}, inplace = True)

    df_4H_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_4H_Ago].tail(1)
    df_4H_Price.rename(columns = {'TradeDateTime':'df_4H_TradeDateTime'}, inplace = True)

    df_12H_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_12H_Ago].tail(1)
    df_12H_Price.rename(columns = {'TradeDateTime':'df_12H_TradeDateTime'}, inplace = True)

    df_24H_Price =    df_main.loc[df_main['TradeDateTime'] <= DateTime_24H_Ago].tail(1)
    df_24H_Price.rename(columns = {'TradeDateTime':'df_24H_TradeDateTime'}, inplace = True)



    #Price Gathering Section
    if(df_CurrentPrice.shape[0] == 0):
        Current_Price = -999
    else:    
        Current_Price = float(df_CurrentPrice['Price'])
        
        if(df_1s_Price.shape[0] != 0):
            _1s_Price = float(df_1s_Price['Price'])
        if(df_2s_Price.shape[0] != 0):
            _2s_Price = float(df_2s_Price['Price'])
        if(df_3s_Price.shape[0] != 0):
            _3s_Price = float(df_3s_Price['Price'])
        if(df_5s_Price.shape[0] != 0):
            _5s_Price = float(df_5s_Price['Price'])
        if(df_10s_Price.shape[0] != 0):
            _10s_Price = float(df_10s_Price['Price'])
        if(df_30s_Price.shape[0] != 0):
            _30s_Price = float(df_30s_Price['Price'])
        if(df_1m_Price.shape[0] != 0):
            _1m_Price = float(df_1m_Price['Price'])
        if(df_5m_Price.shape[0] != 0):
            _5m_Price = float(df_5m_Price['Price'])
        if(df_15m_Price.shape[0] != 0):
            _15m_Price = float(df_15m_Price['Price'])
        if(df_30m_Price.shape[0] != 0):
            _30m_Price = float(df_30m_Price['Price'])     
        if(df_1H_Price.shape[0] != 0):
            _1H_Price = float(df_1H_Price['Price'])                 
        if(df_4H_Price.shape[0] != 0):
            _4H_Price = float(df_4H_Price['Price'])     
        if(df_12H_Price.shape[0] != 0):
            _12H_Price = float(df_12H_Price['Price'])     
        if(df_24H_Price.shape[0] != 0):
            _24H_Price = float(df_24H_Price['Price'])         

    
    #Percent Calculation Section
    if(Current_Price == -999):
        #Hicbisey hesaplanamaz
        Current_Price == -999
    else:
        if(_1s_Price != -999):
            _1s_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_1s_Price),2)
        if(_2s_Price != -999):
            _2s_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_2s_Price),2)
        if(_3s_Price != -999):
            _3s_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_3s_Price),2)
        if(_5s_Price != -999):
            _5s_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_5s_Price),2)
        if(_10s_Price != -999):
            _10s_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_10s_Price),2)
        if(_30s_Price != -999):
            _30s_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_30s_Price),2)                                                
        if(_1m_Price != -999):
            _1m_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_1m_Price),2)
        if(_5m_Price != -999):
            _5m_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_5m_Price),2)
        if(_15m_Price != -999):
            _15m_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_15m_Price),2)
        if(_30m_Price != -999):
            _30m_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_30m_Price),2)  
        if(_1H_Price != -999):
            _1H_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_1H_Price),2)  
        if(_4H_Price != -999):
            _4H_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_4H_Price),2)  
        if(_12H_Price != -999):
            _12H_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_12H_Price),2)  
        if(_24H_Price != -999):
            _24H_PercentChange = round(NusLibGeneric.PercentChange(Current_Price,_24H_Price),2)

    if(SaveLog == True):
    #Insert to df_LOg 
        df_Log = pd.DataFrame({
                                "Symbol":[Symbol],
                                "CurrentDateTime":[CurrentDateTime],
                                "DateTime_1s_Ago":[DateTime_1s_Ago],
                                "DateTime_2s_Ago":[DateTime_2s_Ago],
                                "DateTime_3s_Ago":[DateTime_3s_Ago],
                                "DateTime_5s_Ago":[DateTime_5s_Ago],
                                "DateTime_10s_Ago":[DateTime_10s_Ago],
                                "DateTime_30s_Ago":[DateTime_30s_Ago],
                                "DateTime_1m_Ago":[DateTime_1m_Ago],
                                "DateTime_5m_Ago":[DateTime_5m_Ago],
                                "DateTime_15m_Ago":[DateTime_15m_Ago],
                                "DateTime_30m_Ago":[DateTime_30m_Ago],
                                "DateTime_1H_Ago":[DateTime_1H_Ago],
                                "DateTime_4H_Ago":[DateTime_4H_Ago],
                                "DateTime_12H_Ago":[DateTime_12H_Ago],
                                "DateTime_24H_Ago":[DateTime_24H_Ago]
                            })

    df_PercentChange = pd.DataFrame({
                            "Symbol":[Symbol],
                            "CurrentDateTime":[CurrentDateTime],
                            "_1s_PercentChange":[_1s_PercentChange],
                            "_2s_PercentChange":[_2s_PercentChange],
                            "_3s_PercentChange":[_3s_PercentChange],
                            "_5s_PercentChange":[_5s_PercentChange],
                            "_10s_PercentChange":[_10s_PercentChange],
                            "_30s_PercentChange":[_30s_PercentChange],
                            "_1m_PercentChange":[_1m_PercentChange],
                            "_5m_PercentChange":[_5m_PercentChange],
                            "_15m_PercentChange":[_15m_PercentChange],
                            "_30m_PercentChange":[_30m_PercentChange],
                            "_1H_PercentChange":[_1H_PercentChange],
                            "_4H_PercentChange":[_4H_PercentChange],
                            "_12H_PercentChange":[_12H_PercentChange],
                            "_24H_PercentChange":[_24H_PercentChange]
                        })


    if(SaveLog == True):
    #Insert to df_LOg   
        #Tum Df leri tek satır olarak kaydetmek istediğimiz için indexlerini resetleyelim, concat indexler ile birlestiremye calismasin
        df_CurrentPrice.reset_index(drop = True, inplace=True)
        df_1s_Price.reset_index(drop = True, inplace=True)
        df_2s_Price.reset_index(drop = True, inplace=True)
        df_3s_Price.reset_index(drop = True, inplace=True)
        df_5s_Price.reset_index(drop = True, inplace=True)
        df_10s_Price.reset_index(drop = True, inplace=True)
        df_30s_Price.reset_index(drop = True, inplace=True)
        df_1m_Price.reset_index(drop = True, inplace=True)
        df_5m_Price.reset_index(drop = True, inplace=True)
        df_15m_Price.reset_index(drop = True, inplace=True)
        df_30m_Price.reset_index(drop = True, inplace=True)
        df_1H_Price.reset_index(drop = True, inplace=True)
        df_4H_Price.reset_index(drop = True, inplace=True)
        df_12H_Price.reset_index(drop = True, inplace=True)
        df_24H_Price.reset_index(drop = True, inplace=True)

        df_Log = pd.concat([df_Log,df_CurrentPrice, df_PercentChange, df_1s_Price, df_2s_Price, df_3s_Price, df_5s_Price, df_10s_Price, df_30s_Price, df_1m_Price, df_5m_Price, df_15m_Price, df_30m_Price, df_1H_Price, df_4H_Price, df_12H_Price, df_24H_Price], axis=1)
    

    ApplyHeader = False
    if(LoopCount == 0):
        ApplyHeader = True
        df_PercentChangeToSave = df_PercentChange
        df_LogToSave = df_Log
    else:
        if(SaveLog == True):
            #Insert to df_LOg
            df_LogToSave = df_LogToSave.append(df_Log)
        
        df_PercentChangeToSave = df_PercentChangeToSave.append(df_PercentChange)



    print(CurrentDateTime,str(_24H_PercentChange))
    #print("a")
    LoopCount +=1


df_PercentChangeToSave.to_csv(csv_SavePath + Symbol + '_PercentChange.csv', mode='a', header= True, index=False)

if(SaveLog == True):
    #Insert to df_LOg
    df_LogToSave.to_csv(csv_SavePath + Symbol + '_PercentChange_Logs.csv', mode='a', header= True, index=False)


"""
df['Price'].loc[df['TradeDateTime'] <= CurrentDate].sort_values(by=['TradeDateTime'])

df.sort_values('TradeDateTime', ascending=False)


df.sort_values('score',ascending = False)

"""