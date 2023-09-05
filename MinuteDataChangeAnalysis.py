from ctypes.wintypes import VARIANT_BOOL
import pandas as pd
import pyodbc    
import db_credentials

server =  db_credentials.server 
database = 'Binance'
username = db_credentials.username 
password = db_credentials.password
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

sql = """
    SELECT 
        CONVERT(VARCHAR(8), DateTime, 108) as 'Time',
        Sum(num_trades) '20220505_num_of_trades',
        Sum(C) '20220505_C'
    from 
        [OneMinute]
    where
			Pair = 'BTCUSDT'
        AND DateTime > '2022-05-05' AND DateTime < '2022-05-06'
    Group by 
        CONVERT(VARCHAR(8), DateTime, 108)
    order by 
        CONVERT(VARCHAR(8), DateTime, 108)
"""


df = pd.read_sql_query(sql, conn)

#set Time Column as index 
df = df.set_index('Time')
print(df)

df['20220505_1_min_TradeCount%Change'] = df['20220505_num_of_trades'].pct_change(periods=1)*100
df['20220505_2_min_TradeCount%Change'] = df['20220505_num_of_trades'].pct_change(periods=2)*100
df['20220505_3_min_TradeCount%Change'] = df['20220505_num_of_trades'].pct_change(periods=3)*100
df['20220505_4_min_TradeCount%Change'] = df['20220505_num_of_trades'].pct_change(periods=4)*100
df['20220505_5_min_TradeCount%Change'] = df['20220505_num_of_trades'].pct_change(periods=5)*100

df['20220505_1_min_Close%Change'] = df['20220505_C'].pct_change(periods=1)*100
df['20220505_2_min_Close%Change'] = df['20220505_C'].pct_change(periods=2)*100
df['20220505_3_min_Close%Change'] = df['20220505_C'].pct_change(periods=3)*100
df['20220505_4_min_Close%Change'] = df['20220505_C'].pct_change(periods=4)*100
df['20220505_5_min_Close%Change'] = df['20220505_C'].pct_change(periods=5)*100



#Rolling Expanding min ve Max kullan
#https://towardsdatascience.com/every-pandas-function-you-can-should-use-to-manipulate-time-series-711cb0c5c749
"""
df["20220505_running_min_C"] = df["20220505_C"].expanding().min()  # same as cummin()
df["20220505_running_max_C"] = df["20220505_C"].expanding().max()
"""
print(df)
print(type(df))
#select another day

sql = """
    SELECT 
        CONVERT(VARCHAR(8), DateTime, 108) as 'Time',
        Sum(num_trades) '20220506_num_of_trades',
        Sum(C) '20220506_C'
    from 
        [OneMinute]
    where
			Pair = 'BTCUSDT'
        AND DateTime > '2022-05-06' AND DateTime < '2022-05-07'
    Group by 
        CONVERT(VARCHAR(8), DateTime, 108)
    order by 
        CONVERT(VARCHAR(8), DateTime, 108)
"""

df1 = pd.read_sql_query(sql, conn)

#set Time Column as index 
df1 = df1.set_index('Time')

df1['20220506_1_min_TradeCount%Change'] = df1['20220506_num_of_trades'].pct_change(periods=1)*100
df1['20220506_2_min_TradeCount%Change'] = df1['20220506_num_of_trades'].pct_change(periods=2)*100
df1['20220506_3_min_TradeCount%Change'] = df1['20220506_num_of_trades'].pct_change(periods=3)*100
df1['20220506_4_min_TradeCount%Change'] = df1['20220506_num_of_trades'].pct_change(periods=4)*100
df1['20220506_5_min_TradeCount%Change'] = df1['20220506_num_of_trades'].pct_change(periods=5)*100

df1['20220506_1_min_Close%Change'] = df1['20220506_C'].pct_change(periods=1)*100
df1['20220506_2_min_Close%Change'] = df1['20220506_C'].pct_change(periods=2)*100
df1['20220506_3_min_Close%Change'] = df1['20220506_C'].pct_change(periods=3)*100
df1['20220506_4_min_Close%Change'] = df1['20220506_C'].pct_change(periods=4)*100
df1['20220506_5_min_Close%Change'] = df1['20220506_C'].pct_change(periods=5)*100

#merge two datasets by index
mergedDf = df.merge(df1, left_index=True, right_index=True)



print(mergedDf)
print(type(mergedDf))
mergedDf.to_csv('C:\\MyProjects2\\CC\\Binance_Api\\merged_expanding5.csv')

"""
DELETE DUPLICATES SQL DE YANLIŞ DATA VAR 
Select * from OneMinute WHERE Pair = 'BTCUSDT' AND DateTime = '2022-05-06 13:04:00' 
"""