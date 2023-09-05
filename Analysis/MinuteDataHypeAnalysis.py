from ctypes.wintypes import VARIANT_BOOL
import pandas as pd
import pyodbc    
import db_credentials

server =  db_credentials.server 
database = 'Binance'
username = db_credentials.username 
password = db_credentials.password
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

mergedDf = 0


def CalculateClosePricePercentChange():
#Loop Through Pairs and get Their Close Values to calculate Percent Diff
#  
    for index, row in df_Pairs.iterrows():
        symbol = row["Pair"]
        print(symbol)
        # Get the date data of the Pair

        sql = """
            SELECT 
                CONVERT(VARCHAR(8), DateTime, 108) as 'Time',
                --Sum(num_trades) '20220505_num_of_trades',
                Sum(C) '"""+symbol+"""_20220506_C'
            from 
                [OneMinute]
            where
                    Pair = '""" + symbol + """'
                and DateTime > '2022-05-06' AND DateTime < '2022-05-07'
            Group by 
                CONVERT(VARCHAR(8), DateTime, 108)
            order by 
                CONVERT(VARCHAR(8), DateTime, 108)
        """


        df = pd.read_sql_query(sql, conn)

        #set Time Column as index 
        df = df.set_index('Time')
        #print(df)

        ColumnName_ClosePrice = symbol + "_20220506_C"

        df[ColumnName_ClosePrice + '_1_min_Close%Change'] = df[ColumnName_ClosePrice].pct_change(periods=1)*100

        #Drop Close Price Column , şimdilik bunu istemiyorum
        df = df.drop(ColumnName_ClosePrice, 1)

        if(index==0):
            #ilk loop ise merge edeceğimiz birşey yok, mergedDf'e ilk symboly set ederiz
            mergedDf = df
        else:
            #merge two datasets by index
            mergedDf.join(df)
            #mergedDf = mergedDf.merge(df, left_index=True, right_index=True)



def GetNumberOfTrades():
#Loop Through Pairs and get Their Close Values to calculate Percent Diff
#  
    for index, row in df_Pairs.iterrows():
        symbol = row["Pair"]
        print(symbol)
        # Get the date data of the Pair

        sql = """
            SELECT 
                CONVERT(VARCHAR(8), DateTime, 108) as 'Time',
                Sum(num_trades) '20220506_num_of_trades'
            from 
                [OneMinute]
            where
                    Pair = '""" + symbol + """'
                and DateTime > '2022-05-06' AND DateTime < '2022-05-07'
            Group by 
                CONVERT(VARCHAR(8), DateTime, 108)
            order by 
                CONVERT(VARCHAR(8), DateTime, 108)
        """

        df = pd.read_sql_query(sql, conn)

        #set Time Column as index 
        df = df.set_index('Time')
        #print(df)

        ColumnName_NumberOfTrades = symbol + "_20220506_TradeCount"
        df.rename(columns = {'20220506_num_of_trades':ColumnName_NumberOfTrades}, inplace = True)

        if(index==0):
            #ilk loop ise merge edeceğimiz birşey yok, mergedDf'e ilk symboly set ederiz
            mergedDf = df
        else:
            #merge two datasets by index
            mergedDf = mergedDf.join(df)
            #mergedDf = mergedDf.merge(df, left_index=True, right_index=True)

#Select Pairs for specific date

sql = """
select 'BTCUSDT'  as Pair
UNION ALL
 Select 
        Distinct(Pair)   as Pair
        from         
            [OneMinute]
        where
		DateTime > '2022-05-06' AND DateTime < '2022-05-07'
        AND Pair != 'BTCUSDT'
		Order By Pair Asc
    """


df_Pairs = pd.read_sql_query(sql, conn)

"""
##Close Price Percent Change
CalculateClosePricePercentChange()
mergedDf.to_csv('C:\\MyProjects2\\CC\\Binance_Api\\merged_All.csv')
"""

##Trade Counts
GetNumberOfTrades()
mergedDf.to_csv('C:\\MyProjects2\\CC\\Binance_Api\\merged_All_NumberOfTrades.csv')
