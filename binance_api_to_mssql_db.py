import requests
import json
import pandas as pd
import datetime as dt
import pyodbc
import time
import binance_functions
import binance_api_keys
import db_credentials

binance_api_key = binance_api_keys.binance_api_key
binance_api_secret = binance_api_keys.binance_api_secret

url = 'https://api.binance.com/api/v3/klines'
symbol = 'BTCUSDT'
interval = '1m'


## Loop All symbols to get their values
df_symbols = binance_functions.Binance_getSymbols(binance_api_key,binance_api_secret)

for index, row in df_symbols.iterrows():
    symbol = row["symbol"]
    status = row["status"]
    baseAsset = row["baseAsset"]
    quoteAsset = row["quoteAsset"]

    if(status == "TRADING"):

        start = str(int(dt.datetime(2022,5,1).timestamp()*1000))
        end = str(int(dt.datetime(2022,5,7).timestamp()*1000))

        while(start < end):
            print(symbol)
            par = {'symbol': symbol, 'interval': interval, 'startTime': start, 'endTime': end}
            data = pd.DataFrame(json.loads(requests.get(url, params= par).text))
            #format columns name
            data.columns = ['datetimeUnix', 'O', 'H', 'L', 'C', 'V','close_time', 'qav', 'num_trades','taker_base_vol', 'taker_quote_vol', 'ignore']
            data.index = [dt.datetime.fromtimestamp(x/1000.0) for x in data.datetimeUnix]
            #data['close_time_Human'] = [dt.datetime.fromtimestamp(x/1000.0) for x in data['close_time']]
            data=data.astype(float)
            # Create DateTime Column from Index Column
            data['DateTime'] = data.index

            #

            # server = 'myserver,port' # to specify an alternate port
            server = '.' 
            database = 'Binance' 
            username = 'sa' 
            password = 'utkancikasd123!' 
            cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
            cursor = cnxn.cursor()

            # Insert Dataframe into SQL Server:
            for index, row in data.iterrows():
                cursor.execute("INSERT INTO Binance..OneMinute (Id,Pair,interval,DateTime,baseAsset,quoteAsset,O,H,L,C,V,close_time,qav,num_trades,taker_base_vol,taker_quote_vol,ignore) \
                        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", \
                        row.datetimeUnix, symbol ,interval, row.DateTime, baseAsset, quoteAsset, row.O,row.H,row.L,row.C,row.V,row.close_time,row.qav,row.num_trades,row.taker_base_vol,row.taker_quote_vol,row.ignore)
            cnxn.commit()
            cursor.close()

            start = str(int(data["datetimeUnix"].iloc[-1]))
            print(data["DateTime"].iloc[-1])
            time.sleep(0.3)