import sys
import websocket
from datetime import datetime
import threading
import time
import threading
import csv
import requests
import json

from multiprocessing import Queue, Process, current_process, RawValue, Lock


#NusLib Import
sys.path.insert(1, 'C:\\MyProjects2\\PythonNusLib\\NusLib')       
import NusLibGeneric

WebApiUrl = 'http://127.0.0.1:5002/GetTickData'
FilePath = 'C:\\MyProjects2\\CC\\Binance_Api\\'

StreamType = ""
currency = ""
interval = 0 # StreamType KLine ile kullanılı

def worker_json(queue):
    name = current_process().name
    #print(f'{name} data received: {queue.get()}')
    
    #Convert To List
 
    """
    value_list = list(queue.get().values())
    id = value_list[1]
    s = value_list[2]
    c = float(value_list[3])
    o = float(value_list[4])
    """


    dict =  queue.get()
    #Read From Dictionary
    id = dict.get("E")
    s = dict.get("s")
    c = float(dict.get("c"))
    o = float(dict.get("o"))



    percent_change = NusLibGeneric.PercentChange(c,o)
    #print(f"{s} {id} {percent_change} {dict}")
    print(f"{s} {id} {percent_change}")

def ParseAndProcessData(jsonstring1):

    print("start ParseAndProcessData",
          datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))

    """"""
    json_dicti = json.loads(jsonstring1)
    queue = Queue()

    # Loop along dictionary keys
    for x in json_dicti:
        #id = x.get("E")

        queue.put(x)

    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
          "Start generating procesess")

    processes = [Process(target=worker_json, args=(queue,)) for _ in range(len(json_dicti))] 

    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
          "End generating procesess")

    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
          "Start starting procesess")

    for p in processes:
        p.start()

    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
              "End starting procesess")

    
    for p in processes:
        p.join()
    

    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
          "Completed processes")

    print("bitti")


def on_message(ws, message):
    
    ParseAndProcessData(message)
    ##Onemli: Araya webApi eklemezsek ThreadSafe olarak metin dosyasına yazamıyoruz.
    #NusLibGeneric.AppendToTextFile_ThreadSafe_v1(FilePath, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')  + " | " + message + "\n")

    """
    myobj = {
             'data': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')  + " | " + message, 
             'FilePath': FilePath
            }
    x = requests.post(WebApiUrl, data = myobj)
    """
    
    #print(message)


def on_error(ws, error):
    print(error)
    #close this instance and run a new one

"""
def on_close(close_msg):
    print("### closed ###" + close_msg)
"""
def on_close(self, ws, *args):
    print("### closed ###" + "Additional arguments were passed {}".format(args))


def streamListener(StreamType, currency, interval):
    #websocket.enableTrace(False)
    
    socket = ""

    if(StreamType == "kline"):
        socket = f'wss://stream.binance.com:9443/ws/{currency}@kline_{interval}'
    elif(StreamType == "trades"):
        socket = f'wss://stream.binance.com:9443/ws/{currency}@trade'
    elif(StreamType == "aggtrades"):
        socket = f'wss://stream.binance.com:9443/ws/{currency}@aggTrade'
    elif(StreamType == "miniTicker"):
        socket = f'wss://stream.binance.com:9443/ws/{currency}@miniTicker'
    elif(StreamType == "miniTicker@arr"):
        socket = f'wss://stream.binance.com:9443/ws/!miniTicker@arr'
    elif(StreamType == "depth"):
        socket = f'wss://stream.binance.com:9443/ws/{currency}@depth'        


    print("socket",socket)

    ws = websocket.WebSocketApp(socket,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()



if __name__ == "__main__":
    #ParseAndProcessData(jsonstring1)

    print("input StreamType: 1 kline , 2 trades , 3 aggtrades, 4 miniTicker, 5  All chaged tickers miniTicker@arr... Hit enter for kline")
    StreamType = input()
    if(StreamType == ""):
        StreamType = "kline"
    elif(StreamType == "1"):
        StreamType = "kline"
    elif(StreamType == "2"):
        StreamType = "trades"    
    elif(StreamType == "3"):
        StreamType = "aggtrades"  
    elif(StreamType == "4"):
        StreamType = "miniTicker"
    elif(StreamType == "5"):
        StreamType = "miniTicker@arr"
        currency = "miniTicker_arr"
    elif(StreamType == "6"):
        StreamType = "depth"
    


    if(StreamType == "kline"):
        print("input interval for kline: 1m for 1 minute ... Hit Enter for 1m ")
        interval = input()
        if(interval == ""):
            interval = "1m"

    if(currency == ""):
        print("input currency: santosusdt ... Hit enter for btcusdt")
        currency = input()

    print("input WebApiUr: http://127.0.0.1:5002/GetTickData , Hit Enter for default in example")
    WebApiUrl = input()
    if(WebApiUrl == ""):
        WebApiUrl = "http://127.0.0.1:5002/GetTickData"

    #Create FilePath
    FilePath = FilePath + currency + "_" + StreamType
    if(StreamType == "kline"):
        FilePath += "_" + str(interval)
    FilePath += '.csv'

    #print("FilePath:",FilePath)

    streamListener(StreamType, currency, interval)
