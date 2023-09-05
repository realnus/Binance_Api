import websocket
from datetime import datetime
import threading
from multiprocessing import Process, RawValue, Lock
import time


def on_message(ws, message):
    thread1 = thread("GFG", 1000)
    print(message)
    #OrderCounter()
    """
    print()
    print(str(datetime.datetime.now()) + ": ")
    print(message)
    """

def on_error(ws, error):
    print(error)

def on_close(close_msg):
    print("### closed ###" + close_msg)

def streamKline(currency, interval):
    #websocket.enableTrace(False)
    #socket = f'wss://stream.binance.com:9443/ws/{currency}@kline_{interval}'
    socket = f'wss://stream.binance.com:9443/ws/{currency}@trade'
    #socket = f'wss://stream.binance.com:9443/ws/{currency}@aggTrade'
    
    ws = websocket.WebSocketApp(socket,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()

counter = ReadCounter("o")
streamKline('btcusdt', '1m')
print("asd")
print("dddd")