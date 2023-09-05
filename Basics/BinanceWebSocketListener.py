import websocket
from datetime import datetime
import threading
from multiprocessing import Process, RawValue, Lock
import time


def on_message(ws, message):
    print(message)
   

def on_error(ws, error):
    print(error)

#Bunda hata veriyor parametre sayısı olarak bu sebeple on_close2 metodu oluşturdum
def on_close(close_msg):
    print("### closed ###" + close_msg)

def on_close2(self, ws, *args):
    print("### closed ###" + "Additional arguments were passed {}".format(args))    
    streamKline('btcusdt', '1m')


def streamKline(currency, interval):
    #websocket.enableTrace(False)
    #socket = f'wss://stream.binance.com:9443/ws/{currency}@kline_{interval}'
    socket = f'wss://stream.binance.com:9443/ws/{currency}@trade'
    #socket = f'wss://stream.binance.com:9443/ws/{currency}@aggTrade'
    
    ws = websocket.WebSocketApp(socket,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close2)

    ws.run_forever()

streamKline('btcusdt', '1m')
print("asd")
print("dddd")