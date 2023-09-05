from client import FtxWebsocketClient
from websocket_manager import WebsocketManager
import time
import json

import websocket



"""
this = json.dumps({'op': 'subscribe', 'channel': 'trades', 'market': 'BTC-PERP'})

def on_open(wsapp):
    wsapp.send(this)

def on_message(wsapp, message):
    print(message)

wsapp = FtxWebsocketClient()
wsapp.connect() 
wsapp.WebSocketApp()
"""

# ("wss://ftx.com/ws/", _on_message= on_message, _on_open= on_open)
#wsapp.get_ticker(market='BTC-PERP')


ws = FtxWebsocketClient()
ws.connect()

if __name__ == '__main__':
    
    b = ""
    while(1==1):
    #for i in range(1, 1000):
        a = ws.get_ticker(market='BTC-PERP')
        if (len(a) != 0):
            a_trimmed = json.dumps(a)[0:87]
            if(a_trimmed != b):
                #new data
                print(a)
                b = a_trimmed

        """
        a_trimmed = a[0:87]
        if(a_trimmed != b):
            #new data
            print(a)
            b = a_trimmed
        """

        #print(ws.get_ticker(market='BTC-PERP'))
        #time.sleep(0.2)



"""
import websocket
import json

this = json.dumps({'op': 'subscribe', 'channel': 'trades', 'market': 'BTC- PERP'})

def on_open(wsapp):
    wsapp.send(this)

def on_message(wsapp, message):
    print(message)

wsapp = websocket.WebSocketApp("wss://ftx.com/ws/", on_message=on_message, on_open=on_open)
wsapp.run_forever()
"""