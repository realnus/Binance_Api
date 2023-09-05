import json
import pprint
import time
import websocket

try:
    import thread
except ImportError:
    import _thread as thread

def on_message(ws, message):
    data = json.loads(message)
    pprint.pprint(data)
def on_error(ws, error):
    print(error)
def on_close(ws):
    print("### closed ###")
def on_open(ws):
    def run(*args):
        time.sleep(1)
        message = [
            111,
            {
                "type": 111,  
                "id": 5076386890, #number
                "token": "2YkbJfD9advUbZ8YJ/fOM4HA6ddlg7Nk",
                "username": "nusret.araz@gmail.com"
            }
        ]
        ws.send(json.dumps(message))
    thread.start_new_thread(run, ())

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(
        "wss://ws-feed-pro.btcturk.com/",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()