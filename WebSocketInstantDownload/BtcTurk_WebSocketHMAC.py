import asyncio
import base64
import hashlib
import hmac
import json
import time
import websockets
class InterfaceWS:
    def __init__(self, exchange_name: str = None) -> None:
        self.exchange_name = "BTC Turk"
        self.uri = "wss://ws-feed-pro.btcturk.com/"

    async def authenticate_ws(self) -> bool:
        self.ws = await websockets.connect(self.uri)
        publicKey = "";
        privateKey = "";
        nonce = 3000
        print("\nnonce", nonce)
        baseString = "{}{}".format(publicKey, nonce).encode("utf-8")
        print("\nbaseString", baseString)
        signature = hmac.new(
            base64.b64decode(privateKey), baseString, hashlib.sha256
        ).digest()
        print("\nsignature", signature)
        signature = base64.b64encode(signature)
        print("\nsignature", signature)
        timestamp = round(time.time() * 1000)
        print("\ntimestamp", timestamp)
        hmacMessageObject = [422, {
'type': 422,
'channel': 'trade',
'event': 'BTCUSDT',
'join': True
}
]
        print("hmacMessageObject", hmacMessageObject)
        await self.ws.send(json.dumps(hmacMessageObject))
        while True:
            try:
                response = await asyncio.wait_for(self.ws.recv(), timeout=0.5)
                print("response after auth: ", response)
            except Exception as e:
                print(e)

async def main():
    w = InterfaceWS()
    await w.authenticate_ws()

if __name__ == "__main__":
    asyncio.run(main())