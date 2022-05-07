import os
from binance.client import Client
import json
import time
from datetime import datetime
"""
#Home LOCAL Keys
api_key = '8DF3Q4unckfcOLFS1Rww8i9UysZo8QR4UK0jLSFZrPf4CxSUPxWUIu9KDIr4KEag' #os.environ.get('binance_api')
api_secret = 'sl1HJwpFlZYv8XbtYtQTAurAObXpQBZODvKTepCwoBw6UuYKGNmfq65Qs5JGeW9u' #os.environ.get('binance_secret')
"""
#Mac Local Keys
api_key = 'l1UyyPWpZpMe35uOaYnTzVyLXuiPIT4Ke2kXr8aZBBZItWVqQJgHtovroHtPqoJE'
api_secret = 'MscivGWmN0O2k6qJ0JqItcvEiJWydRTbQHa9qAHIEO0fNGJEHDJ1U0BRKz8uE4cL'


client = Client(api_key, api_secret)

while True:
    """
    orders = client.get_open_orders()

    is_USDTDAI_order = False
    #USDTDAI var mı orderlar içinnde
    for x in orders:
        print(x)
        print(type(x))
        if(x["symbol"] == "USDTDAI"):
            is_USDTDAI_order = True
            break
    """
    #Eger USDTDAI için order yok ise duruma göre olusturalim
    #if(is_USDTDAI_order == False):
        #print("here")

    #Check If we have usdt or DAI , returns dictionary

    try:
        DAI_balance = client.get_asset_balance(asset='DAI')
        USDT_balance = client.get_asset_balance(asset='USDT')

        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),'DAI_balance',DAI_balance,'USDT_balance',USDT_balance)

        if(float(DAI_balance["free"]) >= 11):
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),'USDTDAI','BUY','LIMIT', str(int(float(DAI_balance["free"]))))

            buy_order_limit = client.create_order(
            symbol='USDTDAI',
            side='BUY',
            type='LIMIT',
            timeInForce='GTC',
            quantity=int(float(DAI_balance["free"])),
            price=1.0001)

        if(float(USDT_balance["free"]) >= 11):
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),'USDTDAI','SELL','LIMIT', str(int(float(USDT_balance["free"]))))

            sell_order_limit = client.create_order(
            symbol='USDTDAI',
            side='SELL',
            type='LIMIT',
            timeInForce='GTC',
            quantity=int(float(USDT_balance["free"])),
            price=1.0003)
    except:
        print()
    time.sleep(60)
