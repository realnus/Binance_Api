from operator import truediv
import os
from binance.client import Client
import json

"""
API Key
8DF3Q4unckfcOLFS1Rww8i9UysZo8QR4UK0jLSFZrPf4CxSUPxWUIu9KDIr4KEag
Secret Key
sl1HJwpFlZYv8XbtYtQTAurAObXpQBZODvKTepCwoBw6UuYKGNmfq65Qs5JGeW9u
"""

api_key = '8DF3Q4unckfcOLFS1Rww8i9UysZo8QR4UK0jLSFZrPf4CxSUPxWUIu9KDIr4KEag' #os.environ.get('binance_api')
api_secret = 'sl1HJwpFlZYv8XbtYtQTAurAObXpQBZODvKTepCwoBw6UuYKGNmfq65Qs5JGeW9u' #os.environ.get('binance_secret')
#api_url = 'https://api3.binance.com'

client = Client(api_key, api_secret)
#client.API_URL = api_url

"""res = client.get_exchange_info()
print(client.response.headers)"""

"""# get balances for all assets & some account information
a = client.get_account()
print(a)
print("b")"""


## Tüm pairlerin fiyatlarını alır
"""
prices = client.get_all_tickers()
print(prices)

print("b")

for x in prices:
  print(x)
"""

"""#Get All Orders
orders = client.get_all_orders(symbol='USDTDAI', limit=10)
for x in orders:
  print(x)
"""
orders = client.get_open_orders()

is_USDTDAI_order = false
#USDTDAI var mı orderlar içinnde
for x in orders:
    print(x)
    print(type(x))
    if(x["symbol"] == "USDTDAI"):
        is_USDTDAI_order = True

#Eger USDTDAI için order yok ise duruma göre olusturalim
if(is_USDTDAI_order == False):
    print("here")

    #Check If we have usdt or DAI , returns dictionary
    DAI_balance = client.get_asset_balance(asset='DAI')
    USDT_balance = client.get_asset_balance(asset='USDT')

    if(DAI_balance["free"] >= 650):

        buy_order_limit = client.create_test_order(
        symbol='USDTDAI',
        side='BUY',
        type='LIMIT',
        timeInForce='GTC',
        quantity=650,
        price=1.0001)
    elif(USDT_balance["free"] >= 650):

        sell_order_limit = client.create_test_order(
        symbol='USDTDAI',
        side='SELL',
        type='LIMIT',
        timeInForce='GTC',
        quantity=650,
        price=1.0003)



#Get Open Orders
# orders = client.get_open_orders()






## usdtdai sell 1.0003 , 
## usdtdai buy 1.0001  , DAI nin 650 siyle al

"""
buy_order_limit = client.create_test_order(
    symbol='ETHUSDT',
    side='BUY',
    type='LIMIT',
    timeInForce='GTC',
    quantity=100,
    price=200)"""