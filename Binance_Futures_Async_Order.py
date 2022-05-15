import os
from turtle import position
from binance.client import Client
from binance.enums import *
from binance.exceptions import BinanceAPIException, BinanceOrderException
import json
import time
from datetime import datetime
from binance import AsyncClient
import asyncio
import binance_api_keys


binance_api_key = binance_api_keys.binance_api_key_futures_allip
binance_api_secret = binance_api_keys.binance_api_secret_futures_allip


##********* In short, the Futures API key pair is available from https://testnet.binancefuture.com/

#https://testnet.binancefuture.com/
#realnus@gmail.com
#A1234567890!!
#Futures test api keys
api_key = binance_api_key
api_secret = binance_api_secret
#API Docs :https://binance-docs.github.io/apidocs/futures/en/#change-log

#STOP-LIMIT ORDER
"""
The easiest way to understand a stop-limit order is to break it down into stop price, 
and limit price. The stop price is simply the price that triggers the limit order, and 
the limit price is the price of the limit order that is triggered. This means that once 
your stop price has been reached, your limit order will be immediately placed on the order book.
Although the stop and limit prices can be the same, this is not a requirement. In fact, it would 
be safer for you to set the stop price (trigger price) a bit higher than the limit price for sell 
orders, or a bit lower than the limit price for buy orders. This increases the chances of your limit 
order getting filled after the stop price is reached.
"""

#STOP-MARKET ORDER
"""What is a stop-market order?
Similarly to a stop-limit order, a stop-market order uses a stop price as a trigger. However,
 when the stop price is reached, it triggers a market order* instead.
*Due to extreme market movements, the executed price of market order may be lower/higher than the 
last traded price that user may have seen, user needs to pay attention to the market depth and price fluctuations."""


#Open Futures Positions
async def OpenBuyPosition(_client, _symbol, _type, _quantity):
    BuyOrder = await _client.futures_create_order(
        symbol= _symbol
        ,type=_type
        ,side='BUY'
        ,positionSide = 'LONG'
        ,quantity=_quantity
    )

async def OpenSellPosition(_client, _symbol, _type, _quantity):
    BuyOrder = await _client.futures_create_order(
        symbol=_symbol,
        type=_type,
        side='SELL',
        positionSide = 'SHORT',
        quantity = _quantity
    )

##Close Futures Positions
#https://dev.binance.vision/t/closing-the-long-position-with-hedge-mode/1000/3
async def CloseBuyPosition(_client, _symbol, _type, _quantity):
    BuyOrder = await _client.futures_create_order(
        symbol= _symbol
        ,type= _type
        ,side='SELL'
        ,positionSide = 'LONG'
        ,quantity=_quantity
    )

async def CloseSellPosition(_client, _symbol, _type, _quantity):
    BuyOrder = await _client.futures_create_order(
        symbol= _symbol
        ,type= _type
        ,side='BUY'
        ,positionSide = 'SHORT'
        ,quantity=_quantity
    )

async def main():
    #client = await AsyncClient.create(api_key, api_secret, testnet=True)
    client = await AsyncClient.create(api_key, api_secret)

    res = await client.get_exchange_info()
    
    """    
    e = await OpenBuyPosition(client, 'BTCUSDT', 'MARKET', 0.001)
    f = await OpenSellPosition(client, 'BTCUSDT', 'MARKET', 0.001)
    """
    a = await CloseBuyPosition(client, 'BTCUSDT', 'MARKET', 0.001)
    b = await CloseSellPosition(client, 'BTCUSDT', 'MARKET', 0.001)
    #Bunu bozma açık olan LONG emri kapattı
    """
    BuyOrder = await client.futures_create_order(
        symbol='BTCUSDT',
        type='MARKET',
        #timeInForce='GTC',
        side='SELL'
        ,positionSide = 'LONG'
        ,quantity=0.001
        #,reduceOnly = 'false'
    )
    """
    """
    #Açık olan Short pozisyonu kapatır.
    SellOrder = await client.futures_create_order(
        symbol='BTCUSDT',
        type='MARKET',
        #timeInForce='GTC',
        side='BUY'
        ,positionSide = 'SHORT'
        ,quantity = 0.001
        #,reduceOnly = 'false'

    ) 
    """
    """
    BuyOrder = await client.futures_create_order(
        symbol='BTCUSDT',
        type='MARKET',
        #timeInForce='GTC',
        side='BUY'
        ,positionSide = 'LONG'
        ,quantity=0.001
        #,reduceOnly = True
    )
    
    SellOrder = await client.futures_create_order(
        symbol='BTCUSDT',
        type='MARKET',
        #timeInForce='GTC',
        side='SELL',
        positionSide = 'SHORT',
        quantity = 0.001
    )    
    """

    #print("BuyOrder",BuyOrder)
    #print("SellOrder",SellOrder)


    await client.close_connection()

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

"""
#while True:
"""
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
"""
#Eger USDTDAI için order yok ise duruma göre olusturalim
#if(is_USDTDAI_order == False):
    #print("here")

#Check If we have usdt or DAI , returns dictionary


try:
    BTC_balance = client.futures_account_balance()
    ##USDT_balance = client.get_asset_balance(asset='USDT')
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),'BTC_balance', BTC_balance)
    #print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),'DAI_balance',DAI_balance,'USDT_balance',USDT_balance)
    
    client.futures_change_leverage(symbol='BTCUSDT', leverage=1)
    #print(client.futures_coin_get_position_mode)
    #client.futures_coin_change_position_mode(dualSidePosition=True)

    #client.futures_create_order(symbol='BTCUSDT', side='BUY', type='MARKET', quantity=0.001,timeInForce='GTC')

    # Api uzerinde multiple trade enable olmalı side ve positionSide beraber kullanılacak.
    #{{url}}/fapi/v1/order?symbol=BNBUSDT&side=SELL&positionSide=SHORT&type=MARKET&quantity=10&timestamp={{timestamp}}&signature={{signature}}
    client.futures_create_order(
        symbol='BTCUSDT',
        type='MARKET',
        #timeInForce='GTC',
        side='BUY',
        positionSide = 'LONG',
        quantity=0.001
    )
    
    
    client.futures_create_order(
        symbol='BTCUSDT',
        type='MARKET',
        #timeInForce='GTC',
        side='SELL',
        positionSide = 'SHORT',
        quantity = 0.001
    )
    

    client.futures_create_order(
        symbol='BTCUSDT',
        type='MARKET',
        timeInForce='GTC',
        side='SELL',
        quantity=0.001
        )            

    client.futures_get_open_orders(symbol='BTCUSDT')



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
 
except BinanceAPIException as e:
    # error handling goes here
    print(e)
except BinanceOrderException as e:
    # error handling goes here
    print(e)

"""    