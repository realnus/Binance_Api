import os
from turtle import position
from binance.client import Client
from binance.enums import *
from binance.exceptions import BinanceAPIException, BinanceOrderException
import json
import time
from datetime import datetime
import binance_api_keys

##********* In short, the Futures API key pair is available from https://testnet.binancefuture.com/

#https://testnet.binancefuture.com/
#realnus@gmail.com
#A1234567890!!
#Futures test api keys
api_key = binance_api_keys.binance_api_key_futures
api_secret = binance_api_keys.binance_api_secret_futures
#API Docs :https://binance-docs.github.io/apidocs/futures/en/#change-log




client = Client(api_key, api_secret, testnet=True)

#while True:
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
    
    """
    client.futures_create_order(
        symbol='BTCUSDT',
        type='MARKET',
        timeInForce='GTC',
        side='SELL',
        quantity=0.001
        )            
    """
    client.futures_get_open_orders(symbol='BTCUSDT')


    """
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
    """  
except BinanceAPIException as e:
    # error handling goes here
    print(e)
except BinanceOrderException as e:
    # error handling goes here
    print(e)