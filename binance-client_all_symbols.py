from binance.client import Client
import binance_functions

import binance_api_keys
binance_api_key = binance_api_keys.binance_api_key
binance_api_secret = binance_api_keys.binance_api_secret

a = binance_functions.Binance_getSymbols(binance_api_key,binance_api_secret)


client = Client(binance_api_key, binance_api_secret)
exchange_info = client.get_exchange_info()
symbols = []
for s in exchange_info['symbols']:
    print(s['symbol'])
    symbols.append(s['symbol'])