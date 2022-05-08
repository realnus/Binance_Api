from binance.client import Client
import binance_functions

binance_api_key = 'lQazbGSDAbND9XfLM7cFwTQpI9aWwr9VXWf8JIkFyREvWB4jFTd0uGzawT9v5flE'    #Enter your own API-key here
binance_api_secret = 'd6cN1hjews7Ng6YOHtBDkqnA4L1sY1jpCbgRUi1NlJHQgGViWTdQQAcszDRuckYH' #Enter your own API-secret here



a = binance_functions.Binance_getSymbols(binance_api_key,binance_api_secret)


client = Client(binance_api_key, binance_api_secret)
exchange_info = client.get_exchange_info()
symbols = []
for s in exchange_info['symbols']:
    print(s['symbol'])
    symbols.append(s['symbol'])