from binance.client import Client
import pandas as pd


#Return all symbols wiht status TRADING or BREAK
#returns dataframe symbol, status, baseAsset, quoteAsset
def Binance_getSymbols(binance_api_key,binance_api_secret):

    client = Client(binance_api_key, binance_api_secret)
    col_names =  ['symbol', 'status', 'baseAsset','quoteAsset']
    df_symbols  = pd.DataFrame(columns = col_names)

    exchange_info = client.get_exchange_info()
    for s in exchange_info['symbols']:
        a = s['symbol']
        df_symbols.loc[len(df_symbols)] = [
                                            s['symbol'], 
                                            s['status'], 
                                            s['baseAsset'], 
                                            s['quoteAsset']
                                          ]
    return df_symbols
