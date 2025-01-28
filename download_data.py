from TradingviewData import TradingViewData, Interval
import os


if __name__ == "__main__":
    request = TradingViewData()

    folder_path = "data"
    os.makedirs(folder_path, exist_ok=True)

    cryptos = [
        "BTCUSD", 
        "ETHUSD", 
        "XRPUSD", 
        "SOLUSD", 
        "DOGEUSD", 
        "ADAUSD", 
        "SHIBUSD", 
        "DOTUSD", 
        "AAVEUSD", 
        "XMLUSD"
    ]

    for crypto in cryptos:
        os.makedirs(f"{folder_path}/{crypto[:-3]}", exist_ok=True)
        crypto_data = request.get_hist(symbol=crypto, exchange="CRYPTO", interval=Interval.daily, n_bars=4*365+1)
        