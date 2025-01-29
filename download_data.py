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
        "XLMUSD"
    ]

    for crypto in cryptos:
        crypto_path = f"{folder_path}/{crypto[:-3]}"
        os.makedirs(crypto_path, exist_ok=True)
        crypto_data = request.get_hist(symbol=crypto, exchange="CRYPTO", interval=Interval.daily, n_bars=4*365+1)
        
        for year, year_data in crypto_data.groupby(crypto_data.index.year):
            for month, month_data in year_data.groupby(year_data.index.month):
                month_path = f"{crypto_path}/{year}/{month}"
                os.makedirs(month_path, exist_ok=True)

                file_name = f"{crypto[:-3]}_{year}_{month}.csv"
                file_path = os.path.join(month_path, file_name)
                
                month_data.to_csv(file_path, index=True)