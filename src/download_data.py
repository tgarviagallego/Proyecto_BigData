from TradingviewData import TradingViewData, Interval
import io
import boto3
from credenciales import *

NUM_DATA = 4*365+1

# AWS S3 Configuration
BUCKET_NAME = "trading-view-data"
AWS_REGION = "eu-south-2"

s3 = boto3.client(
    "s3",
    aws_access_key_id=KEY_ID,
    aws_secret_access_key=ACCESS_KEY,
    aws_session_token=SESSION_TOKEN,
    region_name=AWS_REGION
)

def create_bucket(bucket_name: str, region: str) -> None:
    """
    Crea un bucket en S3 si no existe.

    Args:
        bucket_name (str): nombre del bucket
        region (str): region de aws
    """
    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': region}
        )
        print(f"Bucket '{bucket_name}' creado exitosamente en {region}.")

def upload_to_s3(data, bucket: str, key: str) -> None:
    """
    Sube un archivo CSV a S3 desde memoria.
    """
    with io.StringIO() as csv_buffer:
        data.to_csv(csv_buffer, index=True, float_format='%.10f')
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())


if __name__ == "__main__":
    create_bucket(BUCKET_NAME, AWS_REGION)

    request = TradingViewData()

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
        crypto_name = crypto[:-3]
        
        crypto_data = request.get_hist(symbol=crypto, exchange="CRYPTO", interval=Interval.daily, n_bars=NUM_DATA)
        #crypto_data.drop(columns=["volume"], inplace=True)
        crypto_data["open"] = crypto_data["open"].astype("float")
        crypto_data["close"] = crypto_data["close"].astype("float")
        crypto_data["high"] = crypto_data["high"].astype("float")
        crypto_data["low"] = crypto_data["low"].astype("float")

        for year, year_data in crypto_data.groupby(crypto_data.index.year):
            for month, month_data in year_data.groupby(year_data.index.month):
                file_name = f"{crypto_name}_{year}_{month}.csv"
                s3_key = f"{crypto_name}/{year}/{file_name}"  # Ruta en S3

                upload_to_s3(month_data, BUCKET_NAME, s3_key)