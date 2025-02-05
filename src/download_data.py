from TradingviewData import TradingViewData, Interval
import io
import boto3

NUM_DATA = 4*365+1

# AWS S3 Configuration
BUCKET_NAME = "trading-view-data"
AWS_REGION = "eu-south-2"

s3 = boto3.client(
    "s3",
    aws_access_key_id="ASIA5V6I6Y35W363WGK3",
    aws_secret_access_key="DLTGC9vym9RfTuUIjVsiihhqDaDtm6KieJHFVO6/",
    aws_session_token="IQoJb3JpZ2luX2VjEKn//////////wEaCmV1LXNvdXRoLTIiRzBFAiBVTOg5HmC7YZqA36/mEuObSN5EUcoOkff0GqzLhe2+JgIhALOZ6sReF41cjm3INQo4a7sb2QPLlqqnRuzT+7OjBB+2KukCCEsQABoMOTQwNDgyNDE0MzMxIgwj5fYK0ESnMIYskSUqxgI9MJoKs0cXsIFATEYyeD+SABfP7kQw5Aan6GGD5lBTv6sSHJcYOVrZVwt1hu5W62AgiBflJrOqVRO7BcIyczqXEnmlLjwL8UUm3vKjeTp/0972diT/a2ceV8mp+Vo7qG8M6m0TG0cLC5LSzyPBKweV/NAACdjO72FiSS6rquU0oAKapKJ91PXH7U56Y0F1i2lpSQo5B0q/JjDkp6F5jtj2e7PzVS6GWVYBOsp2EOkRs0KZHdRJMc/kclElOOrXRoArOVhfvjdMioUOo+5yZv4Lg0gS95/dgJhQGGGQKEreQxOxXjPpFXLcHLldKNCNjQ59+hU84Kxr7SvI0C9JBgzXpxJCf1ALeZdJQcTVSOenAUhwJLLQ+tyjT343ztfymWpNYb5JTgHWoUm8+DjmlvYMoxS4E+fQZv7SNHsno7HXejNyt/x++DCUzo69BjqnAcg7SAet/7vUHQefbhC+9SnsgcWcbThIBbP5dPKjhlLhbL0n6HRqAakYnr6G5S3yk4Feq69i6nP71Wy56fzN8Bfaes18gTD4byVb5W5HMBumLd4fceQIPFjA4DP13RyFPmpOCpcjaxTrXWabW9EpP3UMZv0YhPCjBRZKnParFjVUrYjhvnF2z5XITU5XPrIB/V+dfqBGtUk8AYB4pGcOS6SUvMBdKChQ",
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
        data.to_csv(csv_buffer, index=True)
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
        crypto_data.drop(columns=["volume"], inplace=True)
        
        for year, year_data in crypto_data.groupby(crypto_data.index.year):
            for month, month_data in year_data.groupby(year_data.index.month):
                file_name = f"{crypto_name}_{year}_{month}.csv"
                s3_key = f"{crypto_name}/{year}/{file_name}"  # Ruta en S3

                upload_to_s3(month_data, BUCKET_NAME, s3_key)