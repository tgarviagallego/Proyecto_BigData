from TradingviewData import TradingViewData, Interval
import io
import boto3

NUM_DATA = 4*365+1

# AWS S3 Configuration
BUCKET_NAME = "trading-view-data"
AWS_REGION = "eu-south-2"

s3 = boto3.client(
    "s3",
    aws_access_key_id="ASIA5V6I6Y35ZHVXQPDU",
    aws_secret_access_key="DXMq8HvWnCjnwDOEH7iTzxGHm8nGUXs/1NoMw5P8",
    aws_session_token="IQoJb3JpZ2luX2VjEJH//////////wEaCmV1LXNvdXRoLTIiRzBFAiBn9u2nwTYtNH1nt4aAIvRzmXO6whiDbDC/bQhCKXJEpwIhAPs89Xmv5AgbU1nzvpNuq9Lj9IbC3NROGN1X3xDFKEmgKukCCDMQABoMOTQwNDgyNDE0MzMxIgxnHGbbCsayuzXfvjgqxgLokcelaTJ1pnWefOyBio3x6o0DXZnM7m56Y80hgRa2PLvr73bpw08TwJIyFnu1dPlgQjgnmEo6LU5QAE1K8jEIr1FwBOffjK3USoDwfZmSpHj+E5NH/hsrr48YU8bZCEf5AO1at/FfZzR8I8ryf/7cbvomLWndZCW2iLha5imzHE7YhmQmyHK1MANnDHiD3ZGi7XquMK6gR6vGgwxBA139F6wwFhF0pCTsB7wga1TDBeZfgdVMjReliXRB6TzsTFtDgSwiPOLl/d7i6n6OOnV/Evwt89eheE903Z/l8svRjuZHcj/JdMPES7mUDRS3gohop6XuxHr6yyKNELEEvV5sChVb7xKdd7NdU6+44vBk1Cz5+qRwQmRdtxo+y3bJbva+S+ctIAbDJnZNmpLXRxvygfsB4GPxLBY6BU0Ysh9KmExuUh606DCQq4m9BjqnATopeMsvlI5F5Dby7otOMRRrBuZJUhGjD0GrWup5mMJKNnw4LGI7oTTXSKy8HD2sg2d3rxPu+2jqCoYI3H1nIF1AB/7vqbv+XFKKcV0AAC7m0q2Pn+L/GLEh2r5u0lPkkTPomFFEwaWcns6AOlAqgnjlR41CX3nyLMOqShxk0gU969boNmAIa3YtQm0O8zvXT/l703ySRRMSwNXczC/wg4idu9RSWfGD",
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
                s3_key = f"{crypto_name}/{year}/{month}/{file_name}"  # Ruta en S3

                upload_to_s3(month_data, BUCKET_NAME, s3_key)