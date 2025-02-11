import boto3
from credenciales import *

BUCKET_NAME = "trading-view-data"
AWS_REGION = "eu-south-2"

DATABASE_NAME = "trade_data_imat3a02"

glue_client = boto3.client(
    "glue",
    aws_access_key_id=KEY_ID,
    aws_secret_access_key=ACCESS_KEY,
    aws_session_token=SESSION_TOKEN,
    region_name=AWS_REGION
)

if __name__ == "__main__":
    # Creamos la base de datos
    try:
        glue_client.create_database(
            DatabaseInput = {
                "Name": DATABASE_NAME,
                "Description": "Base de datos con los datos de trading view"
            }
        )
    except Exception as error:
        print(error)

    # Creamos el crawler
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

    s3_targets = []
    for crypto in cryptos:
        for anio in range(2021, 2026):
            s3_targets.append({"Path": f"s3://{BUCKET_NAME}/{crypto}/{anio}"})

    iam_role = "trading-view-IAM-role"
    crawler_name = "crawler_trading_view_"

    try:
        glue_client.create_crawler(
            Name=crawler_name,
            Role=iam_role,
            DatabaseName=DATABASE_NAME,
            Targets={'S3Targets': s3_targets},
            TablePrefix=f"trade_data_{crypto}_",
            Description=f"Crawler que detecta datos de {crypto} y los guarda en la BBDD"
        )
    except Exception as error:
        print(error)

    glue_client.start_crawler(Name=crawler_name)