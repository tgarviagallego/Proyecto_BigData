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
        # print(error)  # Este error salta si la bbdd estaba ya creada
        pass

    s3_targets = [{"Path": f"s3://{BUCKET_NAME}"}]

    iam_role = "arn:aws:iam::940482414331:role/traiding-view-isamaria"
    crawler_name = "aws-isamaria2"

    try:
        glue_client.create_crawler(
            Name=crawler_name,
            Role=iam_role,
            DatabaseName=DATABASE_NAME,
            Targets={'S3Targets': s3_targets},
            TablePrefix=f"trade_data_sprint2-isamaria",
            Description=f"Crawler que detecta datos del bucket trading-view-data de s3 y los guarda en la BBDD"
        )
    except Exception as error:
        # print(error)  # Este error salta si el crawler estaba ya creado
        pass

    glue_client.start_crawler(Name=crawler_name)
