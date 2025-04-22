from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import boto3

# Vamos a comenzar accediendo a los datos
BUCKET_NAME = "trading-view-data"
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

bronze_path = "s3://{BUCKET_NAME}"

df = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .option("recursiveFileLookup", "true") \
               .csv(bronze_path)

df.show(5)  


# Ahora vamos a limpiar los datos