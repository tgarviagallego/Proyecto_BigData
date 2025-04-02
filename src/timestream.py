import time
import boto3
from botocore.config import Config
import random, json
import subprocess

BOOTSTRAP_SERVERS = [
    "b-1-public.testkafka3.l7t9mi.c2.kafka.eu-south-2.amazonaws.com:9198",
    "b-2-public.testkafka3.l7t9mi.c2.kafka.eu-south-2.amazonaws.com:9198"
]
BOOTSTRAP_STRING = ",".join(BOOTSTRAP_SERVERS)
TOPIC = "imat3a_ETH"
KEY = "A"

class TimestreamWriter:
    def __init__(self):
        self.config = Config(
            retries = dict(
                max_attempts = 10
            )
        )
        # Initialize Timestream client
        self.client = boto3.client('timestream-write', 
                                 region_name='eu-west-1',  # Mantenemos esta region, hemos creado la base de datos de Timestream en eu-west-1 ya que no esta disponible en eu-south-2 (Spain)
                                 config=self.config)
        
    def write_records(self):
        print("Writing records")
        current_time = self._current_milli_time()
        price = self.consume_kafka()
        dimensions = [
                {'Name' :'ETH', 'Value':'Crypto'} # Cambiar 'BTC' por vuestra cryptomoneda en concreto
        ]

        stock_price = {
            'MeasureName':'ETH', # Cambiar 'BTC' por vuestra cryptomoneda en concreto
            'Dimensions': dimensions,
            'MeasureValue': str(price),
            'Time': current_time
        }

        records = [stock_price]
        # La base de datos de Amazon Timestream ha sido creada en eu-west-1 en la cuenta de Testing. Es posible ver esta base de datos y realizar consultas directamente desde AWS
        try:
            result = self.client.write_records(
                DatabaseName='CryptoIcaiDatabase',  # Nombre de la base de datos Timestream (No modificar)
                TableName='CryptoMonedas',        # Nombre de la tabla de Timestream (No modificar)
                Records=records,
                CommonAttributes={}
            )
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.RejectedRecordsException as err:
            self._print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

    @staticmethod
    def consume_kafka():
        """Publica un mensaje JSON en Kafka con clave"""
        KAFKA_CMD = f'''./kafka_2.13-3.6.0/bin/kafka-console-producer.sh \
        --bootstrap-server {BOOTSTRAP_STRING} \
        --consumer.config ./config/client.properties \
        --topic {TOPIC} \
        --property print.key=true \
        --property print.value=true \
        --property print.partition=true \
        --property print.offset=true \
        --property print.timestamp=true \
        --consumer-property group.id=imat3a_group02'''

        result = subprocess.run(KAFKA_CMD, shell=True, capture_output=True, text=True)

        if result.stdout.strip():
            try:
                kafka_message = json.loads(result.stdout.strip())  # Convertir la salida en JSON
                price = kafka_message.get("price", None)  # Extraer el precio
                print("price = ", price)
                if price is not None:
                    price = float(price)
                    return price
            except json.JSONDecodeError:
                pass

    @staticmethod
    def _print_rejected_records_exceptions(err):
        print("RejectedRecords: ", err)
        for rr in err.response["RejectedRecords"]:
            print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
            if "ExistingVersion" in rr:
                print("Rejected record existing version: ", rr["ExistingVersion"])

    @staticmethod
    def _current_milli_time():
        return str(int(round(time.time() * 1000)))

def main():
    # Create database and table if they don't exist
    writer = TimestreamWriter()
    
    # Write records every 5 seconds
    try:
        while True:
            writer.write_records()
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nStopping the writer...")

if __name__ == "__main__":
    main()