import subprocess
import json
import random
import re
import string
import time
from datetime import datetime
from websocket import create_connection, WebSocketConnectionClosedException

BOOTSTRAP_SERVERS = [
    "b-1-public.testkafka3.l7t9mi.c2.kafka.eu-south-2.amazonaws.com:9198",
    "b-2-public.testkafka3.l7t9mi.c2.kafka.eu-south-2.amazonaws.com:9198"
]
BOOTSTRAP_STRING = ",".join(BOOTSTRAP_SERVERS)
TOPIC = "imat3a_ETH"
KEY = "A" 

def generate_session():
    return "qs_" + "".join(random.choice(string.ascii_lowercase) for _ in range(12))

def prepend_header(content):
    return f"~m~{len(content)}~m~{content}"

def construct_message(func, param_list):
    return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

def create_message(func, param_list):
    return prepend_header(construct_message(func, param_list))

def send_message(ws, func, args):
    """Envía un mensaje a TradingView a través del WebSocket"""
    try:
        ws.send(create_message(func, args))
    except WebSocketConnectionClosedException:
        print("⚠️ Conexión cerrada mientras se enviaba un mensaje.")
        reconnect(ws)

def send_ping(ws):
    """Envía un ping para mantener la conexión activa"""
    try:
        ws.send("~h~0")
    except Exception as e:
        print(f"⚠️ Error enviando ping: {e}")

def publish_to_kafka(message_json):
    """Publica un mensaje JSON en Kafka con clave"""
    KAFKA_CMD = f'''echo '{KEY}:{message_json}' | /Users/raquel/Desktop/kafka_2.13-3.6.0/bin/kafka-console-producer.sh \
    --bootstrap-server {BOOTSTRAP_STRING} \
    --producer.config ./config/client.properties \
    --topic {TOPIC} \
    --property parse.key=true \
    --property key.separator=:'''

    result = subprocess.run(KAFKA_CMD, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"✅ Publicado en Kafka: {message_json}")
    else:
        print(f"❌ Error en Kafka: {result.stderr}")

def process_data(data):
    if not data:
        print("⚠️ Datos vacíos recibidos, ignorando...")
        return

    price = data.get("lp", "No disponible")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if price != "No disponible":
        message = json.dumps({
            "price": price, "timestamp": timestamp
        })

        publish_to_kafka(message)
        print(f"📊 {timestamp} - Precio: {price}")

def reconnect(symbol_id, attempt=1):
    wait_time = min(5 * attempt, 60)  
    print(f"🔄 Intentando reconectar en {wait_time} segundos...")
    time.sleep(wait_time)
    start_socket(symbol_id)

def start_socket(symbol_id):
    session = generate_session()
    url = "wss://data.tradingview.com/socket.io/websocket"
    headers = json.dumps({"Origin": "https://www.tradingview.com"})

    try:
        ws = create_connection(url, headers=headers)
        print(f"✅ Conectado a {url}")

        send_message(ws, "quote_create_session", [session])
        send_message(ws, "quote_set_fields", [session, "lp", "ch", "chp", "volume"])
        send_message(ws, "quote_add_symbols", [session, symbol_id])

        while True:
            try:
                result = ws.recv()
                if result.startswith("~m~"):
                    data_match = re.search(r"\{.*\}", result)
                    if data_match:
                        message = json.loads(data_match.group(0))
                        if message["m"] == "qsd":
                            process_data(message["p"][1]["v"])
                elif result.startswith("~h~"):
                    send_ping(ws)

            except WebSocketConnectionClosedException:
                print("⚠️ Conexión cerrada inesperadamente.")
                reconnect(symbol_id)
                break
            except Exception as e:
                continue

    except WebSocketConnectionClosedException as e:
        print(f"⚠️ Error al conectar: {e}. Intentando reconectar...")
        reconnect(symbol_id)
    except Exception as e:
        print(f"⚠️ Error inesperado: {e}. Intentando reconectar...")
        reconnect(symbol_id)

if __name__ == "__main__":
    symbol_id = "BINANCE:ETHUSD"  
    start_socket(symbol_id)
