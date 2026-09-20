from kafka import KafkaConsumer
import json
import uuid
import hashlib
from datetime import datetime

from db_connection import get_lina_connection
from dotenv import load_dotenv
import os 

load_dotenv()  # Load environment variables from .env file

# =======================
# Configurações
# =======================

KAFKA_BOOTSTRAP_SERVERS = [f'{os.getenv("host")}:9092']
KAFKA_TOPIC = 'raw.flights_scrapy'

MODE = 'PRODUCTION'


# =======================
# Kafka Consumer
# =======================

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    value_deserializer=lambda m: json.loads(
        m.decode('utf-8')
    ),

    auto_offset_reset='earliest',

    enable_auto_commit=False,

    group_id='raw-flight-group-prod'
)


# =======================
# PostgreSQL
# =======================

conn = get_lina_connection()
cursor = conn.cursor()


# =======================
# INSERT idempotente
# =======================

INSERT_QUERY = """
INSERT INTO raw.flights_scrapy (
    id,
    data_json,
    data_hash,
    created_at,
    mode
)
VALUES (%s, %s, %s, %s, %s)

ON CONFLICT (data_hash)
DO NOTHING
"""


# =======================
# Função para hash
# =======================

def gerar_hash(data):

    payload = json.dumps(
        data,
        sort_keys=True,
        ensure_ascii=False,
        separators=(',', ':')
    )

    return hashlib.sha256(
        payload.encode('utf-8')
    ).hexdigest()


# =======================
# Loop
# =======================

print("Consumidor iniciado. Aguardando mensagens...")


for msg in consumer:

    data = msg.value

    try:

        data_hash = gerar_hash(data)

        cursor.execute(
            INSERT_QUERY,
            (
                str(uuid.uuid4()),
                json.dumps(
                    data,
                    ensure_ascii=False
                ),
                data_hash,
                datetime.now(),
                MODE
            )
        )

        inserted = cursor.rowcount

        conn.commit()

        # Commit Kafka SOMENTE depois do commit no banco
        consumer.commit()

        if inserted == 1:

            print(
                f"[INSERT] Mensagem inserida | "
                f"partition={msg.partition} "
                f"offset={msg.offset}"
            )

        else:

            print(
                f"[DUPLICADA] Mensagem já existente | "
                f"partition={msg.partition} "
                f"offset={msg.offset}"
            )

    except Exception as e:

        print(f"[ERRO] {e}")

        conn.rollback()