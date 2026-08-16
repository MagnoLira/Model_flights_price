from kafka import KafkaConsumer
import json
import psycopg2
import uuid
from datetime import datetime
from db_connection import get_lina_connection
# =======================
# Configurações
# =======================

KAFKA_BOOTSTRAP_SERVERS = ['192.168.0.33:9092']
KAFKA_TOPIC = 'raw.flights_scrapy'
MODE = 'TESTING'  # ou 'PRODUCTION'

# =======================
# Setup do consumidor Kafka
# =======================

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,  # Não comita automaticamente
    group_id='raw-flight-group-prod' 
)

# =======================
# Conexão com PostgreSQL
# =======================

conn = get_lina_connection()
cursor = conn.cursor()

# =======================
# Query de INSERT
# =======================

INSERT_QUERY = """
INSERT INTO raw.flights_scrapy (
    id, data_json, created_at, mode
) VALUES (%s, %s, %s, %s)
"""

print("Consumidor iniciado. Aguardando mensagens...")

# =======================
# Loop de Consumo
# =======================

for msg in consumer:
    data = msg.value
    print("Mensagem recebida:", data)

    try:
        cursor.execute(INSERT_QUERY, (
            str(uuid.uuid4()),
            json.dumps(data),
            datetime.now(),
            MODE
        ))
        conn.commit()
        consumer.commit() #after the insert in the database
        print("Inserido com sucesso na camada raw!")
    except Exception as e:
        print("Erro ao inserir:", e)
        conn.rollback()
