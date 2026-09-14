"""from kafka import KafkaConsumer
import json

KAFKA_TOPIC = 'lina.raw.flights_scrapy'
KAFKA_BOOTSTRAP_SERVERS = ['192.168.0.33:9092']  

# Inicializa o consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=False,  
    group_id='silver-flight-group-debug'
    )

for msg in consumer:
    payload = msg.value
    print(payload) """ 


### PAYLOAD EXAMPLE THAT APPEARS IN CDC 
### 1 - SKIPPLAGED
# {"before":null,"after":{"id":"eb731ee7-ac71-43a1-bedd-79d30d49db1c",
# "data_json":"{\"raw_text\": \"6h\\n1 stop\\nGOL Linhas Aereas\\n11:00am\\nMCZ\\nGIG\\n5:15pm\\nGYN\\n$163\", \"companhia_bruta\": \"GOL Linhas Aereas\", \"preco_bruto\": null, \"hora_saida_bruta\": \"11:00am\", \"hora_chegada_bruta\": \"5:15pm\", \"data_busca\": \"2025-06-16 21:50:48\", \"link_emissao\": \"https://skiplagged.com/flights/MCZ/GYN/2025-07-10\", \"site\": \"skiplagged\", \"id\": \"904#$382\"}","created_at":1750112894433915,"mode":"TESTING"},
# "source":{"version":"2.5.4.Final","connector":"postgresql","name":"lina","ts_ms":1750123694396,"snapshot":"false","db":"lina","sequence":"[\"28803800\",\"28803800\"]","schema":"raw","table":"flights_scrapy","txId":863,"lsn":28803800,"xmin":null},"op":"c","ts_ms":1750123694727,"transaction":null}
### 2 - LATAM
# {"before":null,"after":{"id":"ae899a52-3e1a-4cc6-a770-80691c7ae252",
# "data_json":"{\"raw_text\": \"VOO . HORA DE SA\\u00cdDA 17:10, PARTIDA DE MACEI\\u00d3, AEROPORTO MACEIO, HORA DE CHEGADA 8:45 DO DIA SEGUINTE, EM GOI\\u00c2NIA, AEROPORTO GOIANIA. VOO 2 PARADAS, COM DURA\\u00c7\\u00c3O TOTAL DE 15 HORAS 35 MINUTOS. PRE\\u00c7O DE UM ADULTO A PARTIR DE 1366,14 REAIS BRASILEIROS. OPERADO PELA LATAM AIRLINES BRASIL.\", \"link_emissao\": \"https://www.latamairlines.com/br/pt/oferta-voos?origin=MCZ&outbound=2025-07-10T15%3A00%3A00.000Z&destination=GYN&adt=1&chd=0&inf=0&trip=OW&cabin=Economy&redemption=false&sort=RECOMMENDED\", \"site\": \"latam\", \"data_busca\": \"2025-06-16 21:50:09\", \"id\": \"904#$382\"}","created_at":1750112894110589,"mode":"TESTING"},
# "source":{"version":"2.5.4.Final","connector":"postgresql","name":"lina","ts_ms":1750123694065,"snapshot":"false","db":"lina","sequence":"[\"28793808\",\"28793808\"]","schema":"raw","table":"flights_scrapy","txId":850,"lsn":28793808,"xmin":null},"op":"c","ts_ms":1750123694203,"transaction":null}



from kafka import KafkaConsumer
import json
import logging
import re

from decimal import Decimal, InvalidOperation
from datetime import datetime
from urllib.parse import urlparse

from db_connection import get_lina_connection


# ============================================================
# CONFIG
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("silver-consumer")

KAFKA_TOPIC = "lina.raw.flights_scrapy"
KAFKA_BOOTSTRAP_SERVERS = ["192.168.0.33:9092"]
KAFKA_GROUP_ID = "silver-flight-group"


# ============================================================
# SQL
# ============================================================

INSERT_QUERY = """
INSERT INTO silver.flights_scrapy (
    raw_id,
    flight_from,
    flight_to,
    company,
    departure_time,
    arrival_time,
    duration_minutes,
    stops,
    self_transfer,
    connection_airports,
    price_original,
    currency_original,
    ticket_date,
    search_timestamp,
    emission_link,
    website,
    mode
)
VALUES (
    %(raw_id)s,
    %(flight_from)s,
    %(flight_to)s,
    %(company)s,
    %(departure_time)s,
    %(arrival_time)s,
    %(duration_minutes)s,
    %(stops)s,
    %(self_transfer)s,
    %(connection_airports)s,
    %(price_original)s,
    %(currency_original)s,
    %(ticket_date)s,
    %(search_timestamp)s,
    %(emission_link)s,
    %(website)s,
    %(mode)s
)
ON CONFLICT (raw_id)
DO NOTHING;
"""


# ============================================================
# HELPERS
# ============================================================

def parse_datetime(value):
    if not value:
        return None

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d"
    ):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass

    return None


def parse_time(value):
    if not value:
        return None

    value = value.strip().lower()

    for fmt in ("%I:%M%p", "%H:%M"):
        try:
            return datetime.strptime(value, fmt).time()
        except ValueError:
            pass

    return None


def parse_date(value):
    if not value:
        return None

    try:
        return datetime.strptime(
            value,
            "%Y-%m-%d"
        ).date()

    except ValueError:
        return None


def parse_decimal(value):
    if value is None:
        return None

    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


# ============================================================
# URL
# ============================================================

def extract_flight_info_from_url(url):

    if not url:
        return None, None, None

    match = re.search(
        r"/flights/"
        r"([A-Z]{3})/"
        r"([A-Z]{3})/"
        r"(\d{4}-\d{2}-\d{2})",
        url,
        re.IGNORECASE
    )

    if not match:
        return None, None, None

    origin, destination, flight_date = match.groups()

    return (
        origin.upper(),
        destination.upper(),
        parse_date(flight_date)
    )


# ============================================================
# RAW TEXT
# ============================================================

def extract_duration_minutes(text):

    if not text:
        return None

    match = re.search(
        r"(?<!\d)"
        r"(\d+)\s*h"
        r"(?:\s*(\d+)\s*m)?",
        text,
        re.IGNORECASE
    )

    if match:
        hours = int(match.group(1))
        minutes = int(match.group(2) or 0)

        return hours * 60 + minutes

    match = re.search(
        r"(?<!\d)(\d+)\s*m(?!\w)",
        text,
        re.IGNORECASE
    )

    if match:
        return int(match.group(1))

    return None


def extract_stops(text):

    if not text:
        return None

    match = re.search(
        r"\b(\d+)\s+stops?\b",
        text,
        re.IGNORECASE
    )

    if match:
        return int(match.group(1))

    if re.search(
        r"\bnonstop\b|\bnon-stop\b",
        text,
        re.IGNORECASE
    ):
        return 0

    return None


def extract_self_transfer(text):

    if not text:
        return False

    return bool(
        re.search(
            r"self[\s-]?transfer",
            text,
            re.IGNORECASE
        )
    )


def extract_price(text, preco_bruto=None):

    # usa campo estruturado primeiro
    if preco_bruto:
        cleaned = re.sub(
            r"[^\d.,]",
            "",
            str(preco_bruto)
        )

        try:
            return Decimal(cleaned)
        except InvalidOperation:
            pass

    # fallback para raw_text
    match = re.search(
        r"\$\s*([\d,.]+)",
        text or ""
    )

    if not match:
        return None

    value = match.group(1).replace(",", "")

    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def extract_connection_airports(
    text,
    origin,
    destination
):

    if not text:
        return []

    codes = re.findall(
        r"(?m)^\s*([A-Z]{3})\s*$",
        text
    )

    connections = []

    for code in codes:

        code = code.upper()

        if code in (origin, destination):
            continue

        if code not in connections:
            connections.append(code)

    return connections


# ============================================================
# TRANSFORM
# ============================================================

def transform(payload):

    after = payload.get("after")

    if not after:
        return None

    op = payload.get("op")

    if op not in ("c", "r", "u"):
        return None

    raw_id = after.get("id")

    data_json = after.get("data_json")

    if not data_json:
        return None

    try:
        raw_data = (
            json.loads(data_json)
            if isinstance(data_json, str)
            else data_json
        )

    except json.JSONDecodeError:
        logger.exception("data_json inválido")
        return None

    website = (
        raw_data
        .get("site", "")
        .strip()
        .lower()
    )

    if website != "skiplagged":
        return None

    text = raw_data.get("raw_text", "")
    url = raw_data.get("link_emissao", "")

    origin, destination, ticket_date = (
        extract_flight_info_from_url(url)
    )

    result = {
        "raw_id": raw_id,

        "flight_from": origin,
        "flight_to": destination,

        "company": raw_data.get(
            "companhia_bruta"
        ),

        "departure_time": parse_time(
            raw_data.get("hora_saida_bruta")
        ),

        "arrival_time": parse_time(
            raw_data.get("hora_chegada_bruta")
        ),

        "duration_minutes":
            extract_duration_minutes(text),

        "stops":
            extract_stops(text),

        "self_transfer":
            extract_self_transfer(text),

        "connection_airports":
            extract_connection_airports(
                text,
                origin,
                destination
            ),

        "price_original":
            extract_price(
                text,
                raw_data.get("preco_bruto")
            ),

        "currency_original": "USD",

        "ticket_date": ticket_date,

        "search_timestamp":
            parse_datetime(
                raw_data.get("data_busca")
            ),

        "emission_link": url,

        "website": website,

        "mode": after.get("mode")
    }

    return result


# ============================================================
# CONSUMER
# ============================================================

def create_consumer():

    return KafkaConsumer(
        KAFKA_TOPIC,

        bootstrap_servers=(
            KAFKA_BOOTSTRAP_SERVERS
        ),

        value_deserializer=lambda m: (
            json.loads(
                m.decode("utf-8")
            )
        ),

        auto_offset_reset="earliest",

        enable_auto_commit=False,

        group_id=KAFKA_GROUP_ID
    )


# ============================================================
# MAIN
# ============================================================

def run_consumer():

    consumer = create_consumer()

    conn = get_lina_connection()

    logger.info(
        "Consumindo tópico %s",
        KAFKA_TOPIC
    )

    try:

        for msg in consumer:

            try:

                result = transform(
                    msg.value
                )

                if not result:

                    consumer.commit()
                    continue

                with conn.cursor() as cursor:

                    cursor.execute(
                        INSERT_QUERY,
                        result
                    )

                conn.commit()

                # só confirma Kafka depois
                # do PostgreSQL
                consumer.commit()

                logger.info(
                    (
                        "Silver inserida | "
                        "%s -> %s | "
                        "%s USD | "
                        "%s"
                    ),
                    result["flight_from"],
                    result["flight_to"],
                    result["price_original"],
                    result["ticket_date"]
                )

            except Exception:

                logger.exception(
                    "Erro processando mensagem"
                )

                conn.rollback()

                # não faz commit Kafka

    finally:

        consumer.close()
        conn.close()


if __name__ == "__main__":
    run_consumer()