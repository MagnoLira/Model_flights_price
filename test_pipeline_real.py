import hashlib
import json
import subprocess
import sys
import time

from datetime import datetime, timedelta
from urllib import request, error

from kafka import KafkaProducer

from database.db_connection import get_lina_connection
from webscraping.browser_skiplagged import Browser_skiplagged
from webscraping.url_builder import urls_builder
from dotenv import load_dotenv
import os


load_dotenv()  # Load environment variables from .env file
# ============================================================
# CONFIGURAÇÃO
# ============================================================

ORIGEM = "THE"
DESTINO = "CGH"

DIAS_A_FRENTE = 3

KAFKA_BOOTSTRAP = f"{os.getenv('host')}:9092"
KAFKA_TOPIC = "raw.flights_scrapy"

PREDICT_URL = "http://flight-api.local/predict"

RAW_TIMEOUT_SECONDS = 60

PROJECT_ROOT = "."


# ============================================================
# HASH
# ============================================================

def gerar_hash(data):
    """
    MESMA implementação usada pelo kafka_topic_raw_consumer.py.
    """

    payload = json.dumps(
        data,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":")
    )

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()


# ============================================================
# SCRAPER
# ============================================================

def buscar_voos():
    data_voo = (
        datetime.now()
        + timedelta(days=DIAS_A_FRENTE)
    ).strftime("%Y-%m-%d")

    url = urls_builder.build_skiplagged_url(
        origin=ORIGEM,
        destination=DESTINO,
        departure_date=data_voo
    )

    print()
    print("=" * 80)
    print("1. SCRAPER")
    print("=" * 80)

    print(f"Rota: {ORIGEM} -> {DESTINO}")
    print(f"Data: {data_voo}")
    print(f"URL: {url}")

    browser = Browser_skiplagged(url)

    try:
        browser.load_page()

        voos = (
            browser
            .get_flights_info_skipplagged()
            or []
        )

    finally:
        browser.quit()

    print(
        f"[OK] {len(voos)} voo(s) encontrado(s)."
    )

    return voos


# ============================================================
# CONSUMER KAFKA -> RAW
# ============================================================

def iniciar_consumer_raw():

    print()
    print("=" * 80)
    print("2. CONSUMER KAFKA -> RAW")
    print("=" * 80)

    processo = subprocess.Popen(
        [
            sys.executable,
            "database/kafka_topic_raw_consumer.py"
        ],
        cwd=PROJECT_ROOT
    )

    print(
        f"[OK] Consumer iniciado | PID={processo.pid}"
    )

    # tempo para KafkaConsumer conectar e entrar no grupo
    time.sleep(3)

    if processo.poll() is not None:
        raise RuntimeError(
            "Consumer Kafka -> RAW encerrou "
            "logo após iniciar."
        )

    return processo


def parar_consumer(processo):

    if processo is None:
        return

    if processo.poll() is not None:
        return

    print()
    print("Encerrando consumer Kafka -> RAW...")

    processo.terminate()

    try:
        processo.wait(timeout=10)

    except subprocess.TimeoutExpired:
        processo.kill()
        processo.wait()

    print("[OK] Consumer encerrado.")


# ============================================================
# PRODUCER
# ============================================================

def enviar_para_kafka(voos):

    print()
    print("=" * 80)
    print("3. PRODUCER -> KAFKA")
    print("=" * 80)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP
    )

    hashes = []

    try:

        for voo in voos:

            data_hash = gerar_hash(voo)

            hashes.append(
                data_hash
            )

            payload = json.dumps(
                voo,
                ensure_ascii=False
            ).encode("utf-8")

            future = producer.send(
                KAFKA_TOPIC,
                value=payload
            )

            # garante confirmação do broker
            future.get(
                timeout=10
            )

        producer.flush()

    finally:
        producer.close()

    print(
        f"[OK] {len(voos)} mensagem(ns) "
        f"publicada(s) no Kafka."
    )

    return hashes


# ============================================================
# RAW
# ============================================================

def esperar_raw(hashes):

    print()
    print("=" * 80)
    print("4. KAFKA -> RAW")
    print("=" * 80)

    hashes_unicos = list(
        set(hashes)
    )

    conn = get_lina_connection()

    inicio = time.time()

    try:

        while (
            time.time() - inicio
            < RAW_TIMEOUT_SECONDS
        ):

            with conn.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT
                        id,
                        data_hash
                    FROM raw.flights_scrapy
                    WHERE data_hash = ANY(%s);
                    """,
                    (hashes_unicos,)
                )

                rows = cursor.fetchall()

            encontrados = {
                row[1]: str(row[0])
                for row in rows
            }

            print(
                f"\rRAW: "
                f"{len(encontrados)}/"
                f"{len(hashes_unicos)}",
                end="",
                flush=True
            )

            if (
                len(encontrados)
                == len(hashes_unicos)
            ):

                print()

                print(
                    "[OK] Todos os eventos "
                    "foram encontrados na RAW."
                )

                return encontrados

            time.sleep(1)

    finally:
        conn.close()

    print()

    raise RuntimeError(
        "Timeout esperando Kafka -> RAW. "
        f"Recebidos: {len(encontrados)}/"
        f"{len(hashes_unicos)}"
    )


# ============================================================
# ETL
# ============================================================

def executar_script(caminho):

    print()
    print(
        f"Executando: "
        f"{sys.executable} {caminho}"
    )

    subprocess.run(
        [
            sys.executable,
            caminho
        ],
        cwd=PROJECT_ROOT,
        check=True
    )


def executar_etls():

    print()
    print("=" * 80)
    print("5. RAW -> SILVER -> GOLD")
    print("=" * 80)

    executar_script(
        "database/etl_raw_to_silver.py"
    )

    executar_script(
        "database/etl_silver_to_gold.py"
    )

    print()
    print(
        "[OK] ETLs concluídas."
    )


# ============================================================
# BUSCAR VOO NA SILVER
# ============================================================

def buscar_voo_silver(hashes):

    conn = get_lina_connection()

    try:

        with conn.cursor() as cursor:

            cursor.execute(
                """
                SELECT
                    s.raw_id,
                    s.flight_from,
                    s.flight_to,
                    s.company,
                    s.departure_time,
                    s.arrival_time,
                    s.duration_minutes,
                    s.stops,
                    s.self_transfer,
                    s.connection_airports,
                    s.price_original,
                    s.ticket_date,
                    s.search_timestamp,
                    r.data_hash
                FROM raw.flights_scrapy r

                INNER JOIN silver.flights_scrapy s
                    ON s.raw_id = r.id

                WHERE r.data_hash = ANY(%s)
                  AND s.price_original IS NOT NULL

                ORDER BY
                    s.search_timestamp DESC,
                    s.price_original ASC

                LIMIT 1;
                """,
                (list(set(hashes)),)
            )

            row = cursor.fetchone()

        if not row:
            return None

        return {
            "raw_id":
                str(row[0]),

            "flight_from":
                row[1],

            "flight_to":
                row[2],

            "company":
                row[3],

            "departure_time":
                row[4],

            "arrival_time":
                row[5],

            "duration_minutes":
                row[6],

            "stops":
                row[7],

            "self_transfer":
                row[8],

            "connection_airports":
                row[9] or [],

            "price_original":
                float(row[10]),

            "ticket_date":
                row[11],

            "search_timestamp":
                row[12],

            "data_hash":
                row[13]
        }

    finally:
        conn.close()


# ============================================================
# GOLD
# ============================================================

def buscar_gold(raw_id):

    conn = get_lina_connection()

    try:

        with conn.cursor() as cursor:

            cursor.execute(
                """
                SELECT
                    raw_id,
                    route,
                    price_usd,
                    days_until_departure,
                    departure_minutes,
                    arrival_minutes,
                    duration_minutes,
                    stops,
                    connection_count
                FROM gold.flight_price_features
                WHERE raw_id = %s;
                """,
                (raw_id,)
            )

            row = cursor.fetchone()

        if not row:
            return None

        return {
            "raw_id":
                str(row[0]),

            "route":
                row[1],

            "price_usd":
                float(row[2]),

            "days_until_departure":
                row[3],

            "departure_minutes":
                row[4],

            "arrival_minutes":
                row[5],

            "duration_minutes":
                row[6],

            "stops":
                row[7],

            "connection_count":
                row[8]
        }

    finally:
        conn.close()


# ============================================================
# PAYLOAD DO MODELO
# ============================================================

def formatar_hora(value):

    if value is None:
        return None

    return value.strftime(
        "%H:%M:%S"
    )


def montar_payload(voo):

    return {
        "flight_from":
            voo["flight_from"],

        "flight_to":
            voo["flight_to"],

        "company":
            voo["company"],

        "departure_time":
            formatar_hora(
                voo["departure_time"]
            ),

        "arrival_time":
            formatar_hora(
                voo["arrival_time"]
            ),

        "duration_minutes":
            voo["duration_minutes"],

        "stops":
            voo["stops"],

        "self_transfer":
            voo["self_transfer"],

        "connection_airports":
            list(
                voo["connection_airports"]
            ),

        "ticket_date":
            voo["ticket_date"].isoformat(),

        "search_timestamp":
            voo["search_timestamp"].strftime(
                "%Y-%m-%d %H:%M:%S"
            )
    }


# ============================================================
# API
# ============================================================

def chamar_modelo(payload):

    print()
    print("=" * 80)
    print("7. MODELO VIA K3S / INGRESS")
    print("=" * 80)

    print(
        json.dumps(
            payload,
            indent=2,
            ensure_ascii=False
        )
    )

    body = json.dumps(
        payload
    ).encode(
        "utf-8"
    )

    req = request.Request(
        PREDICT_URL,
        data=body,
        headers={
            "Content-Type":
                "application/json"
        },
        method="POST"
    )

    try:

        with request.urlopen(
            req,
            timeout=30
        ) as response:

            result = json.loads(
                response
                .read()
                .decode("utf-8")
            )

            return result

    except error.HTTPError as exc:

        body = (
            exc
            .read()
            .decode("utf-8")
        )

        raise RuntimeError(
            f"API retornou "
            f"HTTP {exc.code}: "
            f"{body}"
        )


# ============================================================
# RESULTADO
# ============================================================

def mostrar_resultado(
    voo,
    gold,
    prediction
):

    observado = voo[
        "price_original"
    ]

    previsto = prediction[
        "predicted_price_usd"
    ]

    diferenca = (
        observado
        - previsto
    )

    erro = abs(
        diferenca
    )

    mae = prediction.get(
        "expected_mae_usd"
    )

    print()
    print("=" * 80)
    print("RESULTADO FINAL")
    print("=" * 80)

    print(
        f"RAW ID.............: "
        f"{voo['raw_id']}"
    )

    print(
        f"ROTA...............: "
        f"{prediction['route']}"
    )

    print(
        f"COMPANHIA..........: "
        f"{voo['company']}"
    )

    print(
        f"DATA DO VOO........: "
        f"{voo['ticket_date']}"
    )

    print(
        f"HORÁRIO............: "
        f"{voo['departure_time']} "
        f"-> {voo['arrival_time']}"
    )

    print(
        f"DURAÇÃO............: "
        f"{voo['duration_minutes']} min"
    )

    print(
        f"ESCALAS............: "
        f"{voo['stops']}"
    )

    print()
    print(
        f"PREÇO OBSERVADO....: "
        f"US$ {observado:.2f}"
    )

    print(
        f"PREÇO ESPERADO ML..: "
        f"US$ {previsto:.2f}"
    )

    print(
        f"RESÍDUO............: "
        f"US$ {diferenca:+.2f}"
    )

    print(
        f"ERRO ABSOLUTO......: "
        f"US$ {erro:.2f}"
    )

    print()
    print(
        f"MODELO.............: "
        f"{prediction['model_used']}"
    )

    print(
        f"ROTA CONHECIDA.....: "
        f"{prediction['route_known']}"
    )

    if mae is not None:

        print(
            f"MAE BENCHMARK......: "
            f"US$ {mae:.2f}"
        )

    print()
    print(
        f"GOLD ROUTE.........: "
        f"{gold['route']}"
    )

    print(
        f"GOLD PRICE.........: "
        f"US$ {gold['price_usd']:.2f}"
    )

    print(
        f"ANTECEDÊNCIA.......: "
        f"{gold['days_until_departure']} dias"
    )

    if mae is not None:

        if observado < (
            previsto - mae
        ):

            classificacao = (
                "ABAIXO DO ESPERADO"
            )

        elif observado > (
            previsto + mae
        ):

            classificacao = (
                "ACIMA DO ESPERADO"
            )

        else:

            classificacao = (
                "DENTRO DA FAIXA ESPERADA"
            )

        print()
        print(
            f"CLASSIFICAÇÃO......: "
            f"{classificacao}"
        )

    print("=" * 80)


# ============================================================
# MAIN
# ============================================================

def main():

    consumer_process = None

    print("=" * 80)
    print(
        "TESTE END-TO-END "
        "— FLIGHT PRICE MODEL"
    )
    print("=" * 80)

    try:

        # ----------------------------------------------------
        # 1. SCRAPING
        # ----------------------------------------------------

        voos = buscar_voos()

        if not voos:
            raise RuntimeError(
                "Nenhum voo encontrado."
            )

        # ----------------------------------------------------
        # 2. SOBE CONSUMER
        # ----------------------------------------------------

        consumer_process = (
            iniciar_consumer_raw()
        )

        # ----------------------------------------------------
        # 3. PUBLICA NO KAFKA
        # ----------------------------------------------------

        hashes = enviar_para_kafka(
            voos
        )

        # ----------------------------------------------------
        # 4. ESPERA RAW
        # ----------------------------------------------------

        raw_rows = esperar_raw(
            hashes
        )

        print(
            f"[OK] "
            f"{len(raw_rows)} hash(es) "
            f"confirmado(s) na RAW."
        )

        # O consumer já cumpriu seu papel neste teste
        parar_consumer(
            consumer_process
        )

        consumer_process = None

        # ----------------------------------------------------
        # 5. RAW -> SILVER -> GOLD
        # ----------------------------------------------------

        executar_etls()

        # ----------------------------------------------------
        # 6. RECUPERA UM VOO REAL
        # ----------------------------------------------------

        print()
        print("=" * 80)
        print("6. SILVER / GOLD")
        print("=" * 80)

        voo = buscar_voo_silver(
            hashes
        )

        if voo is None:
            raise RuntimeError(
                "Os eventos chegaram à RAW, "
                "mas nenhum foi encontrado "
                "na Silver."
            )

        print(
            f"[OK] Silver | "
            f"raw_id={voo['raw_id']}"
        )

        gold = buscar_gold(
            voo["raw_id"]
        )

        if gold is None:
            raise RuntimeError(
                "O voo chegou à Silver, "
                "mas não chegou à Gold."
            )

        print(
            f"[OK] Gold | "
            f"route={gold['route']} | "
            f"price={gold['price_usd']}"
        )

        # ----------------------------------------------------
        # 7. INFERÊNCIA
        # ----------------------------------------------------

        payload = montar_payload(
            voo
        )

        prediction = chamar_modelo(
            payload
        )

        # ----------------------------------------------------
        # 8. RESULTADO
        # ----------------------------------------------------

        mostrar_resultado(
            voo,
            gold,
            prediction
        )

    finally:

        if consumer_process:
            parar_consumer(
                consumer_process
            )


if __name__ == "__main__":
    main()