from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import freeze_support
import json

from kafka import KafkaProducer

from browser_skiplagged import Browser_skiplagged
from url_builder import urls_builder
from database.db_connection import get_lina_connection


producer = KafkaProducer(
    bootstrap_servers="192.168.0.33:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def buscar_pares_aeroportos(aer_de):

    conn = get_lina_connection()

    try:
        cursor = conn.cursor()

        query = """
            SELECT
                aer_de,
                aer_para
            FROM flights.aeroportos_config
            WHERE aer_de = %s
        """

        cursor.execute(query, (aer_de,))

        pares = cursor.fetchall()

        return pares

    finally:
        conn.close()


def process_skipplagged(url_info):

    url = (
        url_info["url"]
        if isinstance(url_info, dict)
        else url_info
    )

    site = Browser_skiplagged(url)

    try:

        print(f"\nAbrindo: {url}")

        site.load_page()

        voos = site.get_flights_info_skipplagged()

        return voos

    except Exception as e:

        print(f"Erro no Skiplagged: {url}")
        print(f"Detalhe: {e}")

        return []

    finally:

        site.quit()


def processar_par(aer_de, aer_para, start_date):

    payload = {
        "flight_from": aer_de,
        "flight_to": aer_para,
        "start_date": start_date,
        "final_date": start_date
    }

    url = urls_builder.build_skiplagged_url(payload)

    print("=" * 70)
    print(f"ROTA: {aer_de} -> {aer_para}")
    print(f"URL: {url}")
    print("=" * 70)

    resultado = process_skipplagged(url)

    return resultado


def run_scraping(aer_de, start_date, max_workers=3):

    pares = buscar_pares_aeroportos(aer_de)

    print(f"\nAeroporto de origem: {aer_de}")
    print(f"Pares encontrados no banco: {len(pares)}")

    for origem, destino in pares:
        print(f"  {origem} -> {destino}")

    tarefas = [
        (origem, destino)
        for origem, destino in pares
    ]

    with ProcessPoolExecutor(
        max_workers=max_workers
    ) as executor:

        futures = {
            executor.submit(
                processar_par,
                origem,
                destino,
                start_date
            ): (origem, destino)

            for origem, destino in tarefas
        }

        for future in as_completed(futures):

            origem, destino = futures[future]

            try:

                resultado = future.result()

                if resultado:

                    for voo in resultado:
                        producer.send(
                            "raw.flights_scrapy",
                            value=voo
                        )

                    print(
                        f"[OK] {origem}->{destino}: "
                        f"{len(resultado)} voos enviados"
                    )

                else:

                    print(
                        f"[VAZIO] {origem}->{destino}: "
                        f"nenhum voo"
                    )

            except Exception as e:

                print(
                    f"[ERRO] {origem}->{destino}: {e}"
                )


if __name__ == "__main__":

    freeze_support()

    print("Iniciando scraping Skiplagged...")

    run_scraping(
        aer_de="GRU",
        start_date="2026-09-02",
        max_workers=3
    )

    producer.flush()
    producer.close()

    print("\nScraping finalizado!")