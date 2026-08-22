from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import islice
from datetime import datetime
from multiprocessing import freeze_support
from webscraping.browser_skiplagged import Browser_skiplagged
from webscraping.url_builder import urls_builder
import traceback
import json
from kafka import KafkaProducer


# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='192.168.0.33:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def process_skipplagged(url_info):
    url = url_info["url"] if isinstance(url_info, dict) else url_info
    site = Browser_skiplagged(url)
    try:
        site.load_page()
        return site.get_flights_info_skipplagged()
    except Exception as e:
        print(f"Erro no skipplagged ({url}): {e}")
        return []
    finally:
        site.quit()


# --- Função de execução em batches ---
def process_in_batchs(urls, funcao_processamento, batch_size=3):
    for i, chunk in enumerate(chunked(urls, batch_size)):
        print(f"\n--- Processando batch {i+1} com {len(chunk)} URLs ---")
        with ProcessPoolExecutor(max_workers=batch_size) as executor:
            futures = [executor.submit(funcao_processamento, url_info) for url_info in chunk]
            for future in as_completed(futures):
                resultado = future.result()
                if resultado:
                    for r in resultado:
                        producer.send('raw.flights_scrapy', value=r)
                    print(f'it was sent {len(resultado)} flights to kafka')


def run_scraping(payload):
    urls_skip = urls_builder.gerar_urls(urls_builder.build_skiplagged_url, payload)

    process_in_batchs(urls_skip, process_skipplagged, batch_size=3)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    freeze_support()
    
    payload_teste = {
        "flight_from": "GRU",
        "flight_to": "GYN",
        "start_date": "2026-09-02",
        "final_date": "2026-09-02"
    }
    
    print("Iniciando o scraping de teste...")
    run_scraping(payload_teste)
    print("Scraping finalizado!")