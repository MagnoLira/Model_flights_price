from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import islice
from datetime import datetime
from multiprocessing import freeze_support
from webscraping.browser_latam import Browser_latam
from webscraping.browser_skiplagged import Browser_skiplagged
from webscraping.url_builder import urls_builder
import traceback
import json
from kafka import KafkaProducer






#Kafka setup
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

# --- Individual functions  ---
def process_latam(url_info):
    url = url_info["url"]
    site = Browser_latam(url)
    try:
        site.load_page()
        dados_voo = site.get_flight_info_latam()
        for voo in dados_voo:
            voo['solicitation_id'] = url_info['solicitation_id']
        return dados_voo
    except Exception as e:
        print(f"Erro no latam ({url}): {e}")
    finally:
        site.quit()


def process_skipplagged(url_info):
    url = url_info["url"]
    site = Browser_skiplagged(url)
    try:
        site.load_page()
        dados_voo = site.get_flights_info_skipplagged()
        for voo in dados_voo:
            voo['solicitation_id'] = url_info['solicitation_id']
        return dados_voo
    except Exception as e:
        print(f"Erro no skipplagged ({url}): {e}")
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
                        producer.send('raw.flights_scrapy',value=r)
                    print(f'it was send {len(resultado)} flights to kafka')
                    
def run_scraping(payload):
#    freeze_support()
    urls_latam = urls_builder.gerar_urls(urls_builder.build_latam_url, payload)
    urls_skip = urls_builder.gerar_urls(urls_builder.build_skiplagged_url, payload)

    process_in_batchs(urls_latam, process_latam, batch_size=3)
    process_in_batchs(urls_skip, process_skipplagged, batch_size=3)

    producer.flush()
    producer.close()
