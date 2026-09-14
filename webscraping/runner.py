import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import freeze_support
from datetime import datetime, timedelta

from kafka import KafkaProducer

from webscraping.browser_skiplagged import Browser_skiplagged
from webscraping.url_builder import urls_builder
from database.db_connection import get_lina_connection

# ============================================================
# CONFIGURAÇÕES DE LOTE E PAUSA
# ============================================================

MAX_WORKERS = 3
TAMANHO_LOTE = 3       # Quantos pares de aeroportos pegar por vez
TEMPO_PAUSA_LOTE = 180   # Segundos de descanso entre um lote e outro (para não tomar block)
DIAS_A_FRENTE = 2       # Defina quantos dias a frente quer rodar (0 = só hoje)

KAFKA_TOPIC = "raw.flights_scrapy"

producer = KafkaProducer(
    bootstrap_servers="192.168.0.33:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ============================================================
# FUNÇÕES DE BANCO (CONTROLE DE LOTE)
# ============================================================
AEROPORTOS_VALIDOS = ['CNF', 'CWB', 'NAT', 'JPA', 'MCZ', 'AJU', 'MAO', 'BEL',
'CGB', 'CGR', 'IGU', 'BPS', 'IOS', 'JOI', 'NVT', 'CXJ',
'JDO', 'PMW', 'RBR', 'PVH', 'BVB', 'STM', 'MCP', 'IMP',
'RAO', 'SJP', 'UDI', 'GYN', 'LDB', 'MGF']  # Coloque os IATA que você quer consultar

def buscar_lote_pendente(tamanho):
    """Busca um lote de pares apenas dos aeroportos permitidos que ainda não foram rodados"""
    conn = get_lina_connection()
    try:
        cursor = conn.cursor()
        
        # Cria a string de placeholders para a query (%s, %s, %s...)
        formatos = ','.join(['%s'] * len(AEROPORTOS_VALIDOS))
        
        query = f"""
            SELECT aer_de, aer_para
            FROM flights.aeroportos_config
            WHERE entra_busca = 'N' 
              AND aer_para IS NOT NULL
              AND aer_de IN ({formatos})
            LIMIT %s
        """
        
        # Junta os aeroportos válidos com o tamanho do lote para os parâmetros da query
        parametros = list(AEROPORTOS_VALIDOS) + [tamanho]
        
        cursor.execute(query, parametros)
        return cursor.fetchall()
    finally:
        conn.close()
def marcar_como_processado(pares):
    """Atualiza o status dos pares que acabaram de rodar para 'S' (ou 'F')"""
    if not pares:
        return
    
    conn = get_lina_connection()
    try:
        cursor = conn.cursor()
        # Monta a query para atualizar o lote processado
        # pares é uma lista de tuplas [(aer_de, aer_para), ...]
        query = """
            UPDATE flights.aeroportos_config
            SET entra_busca = 'S'
            WHERE aer_de = %s AND aer_para = %s
        """
        cursor.executemany(query, pares)
        conn.commit()
    finally:
        conn.close()

# ============================================================
# SCRAPING E DATAS (Igual ao seu)
# ============================================================

def process_skipplagged(url):
    site = Browser_skiplagged(url)
    try:
        site.load_page()
        return site.get_flights_info_skipplagged()
    except Exception as e:
        print(f"Erro no Skiplagged: {url} | Detalhe: {e}")
        return []
    finally:
        site.quit()

def processar_par(aer_de, aer_para, start_date):
    url = urls_builder.build_skiplagged_url(
        origin=aer_de, destination=aer_para, departure_date=start_date
    )
    resultado = process_skipplagged(url)
    return aer_de, aer_para, start_date, resultado

def gerar_datas(data_inicial, dias_a_frente):
    inicio = datetime.strptime(data_inicial, "%Y-%m-%d")
    for i in range(dias_a_frente + 1):
        data = inicio + timedelta(days=i)
        yield data.strftime("%Y-%m-%d")

# ============================================================
# LOOP PRINCIPAL EM LOTES
# ============================================================

def executar_sistema_em_lotes():
    print("=" * 80)
    print("INICIANDO PROCESSAMENTO EM LOTES COM PAUSA")
    print("=" * 80)

    data_inicial = datetime.now().strftime("%Y-%m-%d")

    while True:
        # 1. Pega o próximo lote pendente do banco
        pares_lote = buscar_lote_pendente(TAMANHO_LOTE)

        if not pares_lote:
            print("\n[SUCESSO] Todos os registros da tabela foram processados!")
            break

        print(f"\n--- [NOVO LOTE] Pegando {len(pares_lote)} pares do banco ---")

        # 2. Monta as tarefas para esse lote
        tarefas = []
        for aer_de, aer_para in pares_lote:
            for data in gerar_datas(data_inicial, DIAS_A_FRENTE):
                tarefas.append((aer_de, aer_para, data))

        print(f"Total de requisições neste lote: {len(tarefas)}")

        # 3. Executa o lote em paralelo com os workers
        pares_processados_neste_lote = set()
        
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(processar_par, origem, destino, data): (origem, destino)
                for origem, destino, data in tarefas
            }

            for future in as_completed(futures):
                origem, destino = futures[future]
                try:
                    origem, destino, data, resultado = future.result()
                    pares_processados_neste_lote.add((origem, destino))

                    if resultado:
                        for voo in resultado:
                            producer.send(KAFKA_TOPIC, value=voo)
                        producer.flush()
                        print(f"[OK] {origem}->{destino} ({data}): {len(resultado)} enviados")
                    else:
                        print(f"[VAZIO] {origem}->{destino} ({data})")

                except Exception as e:
                    print(f"[ERRO] {origem}->{destino}: {e}")

        # 4. Atualiza no banco que esses pares foram concluídos (muda para 'S')
        marcar_como_processado(list(pares_processados_neste_lote))
        print(f"[LOTE CONCLUÍDO] Pares atualizados no banco para 'S'.")

        # 5. Pausa de respiro para o IP não cair em Block/CAPTCHA
        print(f"Dormindo por {TEMPO_PAUSA_LOTE} segundos para respirar...")
        time.sleep(TEMPO_PAUSA_LOTE)

if __name__ == "__main__":
    freeze_support()
    try:
        executar_sistema_em_lotes()
    finally:
        producer.flush()
        producer.close()