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
# "data_json":"{\"raw_text\": \"6h\\n1 stop\\nGOL Linhas Aereas\\n11:00am\\nMCZ\\nGIG\\n5:15pm\\nGYN\\n$163\", \"companhia_bruta\": \"GOL Linhas Aereas\", \"preco_bruto\": null, \"hora_saida_bruta\": \"11:00am\", \"hora_chegada_bruta\": \"5:15pm\", \"data_busca\": \"2025-06-16 21:50:48\", \"link_emissao\": \"https://skiplagged.com/flights/MCZ/GYN/2025-07-10\", \"site\": \"skiplagged\", \"solicitation_id\": \"904#$382\"}","created_at":1750112894433915,"mode":"TESTING"},
# "source":{"version":"2.5.4.Final","connector":"postgresql","name":"lina","ts_ms":1750123694396,"snapshot":"false","db":"lina","sequence":"[\"28803800\",\"28803800\"]","schema":"raw","table":"flights_scrapy","txId":863,"lsn":28803800,"xmin":null},"op":"c","ts_ms":1750123694727,"transaction":null}
### 2 - LATAM
# {"before":null,"after":{"id":"ae899a52-3e1a-4cc6-a770-80691c7ae252",
# "data_json":"{\"raw_text\": \"VOO . HORA DE SA\\u00cdDA 17:10, PARTIDA DE MACEI\\u00d3, AEROPORTO MACEIO, HORA DE CHEGADA 8:45 DO DIA SEGUINTE, EM GOI\\u00c2NIA, AEROPORTO GOIANIA. VOO 2 PARADAS, COM DURA\\u00c7\\u00c3O TOTAL DE 15 HORAS 35 MINUTOS. PRE\\u00c7O DE UM ADULTO A PARTIR DE 1366,14 REAIS BRASILEIROS. OPERADO PELA LATAM AIRLINES BRASIL.\", \"link_emissao\": \"https://www.latamairlines.com/br/pt/oferta-voos?origin=MCZ&outbound=2025-07-10T15%3A00%3A00.000Z&destination=GYN&adt=1&chd=0&inf=0&trip=OW&cabin=Economy&redemption=false&sort=RECOMMENDED\", \"site\": \"latam\", \"data_busca\": \"2025-06-16 21:50:09\", \"solicitation_id\": \"904#$382\"}","created_at":1750112894110589,"mode":"TESTING"},
# "source":{"version":"2.5.4.Final","connector":"postgresql","name":"lina","ts_ms":1750123694065,"snapshot":"false","db":"lina","sequence":"[\"28793808\",\"28793808\"]","schema":"raw","table":"flights_scrapy","txId":850,"lsn":28793808,"xmin":null},"op":"c","ts_ms":1750123694203,"transaction":null}




from kafka import KafkaConsumer
import json
import logging
import re
from decimal import Decimal
from datetime import datetime
from db_connection import get_lina_connection

# Configura logs para debug
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")

# KAFKA VARIABLES
KAFKA_TOPIC = 'lina.raw.flights_scrapy'
KAFKA_BOOTSTRAP_SERVERS = ['192.168.0.33:9092']

# DB CONNECTION 
conn = get_lina_connection()
cursor = conn.cursor()

insert_query = """INSERT INTO silver.flights_scrapy (
  solicitation_id,
  flight_from,
  flight_to,
  company,
  exit_hour,
  entry_hour,
  miles_cost,
  reais_cost,
  emission_type,
  ticket_date,
  search_date,
  emission_link,
  website,
  mode,
  inserted_at
) VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s)"""

# Support function
def to_date_or_none(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date() if value else None
    except:
        return None
def to_time_or_none(value):
    try:
        return datetime.strptime(value, "%H:%M").time() if value else None
    except:
        return None
    
# Support 
def to_date_flex_or_none(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None
def to_time_flex_or_none(value):
    if not value:
        return None
    for fmt in ("%I:%M%p", "%H:%M"):
        try:
            return datetime.strptime(value.strip().lower(), fmt).time()
        except ValueError:
            continue
    return None



def extract_from_link(url):
    match = re.search(r"flights/([A-Z]{3})/([A-Z]{3})/(\d{4}-\d{2}-\d{2})", url)
    return match.groups() if match else (None, None, None)

# class etl raw to silver
class etl_raw_silver():
    def __init__(self,payload):
        after = payload.get("after")
        self.raw_data = json.loads(after.get("data_json")) if after else {}
        self.url = self.raw_data.get("link_emissao","")
        self.website = self.raw_data.get("site","").lower()
        self.data_busca =self.raw_data.get("data_busca")
        self.mode = after.get("mode")
        self.result = None

    def transform(self):
        if self.website == "latam":
            return self.etl_latam()
        elif self.website == "skiplagged":
            return self.etl_skiplagged()
        else:
            logging.warning("Website não reconhecido: %s", self.website)
    def etl_latam(self):
        texto = self.raw_data.get("raw_text", "")
        saida_hora = re.search(r'HORA DE SAÍDA (\d{1,2}:\d{2})', texto)
        chegada_hora = re.search(r'HORA DE CHEGADA (\d{1,2}:\d{2})', texto)
        valor = re.search(r'PREÇO DE UM ADULTO.*?([\d\.,]+)', texto)
        companhia = re.search(r'OPERADO PELA (.+?)(?:\.|$)', texto)
        aeroportos_raw = re.findall(r'AEROPORTO (.+?)\,|AEROPORTO (.+?)\.', texto)
        data_match = re.search(r"outbound=(\d{4}-\d{2}-\d{2})T", self.url)
        aeroportos_raw = re.findall(r'AEROPORTO ([^,.]+)', texto)
        aeroportos = [a.strip().title() for a in aeroportos_raw]

        if len(aeroportos) < 2:
            logging.warning(f"Não foi possível extrair aeroportos de: {texto}")
            return None

        self.result = {
            "solicitation_id": self.raw_data.get("solicitation_id"),
            "flight_from": aeroportos[0].strip().title(),
            "flight_to": aeroportos[1].strip().title(),
            "company": companhia.group(1) if companhia else None,
            "exit_hour": to_time_or_none(saida_hora.group(1)) if saida_hora else None,
            "entry_hour": to_time_or_none(chegada_hora.group(1)) if chegada_hora else None,
            "miles_cost": None,
            "reais_cost": int(float(valor.group(1).replace('.', '').replace(',', '.'))) if valor else None,
            "emission_type": "milhas" if "milha" in texto.lower() else "dinheiro",
            "ticket_date": to_date_or_none(data_match.group(1)) if data_match else None,
            "search_date": to_date_flex_or_none(self.data_busca),
            "emission_link": self.url,
            "website": self.website,
            "mode": self.mode,
            "inserted_at": datetime.now().date()
        }
        return self.result 

    def etl_skiplagged(self):
        aer_de, aer_para, data_passagem = extract_from_link(self.url)
        texto = self.raw_data.get("raw_text", "")

        
        preco_bruto = self.raw_data.get("preco_bruto", "")
        valor_reais = None
        if preco_bruto:
            try:
                valor_limpo = re.sub(r"[^\d,]", "", preco_bruto).replace(",", ".")
                valor_reais = Decimal(valor_limpo)
            except:
                pass
        else:
            valor_match = re.search(r"\$(\d+(?:\.\d{1,2})?)", texto)
            if valor_match:
                valor_reais = Decimal(valor_match.group(1))

        
        horas = re.findall(r"\b\d{1,2}:\d{2}(?:am|pm)\b", texto, flags=re.IGNORECASE)
        saida_raw = horas[0] if len(horas) >= 1 else None
        chegada_raw = horas[-1] if len(horas) >= 2 else None

        self.result = {
            "solicitation_id": self.raw_data.get("solicitation_id"),
            "flight_from": aer_de,
            "flight_to": aer_para,
            "company": self.raw_data.get("companhia_bruta"),
            "exit_hour": to_time_flex_or_none(saida_raw),
            "entry_hour": to_time_flex_or_none(chegada_raw),
            "miles_cost": None,
            "reais_cost": int(valor_reais) if valor_reais else None,
            "emission_type": "dinheiro",
            "ticket_date": to_date_or_none(data_passagem),
            "search_date": to_date_flex_or_none(self.raw_data.get("data_busca")),
            "emission_link": self.url,
            "website": self.website,
            "mode": self.raw_data.get("mode"),
            "inserted_at": datetime.now().date()
        }
        return self.result
    

# Kafka consumer
def run_consumer():
    consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='silver-flight-group')
    print(f"Consumindo do tópico '{KAFKA_TOPIC}'...\n")
    for msg in consumer:
        payload = msg.value
        process = etl_raw_silver(payload)
        result = process.transform()
        if result:
            try:
                cursor.execute(insert_query, (
                    result["solicitation_id"],
                    result["flight_from"],
                    result["flight_to"],
                    result["company"],
                    result["exit_hour"],
                    result["entry_hour"],
                    result["miles_cost"],
                    result["reais_cost"],
                    result["emission_type"],
                    result["ticket_date"],
                    result["search_date"],
                    result["emission_link"],
                    result["website"],
                    result["mode"],
                    result["inserted_at"]
                ))
                conn.commit()
                print("Registro inserido com sucesso!\n")
            except Exception as e:
                logging.error(f"Erro ao inserir no banco: {e}")
                conn.rollback()
        else:
            print("Mensagem descartada ou sem transformações aplicáveis.\n")
if __name__ == "__main__":
    run_consumer()