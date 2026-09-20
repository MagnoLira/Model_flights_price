import json
import random
import time
from datetime import datetime, timedelta
from multiprocessing import freeze_support

from kafka import KafkaProducer

from webscraping.browser_skiplagged import Browser_skiplagged
from webscraping.url_builder import urls_builder
from database.db_connection import get_lina_connection


# ============================================================
# CONFIGURAÇÃO
# ============================================================

KAFKA_TOPIC = "raw.flights_scrapy"

# ------------------------------------------------------------
# Horizonte de compra
#
# Não fazemos 0,1,2,3,...,60.
# Queremos pontos representativos sem massacrar o site.
# ------------------------------------------------------------

HORIZONTES_DIAS = [
    1,
    3,
    7,
    14,
    30,
    60,
]


# ------------------------------------------------------------
# Pausas
#
# Intencionalmente conservadoras.
# ------------------------------------------------------------

# Entre duas consultas da mesma rota
PAUSA_ENTRE_DATAS_MIN = 35
PAUSA_ENTRE_DATAS_MAX = 70

# Depois de terminar uma rota
PAUSA_ENTRE_ROTAS_MIN = 180
PAUSA_ENTRE_ROTAS_MAX = 300

# Depois de terminar uma rodada completa
# 6 horas
PAUSA_ENTRE_CICLOS = 6 * 60 * 60


# ------------------------------------------------------------
# Quantidade de pares retirada do banco por vez
#
# Deixamos 1 porque não queremos paralelismo agressivo.
# ------------------------------------------------------------

TAMANHO_LOTE = 1


# ============================================================
# AEROPORTOS
# ============================================================

#
# Um conjunto suficientemente diverso, mas não gigantesco.
#
# Inclui:
# - grandes hubs
# - Nordeste
# - Norte
# - Sul
# - Centro-Oeste
# - alguns regionais
#

AEROPORTOS_VALIDOS = [

    # São Paulo
    "GRU",
    "CGH",
    "VCP",

    # Rio
    "GIG",
    "SDU",

    # Grandes hubs
    "BSB",
    "CNF",

    # Nordeste
    "FOR",
    "REC",
    "SSA",
    "NAT",
    "JPA",
    "MCZ",
    "AJU",
    "THE",
    "SLZ",

    # Norte
    "MAO",
    "BEL",

    # Sul
    "POA",
    "FLN",
    "CWB",

    # Centro-Oeste
    "GYN",
    "CGB",
    "CGR",
]


# ============================================================
# KAFKA
# ============================================================

producer = KafkaProducer(

    bootstrap_servers="192.168.0.33:9092",

    value_serializer=lambda v: (
        json.dumps(
            v,
            ensure_ascii=False
        ).encode("utf-8")
    ),

    acks="all",
)


# ============================================================
# HELPERS
# ============================================================

def dormir_aleatorio(
    minimo,
    maximo,
    motivo
):

    segundos = random.randint(
        minimo,
        maximo
    )

    print(
        f"[PAUSA] {motivo}: "
        f"{segundos} segundos"
    )

    time.sleep(
        segundos
    )


# ============================================================
# BANCO
# ============================================================

def buscar_lote_pendente(
    tamanho
):

    """
    Busca rotas que ainda não foram processadas
    nesta rodada.

    Tanto origem quanto destino precisam estar
    dentro da lista permitida.
    """

    conn = get_lina_connection()

    try:

        cursor = conn.cursor()

        placeholders_origem = ",".join(
            ["%s"]
            * len(
                AEROPORTOS_VALIDOS
            )
        )

        placeholders_destino = ",".join(
            ["%s"]
            * len(
                AEROPORTOS_VALIDOS
            )
        )

        query = f"""
            SELECT
                aer_de,
                aer_para

            FROM flights.aeroportos_config

            WHERE entra_busca = 'N'

              AND aer_para IS NOT NULL

              AND aer_de <> aer_para

              AND aer_de IN (
                  {placeholders_origem}
              )

              AND aer_para IN (
                  {placeholders_destino}
              )

            ORDER BY
                aer_de,
                aer_para

            LIMIT %s
        """

        parametros = (
            list(
                AEROPORTOS_VALIDOS
            )
            +
            list(
                AEROPORTOS_VALIDOS
            )
            +
            [tamanho]
        )

        cursor.execute(
            query,
            parametros
        )

        return cursor.fetchall()

    finally:

        conn.close()


def marcar_como_processado(
    aer_de,
    aer_para
):

    conn = get_lina_connection()

    try:

        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE flights.aeroportos_config

            SET entra_busca = 'S'

            WHERE aer_de = %s
              AND aer_para = %s
            """,
            (
                aer_de,
                aer_para
            )
        )

        conn.commit()

    finally:

        conn.close()


def resetar_ciclo():

    """
    Quando todos os pares terminarem,
    libera novamente as rotas para uma
    nova rodada futura.

    Não cria nenhuma coluna nova.
    """

    conn = get_lina_connection()

    try:

        cursor = conn.cursor()

        placeholders_origem = ",".join(
            ["%s"]
            * len(
                AEROPORTOS_VALIDOS
            )
        )

        placeholders_destino = ",".join(
            ["%s"]
            * len(
                AEROPORTOS_VALIDOS
            )
        )

        query = f"""
            UPDATE flights.aeroportos_config

            SET entra_busca = 'N'

            WHERE entra_busca = 'S'

              AND aer_de IN (
                  {placeholders_origem}
              )

              AND aer_para IN (
                  {placeholders_destino}
              )
        """

        parametros = (
            list(
                AEROPORTOS_VALIDOS
            )
            +
            list(
                AEROPORTOS_VALIDOS
            )
        )

        cursor.execute(
            query,
            parametros
        )

        atualizados = cursor.rowcount

        conn.commit()

        print(
            f"[RESET] "
            f"{atualizados} pares "
            f"voltaram para N."
        )

    finally:

        conn.close()


# ============================================================
# SCRAPER
# ============================================================

def process_skipplagged(
    url
):

    site = Browser_skiplagged(
        url
    )

    try:

        site.load_page()

        return (
            site
            .get_flights_info_skipplagged()
        )

    except Exception as e:

        print(
            f"[ERRO SCRAPER] "
            f"{url} | {e}"
        )

        return []

    finally:

        try:

            site.quit()

        except Exception:

            pass


# ============================================================
# DATAS
# ============================================================

def gerar_datas():

    hoje = datetime.now()

    for horizonte in HORIZONTES_DIAS:

        data = (
            hoje
            + timedelta(
                days=horizonte
            )
        )

        yield (
            horizonte,
            data.strftime(
                "%Y-%m-%d"
            )
        )


# ============================================================
# KAFKA
# ============================================================

def enviar_para_kafka(
    voos
):

    enviados = 0

    for voo in voos:

        future = producer.send(
            KAFKA_TOPIC,
            value=voo
        )

        # Espera confirmação do broker.
        # Se der erro, não fingimos que enviou.
        future.get(
            timeout=30
        )

        enviados += 1

    producer.flush()

    return enviados


# ============================================================
# PROCESSAMENTO DE UMA ROTA
# ============================================================

def processar_rota(
    origem,
    destino
):

    print()
    print("=" * 80)

    print(
        f"ROTA: "
        f"{origem} -> {destino}"
    )

    print("=" * 80)

    consultas_ok = 0
    consultas_vazias = 0
    consultas_erro = 0

    for indice, (
        horizonte,
        data
    ) in enumerate(
        gerar_datas()
    ):

        print()

        print(
            f"[CONSULTA] "
            f"{origem}->{destino} | "
            f"D+{horizonte} | "
            f"{data}"
        )

        try:

            url = (
                urls_builder
                .build_skiplagged_url(
                    origin=origem,
                    destination=destino,
                    departure_date=data
                )
            )

            resultado = (
                process_skipplagged(
                    url
                )
            )

            if resultado:

                enviados = (
                    enviar_para_kafka(
                        resultado
                    )
                )

                consultas_ok += 1

                print(
                    f"[OK] "
                    f"{origem}->{destino} | "
                    f"D+{horizonte} | "
                    f"{enviados} voos enviados"
                )

            else:

                consultas_vazias += 1

                print(
                    f"[VAZIO] "
                    f"{origem}->{destino} | "
                    f"D+{horizonte}"
                )

        except Exception as e:

            consultas_erro += 1

            print(
                f"[ERRO] "
                f"{origem}->{destino} | "
                f"D+{horizonte} | "
                f"{e}"
            )

        # ----------------------------------------------------
        # Não dorme depois da última data,
        # pois teremos a pausa maior da rota.
        # ----------------------------------------------------

        if indice < (
            len(
                HORIZONTES_DIAS
            )
            - 1
        ):

            dormir_aleatorio(
                PAUSA_ENTRE_DATAS_MIN,
                PAUSA_ENTRE_DATAS_MAX,
                (
                    f"entre consultas "
                    f"{origem}->{destino}"
                )
            )

    print()

    print(
        f"[RESUMO ROTA] "
        f"{origem}->{destino} | "
        f"OK={consultas_ok} | "
        f"VAZIO={consultas_vazias} | "
        f"ERRO={consultas_erro}"
    )

    return {
        "ok":
            consultas_ok,

        "vazio":
            consultas_vazias,

        "erro":
            consultas_erro,
    }


# ============================================================
# CICLO
# ============================================================

def executar_ciclo():

    total_rotas = 0

    while True:

        pares = buscar_lote_pendente(
            TAMANHO_LOTE
        )

        if not pares:

            print()

            print("=" * 80)

            print(
                "[CICLO CONCLUÍDO] "
                "Nenhuma rota pendente."
            )

            print("=" * 80)

            return total_rotas

        for (
            origem,
            destino
        ) in pares:

            try:

                resultado = processar_rota(
                    origem,
                    destino
                )

                # ------------------------------------------------
                # Só marca como concluída depois que tentamos
                # TODOS os horizontes daquela rota.
                #
                # Mesmo consultas vazias contam como tentativa.
                # ------------------------------------------------

                marcar_como_processado(
                    origem,
                    destino
                )

                total_rotas += 1

                print(
                    f"[ROTA FINALIZADA] "
                    f"{origem}->{destino}"
                )

            except Exception as e:

                # Não marca S.
                # Na próxima passagem ela pode ser tentada novamente.
                print(
                    f"[ERRO FATAL ROTA] "
                    f"{origem}->{destino} | "
                    f"{e}"
                )

            # ------------------------------------------------
            # Pausa grande entre rotas
            # ------------------------------------------------

            dormir_aleatorio(
                PAUSA_ENTRE_ROTAS_MIN,
                PAUSA_ENTRE_ROTAS_MAX,
                "entre rotas"
            )


# ============================================================
# LOOP CONTÍNUO
# ============================================================

def executar_historico():

    print("=" * 80)

    print(
        "COLETOR HISTÓRICO DE PREÇOS"
    )

    print("=" * 80)

    print(
        "Horizontes:",
        HORIZONTES_DIAS
    )

    print(
        "Aeroportos:",
        len(
            AEROPORTOS_VALIDOS
        )
    )

    print(
        "Paralelismo: DESATIVADO"
    )

    print(
        "Consultas são executadas "
        "uma por vez."
    )

    while True:

        inicio = datetime.now()

        print()
        print("=" * 80)

        print(
            "[NOVO CICLO]",
            inicio.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        print("=" * 80)

        try:

            total_rotas = (
                executar_ciclo()
            )

            print(
                f"[CICLO] "
                f"{total_rotas} rotas "
                f"processadas."
            )

        except Exception as e:

            print(
                f"[ERRO CICLO] {e}"
            )

            # Se algo estrutural der errado,
            # não fica martelando serviço/banco/site.
            print(
                "[SEGURANÇA] "
                "Dormindo 30 minutos."
            )

            time.sleep(
                30 * 60
            )

            continue

        # ----------------------------------------------------
        # Rodada terminou.
        #
        # Não reseta e começa imediatamente.
        # Primeiro descansamos várias horas.
        # ----------------------------------------------------

        print()

        print(
            "[DESCANSO] "
            f"Ciclo completo. "
            f"Dormindo "
            f"{PAUSA_ENTRE_CICLOS / 3600:.1f} horas."
        )

        time.sleep(
            PAUSA_ENTRE_CICLOS
        )

        # ----------------------------------------------------
        # Depois do descanso, libera nova rodada.
        # ----------------------------------------------------

        resetar_ciclo()


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":

    freeze_support()

    try:

        executar_historico()

    except KeyboardInterrupt:

        print()
        print(
            "[STOP] Encerramento manual."
        )

    finally:

        try:

            producer.flush()

        finally:

            producer.close()