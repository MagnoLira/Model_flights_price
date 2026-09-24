import json
import random
import time
from datetime import datetime, timedelta
from multiprocessing import freeze_support

from kafka import KafkaProducer

from webscraping.browser_skiplagged import Browser_skiplagged
from webscraping.url_builder import urls_builder


# ============================================================
# CONFIGURAÇÃO
# ============================================================

KAFKA_TOPIC = "raw.flights_scrapy"
KAFKA_BOOTSTRAP = "192.168.0.33:9092"


# ============================================================
# OBJETIVO
# ============================================================
#
# Este coletor NÃO busca ampliar cobertura de rotas.
#
# Objetivo:
#
#   observar as MESMAS rotas em vários dias diferentes
#
# Isso permite posteriormente construir:
#
# - preço anterior
# - variação de preço
# - média móvel
# - mínimo recente
# - tendência
# - dias desde o menor preço
#
# ============================================================


# ============================================================
# ROTAS FIXAS
# ============================================================
#
# Mantemos uma coorte fixa.
#
# Temos:
# - hubs grandes
# - Nordeste
# - Sul
# - Centro-Oeste
# - Norte
# - rotas curtas e longas
#
# Origem -> destino é tratada como rota diferente de
# destino -> origem.
# ============================================================

ROTAS_HISTORICAS = [

    # --------------------------------------------------------
    # São Paulo <-> Nordeste
    # --------------------------------------------------------

    ("GRU", "FOR"),
    ("FOR", "GRU"),

    ("GRU", "REC"),
    ("REC", "GRU"),

    ("GRU", "SSA"),
    ("SSA", "GRU"),

    # --------------------------------------------------------
    # Brasília <-> Nordeste
    # --------------------------------------------------------

    ("BSB", "FOR"),
    ("FOR", "BSB"),

    ("BSB", "REC"),
    ("REC", "BSB"),

    ("BSB", "SSA"),
    ("SSA", "BSB"),

    # --------------------------------------------------------
    # Belo Horizonte <-> Nordeste
    # --------------------------------------------------------

    ("CNF", "FOR"),
    ("FOR", "CNF"),

    ("CNF", "REC"),
    ("REC", "CNF"),

    ("CNF", "SSA"),
    ("SSA", "CNF"),

    # --------------------------------------------------------
    # Nordeste interno
    # --------------------------------------------------------

    ("FOR", "REC"),
    ("REC", "FOR"),

    ("FOR", "SSA"),
    ("SSA", "FOR"),

    ("REC", "SSA"),
    ("SSA", "REC"),

    # --------------------------------------------------------
    # Grandes hubs
    # --------------------------------------------------------

    ("GRU", "BSB"),
    ("BSB", "GRU"),

    ("GRU", "CNF"),
    ("CNF", "GRU"),

    ("GRU", "GIG"),
    ("GIG", "GRU"),

    # --------------------------------------------------------
    # Sul
    # --------------------------------------------------------

    ("GRU", "POA"),
    ("POA", "GRU"),

    ("GRU", "CWB"),
    ("CWB", "GRU"),

    ("GRU", "FLN"),
    ("FLN", "GRU"),

    # --------------------------------------------------------
    # Norte / Centro-Oeste
    # --------------------------------------------------------

    ("GRU", "MAO"),
    ("MAO", "GRU"),

    ("BSB", "CGB"),
    ("CGB", "BSB"),
]


# ============================================================
# HORIZONTES
# ============================================================
#
# Mantemos exatamente os horizontes que já mostraram
# boa cobertura no dataset.
# ============================================================

HORIZONTES_DIAS = [
    1,
    3,
    7,
    14,
    30,
    60,
]


# ============================================================
# PAUSAS
# ============================================================
#
# Conservadoras.
#
# Não existe paralelismo.
# Uma consulta por vez.
# ============================================================

PAUSA_ENTRE_DATAS_MIN = 35
PAUSA_ENTRE_DATAS_MAX = 70

PAUSA_ENTRE_ROTAS_MIN = 180
PAUSA_ENTRE_ROTAS_MAX = 300


# ============================================================
# FREQUÊNCIA DOS CICLOS
# ============================================================
#
# Queremos aproximadamente UMA coleta por dia.
#
# Importante:
#
# não dormimos 24h DEPOIS de terminar o ciclo.
#
# Medimos quanto o ciclo levou e esperamos somente o restante
# necessário para completar ~24h desde o início do ciclo.
#
# Assim evitamos:
#
# ciclo de 4h + sleep de 24h = coleta a cada 28h.
# ============================================================

INTERVALO_CICLO_SEGUNDOS = 24 * 60 * 60


# ============================================================
# KAFKA
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,

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
# SCRAPER
# ============================================================

def process_skiplagged(url):
    """
    Retorna:

        lista -> consulta executada com sucesso
        None  -> erro real no scraper

    Assim conseguimos distinguir:

        []   = consulta válida, mas sem voos
        None = scraper falhou
    """

    site = Browser_skiplagged(
        url
    )

    try:
        site.load_page()

        resultado = (
            site
            .get_flights_info_skipplagged()
        )

        return resultado

    except Exception as e:
        print(
            f"[ERRO SCRAPER] "
            f"{url} | {e}"
        )

        return None

    finally:
        try:
            site.quit()

        except Exception:
            pass


# ============================================================
# DATAS
# ============================================================

def gerar_datas():
    """
    Usa um único instante-base para todos os horizontes
    da rota/ciclo.
    """

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

def enviar_para_kafka(voos):
    enviados = 0

    for voo in voos:

        future = producer.send(
            KAFKA_TOPIC,
            value=voo
        )

        # Só contabiliza depois da confirmação do broker.
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
        f"ROTA HISTÓRICA: "
        f"{origem} -> {destino}"
    )

    print("=" * 80)

    consultas_ok = 0
    consultas_vazias = 0
    consultas_erro = 0
    voos_enviados = 0

    datas = list(
        gerar_datas()
    )

    for indice, (
        horizonte,
        data
    ) in enumerate(datas):

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
                process_skiplagged(
                    url
                )
            )

            # ------------------------------------------------
            # Erro real
            # ------------------------------------------------

            if resultado is None:

                consultas_erro += 1

                print(
                    f"[ERRO] "
                    f"{origem}->{destino} | "
                    f"D+{horizonte}"
                )

            # ------------------------------------------------
            # Consulta válida, mas nenhuma opção encontrada
            # ------------------------------------------------

            elif len(resultado) == 0:

                consultas_vazias += 1

                print(
                    f"[VAZIO] "
                    f"{origem}->{destino} | "
                    f"D+{horizonte}"
                )

            # ------------------------------------------------
            # Temos voos
            # ------------------------------------------------

            else:

                enviados = (
                    enviar_para_kafka(
                        resultado
                    )
                )

                consultas_ok += 1
                voos_enviados += enviados

                print(
                    f"[OK] "
                    f"{origem}->{destino} | "
                    f"D+{horizonte} | "
                    f"{enviados} voos enviados"
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
        # Pausa entre horizontes da mesma rota
        # ----------------------------------------------------

        if indice < (
            len(datas) - 1
        ):

            dormir_aleatorio(
                PAUSA_ENTRE_DATAS_MIN,
                PAUSA_ENTRE_DATAS_MAX,
                (
                    f"entre horizontes "
                    f"{origem}->{destino}"
                )
            )

    print()

    print(
        f"[RESUMO ROTA] "
        f"{origem}->{destino} | "
        f"OK={consultas_ok} | "
        f"VAZIO={consultas_vazias} | "
        f"ERRO={consultas_erro} | "
        f"VOOS={voos_enviados}"
    )

    return {
        "ok": consultas_ok,
        "vazio": consultas_vazias,
        "erro": consultas_erro,
        "voos": voos_enviados,
    }


# ============================================================
# CICLO COMPLETO
# ============================================================

def executar_ciclo():
    """
    Percorre sempre a MESMA coorte de rotas.
    """

    total_rotas = 0
    total_voos = 0
    total_ok = 0
    total_vazias = 0
    total_erros = 0

    for indice, (
        origem,
        destino
    ) in enumerate(
        ROTAS_HISTORICAS
    ):

        try:

            resultado = (
                processar_rota(
                    origem,
                    destino
                )
            )

            total_rotas += 1

            total_voos += (
                resultado["voos"]
            )

            total_ok += (
                resultado["ok"]
            )

            total_vazias += (
                resultado["vazio"]
            )

            total_erros += (
                resultado["erro"]
            )

        except Exception as e:

            total_erros += 1

            print(
                f"[ERRO FATAL ROTA] "
                f"{origem}->{destino} | "
                f"{e}"
            )

        # ----------------------------------------------------
        # Pausa entre rotas
        # ----------------------------------------------------

        if indice < (
            len(ROTAS_HISTORICAS) - 1
        ):

            dormir_aleatorio(
                PAUSA_ENTRE_ROTAS_MIN,
                PAUSA_ENTRE_ROTAS_MAX,
                "entre rotas históricas"
            )

    return {
        "rotas": total_rotas,
        "voos": total_voos,
        "ok": total_ok,
        "vazio": total_vazias,
        "erro": total_erros,
    }


# ============================================================
# LOOP HISTÓRICO
# ============================================================

def executar_historico():

    print("=" * 80)
    print("COLETOR DE PROFUNDIDADE TEMPORAL")
    print("=" * 80)

    print(
        f"Rotas fixas: "
        f"{len(ROTAS_HISTORICAS)}"
    )

    print(
        "Horizontes:",
        HORIZONTES_DIAS
    )

    print(
        "Frequência alvo: "
        "aproximadamente 1 ciclo por dia"
    )

    print(
        "Paralelismo: DESATIVADO"
    )

    print(
        "Kafka:",
        KAFKA_TOPIC
    )

    numero_ciclo = 0

    while True:

        numero_ciclo += 1

        inicio_timestamp = (
            time.time()
        )

        inicio = (
            datetime.now()
        )

        print()
        print()
        print("#" * 80)

        print(
            f"[CICLO {numero_ciclo}] "
            f"INÍCIO: "
            f"{inicio.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        print("#" * 80)

        try:

            resumo = (
                executar_ciclo()
            )

            fim = (
                datetime.now()
            )

            duracao = (
                time.time()
                - inicio_timestamp
            )

            print()
            print("=" * 80)

            print(
                f"[CICLO {numero_ciclo} CONCLUÍDO]"
            )

            print(
                f"Início: "
                f"{inicio.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            print(
                f"Fim: "
                f"{fim.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            print(
                f"Duração: "
                f"{duracao / 3600:.2f} horas"
            )

            print(
                f"Rotas processadas: "
                f"{resumo['rotas']}"
            )

            print(
                f"Consultas OK: "
                f"{resumo['ok']}"
            )

            print(
                f"Consultas vazias: "
                f"{resumo['vazio']}"
            )

            print(
                f"Erros: "
                f"{resumo['erro']}"
            )

            print(
                f"Voos enviados ao Kafka: "
                f"{resumo['voos']}"
            )

            print("=" * 80)

        except Exception as e:

            print(
                f"[ERRO CICLO] {e}"
            )

            print(
                "[SEGURANÇA] "
                "Dormindo 30 minutos antes "
                "de nova tentativa."
            )

            time.sleep(
                30 * 60
            )

            continue

        # ====================================================
        # AGUARDA PRÓXIMO CICLO
        # ====================================================
        #
        # Se o ciclo demorou 4 horas:
        #
        #     espera ~20 horas
        #
        # e não 24 horas.
        #
        # Assim buscamos manter aproximadamente uma coleta
        # por dia.
        # ====================================================

        duracao_ciclo = (
            time.time()
            - inicio_timestamp
        )

        espera = max(
            60 * 60,
            (
                INTERVALO_CICLO_SEGUNDOS
                - duracao_ciclo
            )
        )

        proximo_ciclo = (
            datetime.now()
            + timedelta(
                seconds=espera
            )
        )

        print()

        print(
            "[DESCANSO] "
            f"Dormindo {espera / 3600:.2f} horas."
        )

        print(
            "[PRÓXIMO CICLO] "
            f"Aproximadamente em "
            f"{proximo_ciclo.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        time.sleep(
            espera
        )


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
            "[STOP] "
            "Coletor encerrado manualmente."
        )

    finally:

        try:
            producer.flush()

        finally:
            producer.close()