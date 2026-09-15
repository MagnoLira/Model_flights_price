import json
import logging
import re

from decimal import Decimal, InvalidOperation
from datetime import datetime

from db_connection import get_lina_connection


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("raw-to-silver")


# ============================================================
# SQL
# ============================================================

SELECT_QUERY = """
SELECT
    r.id,
    r.data_json,
    r.mode
FROM raw.flights_scrapy r
LEFT JOIN silver.flights_scrapy s
    ON s.raw_id = r.id
WHERE r.mode = 'PRODUCTION'
  AND r.data_hash IS NOT NULL
  AND s.raw_id IS NULL
ORDER BY r.created_at;
"""


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
            continue

    return None


def parse_time(value):
    if not value:
        return None

    value = str(value).strip().lower()

    for fmt in ("%I:%M%p", "%H:%M"):
        try:
            return datetime.strptime(value, fmt).time()
        except ValueError:
            continue

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
    except (InvalidOperation, ValueError):
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
# RAW TEXT PARSERS
# ============================================================

def extract_duration_minutes(text):

    if not text:
        return None

    # Exemplos:
    # 5h
    # 16h
    # 5h 30m

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

    # Exemplo:
    # 45m
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

    # 1 stop
    # 2 stops
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

    # --------------------------------------------------------
    # Primeiro tenta usar o campo estruturado
    # --------------------------------------------------------

    if preco_bruto not in (None, ""):

        cleaned = re.sub(
            r"[^\d.,]",
            "",
            str(preco_bruto)
        )

        if cleaned:

            # Caso padrão americano:
            # 529.00

            if "," in cleaned and "." not in cleaned:
                cleaned = cleaned.replace(",", ".")

            try:
                return Decimal(cleaned)

            except InvalidOperation:
                pass

    # --------------------------------------------------------
    # Fallback para raw_text
    #
    # $529
    # $529.50
    # $1,529
    # --------------------------------------------------------

    match = re.search(
        r"\$\s*([\d,.]+)",
        text or ""
    )

    if not match:
        return None

    value = match.group(1)

    # Skiplagged em USD:
    # 1,529.50 -> 1529.50
    value = value.replace(",", "")

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

    # Captura somente códigos IATA isolados por linha
    #
    # BSB
    # GRU
    # VCP
    # JTC

    codes = re.findall(
        r"(?m)^\s*([A-Z]{3})\s*$",
        text
    )

    connections = []

    for code in codes:

        code = code.upper()

        if code == origin:
            continue

        if code == destination:
            continue

        if code not in connections:
            connections.append(code)

    return connections


# ============================================================
# TRANSFORM RAW -> SILVER
# ============================================================

def transform_raw_row(
    raw_id,
    data_json,
    mode
):

    if not data_json:
        logger.warning(
            "Registro sem data_json | raw_id=%s",
            raw_id
        )
        return None

    # --------------------------------------------------------
    # Parse do JSON bruto
    # --------------------------------------------------------

    try:

        if isinstance(data_json, str):
            raw_data = json.loads(data_json)
        else:
            raw_data = data_json

    except json.JSONDecodeError:

        logger.exception(
            "data_json inválido | raw_id=%s",
            raw_id
        )

        return None

    # --------------------------------------------------------
    # Hoje somente Skiplagged
    # --------------------------------------------------------

    website = (
        raw_data
        .get("site", "")
        .strip()
        .lower()
    )

    if website != "skiplagged":

        logger.warning(
            "Website ignorado | raw_id=%s | website=%s",
            raw_id,
            website
        )

        return None

    # --------------------------------------------------------
    # Campos brutos
    # --------------------------------------------------------

    text = raw_data.get(
        "raw_text",
        ""
    )

    url = raw_data.get(
        "link_emissao",
        ""
    )

    # --------------------------------------------------------
    # Origem, destino e data da passagem
    # --------------------------------------------------------

    origin, destination, ticket_date = (
        extract_flight_info_from_url(url)
    )

    # --------------------------------------------------------
    # Monta registro Silver
    # --------------------------------------------------------

    result = {

        "raw_id":
            raw_id,

        "flight_from":
            origin,

        "flight_to":
            destination,

        "company":
            raw_data.get(
                "companhia_bruta"
            ),

        "departure_time":
            parse_time(
                raw_data.get(
                    "hora_saida_bruta"
                )
            ),

        "arrival_time":
            parse_time(
                raw_data.get(
                    "hora_chegada_bruta"
                )
            ),

        "duration_minutes":
            extract_duration_minutes(
                text
            ),

        "stops":
            extract_stops(
                text
            ),

        "self_transfer":
            extract_self_transfer(
                text
            ),

        "connection_airports":
            extract_connection_airports(
                text,
                origin,
                destination
            ),

        "price_original":
            extract_price(
                text,
                raw_data.get(
                    "preco_bruto"
                )
            ),

        "currency_original":
            "USD",

        "ticket_date":
            ticket_date,

        "search_timestamp":
            parse_datetime(
                raw_data.get(
                    "data_busca"
                )
            ),

        "emission_link":
            url,

        "website":
            website,

        "mode":
            mode
    }

    return result


# ============================================================
# VALIDATION
# ============================================================

def validate_result(result):

    errors = []

    if not result["raw_id"]:
        errors.append("raw_id")

    if not result["flight_from"]:
        errors.append("flight_from")

    if not result["flight_to"]:
        errors.append("flight_to")

    if result["price_original"] is None:
        errors.append("price_original")

    if result["ticket_date"] is None:
        errors.append("ticket_date")

    if result["search_timestamp"] is None:
        errors.append("search_timestamp")

    return errors


# ============================================================
# ETL
# ============================================================

def run_etl():

    conn = get_lina_connection()

    logger.info(
        "Iniciando ETL Raw -> Silver"
    )

    processed = 0
    inserted = 0
    ignored = 0
    errors_count = 0

    try:

        # ----------------------------------------------------
        # Busca somente registros ainda não presentes na Silver
        # ----------------------------------------------------

        with conn.cursor() as cursor:

            cursor.execute(
                SELECT_QUERY
            )

            rows = cursor.fetchall()

        logger.info(
            "Registros pendentes encontrados: %s",
            len(rows)
        )

        # ----------------------------------------------------
        # Processamento
        # ----------------------------------------------------

        for row in rows:

            raw_id = row[0]
            data_json = row[1]
            mode = row[2]

            processed += 1

            try:

                result = transform_raw_row(
                    raw_id=raw_id,
                    data_json=data_json,
                    mode=mode
                )

                if not result:

                    ignored += 1

                    continue

                validation_errors = (
                    validate_result(
                        result
                    )
                )

                if validation_errors:

                    logger.warning(
                        "Registro inválido | "
                        "raw_id=%s | "
                        "campos=%s",
                        raw_id,
                        validation_errors
                    )

                    errors_count += 1

                    continue

                # ------------------------------------------------
                # INSERT
                # ------------------------------------------------

                with conn.cursor() as cursor:

                    cursor.execute(
                        INSERT_QUERY,
                        result
                    )

                    if cursor.rowcount > 0:
                        inserted += 1

                conn.commit()

                logger.info(
                    "Inserido | "
                    "%s -> %s | "
                    "%s USD | "
                    "voo=%s | "
                    "busca=%s",
                    result["flight_from"],
                    result["flight_to"],
                    result["price_original"],
                    result["ticket_date"],
                    result["search_timestamp"]
                )

            except Exception:

                conn.rollback()

                errors_count += 1

                logger.exception(
                    "Erro processando raw_id=%s",
                    raw_id
                )

        # ----------------------------------------------------
        # Resultado
        # ----------------------------------------------------

        logger.info(
            "ETL finalizado | "
            "processados=%s | "
            "inseridos=%s | "
            "ignorados=%s | "
            "erros=%s",
            processed,
            inserted,
            ignored,
            errors_count
        )

    finally:

        conn.close()


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    run_etl()