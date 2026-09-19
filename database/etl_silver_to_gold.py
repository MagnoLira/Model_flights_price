import logging

from db_connection import get_lina_connection


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("silver-to-gold")


# ============================================================
# SQL
# ============================================================

CREATE_SCHEMA_QUERY = """
CREATE SCHEMA IF NOT EXISTS gold;
"""


CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS gold.flight_price_features (

    raw_id UUID PRIMARY KEY,

    flight_from VARCHAR(3) NOT NULL,
    flight_to VARCHAR(3) NOT NULL,
    route VARCHAR(10) NOT NULL,

    company VARCHAR(150),

    departure_minutes INTEGER,
    arrival_minutes INTEGER,

    duration_minutes INTEGER,
    stops INTEGER,
    self_transfer BOOLEAN,

    connection_count INTEGER,

    days_until_departure INTEGER,

    departure_weekday INTEGER,
    search_weekday INTEGER,

    departure_month INTEGER,
    search_hour INTEGER,

    search_timestamp TIMESTAMP,

    price_usd NUMERIC(12,2),

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


# ============================================================
# MIGRATION
# ============================================================

# Importante:
# CREATE TABLE IF NOT EXISTS não adiciona novas colunas
# em uma tabela que já existe.
#
# Portanto fazemos uma migration explícita.

ALTER_TABLE_QUERY = """
ALTER TABLE gold.flight_price_features
ADD COLUMN IF NOT EXISTS search_timestamp TIMESTAMP;
"""


# ============================================================
# INDEXES
# ============================================================

CREATE_INDEXES_QUERY = """
CREATE INDEX IF NOT EXISTS idx_gold_route
ON gold.flight_price_features(route);

CREATE INDEX IF NOT EXISTS idx_gold_company
ON gold.flight_price_features(company);

CREATE INDEX IF NOT EXISTS idx_gold_days_until_departure
ON gold.flight_price_features(days_until_departure);

CREATE INDEX IF NOT EXISTS idx_gold_route_days
ON gold.flight_price_features(
    route,
    days_until_departure
);

CREATE INDEX IF NOT EXISTS idx_gold_search_timestamp
ON gold.flight_price_features(search_timestamp);
"""


# ============================================================
# BACKFILL
# ============================================================

# Corrige registros antigos que entraram na Gold
# antes de search_timestamp começar a ser persistido.

BACKFILL_SEARCH_TIMESTAMP_QUERY = """
UPDATE gold.flight_price_features g

SET search_timestamp = s.search_timestamp

FROM silver.flights_scrapy s

WHERE g.raw_id = s.raw_id

  AND g.search_timestamp IS NULL

  AND s.search_timestamp IS NOT NULL;
"""


# ============================================================
# INSERT SILVER -> GOLD
# ============================================================

INSERT_QUERY = """
INSERT INTO gold.flight_price_features (

    raw_id,

    flight_from,
    flight_to,
    route,

    company,

    departure_minutes,
    arrival_minutes,

    duration_minutes,
    stops,
    self_transfer,

    connection_count,

    days_until_departure,

    departure_weekday,
    search_weekday,

    departure_month,
    search_hour,

    search_timestamp,

    price_usd
)

SELECT

    s.raw_id,

    s.flight_from,
    s.flight_to,

    s.flight_from
        || '_'
        || s.flight_to
        AS route,

    s.company,

    CASE
        WHEN s.departure_time IS NOT NULL
        THEN
            EXTRACT(
                HOUR FROM s.departure_time
            )::INTEGER * 60
            +
            EXTRACT(
                MINUTE FROM s.departure_time
            )::INTEGER

        ELSE NULL
    END AS departure_minutes,


    CASE
        WHEN s.arrival_time IS NOT NULL
        THEN
            EXTRACT(
                HOUR FROM s.arrival_time
            )::INTEGER * 60
            +
            EXTRACT(
                MINUTE FROM s.arrival_time
            )::INTEGER

        ELSE NULL
    END AS arrival_minutes,


    s.duration_minutes,

    s.stops,

    s.self_transfer,


    CARDINALITY(
        s.connection_airports
    ) AS connection_count,


    (
        s.ticket_date
        -
        s.search_timestamp::date
    ) AS days_until_departure,


    EXTRACT(
        ISODOW
        FROM s.ticket_date
    )::INTEGER
    AS departure_weekday,


    EXTRACT(
        ISODOW
        FROM s.search_timestamp
    )::INTEGER
    AS search_weekday,


    EXTRACT(
        MONTH
        FROM s.ticket_date
    )::INTEGER
    AS departure_month,


    EXTRACT(
        HOUR
        FROM s.search_timestamp
    )::INTEGER
    AS search_hour,


    s.search_timestamp,


    s.price_original
    AS price_usd


FROM silver.flights_scrapy s


LEFT JOIN gold.flight_price_features g

    ON g.raw_id = s.raw_id


WHERE g.raw_id IS NULL

  AND s.mode = 'PRODUCTION'

  AND s.website = 'skiplagged'

  AND s.ticket_date IS NOT NULL

  AND s.search_timestamp IS NOT NULL

  AND s.ticket_date >= s.search_timestamp::date

  AND s.price_original IS NOT NULL

  AND s.price_original > 0

  AND s.flight_from IS NOT NULL

  AND s.flight_to IS NOT NULL


ON CONFLICT (raw_id)
DO NOTHING;
"""


# ============================================================
# QUALITY QUERIES
# ============================================================

COUNT_SILVER_QUERY = """
SELECT COUNT(*)

FROM silver.flights_scrapy

WHERE mode = 'PRODUCTION'
  AND website = 'skiplagged';
"""


COUNT_GOLD_QUERY = """
SELECT COUNT(*)

FROM gold.flight_price_features;
"""


COUNT_NULL_SEARCH_TIMESTAMP_QUERY = """
SELECT COUNT(*)

FROM gold.flight_price_features

WHERE search_timestamp IS NULL;
"""


GOLD_SUMMARY_QUERY = """
SELECT

    COUNT(*) AS total,

    COUNT(DISTINCT route) AS routes,

    COUNT(DISTINCT company) AS companies,

    MIN(days_until_departure) AS min_days,

    MAX(days_until_departure) AS max_days,

    MIN(price_usd) AS min_price,

    ROUND(
        AVG(price_usd),
        2
    ) AS avg_price,

    MAX(price_usd) AS max_price,

    MIN(search_timestamp)
        AS first_search_timestamp,

    MAX(search_timestamp)
        AS last_search_timestamp

FROM gold.flight_price_features;
"""


# ============================================================
# DATABASE SETUP
# ============================================================

def setup_gold(conn):

    logger.info(
        "Criando schema e tabela Gold caso necessário..."
    )

    with conn.cursor() as cursor:

        cursor.execute(
            CREATE_SCHEMA_QUERY
        )

        cursor.execute(
            CREATE_TABLE_QUERY
        )

        # --------------------------------------------
        # Migration para tabelas já existentes
        # --------------------------------------------

        cursor.execute(
            ALTER_TABLE_QUERY
        )

        cursor.execute(
            CREATE_INDEXES_QUERY
        )

    conn.commit()


# ============================================================
# BACKFILL
# ============================================================

def backfill_gold(conn):

    logger.info(
        "Executando backfill de search_timestamp..."
    )

    with conn.cursor() as cursor:

        cursor.execute(
            BACKFILL_SEARCH_TIMESTAMP_QUERY
        )

        updated = cursor.rowcount

    conn.commit()

    logger.info(
        "Registros corrigidos no backfill: %s",
        updated
    )

    return updated


# ============================================================
# LOAD GOLD
# ============================================================

def load_gold(conn):

    logger.info(
        "Iniciando carga Silver -> Gold"
    )

    with conn.cursor() as cursor:

        cursor.execute(
            INSERT_QUERY
        )

        inserted = cursor.rowcount

    conn.commit()

    logger.info(
        "Registros inseridos na Gold: %s",
        inserted
    )

    return inserted


# ============================================================
# VALIDATION
# ============================================================

def validate_gold(conn):

    with conn.cursor() as cursor:

        # ----------------------------------------------------
        # Silver
        # ----------------------------------------------------

        cursor.execute(
            COUNT_SILVER_QUERY
        )

        silver_count = (
            cursor.fetchone()[0]
        )

        # ----------------------------------------------------
        # Gold
        # ----------------------------------------------------

        cursor.execute(
            COUNT_GOLD_QUERY
        )

        gold_count = (
            cursor.fetchone()[0]
        )

        # ----------------------------------------------------
        # Timestamp NULL
        # ----------------------------------------------------

        cursor.execute(
            COUNT_NULL_SEARCH_TIMESTAMP_QUERY
        )

        null_search_timestamp = (
            cursor.fetchone()[0]
        )

        # ----------------------------------------------------
        # Summary
        # ----------------------------------------------------

        cursor.execute(
            GOLD_SUMMARY_QUERY
        )

        summary = (
            cursor.fetchone()
        )

    logger.info(
        "Silver PRODUCTION: %s",
        silver_count
    )

    logger.info(
        "Gold: %s",
        gold_count
    )

    logger.info(
        "Gold search_timestamp NULL: %s",
        null_search_timestamp
    )

    logger.info(
        (
            "Gold summary | "
            "total=%s | "
            "routes=%s | "
            "companies=%s | "
            "days=%s..%s | "
            "price=%s..%s | "
            "avg=%s | "
            "search=%s..%s"
        ),

        summary[0],  # total
        summary[1],  # routes
        summary[2],  # companies

        summary[3],  # min days
        summary[4],  # max days

        summary[5],  # min price
        summary[7],  # max price
        summary[6],  # avg price

        summary[8],  # first search
        summary[9],  # last search
    )

    # --------------------------------------------------------
    # Quality Gate
    # --------------------------------------------------------

    if null_search_timestamp > 0:

        raise RuntimeError(
            "Gold contém "
            f"{null_search_timestamp} registro(s) "
            "com search_timestamp NULL."
        )


# ============================================================
# MAIN
# ============================================================

def run_etl():

    conn = get_lina_connection()

    try:

        # ----------------------------------------------------
        # Schema / migrations
        # ----------------------------------------------------

        setup_gold(
            conn
        )

        # ----------------------------------------------------
        # Corrige histórico
        # ----------------------------------------------------

        backfill_gold(
            conn
        )

        # ----------------------------------------------------
        # Novos registros
        # ----------------------------------------------------

        load_gold(
            conn
        )

        # ----------------------------------------------------
        # Quality checks
        # ----------------------------------------------------

        validate_gold(
            conn
        )

    except Exception:

        conn.rollback()

        logger.exception(
            "Erro durante ETL Silver -> Gold"
        )

        raise

    finally:

        conn.close()


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":

    run_etl()