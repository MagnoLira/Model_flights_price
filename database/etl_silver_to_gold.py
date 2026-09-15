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

    price_usd NUMERIC(12,2),

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


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
"""


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

    price_usd
)
SELECT
    s.raw_id,

    s.flight_from,
    s.flight_to,

    s.flight_from || '_' || s.flight_to AS route,

    s.company,

    CASE
        WHEN s.departure_time IS NOT NULL THEN
            EXTRACT(HOUR FROM s.departure_time)::INTEGER * 60
            +
            EXTRACT(MINUTE FROM s.departure_time)::INTEGER
        ELSE NULL
    END AS departure_minutes,

    CASE
        WHEN s.arrival_time IS NOT NULL THEN
            EXTRACT(HOUR FROM s.arrival_time)::INTEGER * 60
            +
            EXTRACT(MINUTE FROM s.arrival_time)::INTEGER
        ELSE NULL
    END AS arrival_minutes,

    s.duration_minutes,
    s.stops,
    s.self_transfer,

    CARDINALITY(s.connection_airports) AS connection_count,

    (
        s.ticket_date
        -
        s.search_timestamp::date
    ) AS days_until_departure,

    EXTRACT(
        ISODOW FROM s.ticket_date
    )::INTEGER AS departure_weekday,

    EXTRACT(
        ISODOW FROM s.search_timestamp
    )::INTEGER AS search_weekday,

    EXTRACT(
        MONTH FROM s.ticket_date
    )::INTEGER AS departure_month,

    EXTRACT(
        HOUR FROM s.search_timestamp
    )::INTEGER AS search_hour,

    s.price_original AS price_usd

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


GOLD_SUMMARY_QUERY = """
SELECT
    COUNT(*) AS total,

    COUNT(DISTINCT route) AS routes,

    COUNT(DISTINCT company) AS companies,

    MIN(days_until_departure) AS min_days,
    MAX(days_until_departure) AS max_days,

    MIN(price_usd) AS min_price,
    ROUND(AVG(price_usd), 2) AS avg_price,
    MAX(price_usd) AS max_price

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

        cursor.execute(
            CREATE_INDEXES_QUERY
        )

    conn.commit()


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

        cursor.execute(
            COUNT_SILVER_QUERY
        )

        silver_count = cursor.fetchone()[0]

        cursor.execute(
            COUNT_GOLD_QUERY
        )

        gold_count = cursor.fetchone()[0]

        cursor.execute(
            GOLD_SUMMARY_QUERY
        )

        summary = cursor.fetchone()

    logger.info(
        "Silver PRODUCTION: %s",
        silver_count
    )

    logger.info(
        "Gold: %s",
        gold_count
    )

    logger.info(
        (
            "Gold summary | "
            "total=%s | "
            "routes=%s | "
            "companies=%s | "
            "days=%s..%s | "
            "price=%s..%s | "
            "avg=%s"
        ),
        summary[0],
        summary[1],
        summary[2],
        summary[3],
        summary[4],
        summary[5],
        summary[7],
        summary[6]
    )


# ============================================================
# MAIN
# ============================================================

def run_etl():

    conn = get_lina_connection()

    try:

        setup_gold(
            conn
        )

        load_gold(
            conn
        )

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