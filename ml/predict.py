from pathlib import Path
from datetime import datetime

import joblib
import pandas as pd

from catboost import CatBoostRegressor


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

MODEL_DIR = (
    BASE_DIR
    / "models"
)

COLD_MODEL_PATH = (
    MODEL_DIR
    / "flight_price_catboost_cold_v3.cbm"
)

WARM_MODEL_PATH = (
    MODEL_DIR
    / "flight_price_catboost_warm_v3.cbm"
)

METADATA_PATH = (
    MODEL_DIR
    / "flight_price_catboost_v3_metadata.joblib"
)


# ============================================================
# LOAD ARTIFACTS
# ============================================================

def load_artifacts():

    if not COLD_MODEL_PATH.exists():

        raise FileNotFoundError(
            f"Modelo cold não encontrado: "
            f"{COLD_MODEL_PATH}"
        )

    if not WARM_MODEL_PATH.exists():

        raise FileNotFoundError(
            f"Modelo warm não encontrado: "
            f"{WARM_MODEL_PATH}"
        )

    if not METADATA_PATH.exists():

        raise FileNotFoundError(
            f"Metadata não encontrada: "
            f"{METADATA_PATH}"
        )

    cold_model = (
        CatBoostRegressor()
    )

    cold_model.load_model(
        str(
            COLD_MODEL_PATH
        )
    )

    warm_model = (
        CatBoostRegressor()
    )

    warm_model.load_model(
        str(
            WARM_MODEL_PATH
        )
    )

    metadata = joblib.load(
        METADATA_PATH
    )

    return (
        cold_model,
        warm_model,
        metadata
    )


# ============================================================
# GLOBAL ARTIFACTS
# ============================================================

(
    COLD_MODEL,
    WARM_MODEL,
    METADATA
) = load_artifacts()


FEATURES = (
    METADATA[
        "features"
    ]
)

CATEGORICAL_FEATURES = (
    METADATA[
        "categorical_features"
    ]
)

WARM_KNOWN_ROUTES = set(
    METADATA.get(
        "warm_known_routes",
        []
    )
)


# ============================================================
# HELPERS
# ============================================================

def parse_time_to_minutes(value):

    if value is None:

        raise ValueError(
            "Horário não pode ser None."
        )

    if isinstance(
        value,
        datetime
    ):

        return (
            value.hour * 60
            +
            value.minute
        )

    value = str(
        value
    ).strip()

    for fmt in (
        "%H:%M",
        "%H:%M:%S",
        "%I:%M%p",
        "%I:%M %p",
    ):

        try:

            parsed = datetime.strptime(
                value,
                fmt
            )

            return (
                parsed.hour * 60
                +
                parsed.minute
            )

        except ValueError:

            continue

    raise ValueError(
        f"Horário inválido: {value}"
    )


def parse_date(value):

    if isinstance(
        value,
        datetime
    ):

        return value.date()

    if hasattr(
        value,
        "year"
    ) and hasattr(
        value,
        "month"
    ) and hasattr(
        value,
        "day"
    ):

        return value

    return datetime.strptime(
        str(value),
        "%Y-%m-%d"
    ).date()


def parse_datetime(value):

    if isinstance(
        value,
        datetime
    ):

        return value

    value = str(
        value
    )

    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    )

    for fmt in formats:

        try:

            return datetime.strptime(
                value,
                fmt
            )

        except ValueError:

            continue

    raise ValueError(
        f"Timestamp inválido: {value}"
    )


# ============================================================
# FEATURE ENGINEERING
# ============================================================

def build_features(
    flight
):

    flight_from = str(
        flight[
            "flight_from"
        ]
    ).upper()

    flight_to = str(
        flight[
            "flight_to"
        ]
    ).upper()

    route = (
        f"{flight_from}_{flight_to}"
    )

    ticket_date = parse_date(
        flight[
            "ticket_date"
        ]
    )

    search_timestamp = (
        parse_datetime(
            flight[
                "search_timestamp"
            ]
        )
    )

    days_until_departure = (
        ticket_date
        -
        search_timestamp.date()
    ).days

    if days_until_departure < 0:

        raise ValueError(
            "ticket_date não pode ser "
            "anterior à search_timestamp."
        )

    departure_minutes = (
        parse_time_to_minutes(
            flight[
                "departure_time"
            ]
        )
    )

    arrival_minutes = (
        parse_time_to_minutes(
            flight[
                "arrival_time"
            ]
        )
    )

    connection_airports = (
        flight.get(
            "connection_airports",
            []
        )
        or []
    )

    connection_count = len(
        connection_airports
    )

    departure_weekday = (
        ticket_date.isoweekday()
    )

    search_hour = (
        search_timestamp.hour
    )

    features = {

        "flight_from":
            flight_from,

        "flight_to":
            flight_to,

        "route":
            route,

        "company":
            str(
                flight[
                    "company"
                ]
            ),

        "departure_minutes":
            departure_minutes,

        "arrival_minutes":
            arrival_minutes,

        "duration_minutes":
            int(
                flight[
                    "duration_minutes"
                ]
            ),

        "stops":
            int(
                flight[
                    "stops"
                ]
            ),

        "self_transfer":
            bool(
                flight[
                    "self_transfer"
                ]
            ),

        "connection_count":
            connection_count,

        "days_until_departure":
            days_until_departure,

        "departure_weekday":
            departure_weekday,

        "search_hour":
            search_hour,
    }

    return features


# ============================================================
# MODEL ERROR
# ============================================================

def get_expected_mae(
    model_type
):

    metrics = (
        METADATA.get(
            "metrics",
            []
        )
    )

    target_model_name = (
        "Warm - CatBoost"
        if model_type == "warm"
        else "Cold - CatBoost"
    )

    for item in metrics:

        if (
            item.get("model")
            ==
            target_model_name
        ):

            return item.get(
                "MAE"
            )

    return None


# ============================================================
# PREDICT
# ============================================================

def predict_flight(
    flight
):

    features = build_features(
        flight
    )

    route = (
        features[
            "route"
        ]
    )

    route_known = (
        route
        in
        WARM_KNOWN_ROUTES
    )

    if route_known:

        model = (
            WARM_MODEL
        )

        model_type = (
            "warm"
        )

    else:

        model = (
            COLD_MODEL
        )

        model_type = (
            "cold"
        )

    X = pd.DataFrame(
        [
            features
        ]
    )

    X = X[
        FEATURES
    ]

    for col in CATEGORICAL_FEATURES:

        X[col] = (
            X[col]
            .astype(str)
        )

    prediction = float(
        model.predict(
            X
        )[0]
    )

    expected_mae = (
        get_expected_mae(
            model_type
        )
    )

    result = {

        "predicted_price_usd":
            round(
                prediction,
                2
            ),

        "route":
            route,

        "route_known":
            route_known,

        "model_used":
            model_type,

        "expected_mae_usd":
            (
                round(
                    float(
                        expected_mae
                    ),
                    2
                )
                if expected_mae
                is not None
                else None
            ),

        "features":
            features,
    }

    return result


# ============================================================
# MANUAL TEST
# ============================================================

if __name__ == "__main__":

    example = {

        "flight_from":
            "BSB",

        "flight_to":
            "JTC",

        "company":
            "Azul Linhas Aereas",

        "departure_time":
            "05:25",

        "arrival_time":
            "10:25",

        "duration_minutes":
            300,

        "stops":
            1,

        "self_transfer":
            False,

        "connection_airports":
            [
                "VCP"
            ],

        "ticket_date":
            "2026-09-20",

        "search_timestamp":
            "2026-09-15 18:30:00",
    }

    result = predict_flight(
        example
    )

    print(
        result
    )