import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from catboost import CatBoostRegressor

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score
)

from database.db_connection import get_lina_connection


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

MODEL_DIR = BASE_DIR / "models"

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
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(
    "train-flight-price-model"
)


# ============================================================
# QUERY
# ============================================================

QUERY = """
SELECT
    g.raw_id,

    g.flight_from,
    g.flight_to,
    g.route,
    g.company,

    g.departure_minutes,
    g.arrival_minutes,

    g.duration_minutes,
    g.stops,
    g.self_transfer,
    g.connection_count,

    g.days_until_departure,

    g.departure_weekday,
    g.search_hour,

    g.price_usd,

    s.search_timestamp

FROM gold.flight_price_features g

JOIN silver.flights_scrapy s
    ON s.raw_id = g.raw_id

ORDER BY s.search_timestamp;
"""


# ============================================================
# FEATURES
# ============================================================

CATEGORICAL_FEATURES = [
    "flight_from",
    "flight_to",
    "route",
    "company",
]


NUMERIC_FEATURES = [
    "departure_minutes",
    "arrival_minutes",
    "duration_minutes",
    "stops",
    "self_transfer",
    "connection_count",
    "days_until_departure",
    "departure_weekday",
    "search_hour",
]


FEATURES = (
    CATEGORICAL_FEATURES
    + NUMERIC_FEATURES
)


TARGET = "price_usd"


# ============================================================
# LOAD DATA
# ============================================================

def load_data():

    logger.info(
        "Carregando dados da Gold..."
    )

    conn = get_lina_connection()

    try:

        df = pd.read_sql(
            QUERY,
            conn
        )

    finally:

        conn.close()

    df["search_timestamp"] = pd.to_datetime(
        df["search_timestamp"]
    )

    df = (
        df
        .sort_values(
            "search_timestamp"
        )
        .reset_index(
            drop=True
        )
    )

    logger.info(
        "Registros carregados: %s",
        len(df)
    )

    return df


# ============================================================
# METRICS
# ============================================================

def calculate_metrics(
    y_true,
    y_pred,
    model_name
):

    if len(y_true) == 0:

        return {
            "model": model_name,
            "n": 0,
            "MAE": None,
            "RMSE": None,
            "R2": None,
            "MAPE": None,
        }

    y_true = np.asarray(
        y_true,
        dtype=float
    )

    y_pred = np.asarray(
        y_pred,
        dtype=float
    )

    mae = mean_absolute_error(
        y_true,
        y_pred
    )

    rmse = np.sqrt(
        mean_squared_error(
            y_true,
            y_pred
        )
    )

    r2 = (
        r2_score(
            y_true,
            y_pred
        )
        if len(y_true) >= 2
        else np.nan
    )

    nonzero = (
        y_true != 0
    )

    mape = (
        np.mean(
            np.abs(
                (
                    y_true[nonzero]
                    -
                    y_pred[nonzero]
                )
                /
                y_true[nonzero]
            )
        ) * 100
        if nonzero.any()
        else np.nan
    )

    result = {
        "model": model_name,
        "n": len(y_true),
        "MAE": mae,
        "RMSE": rmse,
        "R2": r2,
        "MAPE": mape,
    }

    logger.info(
        "%s | "
        "n=%s | "
        "MAE=%.2f | "
        "RMSE=%.2f | "
        "R2=%.4f | "
        "MAPE=%.2f%%",
        model_name,
        len(y_true),
        mae,
        rmse,
        r2,
        mape
    )

    return result


# ============================================================
# PREPARE FEATURES
# ============================================================

def prepare_features(df):

    X = df[
        FEATURES
    ].copy()

    for col in CATEGORICAL_FEATURES:

        X[col] = (
            X[col]
            .astype(str)
        )

    return X


# ============================================================
# BASELINES
# ============================================================

def global_median_baseline(
    train_df,
    test_df,
    name
):

    median_price = (
        train_df[
            TARGET
        ]
        .median()
    )

    predictions = np.full(
        len(test_df),
        median_price
    )

    return calculate_metrics(
        test_df[TARGET],
        predictions,
        name
    )


def route_median_baseline(
    train_df,
    test_df,
    name
):

    global_median = (
        train_df[
            TARGET
        ]
        .median()
    )

    route_medians = (
        train_df
        .groupby(
            "route"
        )[TARGET]
        .median()
    )

    predictions = (
        test_df[
            "route"
        ]
        .map(
            route_medians
        )
        .fillna(
            global_median
        )
        .values
    )

    return calculate_metrics(
        test_df[TARGET],
        predictions,
        name
    )


# ============================================================
# TRAIN CATBOOST
# ============================================================

def train_catboost(
    train_df,
    validation_df,
    model_label
):

    X_train = prepare_features(
        train_df
    )

    y_train = (
        train_df[
            TARGET
        ]
    )

    X_validation = prepare_features(
        validation_df
    )

    y_validation = (
        validation_df[
            TARGET
        ]
    )

    logger.info(
        "Treinando %s | "
        "train=%s | "
        "validation=%s",
        model_label,
        len(train_df),
        len(validation_df)
    )

    model = CatBoostRegressor(
        loss_function="MAE",
        eval_metric="MAE",

        iterations=2000,

        learning_rate=0.03,
        depth=8,

        random_seed=42,

        early_stopping_rounds=120,

        verbose=100,

        allow_writing_files=False
    )

    model.fit(
        X_train,
        y_train,

        cat_features=(
            CATEGORICAL_FEATURES
        ),

        eval_set=(
            X_validation,
            y_validation
        ),

        use_best_model=True
    )

    return model


# ============================================================
# EVALUATION
# ============================================================

def evaluate_model(
    model,
    df,
    name
):

    if df.empty:

        return calculate_metrics(
            [],
            [],
            name
        )

    X = prepare_features(
        df
    )

    predictions = (
        model.predict(
            X
        )
    )

    return calculate_metrics(
        df[TARGET],
        predictions,
        name
    )


# ============================================================
# COLD START SPLIT
# ============================================================

def build_cold_start_split(df):

    search_date = (
        df[
            "search_timestamp"
        ]
        .dt.date
    )

    train_cutoff = (
        pd.Timestamp(
            "2026-08-26"
        )
        .date()
    )

    validation_date = (
        pd.Timestamp(
            "2026-08-27"
        )
        .date()
    )

    test_date = (
        pd.Timestamp(
            "2026-08-28"
        )
        .date()
    )

    train_df = df[
        search_date
        <= train_cutoff
    ].copy()

    validation_df = df[
        search_date
        == validation_date
    ].copy()

    raw_test_df = df[
        search_date
        == test_date
    ].copy()

    train_routes = set(
        train_df[
            "route"
        ].unique()
    )

    cold_test_df = raw_test_df[
        ~raw_test_df[
            "route"
        ].isin(
            train_routes
        )
    ].copy()

    logger.info("")
    logger.info(
        "========== COLD START =========="
    )

    logger.info(
        "Cold train: %s",
        len(train_df)
    )

    logger.info(
        "Cold validation: %s",
        len(validation_df)
    )

    logger.info(
        "Cold test: %s",
        len(cold_test_df)
    )

    logger.info(
        "Rotas inéditas cold test: %s",
        cold_test_df[
            "route"
        ].nunique()
    )

    return (
        train_df,
        validation_df,
        cold_test_df
    )


# ============================================================
# WARM START SPLIT
# ============================================================

def build_warm_start_split(
    df,
    min_route_samples=10,
    train_ratio=0.70,
    validation_ratio=0.15
):

    train_parts = []
    validation_parts = []
    test_parts = []

    eligible_routes = 0
    ignored_routes = 0

    for route, route_df in (
        df.groupby(
            "route"
        )
    ):

        route_df = (
            route_df
            .sort_values(
                "search_timestamp"
            )
            .reset_index(
                drop=True
            )
        )

        n = len(
            route_df
        )

        if n < min_route_samples:

            ignored_routes += 1
            continue

        train_end = int(
            n
            * train_ratio
        )

        valid_end = int(
            n
            * (
                train_ratio
                +
                validation_ratio
            )
        )

        train_end = max(
            train_end,
            1
        )

        valid_end = max(
            valid_end,
            train_end + 1
        )

        valid_end = min(
            valid_end,
            n - 1
        )

        route_train = (
            route_df.iloc[
                :train_end
            ]
        )

        route_validation = (
            route_df.iloc[
                train_end:valid_end
            ]
        )

        route_test = (
            route_df.iloc[
                valid_end:
            ]
        )

        if (
            route_train.empty
            or route_validation.empty
            or route_test.empty
        ):

            ignored_routes += 1
            continue

        eligible_routes += 1

        train_parts.append(
            route_train
        )

        validation_parts.append(
            route_validation
        )

        test_parts.append(
            route_test
        )

    if not train_parts:

        raise RuntimeError(
            "Nenhuma rota elegível "
            "para warm start."
        )

    train_df = pd.concat(
        train_parts,
        ignore_index=True
    )

    validation_df = pd.concat(
        validation_parts,
        ignore_index=True
    )

    test_df = pd.concat(
        test_parts,
        ignore_index=True
    )

    train_df = (
        train_df
        .sort_values(
            "search_timestamp"
        )
    )

    validation_df = (
        validation_df
        .sort_values(
            "search_timestamp"
        )
    )

    test_df = (
        test_df
        .sort_values(
            "search_timestamp"
        )
    )

    logger.info("")
    logger.info(
        "========== WARM START =========="
    )

    logger.info(
        "Rotas elegíveis: %s",
        eligible_routes
    )

    logger.info(
        "Rotas ignoradas: %s",
        ignored_routes
    )

    logger.info(
        "Warm train: %s",
        len(train_df)
    )

    logger.info(
        "Warm validation: %s",
        len(validation_df)
    )

    logger.info(
        "Warm test: %s",
        len(test_df)
    )

    logger.info(
        "Rotas no warm train: %s",
        train_df[
            "route"
        ].nunique()
    )

    logger.info(
        "Rotas no warm test: %s",
        test_df[
            "route"
        ].nunique()
    )

    train_routes = set(
        train_df[
            "route"
        ].unique()
    )

    test_routes = set(
        test_df[
            "route"
        ].unique()
    )

    missing_routes = (
        test_routes
        -
        train_routes
    )

    if missing_routes:

        raise RuntimeError(
            "Warm split inválido. "
            f"Rotas inéditas: {missing_routes}"
        )

    return (
        train_df,
        validation_df,
        test_df
    )


# ============================================================
# FEATURE IMPORTANCE
# ============================================================

def get_feature_importance(
    model,
    label
):

    importance_df = pd.DataFrame({
        "feature":
            FEATURES,

        "importance":
            model.get_feature_importance()
    })

    importance_df = (
        importance_df
        .sort_values(
            "importance",
            ascending=False
        )
        .reset_index(
            drop=True
        )
    )

    logger.info(
        "\nFeature Importance - %s:\n%s",
        label,
        importance_df.to_string(
            index=False
        )
    )

    return importance_df


# ============================================================
# COLD BENCHMARK
# ============================================================

def run_cold_start(df):

    (
        train_df,
        validation_df,
        test_df
    ) = build_cold_start_split(
        df
    )

    metrics = []

    metrics.append(
        global_median_baseline(
            train_df,
            test_df,
            "Cold - Global Median"
        )
    )

    metrics.append(
        route_median_baseline(
            train_df,
            test_df,
            "Cold - Route Median"
        )
    )

    model = train_catboost(
        train_df,
        validation_df,
        "Cold Start"
    )

    metrics.append(
        evaluate_model(
            model,
            test_df,
            "Cold - CatBoost"
        )
    )

    importance_df = (
        get_feature_importance(
            model,
            "Cold Start"
        )
    )

    return (
        model,
        metrics,
        importance_df
    )


# ============================================================
# WARM BENCHMARK
# ============================================================

def run_warm_start(df):

    (
        train_df,
        validation_df,
        test_df
    ) = build_warm_start_split(
        df
    )

    metrics = []

    metrics.append(
        global_median_baseline(
            train_df,
            test_df,
            "Warm - Global Median"
        )
    )

    metrics.append(
        route_median_baseline(
            train_df,
            test_df,
            "Warm - Route Median"
        )
    )

    model = train_catboost(
        train_df,
        validation_df,
        "Warm Start"
    )

    metrics.append(
        evaluate_model(
            model,
            test_df,
            "Warm - CatBoost"
        )
    )

    importance_df = (
        get_feature_importance(
            model,
            "Warm Start"
        )
    )

    warm_known_routes = sorted(
        train_df[
            "route"
        ]
        .unique()
        .tolist()
    )

    return (
        model,
        metrics,
        importance_df,
        warm_known_routes
    )


# ============================================================
# SAVE MODELS
# ============================================================

def save_models(
    cold_model,
    warm_model,
    metrics_df,
    cold_importance,
    warm_importance,
    warm_known_routes
):

    MODEL_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    cold_model.save_model(
        str(
            COLD_MODEL_PATH
        )
    )

    warm_model.save_model(
        str(
            WARM_MODEL_PATH
        )
    )

    metadata = {

        "version":
            "v3",

        "target":
            TARGET,

        "features":
            FEATURES,

        "categorical_features":
            CATEGORICAL_FEATURES,

        "numeric_features":
            NUMERIC_FEATURES,

        "warm_known_routes":
            warm_known_routes,

        "metrics":
            metrics_df.to_dict(
                orient="records"
            ),

        "cold_feature_importance":
            cold_importance.to_dict(
                orient="records"
            ),

        "warm_feature_importance":
            warm_importance.to_dict(
                orient="records"
            ),
    }

    joblib.dump(
        metadata,
        METADATA_PATH
    )

    logger.info(
        "Cold model salvo em: %s",
        COLD_MODEL_PATH
    )

    logger.info(
        "Warm model salvo em: %s",
        WARM_MODEL_PATH
    )

    logger.info(
        "Metadata salva em: %s",
        METADATA_PATH
    )


# ============================================================
# MAIN
# ============================================================

def main():

    df = load_data()

    (
        cold_model,
        cold_metrics,
        cold_importance
    ) = run_cold_start(
        df
    )

    (
        warm_model,
        warm_metrics,
        warm_importance,
        warm_known_routes
    ) = run_warm_start(
        df
    )

    all_metrics = (
        cold_metrics
        +
        warm_metrics
    )

    metrics_df = pd.DataFrame(
        all_metrics
    )

    logger.info(
        "\n"
        "========== FINAL COMPARISON ==========\n%s",
        metrics_df.to_string(
            index=False
        )
    )

    save_models(
        cold_model,
        warm_model,
        metrics_df,
        cold_importance,
        warm_importance,
        warm_known_routes
    )


if __name__ == "__main__":
    main()