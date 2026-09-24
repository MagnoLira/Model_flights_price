import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from catboost import CatBoostRegressor

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    mean_absolute_percentage_error,
)

from database.db_connection import get_lina_connection


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("train-flight-price-v6")


# ============================================================
# CONFIG
# ============================================================

RANDOM_SEED = 42

MIN_WARM_ROUTE_SAMPLES = 10
MIN_WARM_ROUTE_BATCHES = 3

WARM_TRAIN_RATIO = 0.70
WARM_VALIDATION_RATIO = 0.15

COLD_TRAIN_ROUTE_RATIO = 0.70
COLD_VALIDATION_ROUTE_RATIO = 0.15


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

MODEL_DIR = BASE_DIR / "models"

COLD_MODEL_PATH = (
    MODEL_DIR
    / "flight_price_catboost_cold_v6.cbm"
)

WARM_MODEL_PATH = (
    MODEL_DIR
    / "flight_price_catboost_warm_v6.cbm"
)

METADATA_PATH = (
    MODEL_DIR
    / "flight_price_catboost_v6_metadata.joblib"
)


# ============================================================
# FEATURES
# ============================================================

TARGET = "price_usd"


WARM_CATEGORICAL_FEATURES = [
    "flight_from",
    "flight_to",
    "route",
    "company",
]


COLD_CATEGORICAL_WITH_ROUTE = [
    "flight_from",
    "flight_to",
    "route",
    "company",
]


COLD_CATEGORICAL_NO_ROUTE = [
    "flight_from",
    "flight_to",
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


WARM_FEATURES = (
    WARM_CATEGORICAL_FEATURES
    + NUMERIC_FEATURES
)


COLD_FEATURES_WITH_ROUTE = (
    COLD_CATEGORICAL_WITH_ROUTE
    + NUMERIC_FEATURES
)


COLD_FEATURES_NO_ROUTE = (
    COLD_CATEGORICAL_NO_ROUTE
    + NUMERIC_FEATURES
)


# ============================================================
# MODEL CANDIDATES
# ============================================================

#
# A V6 não faz tuning gigantesco.
#
# Queremos testar hipóteses claras.
#

WARM_CANDIDATES = [

    {
        "name": "warm_depth8_baseline",
        "params": {
            "iterations": 2500,
            "learning_rate": 0.03,
            "depth": 8,
            "l2_leaf_reg": 3,
            "random_strength": 1.0,
        },
    },

    {
        "name": "warm_depth6_l2_10",
        "params": {
            "iterations": 3000,
            "learning_rate": 0.03,
            "depth": 6,
            "l2_leaf_reg": 10,
            "random_strength": 1.0,
        },
    },

    {
        "name": "warm_depth6_l2_20",
        "params": {
            "iterations": 3000,
            "learning_rate": 0.03,
            "depth": 6,
            "l2_leaf_reg": 20,
            "random_strength": 1.0,
        },
    },

    {
        "name": "warm_depth5_l2_15",
        "params": {
            "iterations": 3000,
            "learning_rate": 0.03,
            "depth": 5,
            "l2_leaf_reg": 15,
            "random_strength": 1.0,
        },
    },

    {
        "name": "warm_depth6_slow",
        "params": {
            "iterations": 3500,
            "learning_rate": 0.02,
            "depth": 6,
            "l2_leaf_reg": 10,
            "random_strength": 1.0,
        },
    },
]


COLD_CANDIDATES = [

    # --------------------------------------------------------
    # WITH ROUTE
    # --------------------------------------------------------

    {
        "name": "cold_route_depth8",
        "use_route": True,
        "params": {
            "iterations": 2500,
            "learning_rate": 0.03,
            "depth": 8,
            "l2_leaf_reg": 3,
            "random_strength": 1.0,
        },
    },

    {
        "name": "cold_route_depth6_l2_10",
        "use_route": True,
        "params": {
            "iterations": 3000,
            "learning_rate": 0.03,
            "depth": 6,
            "l2_leaf_reg": 10,
            "random_strength": 1.0,
        },
    },

    {
        "name": "cold_route_depth5_l2_15",
        "use_route": True,
        "params": {
            "iterations": 3000,
            "learning_rate": 0.03,
            "depth": 5,
            "l2_leaf_reg": 15,
            "random_strength": 1.0,
        },
    },

    # --------------------------------------------------------
    # WITHOUT ROUTE
    # --------------------------------------------------------

    {
        "name": "cold_no_route_depth8",
        "use_route": False,
        "params": {
            "iterations": 2500,
            "learning_rate": 0.03,
            "depth": 8,
            "l2_leaf_reg": 3,
            "random_strength": 1.0,
        },
    },

    {
        "name": "cold_no_route_depth6_l2_10",
        "use_route": False,
        "params": {
            "iterations": 3000,
            "learning_rate": 0.03,
            "depth": 6,
            "l2_leaf_reg": 10,
            "random_strength": 1.0,
        },
    },

    {
        "name": "cold_no_route_depth5_l2_15",
        "use_route": False,
        "params": {
            "iterations": 3000,
            "learning_rate": 0.03,
            "depth": 5,
            "l2_leaf_reg": 15,
            "random_strength": 1.0,
        },
    },
]


# ============================================================
# SQL
# ============================================================

GOLD_QUERY = """
SELECT
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

FROM gold.flight_price_features

WHERE price_usd IS NOT NULL
  AND days_until_departure >= 0

ORDER BY search_timestamp;
"""


# ============================================================
# DATA
# ============================================================

def load_data():

    logger.info(
        "Carregando dados da Gold..."
    )

    conn = get_lina_connection()

    try:

        df = pd.read_sql(
            GOLD_QUERY,
            conn
        )

    finally:

        conn.close()

    logger.info(
        "Registros carregados: %s",
        len(df)
    )

    # --------------------------------------------------------
    # Timestamp
    # --------------------------------------------------------

    df["search_timestamp"] = pd.to_datetime(
        df["search_timestamp"],
        errors="coerce"
    )

    invalid_timestamps = (
        df["search_timestamp"]
        .isna()
        .sum()
    )

    logger.info(
        "search_timestamp inválidos: %s",
        invalid_timestamps
    )

    # --------------------------------------------------------
    # Categoricals
    # --------------------------------------------------------

    all_categorical_columns = [
        "flight_from",
        "flight_to",
        "route",
        "company",
    ]

    for column in all_categorical_columns:

        df[column] = (
            df[column]
            .fillna("UNKNOWN")
            .astype(str)
        )

    # --------------------------------------------------------
    # Boolean
    # --------------------------------------------------------

    df["self_transfer"] = (
        df["self_transfer"]
        .fillna(False)
        .astype(int)
    )

    # --------------------------------------------------------
    # Required columns
    # --------------------------------------------------------

    required_columns = list(
        set(
            WARM_FEATURES
            + COLD_FEATURES_WITH_ROUTE
            + COLD_FEATURES_NO_ROUTE
            + [
                TARGET,
                "search_timestamp",
            ]
        )
    )

    before = len(df)

    df = df.dropna(
        subset=required_columns
    ).copy()

    removed = (
        before
        - len(df)
    )

    if removed > 0:

        logger.warning(
            "Registros removidos por NULL/NaT: %s",
            removed
        )

    df = (
        df
        .sort_values(
            "search_timestamp"
        )
        .reset_index(drop=True)
    )

    logger.info(
        "Dataset final: %s registros",
        len(df)
    )

    logger.info(
        "Rotas totais: %s",
        df["route"].nunique()
    )

    logger.info(
        "Search timestamps distintos: %s",
        df["search_timestamp"].nunique()
    )

    return df


# ============================================================
# METRICS
# ============================================================

def calculate_metrics(
    y_true,
    y_pred
):

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

    r2 = r2_score(
        y_true,
        y_pred
    )

    mape = (
        mean_absolute_percentage_error(
            y_true,
            y_pred
        )
        * 100
    )

    return {
        "MAE": float(mae),
        "RMSE": float(rmse),
        "R2": float(r2),
        "MAPE": float(mape),
    }


def log_metrics(
    name,
    y_true,
    y_pred
):

    metrics = calculate_metrics(
        y_true,
        y_pred
    )

    logger.info(
        "%s | "
        "n=%s | "
        "MAE=%.2f | "
        "RMSE=%.2f | "
        "R2=%.4f | "
        "MAPE=%.2f%%",
        name,
        len(y_true),
        metrics["MAE"],
        metrics["RMSE"],
        metrics["R2"],
        metrics["MAPE"],
    )

    return {
        "model": name,
        "n": int(len(y_true)),
        **metrics,
    }


# ============================================================
# BASELINES
# ============================================================

def global_median_baseline(
    train_df,
    target_df
):

    median = (
        train_df[TARGET]
        .median()
    )

    return np.full(
        len(target_df),
        median
    )


def route_median_baseline(
    train_df,
    target_df
):

    global_median = (
        train_df[TARGET]
        .median()
    )

    route_medians = (
        train_df
        .groupby("route")[TARGET]
        .median()
        .to_dict()
    )

    return (
        target_df["route"]
        .map(route_medians)
        .fillna(global_median)
        .values
    )


# ============================================================
# MODEL
# ============================================================

def create_model(
    params
):

    return CatBoostRegressor(

        loss_function="MAE",

        eval_metric="MAE",

        random_seed=RANDOM_SEED,

        early_stopping_rounds=150,

        allow_writing_files=False,

        verbose=False,

        **params,
    )


def fit_candidate(
    train_df,
    validation_df,
    features,
    categorical_features,
    candidate_name,
    params
):

    logger.info(
        "Treinando candidato: %s",
        candidate_name
    )

    model = create_model(
        params
    )

    model.fit(

        train_df[features],

        train_df[TARGET],

        cat_features=categorical_features,

        eval_set=(
            validation_df[features],
            validation_df[TARGET]
        ),

        use_best_model=True,
    )

    train_predictions = model.predict(
        train_df[features]
    )

    validation_predictions = model.predict(
        validation_df[features]
    )

    train_metrics = calculate_metrics(
        train_df[TARGET],
        train_predictions
    )

    validation_metrics = calculate_metrics(
        validation_df[TARGET],
        validation_predictions
    )

    best_iteration = (
        model.get_best_iteration()
    )

    logger.info(
        (
            "%s | "
            "best_iteration=%s | "
            "train_MAE=%.2f | "
            "validation_MAE=%.2f | "
            "gap=%.2f"
        ),
        candidate_name,
        best_iteration,
        train_metrics["MAE"],
        validation_metrics["MAE"],
        (
            validation_metrics["MAE"]
            - train_metrics["MAE"]
        )
    )

    result = {

        "candidate":
            candidate_name,

        "best_iteration":
            int(best_iteration),

        "train_MAE":
            train_metrics["MAE"],

        "train_RMSE":
            train_metrics["RMSE"],

        "train_R2":
            train_metrics["R2"],

        "train_MAPE":
            train_metrics["MAPE"],

        "validation_MAE":
            validation_metrics["MAE"],

        "validation_RMSE":
            validation_metrics["RMSE"],

        "validation_R2":
            validation_metrics["R2"],

        "validation_MAPE":
            validation_metrics["MAPE"],

        "generalization_gap_MAE":
            (
                validation_metrics["MAE"]
                - train_metrics["MAE"]
            ),

        "params":
            params.copy(),
    }

    return (
        model,
        result,
    )


# ============================================================
# FEATURE IMPORTANCE
# ============================================================

def get_feature_importance(
    model,
    features
):

    importance = pd.DataFrame(
        {
            "feature":
                features,

            "importance":
                model.get_feature_importance(),
        }
    )

    return (
        importance
        .sort_values(
            "importance",
            ascending=False
        )
        .reset_index(drop=True)
    )


# ============================================================
# WARM ROUTE ELIGIBILITY
# ============================================================

def get_warm_eligible_routes(
    df
):

    stats = (
        df
        .groupby("route")
        .agg(
            samples=(
                "raw_id",
                "size"
            ),

            batches=(
                "search_timestamp",
                "nunique"
            )
        )
    )

    eligible = stats[
        (
            stats["samples"]
            >= MIN_WARM_ROUTE_SAMPLES
        )
        &
        (
            stats["batches"]
            >= MIN_WARM_ROUTE_BATCHES
        )
    ].copy()

    ignored = stats.drop(
        index=eligible.index
    )

    return (
        eligible,
        ignored
    )


# ============================================================
# WARM SPLIT
# ============================================================

def make_warm_route_grouped_split(
    df
):

    (
        eligible_stats,
        ignored_stats,
    ) = get_warm_eligible_routes(
        df
    )

    train_parts = []
    validation_parts = []
    test_parts = []

    for route in (
        eligible_stats
        .index
        .tolist()
    ):

        route_df = (
            df[
                df["route"]
                == route
            ]
            .sort_values(
                "search_timestamp"
            )
            .copy()
        )

        batches = (
            route_df[
                "search_timestamp"
            ]
            .drop_duplicates()
            .sort_values()
            .tolist()
        )

        n_batches = len(
            batches
        )

        train_end = int(
            np.floor(
                n_batches
                * WARM_TRAIN_RATIO
            )
        )

        validation_end = int(
            np.floor(
                n_batches
                * (
                    WARM_TRAIN_RATIO
                    + WARM_VALIDATION_RATIO
                )
            )
        )

        train_end = max(
            train_end,
            1
        )

        train_end = min(
            train_end,
            n_batches - 2
        )

        validation_end = max(
            validation_end,
            train_end + 1
        )

        validation_end = min(
            validation_end,
            n_batches - 1
        )

        train_batches = set(
            batches[
                :train_end
            ]
        )

        validation_batches = set(
            batches[
                train_end:
                validation_end
            ]
        )

        test_batches = set(
            batches[
                validation_end:
            ]
        )

        route_train = route_df[
            route_df[
                "search_timestamp"
            ].isin(
                train_batches
            )
        ].copy()

        route_validation = route_df[
            route_df[
                "search_timestamp"
            ].isin(
                validation_batches
            )
        ].copy()

        route_test = route_df[
            route_df[
                "search_timestamp"
            ].isin(
                test_batches
            )
        ].copy()

        assert not route_train.empty
        assert not route_validation.empty
        assert not route_test.empty

        assert train_batches.isdisjoint(
            validation_batches
        )

        assert train_batches.isdisjoint(
            test_batches
        )

        assert validation_batches.isdisjoint(
            test_batches
        )

        train_parts.append(
            route_train
        )

        validation_parts.append(
            route_validation
        )

        test_parts.append(
            route_test
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

    # --------------------------------------------------------
    # Warm route check
    # --------------------------------------------------------

    train_routes = set(
        train_df["route"].unique()
    )

    validation_routes = set(
        validation_df["route"].unique()
    )

    test_routes = set(
        test_df["route"].unique()
    )

    assert (
        train_routes
        == validation_routes
        == test_routes
    )

    # --------------------------------------------------------
    # Leakage check
    # route + search timestamp
    # --------------------------------------------------------

    def batch_keys(
        frame
    ):

        return set(
            zip(
                frame["route"],
                frame["search_timestamp"]
            )
        )

    train_keys = batch_keys(
        train_df
    )

    validation_keys = batch_keys(
        validation_df
    )

    test_keys = batch_keys(
        test_df
    )

    assert train_keys.isdisjoint(
        validation_keys
    )

    assert train_keys.isdisjoint(
        test_keys
    )

    assert validation_keys.isdisjoint(
        test_keys
    )

    return {
        "train":
            train_df,

        "validation":
            validation_df,

        "test":
            test_df,

        "eligible_routes":
            eligible_stats,

        "ignored_routes":
            ignored_stats,
    }


# ============================================================
# COLD ROUTE HOLDOUT
# ============================================================

def make_cold_route_holdout_split(
    df
):

    routes = np.array(
        sorted(
            df["route"]
            .dropna()
            .unique()
        )
    )

    rng = np.random.default_rng(
        RANDOM_SEED
    )

    rng.shuffle(
        routes
    )

    n_routes = len(
        routes
    )

    train_end = int(
        np.floor(
            n_routes
            * COLD_TRAIN_ROUTE_RATIO
        )
    )

    validation_end = int(
        np.floor(
            n_routes
            * (
                COLD_TRAIN_ROUTE_RATIO
                + COLD_VALIDATION_ROUTE_RATIO
            )
        )
    )

    train_routes = set(
        routes[
            :train_end
        ]
    )

    validation_routes = set(
        routes[
            train_end:
                validation_end
        ]
    )

    test_routes = set(
        routes[
            validation_end:
        ]
    )

    train_df = df[
        df["route"].isin(
            train_routes
        )
    ].copy()

    validation_df = df[
        df["route"].isin(
            validation_routes
        )
    ].copy()

    test_df = df[
        df["route"].isin(
            test_routes
        )
    ].copy()

    # --------------------------------------------------------
    # Leakage checks
    # --------------------------------------------------------

    assert train_routes.isdisjoint(
        validation_routes
    )

    assert train_routes.isdisjoint(
        test_routes
    )

    assert validation_routes.isdisjoint(
        test_routes
    )

    return {
        "train":
            train_df,

        "validation":
            validation_df,

        "test":
            test_df,

        "train_routes":
            sorted(
                train_routes
            ),

        "validation_routes":
            sorted(
                validation_routes
            ),

        "test_routes":
            sorted(
                test_routes
            ),
    }


# ============================================================
# WARM MODEL SELECTION
# ============================================================

def run_warm_experiment(
    df
):

    logger.info("")

    logger.info(
        "========== WARM V6 =========="
    )

    split = (
        make_warm_route_grouped_split(
            df
        )
    )

    train_df = split[
        "train"
    ]

    validation_df = split[
        "validation"
    ]

    test_df = split[
        "test"
    ]

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
        "Warm routes: %s",
        train_df[
            "route"
        ].nunique()
    )

    logger.info(
        "Warm leakage sanity check: OK"
    )

    # --------------------------------------------------------
    # Baselines on TEST
    #
    # Não são usados para model selection.
    # --------------------------------------------------------

    global_test_predictions = (
        global_median_baseline(
            train_df,
            test_df
        )
    )

    route_test_predictions = (
        route_median_baseline(
            train_df,
            test_df
        )
    )

    baseline_global_metrics = (
        log_metrics(
            "Warm V6 - Global Median",
            test_df[TARGET],
            global_test_predictions
        )
    )

    baseline_route_metrics = (
        log_metrics(
            "Warm V6 - Route Median",
            test_df[TARGET],
            route_test_predictions
        )
    )

    # --------------------------------------------------------
    # Candidate selection using VALIDATION ONLY
    # --------------------------------------------------------

    candidate_models = {}

    candidate_results = []

    for candidate in WARM_CANDIDATES:

        model, result = fit_candidate(

            train_df=
                train_df,

            validation_df=
                validation_df,

            features=
                WARM_FEATURES,

            categorical_features=
                WARM_CATEGORICAL_FEATURES,

            candidate_name=
                candidate["name"],

            params=
                candidate["params"],
        )

        candidate_models[
            candidate["name"]
        ] = model

        candidate_results.append(
            result
        )

    candidate_results_df = pd.DataFrame(
        candidate_results
    )

    candidate_results_df = (
        candidate_results_df
        .sort_values(
            "validation_MAE"
        )
        .reset_index(drop=True)
    )

    logger.info(
        "\nWarm candidate comparison:\n%s",
        candidate_results_df[
            [
                "candidate",
                "best_iteration",
                "train_MAE",
                "validation_MAE",
                "generalization_gap_MAE",
                "validation_R2",
                "validation_MAPE",
            ]
        ].to_string(
            index=False
        )
    )

    # --------------------------------------------------------
    # WINNER
    # --------------------------------------------------------

    winner_name = (
        candidate_results_df
        .iloc[0]["candidate"]
    )

    winner_model = (
        candidate_models[
            winner_name
        ]
    )

    winner_config = next(
        x
        for x in WARM_CANDIDATES
        if x["name"] == winner_name
    )

    logger.info(
        "Warm winner por validation MAE: %s",
        winner_name
    )

    # --------------------------------------------------------
    # TEST ONCE
    # --------------------------------------------------------

    test_predictions = (
        winner_model.predict(
            test_df[
                WARM_FEATURES
            ]
        )
    )

    test_metrics = (
        log_metrics(
            (
                "Warm V6 - "
                f"{winner_name}"
            ),
            test_df[TARGET],
            test_predictions,
        )
    )

    # --------------------------------------------------------
    # IMPORTANCE
    # --------------------------------------------------------

    importance = (
        get_feature_importance(
            winner_model,
            WARM_FEATURES
        )
    )

    logger.info(
        "\nFeature Importance "
        "- Warm V6 winner:\n%s",
        importance.to_string(
            index=False
        )
    )

    info = {

        "winner":
            winner_name,

        "winner_params":
            winner_config[
                "params"
            ],

        "train_rows":
            len(train_df),

        "validation_rows":
            len(validation_df),

        "test_rows":
            len(test_df),

        "routes":
            train_df[
                "route"
            ].nunique(),

        "eligible_routes":
            len(
                split[
                    "eligible_routes"
                ]
            ),

        "ignored_routes":
            len(
                split[
                    "ignored_routes"
                ]
            ),

        "candidate_results":
            candidate_results_df
            .to_dict(
                orient="records"
            ),

        "baseline_global":
            baseline_global_metrics,

        "baseline_route":
            baseline_route_metrics,

        "test_metrics":
            test_metrics,
    }

    known_routes = sorted(
        train_df[
            "route"
        ]
        .unique()
        .tolist()
    )

    return (
        winner_model,
        importance,
        known_routes,
        info,
        test_metrics,
    )


# ============================================================
# COLD MODEL SELECTION
# ============================================================

def run_cold_experiment(
    df
):

    logger.info("")

    logger.info(
        "========== COLD V6 =========="
    )

    split = (
        make_cold_route_holdout_split(
            df
        )
    )

    train_df = split[
        "train"
    ]

    validation_df = split[
        "validation"
    ]

    test_df = split[
        "test"
    ]

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
        len(test_df)
    )

    logger.info(
        "Cold routes train/validation/test: "
        "%s/%s/%s",
        len(
            split[
                "train_routes"
            ]
        ),
        len(
            split[
                "validation_routes"
            ]
        ),
        len(
            split[
                "test_routes"
            ]
        ),
    )

    logger.info(
        "Cold route leakage sanity check: OK"
    )

    # --------------------------------------------------------
    # BASELINE
    # --------------------------------------------------------

    global_test_predictions = (
        global_median_baseline(
            train_df,
            test_df
        )
    )

    baseline_global_metrics = (
        log_metrics(
            "Cold V6 - Global Median",
            test_df[TARGET],
            global_test_predictions
        )
    )

    # --------------------------------------------------------
    # Candidate selection using VALIDATION ONLY
    # --------------------------------------------------------

    candidate_models = {}

    candidate_results = []

    for candidate in COLD_CANDIDATES:

        use_route = (
            candidate[
                "use_route"
            ]
        )

        if use_route:

            features = (
                COLD_FEATURES_WITH_ROUTE
            )

            categorical_features = (
                COLD_CATEGORICAL_WITH_ROUTE
            )

        else:

            features = (
                COLD_FEATURES_NO_ROUTE
            )

            categorical_features = (
                COLD_CATEGORICAL_NO_ROUTE
            )

        model, result = fit_candidate(

            train_df=
                train_df,

            validation_df=
                validation_df,

            features=
                features,

            categorical_features=
                categorical_features,

            candidate_name=
                candidate["name"],

            params=
                candidate["params"],
        )

        result[
            "use_route"
        ] = use_route

        candidate_models[
            candidate["name"]
        ] = {
            "model":
                model,

            "features":
                features,

            "categorical_features":
                categorical_features,
        }

        candidate_results.append(
            result
        )

    candidate_results_df = pd.DataFrame(
        candidate_results
    )

    candidate_results_df = (
        candidate_results_df
        .sort_values(
            "validation_MAE"
        )
        .reset_index(drop=True)
    )

    logger.info(
        "\nCold candidate comparison:\n%s",
        candidate_results_df[
            [
                "candidate",
                "use_route",
                "best_iteration",
                "train_MAE",
                "validation_MAE",
                "generalization_gap_MAE",
                "validation_R2",
                "validation_MAPE",
            ]
        ].to_string(
            index=False
        )
    )

    # --------------------------------------------------------
    # WINNER
    # --------------------------------------------------------

    winner_name = (
        candidate_results_df
        .iloc[0]["candidate"]
    )

    winner_data = (
        candidate_models[
            winner_name
        ]
    )

    winner_model = (
        winner_data[
            "model"
        ]
    )

    winner_features = (
        winner_data[
            "features"
        ]
    )

    winner_categorical = (
        winner_data[
            "categorical_features"
        ]
    )

    winner_config = next(
        x
        for x in COLD_CANDIDATES
        if x["name"] == winner_name
    )

    logger.info(
        "Cold winner por validation MAE: %s",
        winner_name
    )

    logger.info(
        "Cold winner usa route: %s",
        winner_config[
            "use_route"
        ]
    )

    # --------------------------------------------------------
    # TEST ONCE
    # --------------------------------------------------------

    test_predictions = (
        winner_model.predict(
            test_df[
                winner_features
            ]
        )
    )

    test_metrics = (
        log_metrics(
            (
                "Cold V6 - "
                f"{winner_name}"
            ),
            test_df[TARGET],
            test_predictions,
        )
    )

    # --------------------------------------------------------
    # IMPORTANCE
    # --------------------------------------------------------

    importance = (
        get_feature_importance(
            winner_model,
            winner_features
        )
    )

    logger.info(
        "\nFeature Importance "
        "- Cold V6 winner:\n%s",
        importance.to_string(
            index=False
        )
    )

    info = {

        "winner":
            winner_name,

        "winner_params":
            winner_config[
                "params"
            ],

        "winner_use_route":
            winner_config[
                "use_route"
            ],

        "winner_features":
            winner_features,

        "winner_categorical_features":
            winner_categorical,

        "train_rows":
            len(train_df),

        "validation_rows":
            len(validation_df),

        "test_rows":
            len(test_df),

        "train_routes":
            len(
                split[
                    "train_routes"
                ]
            ),

        "validation_routes":
            len(
                split[
                    "validation_routes"
                ]
            ),

        "test_routes":
            len(
                split[
                    "test_routes"
                ]
            ),

        "candidate_results":
            candidate_results_df
            .to_dict(
                orient="records"
            ),

        "baseline_global":
            baseline_global_metrics,

        "test_metrics":
            test_metrics,
    }

    return (
        winner_model,
        importance,
        info,
        test_metrics,
        winner_features,
        winner_categorical,
    )


# ============================================================
# SAVE
# ============================================================

def save_artifacts(
    cold_model,
    warm_model,
    warm_importance,
    cold_importance,
    warm_known_routes,
    warm_info,
    cold_info,
    warm_test_metrics,
    cold_test_metrics,
    cold_features,
    cold_categorical_features,
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
            "v6",

        "selection_rule":
            (
                "candidate selection performed "
                "using validation MAE only; "
                "test evaluated after winner selection"
            ),

        "target":
            TARGET,

        # ----------------------------------------------------
        # WARM
        # ----------------------------------------------------

        "warm_features":
            WARM_FEATURES,

        "warm_categorical_features":
            WARM_CATEGORICAL_FEATURES,

        "warm_known_routes":
            warm_known_routes,

        "warm_experiment":
            warm_info,

        "warm_feature_importance":
            warm_importance
            .to_dict(
                orient="records"
            ),

        "warm_test_metrics":
            warm_test_metrics,

        # ----------------------------------------------------
        # COLD
        # ----------------------------------------------------

        "cold_features":
            cold_features,

        "cold_categorical_features":
            cold_categorical_features,

        "cold_experiment":
            cold_info,

        "cold_feature_importance":
            cold_importance
            .to_dict(
                orient="records"
            ),

        "cold_test_metrics":
            cold_test_metrics,

        # ----------------------------------------------------
        # Shared numeric features
        # ----------------------------------------------------

        "numeric_features":
            NUMERIC_FEATURES,
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
# FINAL SUMMARY
# ============================================================

def log_final_summary(
    warm_info,
    cold_info,
    warm_test_metrics,
    cold_test_metrics,
):

    logger.info("")

    logger.info(
        "========== FINAL V6 =========="
    )

    logger.info(
        (
            "WARM WINNER | "
            "%s | "
            "TEST MAE=%.2f | "
            "RMSE=%.2f | "
            "R2=%.4f | "
            "MAPE=%.2f%%"
        ),
        warm_info[
            "winner"
        ],
        warm_test_metrics[
            "MAE"
        ],
        warm_test_metrics[
            "RMSE"
        ],
        warm_test_metrics[
            "R2"
        ],
        warm_test_metrics[
            "MAPE"
        ],
    )

    logger.info(
        (
            "COLD WINNER | "
            "%s | "
            "use_route=%s | "
            "TEST MAE=%.2f | "
            "RMSE=%.2f | "
            "R2=%.4f | "
            "MAPE=%.2f%%"
        ),
        cold_info[
            "winner"
        ],
        cold_info[
            "winner_use_route"
        ],
        cold_test_metrics[
            "MAE"
        ],
        cold_test_metrics[
            "RMSE"
        ],
        cold_test_metrics[
            "R2"
        ],
        cold_test_metrics[
            "MAPE"
        ],
    )


# ============================================================
# MAIN
# ============================================================

def main():

    # --------------------------------------------------------
    # DATA
    # --------------------------------------------------------

    df = load_data()

    # --------------------------------------------------------
    # WARM
    # --------------------------------------------------------

    (
        warm_model,
        warm_importance,
        warm_known_routes,
        warm_info,
        warm_test_metrics,
    ) = run_warm_experiment(
        df
    )

    # --------------------------------------------------------
    # COLD
    # --------------------------------------------------------

    (
        cold_model,
        cold_importance,
        cold_info,
        cold_test_metrics,
        cold_features,
        cold_categorical_features,
    ) = run_cold_experiment(
        df
    )

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    log_final_summary(
        warm_info=
            warm_info,

        cold_info=
            cold_info,

        warm_test_metrics=
            warm_test_metrics,

        cold_test_metrics=
            cold_test_metrics,
    )

    # --------------------------------------------------------
    # SAVE
    # --------------------------------------------------------

    save_artifacts(

        cold_model=
            cold_model,

        warm_model=
            warm_model,

        warm_importance=
            warm_importance,

        cold_importance=
            cold_importance,

        warm_known_routes=
            warm_known_routes,

        warm_info=
            warm_info,

        cold_info=
            cold_info,

        warm_test_metrics=
            warm_test_metrics,

        cold_test_metrics=
            cold_test_metrics,

        cold_features=
            cold_features,

        cold_categorical_features=
            cold_categorical_features,
    )


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    main()