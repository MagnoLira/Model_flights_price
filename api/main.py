from fastapi import FastAPI, HTTPException

from api.schemas import (
    FlightPredictionRequest,
    FlightPredictionResponse
)

from ml.predict import predict_flight


app = FastAPI(
    title="Flight Price Model API",
    description="API para previsão de preços de voos.",
    version="1.0.0"
)


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
def health():

    return {
        "status": "ok"
    }


# ============================================================
# PREDICT
# ============================================================

@app.post(
    "/predict",
    response_model=FlightPredictionResponse
)
def predict(
    request: FlightPredictionRequest
):

    try:

        payload = request.model_dump()

        result = predict_flight(
            payload
        )

        mae = result.get(
            "expected_mae_usd"
        )

        price = result[
            "predicted_price_usd"
        ]

        price_range = None

        if mae is not None:

            price_range = {
                "low": round(
                    max(
                        0,
                        price - mae
                    ),
                    2
                ),

                "high": round(
                    price + mae,
                    2
                )
            }

        return {
            "predicted_price_usd":
                price,

            "route":
                result[
                    "route"
                ],

            "route_known":
                result[
                    "route_known"
                ],

            "model_used":
                result[
                    "model_used"
                ],

            "expected_mae_usd":
                mae,

            "approx_price_range_usd":
                price_range
        }

    except ValueError as exc:

        raise HTTPException(
            status_code=400,
            detail=str(exc)
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail="Erro interno durante a previsão."
        )