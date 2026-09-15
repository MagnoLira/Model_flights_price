from typing import List
from pydantic import BaseModel, Field


class FlightPredictionRequest(BaseModel):
    flight_from: str = Field(min_length=3, max_length=3)
    flight_to: str = Field(min_length=3, max_length=3)

    company: str

    departure_time: str
    arrival_time: str

    duration_minutes: int = Field(gt=0)
    stops: int = Field(ge=0)

    self_transfer: bool = False

    connection_airports: List[str] = []

    ticket_date: str
    search_timestamp: str


class PriceRange(BaseModel):
    low: float
    high: float


class FlightPredictionResponse(BaseModel):
    predicted_price_usd: float

    route: str
    route_known: bool

    model_used: str

    expected_mae_usd: float | None

    approx_price_range_usd: PriceRange | None