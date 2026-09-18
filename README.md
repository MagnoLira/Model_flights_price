# Flight Price Model

## About

This repository implements an end-to-end machine learning and data engineering system for estimating expected flight ticket prices.

The main question answered by the model is:

> Given the characteristics of a flight and the context in which it was searched, what price should we expect for this flight?

The project currently includes:

- flight web scraping;
- event streaming with Kafka;
- PostgreSQL Medallion Architecture;
- feature engineering;
- CatBoost regression models;
- cold-start and warm-start inference;
- FastAPI model serving;
- Docker containerization;
- Kubernetes deployment with k3s;
- Traefik Ingress;
- end-to-end validation using newly scraped flights.

The current flight source is Skiplagged.

---

## Architecture

![Arquitetura Model Flight](image.png)

The current system architecture is:

```text
Skiplagged
    ↓
Web Scraper
    ↓
Kafka
    ↓
RAW
    ↓
SILVER
    ↓
GOLD
    ↓
Machine Learning
    ↓
CatBoost Artifacts
    ↓
FastAPI
    ↓
Docker
    ↓
k3s / Kubernetes
    ↓
Traefik Ingress
```

The deployed inference path is:

```text
Client
   ↓
Traefik
   ↓
Ingress
   ↓
Kubernetes Service
   ↓
API Pod
   ↓
FastAPI
   ↓
CatBoost
```

During local deployment, the API is available through:

```text
http://flight-api.local
```

---

## Web Scraping

The scraping layer is responsible for generating valid flight-search URLs and extracting flight information from Skiplagged.

### URL Builder

The URL Builder receives parameters such as:

- origin airport;
- destination airport;
- departure date.

Example:

```text
Origin: BSB
Destination: JTC
Date: 2026-09-18
```

Generated URL:

```text
https://skiplagged.com/flights/BSB/JTC/2026-09-18
```

The scraper extracts information such as:

- airline;
- departure time;
- arrival time;
- duration;
- number of stops;
- connection airports;
- self-transfer information;
- observed ticket price;
- search timestamp;
- ticket date.

The basic scraping flow is:

```text
Request parameters
       ↓
URL Builder
       ↓
Skiplagged URL
       ↓
Browser automation
       ↓
Raw flight information
```

Additional documentation:

[lina_doc](https://docs.google.com/document/d/1WBCA--lDNthuq8b0suJ18N2jxCPG220BMIYehpolZs8/edit?tab=t.0#heading=h.hubdlbtrjkyq)

---

## Kafka

Scraped flight events are published to the Kafka topic:

```text
raw.flights_scrapy
```

Kafka decouples scraping from database ingestion.

The consumer responsible for persisting Kafka events into PostgreSQL is:

```text
database/kafka_topic_raw_consumer.py
```

The flow is:

```text
Scraper
   ↓
Kafka Producer
   ↓
raw.flights_scrapy topic
   ↓
Kafka Consumer
   ↓
PostgreSQL RAW
```

Each raw payload receives a deterministic SHA-256 hash:

```text
data_hash
```

The hash provides idempotency and prevents duplicate raw events.

The Kafka consumer commits the Kafka offset only after the PostgreSQL transaction succeeds.

---

## Medallion Architecture

The project uses a Medallion-style architecture:

```text
RAW
 ↓
SILVER
 ↓
GOLD
```

Each layer has a different responsibility.

---

## RAW Layer

The RAW layer stores the original event received from the scraper.

Table:

```text
raw.flights_scrapy
```

Important fields include:

```text
id
data_json
data_hash
created_at
mode
```

The RAW layer preserves the original payload and provides traceability for all downstream transformations.

---

## SILVER Layer

The SILVER layer transforms raw scraper payloads into structured flight records.

Table:

```text
silver.flights_scrapy
```

The ETL is implemented in:

```text
database/etl_raw_to_silver.py
```

Examples of transformations include:

```text
Skiplagged URL
      ↓
origin
destination
ticket_date
```

and:

```text
raw_text
   ↓
duration_minutes
stops
self_transfer
connection_airports
price_original
```

Main fields include:

```text
raw_id
flight_from
flight_to
company
departure_time
arrival_time
duration_minutes
stops
self_transfer
connection_airports
price_original
currency_original
ticket_date
search_timestamp
emission_link
website
mode
```

`raw_id` preserves direct lineage back to the original RAW event.

---

## GOLD Layer

The GOLD layer contains the machine-learning-ready representation of each flight.

Table:

```text
gold.flight_price_features
```

The transformation is implemented in:

```text
database/etl_silver_to_gold.py
```

Current model features include:

### Categorical features

```text
flight_from
flight_to
route
company
```

### Numerical and binary features

```text
departure_minutes
arrival_minutes
duration_minutes
stops
self_transfer
connection_count
days_until_departure
departure_weekday
search_hour
```

Target:

```text
price_usd
```

Example:

```text
CNF -> MCZ
LATAM
departure: 18:00
arrival: 02:05
duration: 480 minutes
stops: 1
days until departure: 6
```

is transformed into the feature vector consumed by CatBoost.

---

## Machine Learning Problem

The current task is formulated as a supervised regression problem.

Conceptually:

```text
Flight characteristics
        +
Search context
        ↓
      CatBoost
        ↓
Expected flight price
```

The model estimates:

> The expected ticket price given the characteristics of a flight and the search context.

The model does **not** currently answer:

- Should I buy this ticket now?
- Will this ticket become cheaper tomorrow?
- What will the exact future price be?
- What is the minimum possible price for this flight?

Instead, it estimates a reference price.

This allows the application to compare:

```text
Observed market price
          vs
Model expected price
```

Example:

```text
Observed price:       USD 356.00
Model expected price: USD 382.74
Deviation:            USD -26.74
```

This deviation can later be used by a decision layer to determine whether a ticket appears below, around, or above the expected market price.

---

## Why CatBoost?

CatBoost was selected because the dataset is primarily tabular and contains several categorical variables.

It can naturally model relationships involving:

- route;
- airport;
- airline;
- booking lead time;
- duration;
- number of stops;
- departure time.

It also avoids the need for large one-hot encoded feature matrices.

---

## Warm Start and Cold Start

The system currently maintains two CatBoost models.

### Warm Start Model

The warm model is used when the route had sufficient historical representation during training.

During the current training procedure, routes require at least 10 observations to become eligible for the warm-start dataset.

At training time:

```text
Eligible warm routes: 367
Ignored routes:       92
```

The set of known routes is stored inside the model metadata:

```text
warm_known_routes
```

This list is frozen together with the trained model artifacts.

Current warm benchmark:

```text
MAE   = 78.09 USD
RMSE  = 123.78 USD
R²    = 0.7333
MAPE  = 17.10%
```

Route-median baseline:

```text
MAE = 103.85 USD
```

The CatBoost model therefore improves over a simple historical route median in the current evaluation.

### Cold Start Model

The cold model is used when the exact route is not part of the warm model's known-route set.

The objective is to estimate prices using patterns learned from variables such as:

```text
origin
destination
airline
duration
stops
booking lead time
departure time
```

even when the exact origin-destination pair was not sufficiently represented during training.

Current cold benchmark:

```text
MAE   = 109.37 USD
RMSE  = 141.01 USD
R²    = 0.4763
MAPE  = 23.74%
```

Global-median baseline:

```text
MAE = 160.69 USD
```

---

## How Warm / Cold Selection Works

The prediction service does not inspect the current GOLD table to determine whether a route is known.

Instead, the known-route list is stored inside the model metadata at training time.

Conceptually:

```text
route in warm_known_routes
        ↓
       yes
        ↓
   Warm Model
```

Otherwise:

```text
route not in warm_known_routes
        ↓
       Cold Model
```

This prevents training-serving inconsistencies.

A route that appears in the database after model training remains a cold-start route until the model is retrained.

---

## Real Cold-Start Example

A newly tested route produced:

```text
Route: THE -> CGH
Company: Azul Linhas Aereas
Departure: 20:35
Arrival: 19:25
Duration: 1380 minutes
Stops: 1
Booking lead time: 3 days
```

The route was not part of the warm model metadata:

```text
route_known = false
model_used  = cold
```

Results:

```text
Observed price:       USD 478.00
Model expected price: USD 455.04
Deviation:            USD +22.96
Cold benchmark MAE:   USD 109.37
```

The observed value was therefore well within the historical error range of the cold-start model for this example.

---

## Important Features

Some of the most relevant features observed during model training are:

```text
flight_to
route
company
days_until_departure
duration_minutes
flight_from
search_hour
stops
connection_count
```

One particularly important feature is:

```text
days_until_departure
```

The current dataset shows a strong relationship between booking lead time and observed prices.

For example, historical average prices were substantially higher close to the departure date than several days before departure.

This should be interpreted as an observed predictive relationship rather than a causal conclusion.

---

## Model Training

Training is implemented in:

```text
ml/train_model.py
```

The training procedure generates:

```text
ml/models/flight_price_catboost_cold_v3.cbm
ml/models/flight_price_catboost_warm_v3.cbm
ml/models/flight_price_catboost_v3_metadata.joblib
```

The metadata contains information such as:

```text
features
categorical_features
numeric_features
warm_known_routes
evaluation metrics
feature importance
```

---

## Model Serving

Inference logic is implemented in:

```text
ml/predict.py
```

The prediction layer:

1. receives business-level flight information;
2. recreates the same model features used during training;
3. determines whether the route is warm or cold;
4. selects the appropriate CatBoost artifact;
5. returns the predicted price.

This prevents feature transformation logic from being duplicated inside the API layer.

---

## FastAPI

The model is exposed through FastAPI.

Main endpoint:

```text
POST /predict
```

Health endpoint:

```text
GET /health
```

Example request:

```json
{
  "flight_from": "CNF",
  "flight_to": "MCZ",
  "company": "LATAM",
  "departure_time": "18:00",
  "arrival_time": "02:05",
  "duration_minutes": 480,
  "stops": 1,
  "self_transfer": false,
  "connection_airports": ["GRU"],
  "ticket_date": "2026-09-22",
  "search_timestamp": "2026-09-16 21:07:31"
}
```

Example response:

```json
{
  "predicted_price_usd": 382.74,
  "route": "CNF_MCZ",
  "route_known": true,
  "model_used": "warm",
  "expected_mae_usd": 78.09,
  "approx_price_range_usd": {
    "low": 304.65,
    "high": 460.83
  }
}
```

`expected_mae_usd` is the aggregate benchmark MAE of the selected model.

It should not be interpreted as a formal prediction interval or confidence interval.

---

## Docker

The inference application is containerized.

Current image:

```text
flight-price-api:v1
```

The inference container contains:

```text
FastAPI
predict.py
CatBoost artifacts
```

Training is intentionally separated from the serving container.

---

## Kubernetes / k3s

The inference API is deployed using k3s, a lightweight Kubernetes distribution.

The deployment currently contains two API replicas.

```text
Traefik
   ↓
Ingress
   ↓
flight-price-api Service
   ↓
Pod 1
Pod 2
```

The Kubernetes configuration is stored under:

```text
k8s/
```

including:

```text
deployment.yaml
service.yaml
ingress.yaml
```

The deployment includes:

- two replicas;
- liveness probe;
- readiness probe;
- CPU requests and limits;
- memory requests and limits.

The k3s data directory is configured outside the system root partition:

```text
/mnt/armazenamento/k3s
```

---

## Ingress

Traefik acts as the Kubernetes Ingress Controller.

The API can currently be accessed through:

```text
http://flight-api.local
```

Example:

```bash
curl http://flight-api.local/health
```

Expected response:

```json
{
  "status": "ok"
}
```

The inference flow is therefore:

```text
Client
   ↓
flight-api.local
   ↓
Traefik
   ↓
Ingress
   ↓
Kubernetes Service
   ↓
API Pod
   ↓
CatBoost
```

No `kubectl port-forward` is required for normal local access.

---

## End-to-End Test

The repository contains:

```text
test_pipeline_real.py
```

This script validates the entire architecture using newly scraped live data.

A single command:

```bash
python test_pipeline_real.py
```

executes:

```text
Skiplagged
    ↓
Kafka Producer
    ↓
Kafka Topic
    ↓
Kafka Consumer
    ↓
RAW
    ↓
SILVER
    ↓
GOLD
    ↓
Traefik Ingress
    ↓
FastAPI
    ↓
CatBoost
```

The test calculates the same SHA-256 hash used by the Kafka consumer and tracks the exact events generated during the execution.

This allows the system to verify that the same flight record successfully propagated through all pipeline layers.

---

## End-to-End Example — Warm Route

Real test:

```text
Route: CNF -> MCZ
Company: LATAM
Departure: 18:00
Arrival: 02:05
Duration: 480 minutes
Stops: 1
Booking lead time: 6 days
```

Result:

```text
Observed price:       USD 356.00
Model expected price: USD 382.74
Deviation:            USD -26.74

Model:                warm
Route known:          true
Benchmark MAE:        USD 78.09
```

The observed ticket was within the model's expected error range.

---

## End-to-End Example — Cold Route

Real test:

```text
Route: THE -> CGH
Company: Azul Linhas Aereas
Duration: 1380 minutes
Stops: 1
Booking lead time: 3 days
```

Result:

```text
Observed price:       USD 478.00
Model expected price: USD 455.04
Deviation:            USD +22.96

Model:                cold
Route known:          false
Benchmark MAE:        USD 109.37
```

This example demonstrates that the cold-start model can estimate a reasonable price even when the exact route was not part of the warm training universe.

---

## Current Dataset

At the initial model-training stage, the GOLD dataset contained approximately:

```text
9,192 flight observations
459 routes
4 airline categories
```

The current validated range of:

```text
days_until_departure
```

is:

```text
0 to 7 days
```

Therefore, predictions far outside this booking horizon should currently be considered out of the validated model domain.

---

## Overfitting and Validation

The warm model presents evidence of a train-validation generalization gap.

Near the end of training:

```text
Training MAE   ≈ 29.6
Validation MAE ≈ 65.6
Test MAE       ≈ 78.1
```

This indicates some overfitting risk.

However, the warm model still substantially outperforms the route-median baseline on the held-out test set:

```text
Route Median MAE = 103.85 USD
CatBoost MAE     = 78.09 USD
```

Therefore, the current evidence suggests:

```text
some overfitting risk
+
meaningful out-of-sample predictive signal
```

The current validation procedure should still be improved before making strong production-performance claims.

---

## Validation Improvement

Multiple itineraries can originate from the same scraper execution and therefore share the same search timestamp.

A more rigorous evaluation should prevent observations from the same search batch from appearing across training and test partitions.

The planned evaluation strategy is:

```text
Grouped Temporal Split
        ↓
group by search batch / search_timestamp
        ↓
past batches → training
future batches → validation/test
```

This will provide a stronger estimate of real-world generalization.

---

## Current Limitations

The current system is an experimental first version.

Important limitations include:

- limited historical time coverage;
- booking horizons currently concentrated between 0 and 7 days;
- no full seasonal cycle;
- no holiday features;
- no event-demand features;
- no historical price trajectory features;
- no explicit airline promotion information;
- no formal prediction intervals;
- limited evidence regarding long-term temporal generalization.

The current model also predicts expected price, not future price direction.

---

## Future Work

Planned improvements include:

1. Increase historical data collection.
2. Expand booking lead times.
3. Use grouped temporal validation by search batch.
4. Add flight price history features.
5. Add seasonal features.
6. Add holiday and event features.
7. Add historical USD/BRL exchange-rate enrichment.
8. Add automated retraining.
9. Add model versioning.
10. Add CI/CD.
11. Add Prometheus metrics.
12. Add Grafana dashboards.
13. Add prediction and data drift monitoring.
14. Add model-performance monitoring.
15. Add formal prediction intervals.
16. Build a decision layer for below/within/above expected market price.
17. Evaluate future-price-direction models separately from expected-price models.
