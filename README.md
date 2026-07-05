# Stock-Flow: Real-Time Stock Market Data Pipeline

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-000000?style=for-the-badge&logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

---

## 📌 Project Overview

This project demonstrates an **end-to-end real-time data pipeline** using the **Modern Data Stack**.

We capture **live stock market data** from an external API, stream it in real time, orchestrate transformations, and deliver analytics-ready insights — all in one unified project.

Rather than pulling API data straight into a dashboard, Stock-Flow treats each stage of the journey as its own layer — ingestion, streaming, storage, transformation, and visualization are all decoupled from one another. That separation is what makes the pipeline replayable, testable, and easy to extend.

The pipeline tracks five tech stocks: **AAPL · AMZN · GOOGL · MSFT · TSLA**.

<p align="center">
  <img src="assets/architecture.jpeg" alt="Stock-Flow Architecture" width="800">
</p>

**Questions the project answers:**
- How do you capture live market data without coupling the API directly to downstream systems?
- How do you keep raw events recoverable before they're transformed?
- How do you turn raw data into analytics-ready tables using a medallion architecture?
- How do you let business users explore price, performance, and volatility interactively?

---

## 🏗️ Architecture

```mermaid
flowchart LR
    A[Stock API] --> B[Python Producer]
    B --> C[(Kafka Topic)]
    C --> D[Python Consumer]
    D --> E[(MinIO Raw Storage)]
    E --> F[Airflow DAG]
    F --> G[(Snowflake)]
    G --> H[Bronze]
    H --> I[Silver]
    I --> J[Gold]
    J --> K[Power BI]
```

| Layer | Tool | Role |
|---|---|---|
| Source | Stock API | Live market quotes |
| Ingestion | Python | Fetch & serialize events |
| Streaming | Kafka | Buffer & distribute events |
| Raw storage | MinIO | Recoverable event landing zone |
| Orchestration | Airflow | Schedule warehouse loads |
| Warehouse | Snowflake | Analytical storage |
| Transformation | dbt | Modeled, tested SQL layers |
| Analytics | Power BI | Interactive dashboard |
| Infra | Docker | Local service management |

---

## 📂 Repository Structure

```
stock-flow/
├── producer/producer.py
├── consumer/consumer.py
├── dag/minio_to_snowflake.py
├── dbt_stocks/
│   ├── dbt_project.yml
│   └── models/
│       ├── bronze/   → bronze_stg_stock_quotes.sql
│       ├── silver/   → silver_clean_stock_quotes.sql
│       └── gold/     → gold_kpi.sql · gold_candlestick.sql · gold_treechart.sql
├── powerbi/StockFlow.pbix
├── assets/architecture.png
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## 🔄 How Data Moves

**1. Ingestion** — a Python producer polls the stock API on a fixed interval, serializes each quote to JSON, and publishes it to a Kafka topic.

```json
{
  "symbol": "AAPL",
  "current_price": 393.45,
  "change_amount": 14.25,
  "change_percent": 4.84,
  "event_time": "2026-07-05T18:30:00"
}
```

**2. Streaming** — Kafka decouples the producer from everything downstream. If storage or the warehouse slows down, events wait safely in the topic instead of getting dropped.

**3. Raw storage** — a consumer reads from Kafka and writes events into MinIO, giving the pipeline a replayable, auditable landing zone before anything touches the warehouse.

**4. Orchestration** — an Airflow DAG picks up new raw files, loads them into Snowflake, and runs on a recurring schedule with visibility into failures.

**5. Transformation (dbt, medallion architecture)**

| Layer | Model | Purpose |
|---|---|---|
| 🥉 Bronze | `bronze_stg_stock_quotes` | Standardizes raw schema, stays close to source |
| 🥈 Silver | `silver_clean_stock_quotes` | Casts types, dedupes, validates records |
| 🥇 Gold | `gold_kpi`, `gold_candlestick`, `gold_treechart` | Dashboard-ready business models |

Keeping this logic in dbt means Power BI only ever consumes clean, business-ready tables — never raw computation.

**6. Analytics** — Power BI connects to the Gold models and renders the dashboard below.

---

## 📊 Dashboard

![Power BI Dashboard](PowerBI/Dashboard.jpg)

**Components:**
- **Stock selector** — filter across AAPL, AMZN, GOOGL, MSFT, TSLA
- **KPI cards** — current price, % change, change amount, most volatile stock
- **Price trend** — closing price over time
- **Market map** — treemap colored by direction (teal = up, red = down)
- **Performance ranking** — horizontal bar chart of % change across stocks
- **Risk positioning** — scatter plot of normalized volatility (0–100 scale on both axes)

### Volatility normalization

Both axes of the risk scatter plot use min-max normalization so stocks with different raw volatility scales stay comparable:

```
Normalized Value = (Current − Minimum) / (Maximum − Minimum) × 100
```

<details>
<summary>DAX — Volatility Index & Relative Volatility Index</summary>

```dax
Volatility Index =
VAR MinVol = MINX(ALL(GOLD_TREECHART[SYMBOL]), CALCULATE(MAX(GOLD_TREECHART[VOLATILITY])))
VAR MaxVol = MAXX(ALL(GOLD_TREECHART[SYMBOL]), CALCULATE(MAX(GOLD_TREECHART[VOLATILITY])))
VAR CurrentVol = MAX(GOLD_TREECHART[VOLATILITY])
RETURN DIVIDE(CurrentVol - MinVol, MaxVol - MinVol, 0) * 100
```

```dax
Relative Volatility Index =
VAR MinValue = MINX(ALL(GOLD_TREECHART[SYMBOL]), CALCULATE(MAX(GOLD_TREECHART[RELATIVE_VOLATILITY])))
VAR MaxValue = MAXX(ALL(GOLD_TREECHART[SYMBOL]), CALCULATE(MAX(GOLD_TREECHART[RELATIVE_VOLATILITY])))
VAR CurrentValue = MAX(GOLD_TREECHART[RELATIVE_VOLATILITY])
RETURN DIVIDE(CurrentValue - MinValue, MaxValue - MinValue, 0) * 100
```

</details>

---

## 🚀 Getting Started

**Prerequisites:** Docker Desktop, Python 3.x, a Snowflake account, dbt (Snowflake adapter), Power BI Desktop, a stock API key.

```bash
# 1. Clone
git clone <your-repository-url>
cd stock-flow

# 2. Configure environment
cp .env.example .env   # fill in API, Kafka, MinIO, Snowflake credentials

# 3. Start infrastructure
docker compose up -d
docker compose ps       # verify services are healthy

# 4. Install dependencies
pip install -r requirements.txt

# 5. Run producer & consumer (separate terminals)
python producer/producer.py
python consumer/consumer.py

# 6. Trigger the Airflow DAG
# open the Airflow UI → enable → trigger the stock pipeline DAG

# 7. Run dbt
cd dbt_stocks
dbt debug && dbt run && dbt test

# 8. Connect Power BI to the Gold models and refresh
```

> Never commit `.env`, API keys, or Snowflake credentials.

---

## 🗺️ Status

| Component | Status |
|---|---|
| Live API ingestion | ✅ Done |
| Kafka streaming | ✅ Done |
| MinIO raw storage | ✅ Done |
| Airflow orchestration | ✅ Done |
| Snowflake warehouse | ✅ Done |
| dbt transformations | ✅ Done |
| Power BI dashboard | ✅ Done |
| Cloud deployment | 🔜 Planned |
| CI/CD | 🔜 Planned |
| Monitoring & alerting | 🔜 Planned |

---

## ⚠️ Limitations

- Fixed watchlist of five stocks, bounded by API rate limits
- Runs on local Docker infra, not cloud-deployed
- No schema registry or dead-letter queue yet
- Dashboard freshness depends on upstream pipeline runs

## 🔮 Future Scope

- Configurable watchlists, Kafka Schema Registry, dead-letter queues
- Great Expectations / Soda for data-quality monitoring
- CI/CD for dbt and Python, Airflow failure alerts, historical backfills
- Technical indicators (RSI, MACD, moving averages), anomaly detection, forecasting
- Row-level security in Power BI, end-to-end latency monitoring

---

## 💡 Why This Project

The value here isn't any single tool — it's the integration: a live market event travels from Kafka to MinIO to Snowflake to dbt to Power BI, with every layer serving one clear purpose. It's a working demonstration of real-time ingestion, cloud warehousing, analytics engineering, orchestration, and BI development as one connected system.

---

## 👤 Author

**Rohit Raj**
M.Sc. Economics & Management, IIIT Lucknow
Interested in Data Analytics, Product Analytics, and Data Engineering.

LinkedIn · GitHub · Email — *add links*

---

If this was useful, a ⭐ on the repo is appreciated. Contributions and feedback welcome.