# crypto-streaming-pipeline

Real-time crypto trading pipeline on GCP — portfolio project.

## Architecture

```
Coinbase WebSocket
        │
        ▼
  Python Producer
        │
        ▼
   Pub/Sub Topic
   (crypto-trades)
        │
        ├──────────────────────────────────┐
        ▼                                  ▼
 Dataflow (Apache Beam)         Dataproc (Spark Structured Streaming)
 - Windowing (1 min avg)        - Anomaly detection
 - Price aggregations           - Volatility metrics
        │                                  │
        ├──────────┐                       ▼
        ▼          ▼                   Bigtable
    BigQuery      GCS                (low-latency lookups)
   (analytics)  (Parquet backup)
```

## Project Structure

```
crypto-streaming-pipeline/
├── producer/              # Coinbase WebSocket → Pub/Sub
├── beam_pipeline/         # Dataflow (Apache Beam) consumer
├── spark_pipeline/        # Dataproc (Spark Structured Streaming) consumer
├── infra/                 # Terraform — GCP resources
├── dags/                  # Cloud Composer (Airflow) DAGs
├── scripts/               # Setup & utility scripts
├── tests/
├── config/
├── requirements.txt
└── setup.py
```

## Phases

| Phase | Component | Status |
|-------|-----------|--------|
| 1a | Coinbase WebSocket producer → Pub/Sub | 🚧 In Progress |
| 1b | Dataflow Beam pipeline → BigQuery + GCS | 📋 Planned |
| 2 | Spark Structured Streaming → Bigtable | 📋 Planned |

## GCP Services Used

- **Pub/Sub** — message broker (decoupling producer/consumers)
- **Dataflow** — managed Apache Beam (batch & streaming)
- **Dataproc** — managed Spark
- **BigQuery** — analytics data warehouse
- **Cloud Storage (GCS)** — Parquet backup / data lake
- **Bigtable** — low-latency NoSQL for real-time lookups
- **Cloud Composer** — managed Airflow (orchestration)

## Documentation

Detailed docs for each source folder:

| Folder README | Contents |
|---|---|
| `config/README.md` | All `settings.py` variables, loading mechanism, design decisions, known limitations |
| `producer/README.md` | Pydantic schemas, WebSocket callbacks, Pub/Sub publish flow, exponential backoff, known gaps |
| `beam_pipeline/README.md` | BigQuery schemas, all Beam transform classes, full pipeline graph, execution modes, known gaps |
| `scripts/README.md` | `setup_gcp.sh` walkthrough: GCP API enablement, resource creation, IAM setup, idempotency pattern |

## Quick Start

```bash
# 1. Setup environment (using uv)
uv venv
source .venv/Scripts/activate  # Windows: .venv\Scripts\activate
uv pip install -r requirements.txt

# 2. Configure GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
export GCP_PROJECT_ID="your-project-id"

# 3. Run producer
python producer/coinbase_producer.py
```
