# crypto-streaming-pipeline

**Real-time crypto trade pipeline on GCP. It streams every BTC-USD, ETH-USD and SOL-USD trade from Coinbase's public WebSocket, validates it and fans it out to BigQuery (raw trades, 1-minute OHLCV aggregates and a pipeline heartbeat) and to Parquet on Cloud Storage. A backfill job detects missing windows and repairs them from the Coinbase REST API.**

Portfolio project.

## Architecture

```
Coinbase WebSocket (BTC-USD, ETH-USD, SOL-USD)
        │
        ▼
Python producer  ── Pydantic validation, exponential backoff
        │
        ▼
Pub/Sub topic (crypto-trades-raw)
        │
        ▼
Apache Beam pipeline  (DirectRunner locally, Dataflow on GCP)
  ParseTradeFn ── invalid messages to a dead-letter output
        │
        ├─► A  raw trades ───────────────────────────► BigQuery: trades_raw
        ├─► B  1-minute fixed windows → OHLCV per product ► BigQuery: trades_aggregated_1min
        ├─► C  1-minute windows → Parquet files ──────► GCS: parquet/trades/
        └─► D  heartbeat per product and window ─────► BigQuery: pipeline_heartbeat

Backfill job (batch, on demand)
  pipeline_heartbeat ─► find missing 1-minute windows ─► Coinbase REST candles
        ─► trades_aggregated_1min + pipeline_heartbeat
```

## Project structure

```
crypto-streaming-pipeline/
├── producer/        # Coinbase WebSocket → Pub/Sub (Pydantic schemas, backoff)
├── beam_pipeline/   # Apache Beam / Dataflow consumer: transforms, BigQuery schemas
├── backfill/        # Gap detection on the heartbeat table + REST backfill
├── config/          # Central settings
├── scripts/         # setup_gcp.sh: APIs, Pub/Sub, BigQuery, GCS and IAM setup
├── requirements.txt
└── setup.py
```

## Phases

| Phase | Component | Status |
|-------|-----------|--------|
| 1a | Coinbase WebSocket producer → Pub/Sub → Beam → BigQuery | ✅ Done |
| 1b | GCS Parquet backup + pipeline heartbeat | ✅ Done |
| 2 | Gap detection and historical backfill via the Coinbase REST API | ✅ Done |
| 3 | Looker Studio dashboard on BigQuery materialized views | 📋 Planned |
| 4 | Daily batch reconciliation (Parquet → staging → `MERGE`) | 📋 Planned |
| 5 | ML feature engineering → BigQuery ML → Vertex AI | 📋 Planned |

Ideas for later phases: a Spark Structured Streaming consumer on Dataproc for anomaly and volatility metrics, Bigtable for low-latency lookups, Terraform for the infrastructure and Cloud Composer for scheduling the backfill.

## GCP services used

- **Pub/Sub:** message broker that decouples the producer from consumers
- **Dataflow:** managed Apache Beam for the streaming pipeline
- **BigQuery:** analytics warehouse (raw trades, 1-minute aggregates, heartbeat)
- **Cloud Storage:** Parquet backup and data lake

## Documentation

Each source folder has its own README:

| Folder README | Contents |
|---|---|
| `config/README.md` | All `settings.py` variables, loading mechanism, design decisions, known limitations |
| `producer/README.md` | Pydantic schemas, WebSocket callbacks, Pub/Sub publish flow, exponential backoff, known gaps |
| `beam_pipeline/README.md` | BigQuery schemas, all Beam transform classes, full pipeline graph, execution modes, known gaps |
| `backfill/README.md` | Gap detection logic, Coinbase candle endpoint, mapping to the aggregate schema, dry-run mode |
| `scripts/README.md` | `setup_gcp.sh` walkthrough: GCP API enablement, resource creation, IAM setup, idempotency pattern |

## Quick start

```bash
# 1. Set up the environment (using uv)
uv venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
uv pip install -r requirements.txt

# 2. Configure GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
export GCP_PROJECT_ID="your-project-id"

# 3. Run the producer
python producer/coinbase_producer.py

# 4. Run the Beam pipeline locally (use --runner=DataflowRunner on GCP)
python beam_pipeline/pipeline.py --runner=DirectRunner

# 5. Find and repair gaps from the last 24 hours (add --dry-run to only report them)
python backfill/backfill_runner.py --lookback-hours 24
```
