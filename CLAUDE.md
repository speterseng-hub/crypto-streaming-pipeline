# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies (using uv)
uv venv
uv pip install -r requirements.txt

# Run the Coinbase WebSocket producer
python producer/coinbase_producer.py

# Run the Beam pipeline locally (DirectRunner)
python beam_pipeline/pipeline.py --runner=DirectRunner

# Run the Beam pipeline on GCP Dataflow
python beam_pipeline/pipeline.py \
    --runner=DataflowRunner \
    --project=$GCP_PROJECT_ID \
    --region=$GCP_REGION \
    --temp_location=$GCS_TEMP_LOCATION \
    --staging_location=$GCS_STAGING_LOCATION \
    --streaming

# Provision GCP resources
export GCP_PROJECT_ID=your-project-id
export GCS_BUCKET=your-bucket-name
bash scripts/setup_gcp.sh
```

No test runner or linter is configured yet.

## Architecture

Real-time crypto trading analytics pipeline on GCP:

```
Coinbase WebSocket
      ↓
producer/coinbase_producer.py  (WebSocket client, Pydantic validation, exponential backoff)
      ↓
Google Cloud Pub/Sub (crypto-trades-raw)
      ↓
beam_pipeline/pipeline.py  (Dataflow / DirectRunner)
      ├─ Branch A → BigQuery: trades_raw  (every trade event)
      ├─ Branch B → 1-min FixedWindow → CombinePerKey → BigQuery: trades_aggregated_1min
      └─ Branch C → GCS Parquet backup (Phase 1b, planned)
```

## Roadmap

| Phase | Component | Status |
|---|---|---|
| 1a | Coinbase WebSocket producer → Pub/Sub → BigQuery | ✅ Done |
| 1b | GCS Parquet backup branch + pipeline heartbeat / gap detection | 📋 Next |
| 2 | Historical backfill via Coinbase REST API | 📋 Planned |
| 3 | Looker Studio dashboard with materialized views | 📋 Planned |
| 4 | Daily batch reconciliation (Lambda-Lite: GCS Parquet → BQ staging → MERGE) | 📋 Planned |
| 5 | ML feature engineering → BigQuery ML baseline → Vertex AI | 📋 Planned |

## Running locally (Git Bash on Windows)

```bash
# Load env vars (handles Windows CRLF)
set -a && source .env && set +a
export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/sa-key.json
export PYTHONPATH=$(pwd)

# Terminal 1 — producer
python producer/coinbase_producer.py

# Terminal 2 — Beam pipeline
python beam_pipeline/pipeline.py --runner=DirectRunner
```

## Key Modules

- `config/settings.py` — Single source of truth for all GCP resource names, topics, table IDs, and connection config. Reads from `.env` or environment variables.
- `producer/schemas.py` — Pydantic models (`CoinbaseTrade`, `PubSubMessage`) used at the WebSocket boundary for validation.
- `producer/coinbase_producer.py` — Connects to the Coinbase public trades WebSocket feed, validates messages, publishes to Pub/Sub.
- `beam_pipeline/transforms.py` — All Beam `DoFn`/`CombineFn` classes: `ParseTradeFn`, `KeyByProductFn`, `AggregateTradesFn`, `FormatForBigQueryFn`, `FormatRawForBigQueryFn`. Dead-letter messages are currently logged (TODO: route to GCS/BQ).
- `beam_pipeline/bq_schemas.py` — BigQuery table schema definitions (`TRADES_RAW_SCHEMA`, `TRADES_AGGREGATED_SCHEMA`).
- `beam_pipeline/pipeline.py` — Wires all transforms into the full pipeline; supports both `DirectRunner` and `DataflowRunner`.
- `setup.py` — Package metadata used when distributing code to Dataflow workers.

## Environment Setup

Copy `.env.example` to `.env` and fill in `GCP_PROJECT_ID`, `GCP_REGION`, `GCS_BUCKET`, and other GCP resource identifiers. The `config/settings.py` module loads these at runtime.

Default trading pairs: `BTC-USD`, `ETH-USD`, `SOL-USD`.

## Documentation

Each source folder has a detailed README covering every file, class, function, and design decision:

- `config/README.md` — All variables in `settings.py`: loading mechanism, hard-fail vs default pattern, derived GCS paths, known limitations.
- `producer/README.md` — `CoinbaseTrade` and `PubSubMessage` models, every function in `coinbase_producer.py`, WebSocket lifecycle, exponential backoff, Pub/Sub batch settings, known gaps.
- `beam_pipeline/README.md` — BQ schema constants, all `DoFn`/`CombineFn` methods explained, full pipeline graph, execution modes, known gaps.
- `scripts/README.md` — `setup_gcp.sh` step-by-step: API enablement, GCS bucket, Pub/Sub topic/subscription, BigQuery dataset, service account IAM roles, idempotency pattern.
