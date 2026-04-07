# CLAUDE.md

## Objective

This is a learning project for GCP data engineering — each phase introduces a new GCP concept (Pub/Sub, Dataflow, BigQuery, GCS, Vertex AI). Prefer approaches that are clear and idiomatic over clever or over-engineered ones.

## Architecture

Real-time crypto trading analytics pipeline on GCP:

```
Coinbase WebSocket
      ↓
producer/coinbase_producer.py  (WebSocket client, Pydantic validation, exponential backoff)
      ↓
Google Cloud Pub/Sub  (topic: crypto-trades-raw)
      ↓
beam_pipeline/pipeline.py  (Dataflow / DirectRunner)
      ├─ Branch A → BigQuery: trades_raw            (every trade event)
      ├─ Branch B → 1-min FixedWindow → CombinePerKey → BigQuery: trades_aggregated_1min
      └─ Branch C → GCS Parquet backup              (gs://<bucket>/parquet/trades/)
            ↓
backfill/gap_detector.py  (reads pipeline_heartbeat BQ table, finds gaps)
      ↓
backfill/coinbase_rest_client.py  (Coinbase REST API, fills gaps)
      ↓
backfill/backfill_runner.py  (orchestrates gap detection → REST fetch → BQ write)
```

## GCP Resources

- **Credentials file:** `sa-key.json` (project root, gitignored — never commit)

All other resource names (Pub/Sub, BigQuery, GCS) are in `config/settings.py`, which reads from `.env`.

## Configuration

All config lives in `config/settings.py`, which reads from env vars (with `.env` fallback via `python-dotenv`). `GCP_PROJECT_ID` and `GCS_BUCKET` are required (no default); 

Never hardcode GCP resource names in pipeline or producer code — always import from `config.settings`.

## Rules

- **Never mock GCP clients in tests** — use real service calls or emulators. Mocks hide schema/auth issues.
- **Pydantic schemas are the contract** — all trade data must pass through `producer/schemas.py`  
- **DirectRunner is for local dev only.** Dataflow is the production runner. When suggesting pipeline changes, consider whether they'll work at Dataflow scale, not just locally.

## Roadmap

| Phase | Component | Status |
|---|---|---|
| 1a | Coinbase WebSocket producer → Pub/Sub → BigQuery | ✅ Done |
| 1b | GCS Parquet backup branch + pipeline heartbeat / gap detection | ✅ Done |
| 2 | Historical backfill via Coinbase REST API | ✅ Done |
| 3 | Looker Studio dashboard with materialized views | 📋 Planned |
| 4 | Daily batch reconciliation (Lambda-Lite: GCS Parquet → BQ staging → MERGE) | 📋 Planned |
| 5 | ML feature engineering → BigQuery ML baseline → Vertex AI | 📋 Planned |

Each source folder has a detailed README covering every file, class, function, and design decision.
