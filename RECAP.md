# Project Recap — Crypto Streaming Pipeline

> Last updated: 2026-03-19

---

## What This Project Is

A real-time crypto trading analytics pipeline running on GCP. It captures every trade from Coinbase's public WebSocket feed, validates it, and fans it out into multiple sinks for different use cases.

---

## Architecture

```
Coinbase WebSocket (BTC-USD, ETH-USD, SOL-USD)
        ↓
producer/coinbase_producer.py
  - Connects via websocket-client
  - Validates each message with Pydantic (CoinbaseTrade model)
  - Publishes to Pub/Sub with exponential backoff on failure
        ↓
Google Cloud Pub/Sub  (topic: crypto-trades-raw)
        ↓
beam_pipeline/pipeline.py  (Apache Beam — DirectRunner locally, Dataflow on GCP)
        ↓
  ParseTradeFn → validates fields, dead-letters bad messages
        ↓
  ┌─────────────────────────────────────────────────────────┐
  │  valid_trades PCollection (unbounded, global window)    │
  └──────────┬───────────────────────┬──────────────────────┘
             │                       │
     Branch A (no window)    FixedWindow(60s) + KeyByProduct
             │                       │
     FormatRawForBQ          CombinePerKey(AggregateTradesFn)
             │                       │
     WriteToBigQuery         ┌───────┴──────────────────┐
     trades_raw              │                          │
                       Branch B                   Branch D
                       FormatForBigQueryFn         HeartbeatFn
                       WriteToBigQuery             WriteToBigQuery
                       trades_aggregated_1min      pipeline_heartbeat

  valid_trades also feeds:
     Branch C
     WindowInto(FixedWindows(60s), trigger=Repeatedly(AfterProcessingTime(60s)))
     FormatRawForBigQueryFn
     WriteToFiles (ParquetSink)
     gs://crypto-pipeline-v1/parquet/trades/
```

---

## GCP Resources

| Resource | Name |
|---|---|
| Project | `double-archive-274200` |
| Pub/Sub topic | `crypto-trades-raw` |
| Pub/Sub subscription | `crypto-trades-beam-sub` |
| BQ dataset | `crypto_streaming` |
| BQ table — raw trades | `trades_raw` |
| BQ table — 1-min aggregates | `trades_aggregated_1min` |
| BQ table — heartbeat | `pipeline_heartbeat` |
| GCS bucket | `crypto-pipeline-v1` |
| GCS Parquet prefix | `gs://crypto-pipeline-v1/parquet/trades/` |
| Service account | `crypto-pipeline-sa@double-archive-274200.iam.gserviceaccount.com` |

---

## Key Files

| File | What It Does |
|---|---|
| `config/settings.py` | Single source of truth for all GCP resource names. Reads from `.env`. Hard-fails on missing `GCP_PROJECT_ID` and `GCS_BUCKET`. |
| `producer/schemas.py` | Pydantic models: `CoinbaseTrade` (validates a trade), `PubSubMessage` (wraps it for publishing). Uses `field_validator` and `@classmethod`. |
| `producer/coinbase_producer.py` | WebSocket client. Connects, subscribes, validates messages, publishes to Pub/Sub. Exponential backoff on disconnect. |
| `beam_pipeline/bq_schemas.py` | BigQuery table schemas as dicts. Three tables: `TRADES_RAW_SCHEMA`, `TRADES_AGGREGATED_SCHEMA`, `PIPELINE_HEARTBEAT_SCHEMA`. |
| `beam_pipeline/transforms.py` | All Beam transforms: `ParseTradeFn`, `KeyByProductFn`, `AggregateTradesFn`, `FormatForBigQueryFn`, `FormatRawForBigQueryFn`, `HeartbeatFn`, `ParquetSink`. |
| `beam_pipeline/pipeline.py` | Wires everything together. Four output branches (A–D). Supports DirectRunner and DataflowRunner. |
| `scripts/setup_gcp.sh` | Provisions all GCP resources idempotently: APIs, GCS bucket, Pub/Sub, BigQuery, service account + IAM roles. |

---

## BigQuery Tables

### `trades_raw`
Every individual trade event. Written immediately (no windowing — Branch A).

| Column | Type | Notes |
|---|---|---|
| trade_id | STRING | Coinbase trade ID |
| product_id | STRING | e.g. BTC-USD |
| price | FLOAT64 | Trade price |
| size | FLOAT64 | Trade size (quantity) |
| side | STRING | BUY or SELL |
| timestamp | TIMESTAMP | Coinbase event time |

### `trades_aggregated_1min`
One row per (product, 1-minute window). Written after each window closes (Branch B).

| Column | Type | Notes |
|---|---|---|
| product_id | STRING | |
| window_start | TIMESTAMP | Window open time |
| window_end | TIMESTAMP | Window close time |
| trade_count | INT64 | Number of trades in window |
| avg_price | FLOAT64 | |
| total_volume | FLOAT64 | Sum of all trade sizes |
| min_price | FLOAT64 | |
| max_price | FLOAT64 | |
| buy_count | INT64 | |
| sell_count | INT64 | |

### `pipeline_heartbeat`
One row per (product, 1-minute window) — a "proof of life" for each window. A **missing row** means that product had no trades during that minute (producer was down, or no activity). Used for gap detection in Phase 2.

| Column | Type | Notes |
|---|---|---|
| product_id | STRING | |
| window_start | TIMESTAMP | |
| window_end | TIMESTAMP | |
| trade_count | INT64 | Should match trades_aggregated_1min |

**Gap detection query:**
```sql
-- Find all minutes where BTC-USD had no trades
WITH
  spine AS (
    SELECT ts AS window_start
    FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR), MINUTE),
      TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE),
      INTERVAL 1 MINUTE
    )) AS ts
  )
SELECT spine.window_start
FROM spine
LEFT JOIN crypto_streaming.pipeline_heartbeat hb
  ON hb.window_start = spine.window_start
  AND hb.product_id = 'BTC-USD'
WHERE hb.window_start IS NULL
ORDER BY spine.window_start;
```

---

## GCS Parquet Files

Written to `gs://crypto-pipeline-v1/parquet/trades/` once per 60-second window (Branch C).

File naming: `trades-{window_start}-{window_end}-{shard}-of-{total}.parquet`

Example:
```
trades-2026-03-19T17:04:00-2026-03-19T17:05:00-00000-of-00002.parquet
trades-2026-03-19T17:04:00-2026-03-19T17:05:00-00001-of-00002.parquet
```

The window boundaries in the filename make it easy to identify which time period each file covers. In Phase 4, a batch pipeline will read `yesterday's` files by filtering on the filename prefix.

**Schema:** same 6 fields as `trades_raw` (trade_id, product_id, price, size, side, timestamp).

---

## Beam Concepts Used

| Concept | Where Used | What It Does |
|---|---|---|
| `DoFn` | `ParseTradeFn`, `FormatRawForBigQueryFn`, `HeartbeatFn` | Applied to each element individually |
| `CombineFn` | `AggregateTradesFn` | Aggregates many elements into one (create → add → merge → extract) |
| `FixedWindows` | Branches B, C | Groups elements into non-overlapping 60s buckets |
| `CombinePerKey` | Branch B | Runs `AggregateTradesFn` once per `(product_id, window)` |
| `WindowParam` | `FormatForBigQueryFn`, `HeartbeatFn` | Injects window start/end into the DoFn |
| Side outputs | `ParseTradeFn` | Routes bad messages to `dead_letter` tag instead of main output |
| `WriteToBigQuery` | Branches A, B, D | Streaming inserts — fires immediately, no window needed |
| `WriteToFiles` + `FileSink` | Branch C | Streaming-safe Parquet writes (old `WriteToParquet` is batch-only) |
| `Repeatedly(AfterProcessingTime)` | Branch C window | Fires trigger on wall-clock time, not watermark — needed because DirectRunner watermark can lag behind live Pub/Sub |
| `AccumulationMode.DISCARDING` | Branch C window | Each trigger pane only contains new elements — prevents duplicates |

---

## Known Quirks & Decisions

### `WriteToFiles` instead of `WriteToParquet`
`WriteToParquet` uses Beam's old `Write` sink API which internally re-applies global windowing before `GroupByKey`. This makes it batch-only — it throws `ValueError: GroupByKey cannot be applied to an unbounded PCollection with global windowing` in streaming mode. `WriteToFiles` with a custom `FileSink` is the streaming-safe replacement.

### `Repeatedly(AfterProcessingTime)` trigger
The default trigger for `FixedWindows` fires when the **watermark** passes the window end. In DirectRunner, the watermark tracks event timestamps from Pub/Sub messages. After draining the initial backlog, the watermark can lag behind the current time, causing windows to never close. Using `Repeatedly(AfterProcessingTime(60))` fires based on wall-clock time instead, guaranteeing a Parquet flush every 60 seconds regardless of watermark state.

### `AfterProcessingTime` alone is unsafe
Beam rejects `AfterProcessingTime` on its own with `MAY_FINISH` error — it fires once and stops, which could drop late-arriving elements. Wrapping it in `Repeatedly(...)` re-arms the trigger after each fire.

### DirectRunner vs Dataflow
DirectRunner is good for local testing but has reliability issues with long-running streaming (watermark lag, occasional stalls). Dataflow handles live Pub/Sub streams reliably. Everything in this project runs on both — switch with `--runner=DataflowRunner`.

### Orphaned temp files warning
`WriteToFiles` logs warnings about orphaned temp files in `gs://crypto-pipeline-v1/temp/`. This happens when a trigger fires mid-window and Beam retries a bundle — the temp file from the failed attempt isn't cleaned up. Non-fatal; the successfully moved files are intact.

---

## How to Run Locally

```bash
# Load env vars (handles Windows CRLF line endings)
set -a && source .env && set +a
export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/sa-key.json
export PYTHONPATH=$(pwd)

# Terminal 1 — producer
python producer/coinbase_producer.py

# Terminal 2 — pipeline
python beam_pipeline/pipeline.py --runner=DirectRunner
```

`.env` must contain:
```
GCP_PROJECT_ID=double-archive-274200
GCS_BUCKET=crypto-pipeline-v1
GCP_REGION=us-central1
```

---

## Phase Status

| Phase | What | Status |
|---|---|---|
| 1a | Coinbase WS → Pub/Sub → BigQuery (raw + aggregated) | ✅ Done |
| 1b | GCS Parquet backup + pipeline heartbeat table | ✅ Done |
| 2 | Historical backfill via Coinbase REST API (uses heartbeat gaps) | Next |
| 3 | Looker Studio dashboard with materialized views | Planned |
| 4 | Daily batch reconciliation (GCS Parquet → BQ staging → MERGE) | Planned |
| 5 | ML feature engineering → BigQuery ML → Vertex AI | Planned |

---

## What Phase 2 Will Build

The `pipeline_heartbeat` table tells us exactly which minutes had no data. Phase 2 adds:

- `backfill/gap_detector.py` — queries heartbeat for missing windows
- `backfill/coinbase_rest_client.py` — fetches historical candles from Coinbase REST API
- `backfill/backfill_runner.py` — orchestrates: find gaps → fetch → load into BQ

Limitation: Coinbase REST gives pre-aggregated 1-min candles (OHLCV), not individual trades. So `trades_aggregated_1min` can be fully backfilled, but `trades_raw` can only be backfilled for the recent ~24–48h window where individual trade history is available.
