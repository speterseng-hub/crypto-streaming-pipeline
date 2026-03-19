# config/

Central configuration module. All GCP resource names, connection URLs, and runtime settings live here. Every other module imports from this package instead of reading environment variables directly.

---

## Files

### `settings.py`

**Purpose:** Single source of truth for all configuration. Reads from a `.env` file (via `python-dotenv`) or from environment variables set in the shell.

**How it loads:** `load_dotenv()` is called at module import time, which reads the `.env` file in the project root and sets environment variables. After that, `os.environ` and `os.getenv` read those values.

---

### Variables

#### GCP Core

| Variable | Type | Required | Default | Description |
|---|---|---|---|---|
| `GCP_PROJECT_ID` | `str` | yes | — | GCP project ID. Hard-fails at import if missing (`os.environ[...]`). |
| `GCP_REGION` | `str` | no | `"us-central1"` | GCP region for Dataflow, GCS, BigQuery. |

**Design decision:** `GCP_PROJECT_ID` uses `os.environ["KEY"]` (raises `KeyError` if missing) instead of `os.getenv("KEY")` (returns `None`). This causes a hard failure at startup rather than a cryptic error deep in a Dataflow worker.

---

#### Pub/Sub

| Variable | Type | Default | Description |
|---|---|---|---|
| `PUBSUB_TOPIC_ID` | `str` | `"crypto-trades-raw"` | Topic the producer publishes to. |
| `PUBSUB_SUBSCRIPTION_ID` | `str` | `"crypto-trades-beam-sub"` | Subscription the Beam pipeline reads from. |

**Why separate topic and subscription?** Pub/Sub decouples publishers from subscribers. Multiple subscribers (Beam, Spark, a future alerting service) can each have their own subscription to the same topic and receive all messages independently.

---

#### BigQuery

| Variable | Type | Default | Description |
|---|---|---|---|
| `BQ_DATASET` | `str` | `"crypto_streaming"` | Dataset containing all pipeline output tables. |
| `BQ_TABLE_RAW` | `str` | `"trades_raw"` | Every individual trade event. |
| `BQ_TABLE_AGGREGATED` | `str` | `"trades_aggregated_1min"` | 1-minute window aggregates per trading pair. |

---

#### GCS (Google Cloud Storage)

| Variable | Type | Description |
|---|---|---|
| `GCS_BUCKET` | `str` | Bucket name. Required, no default. Used to derive all other GCS paths. |
| `GCS_TEMP_LOCATION` | `str` | `gs://<bucket>/temp` — Dataflow temp files during job execution. |
| `GCS_STAGING_LOCATION` | `str` | `gs://<bucket>/staging` — Dataflow worker packages uploaded here before job starts. |
| `GCS_PARQUET_PREFIX` | `str` | `gs://<bucket>/parquet/trades` — Prefix for Parquet output files (Phase 1b). |

**Design decision:** `GCS_TEMP_LOCATION` and `GCS_STAGING_LOCATION` are derived from `GCS_BUCKET` at import time rather than requiring separate env vars. Changing the bucket propagates to all three paths automatically.

---

#### Bigtable

| Variable | Type | Default | Description |
|---|---|---|---|
| `BIGTABLE_INSTANCE_ID` | `str` | `"crypto-realtime"` | Bigtable instance for Phase 2 low-latency lookups. |
| `BIGTABLE_TABLE_ID` | `str` | `"trade-metrics"` | Table within that instance. |

---

#### Coinbase WebSocket

| Variable | Type | Description |
|---|---|---|
| `COINBASE_WS_URL` | `str` | Hardcoded to `"wss://advanced-trade-ws.coinbase.com"`. Not configurable — this is a fixed external endpoint. |
| `TRADING_PAIRS` | `list[str]` | Parsed from `TRADING_PAIRS` env var (comma-separated). Default: `["BTC-USD", "ETH-USD", "SOL-USD"]`. |

---

## Known Limitations

- **Module-level execution:** All variables are evaluated at import time. This makes the module hard to mock in tests — importing any module that depends on `settings.py` requires `GCP_PROJECT_ID` and `GCS_BUCKET` to be set.
- **No type coercion:** Values are raw strings from env vars; only `TRADING_PAIRS` is explicitly converted (`.split(",")`).
- **Trailing comma bug:** `TRADING_PAIRS="BTC-USD,"` would produce `["BTC-USD", ""]`. No strip/filter applied.
