# backfill/

Phase 2 of the pipeline. Detects windows of time where the streaming pipeline produced no data, fetches historical OHLCV candles from the Coinbase REST API for those windows, and writes backfilled rows into BigQuery so downstream queries have a continuous record.

The backfill module does not replay raw trades — that data is gone once the streaming window closes. Instead it uses Coinbase's public candle (OHLCV) endpoint as the closest available substitute, then maps those candles into the same schema used by the streaming aggregation branch.

---

## Why this module exists

The streaming pipeline is always-on but not perfectly reliable. Network blips, Pub/Sub delivery delays, or a restarted producer can leave 1-minute windows with no data in `trades_aggregated_1min`. Without any repair mechanism, these gaps would appear as missing rows in dashboards and break time-series queries.

The `pipeline_heartbeat` table (written by the Beam pipeline's Phase 1b branch) acts as a ledger: one row per product per 1-minute window that was actually processed. Any window in the expected time range that has no corresponding heartbeat row is a gap. This module finds those gaps and fills them.

---

## Architecture / flow

```
pipeline_heartbeat (BigQuery)
        |
        | gap_detector.find_gaps()
        | LEFT JOIN expected windows vs. actual rows
        v
List of (product_id, window_start, window_end) gaps
        |
        | grouped by product_id
        v
coinbase_rest_client.fetch_candles()
        | GET /api/v3/brokerage/market/products/{id}/candles
        | paginated in 300-candle chunks
        v
List of OHLCV candle dicts
        |
        | _to_aggregated_row()      _to_heartbeat_row()
        v                                    v
trades_aggregated_1min (BQ)     pipeline_heartbeat (BQ)
  (backfilled OHLCV data)        (synthetic rows, prevents re-processing)
```

The two BigQuery writes at the end are intentional and coupled: inserting into `pipeline_heartbeat` after a successful aggregation write is what prevents the backfill runner from re-detecting and re-inserting the same windows on the next run.

---

## Files

### `__init__.py`

Empty. Marks the directory as a Python package so the module can be invoked as `python -m backfill.backfill_runner` and imported as `from backfill.gap_detector import find_gaps`.

---

### `gap_detector.py`

**Purpose:** Query BigQuery to identify which 1-minute windows are missing from `pipeline_heartbeat` within a given lookback period. Returns a list of gap descriptors that the runner will pass to the REST client.

#### Module-level imports and dependencies

- `google.cloud.bigquery` — executes the gap query against BigQuery.
- `config.settings` — provides `GCP_PROJECT_ID`, `BQ_DATASET`, `BQ_TABLE_HEARTBEAT`, and `TRADING_PAIRS`.

#### Functions

---

##### `find_gaps(lookback_hours: int = 24) -> list[dict]`

Builds and executes a BigQuery SQL query that computes the set difference between every expected 1-minute window and every observed heartbeat row.

**Arguments:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lookback_hours` | `int` | `24` | How many hours back from now to consider. Larger values catch older gaps but cost more to query. |

**Returns:**

A list of dicts, one per missing window, each containing:

| Key | Type | Description |
|---|---|---|
| `product_id` | `str` | Trading pair, e.g. `"BTC-USD"` |
| `window_start` | `datetime` | Start of the missing window (timezone-aware UTC) |
| `window_end` | `datetime` | End of the missing window, exactly 60 seconds after `window_start` (timezone-aware UTC) |

Returns an empty list if no gaps exist.

**How the query works:**

The query has two CTEs:

1. `expected` — generates every expected (product_id, window_start) combination by cross-joining `TRADING_PAIRS` against a `GENERATE_TIMESTAMP_ARRAY` of 1-minute buckets covering the lookback window. Both endpoints are truncated to the minute with `TIMESTAMP_TRUNC` so bucket boundaries are clean.

2. `actual` — selects all (product_id, window_start) rows from `pipeline_heartbeat` within the same time range.

A `LEFT JOIN` of `expected` onto `actual` with `WHERE a.window_start IS NULL` produces only the rows that exist in `expected` but not in `actual` — the gaps.

**Design decision — generate in SQL rather than Python:**
Generating the expected windows inside BigQuery (using `GENERATE_TIMESTAMP_ARRAY`) keeps all the time arithmetic server-side and avoids the overhead of fetching the full heartbeat table to Python. The query scans only the relevant date partition of `pipeline_heartbeat`.

**Design decision — `TRADING_PAIRS` injected as a literal:**
`settings.TRADING_PAIRS` is interpolated directly into the SQL string as `'BTC-USD', 'ETH-USD', 'SOL-USD'`. This avoids parameterized query complexity for a value that is not user-supplied and changes only via environment variable.

---

##### `_ensure_utc(dt) -> datetime`

Private helper. Attaches the UTC timezone to a naive `datetime` returned by BigQuery. Does nothing if the datetime already has timezone info.

**Why this is needed:** The BigQuery Python client occasionally returns `TIMESTAMP` values as naive `datetime` objects depending on the version and context. Downstream code (the REST client, the runner) compares datetimes and calls `.isoformat()` — both operations require timezone-aware objects to be unambiguous.

---

### `coinbase_rest_client.py`

**Purpose:** Fetch 1-minute OHLCV candles from the Coinbase Advanced Trade public REST API. Handles pagination transparently and returns normalized candle dicts.

#### Module-level constants

| Constant | Value | Description |
|---|---|---|
| `CANDLES_URL` | `https://api.coinbase.com/api/v3/brokerage/market/products/{product_id}/candles` | URL template for the candles endpoint. `{product_id}` is filled by `str.format()` at call time. |
| `MAX_CANDLES_PER_REQUEST` | `300` | Coinbase hard limit on candles per API call. Requests that span more than 300 minutes are automatically split into chunks. |
| `RATE_LIMIT_DELAY` | `0.2` | Seconds to sleep between paginated requests, giving an effective rate of ~5 req/s against a limit of ~10 req/s. |

#### Functions

---

##### `fetch_candles(product_id: str, start: datetime, end: datetime) -> list[dict]`

Fetches all 1-minute candles for `product_id` in the half-open interval `[start, end)`. Paginates automatically if the range spans more than 300 minutes.

**Arguments:**

| Parameter | Type | Description |
|---|---|---|
| `product_id` | `str` | Trading pair, e.g. `"BTC-USD"`. Used in the URL path. |
| `start` | `datetime` | Inclusive range start (timezone-aware UTC). |
| `end` | `datetime` | Exclusive range end (timezone-aware UTC). |

**Returns:**

A list of dicts, one per candle, each containing:

| Key | Type | Description |
|---|---|---|
| `product_id` | `str` | Echoed from the argument |
| `window_start` | `datetime` | Candle open time (timezone-aware UTC) |
| `window_end` | `datetime` | Candle close time, exactly 60 seconds after `window_start` (timezone-aware UTC) |
| `open` | `float` | Opening price |
| `high` | `float` | Highest price |
| `low` | `float` | Lowest price |
| `close` | `float` | Closing price |
| `volume` | `float` | Total volume traded in base currency |

**Pagination logic:**

`start` and `end` are converted to UNIX timestamps. The loop advances `current_start` by `MAX_CANDLES_PER_REQUEST * 60` seconds per iteration until `current_start >= end_ts`:

```
[start_ts ──── +18000s ──── current_end]  request 1 (300 candles)
               [current_end ──── +18000s ──── next_end]  request 2
               ...
```

`time.sleep(RATE_LIMIT_DELAY)` is called after each request to stay well under Coinbase's rate limit.

**Design decision — UNIX timestamps for API params:**
The Coinbase candles endpoint takes `start` and `end` as UNIX epoch integers (not ISO strings). Converting from `datetime` once at the top of the function keeps the pagination loop simple and avoids repeated `datetime` arithmetic.

**Design decision — `window_end` computed locally:**
The API only returns `start` for each candle (the `c["start"]` field). `window_end` is always `candle_start + 60` for 1-minute granularity. This is computed in Python rather than relying on the API to supply it.

**Error handling:**
`resp.raise_for_status()` raises `requests.HTTPError` for 4xx/5xx responses. The caller (`run_backfill`) does not catch this — a failed request will abort the entire backfill run. This is intentional for now: partial inserts with undetected gaps are harder to diagnose than a hard failure.

---

### `backfill_runner.py`

**Purpose:** Orchestrator for the full backfill workflow. Calls `find_gaps`, groups the results by product, fetches candles for each product, maps candles to BigQuery row format, and inserts rows into both `trades_aggregated_1min` and `pipeline_heartbeat`. Also serves as the CLI entry point.

#### Module-level setup

- `structlog.get_logger()` — structured logging. All log lines carry named key-value fields (e.g., `product_id=`, `count=`) making them easy to filter in Cloud Logging.
- `bigquery.Client` — instantiated once per `run_backfill` call, shared across all inserts.

---

#### Row builder functions

---

##### `_to_aggregated_row(candle: dict) -> dict`

Maps a single OHLCV candle dict to a `trades_aggregated_1min` BigQuery row.

**Input:** Candle dict as returned by `fetch_candles`.

**Output:** Dict with keys matching `TRADES_AGGREGATED_SCHEMA`:

| Field | Source | Note |
|---|---|---|
| `product_id` | `candle["product_id"]` | Passed through |
| `window_start` | `candle["window_start"].isoformat()` | BigQuery accepts ISO 8601 strings for TIMESTAMP fields |
| `window_end` | `candle["window_end"].isoformat()` | Same |
| `trade_count` | `0` | Not derivable from candles — see data fidelity note |
| `avg_price` | `(open + close) / 2` | Approximation — see data fidelity note |
| `total_volume` | `candle["volume"]` | Exact |
| `min_price` | `candle["low"]` | Exact |
| `max_price` | `candle["high"]` | Exact |
| `buy_count` | `0` | Not derivable from candles |
| `sell_count` | `0` | Not derivable from candles |

**Data fidelity note:**
OHLCV candles are not individual trades. Three fields in `trades_aggregated_1min` that the streaming pipeline computes from real trades cannot be recovered from candles:

- `trade_count` — the number of individual trade executions within the window. Set to `0`.
- `buy_count` / `sell_count` — directional split of trades. Set to `0`.
- `avg_price` — the streaming pipeline computes this as `sum(price * size) / sum(size)` (a volume-weighted average). The backfill approximates it as `(open + close) / 2`, which is a time-weighted midpoint. These will differ, especially in volatile windows. Backfilled rows can be identified in queries by checking `trade_count = 0`.

---

##### `_to_heartbeat_row(candle: dict) -> dict`

Creates a synthetic `pipeline_heartbeat` row from a candle.

**Input:** Candle dict as returned by `fetch_candles`.

**Output:** Dict with keys: `product_id`, `window_start` (ISO string), `window_end` (ISO string), `trade_count` (always `0`).

**Purpose:** After inserting backfilled aggregation rows, inserting matching heartbeat rows ensures that `find_gaps` will not re-detect these windows as gaps on the next run. The `trade_count=0` distinguishes a synthetic heartbeat from one written by the live streaming pipeline.

---

#### Main function

---

##### `run_backfill(lookback_hours: int = 24, dry_run: bool = False) -> None`

Orchestrates the full backfill workflow.

**Arguments:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lookback_hours` | `int` | `24` | Passed through to `find_gaps`. Controls how far back to search. |
| `dry_run` | `bool` | `False` | If `True`, detects gaps and fetches candles but does not write anything to BigQuery. Logs what would be inserted instead. |

**Step-by-step flow:**

1. Instantiate a `bigquery.Client` and resolve the fully-qualified table IDs from `settings`.
2. Call `find_gaps(lookback_hours)`. If no gaps are found, log and return early.
3. Sort gaps by `(product_id, window_start)` so `itertools.groupby` can group contiguous windows by product correctly.
4. For each product group:
   a. Determine the overall range: `range_start = group[0]["window_start"]`, `range_end = group[-1]["window_end"]`. This allows a single `fetch_candles` call per product rather than one call per gap window.
   b. Call `fetch_candles(product_id, range_start, range_end)`.
   c. Build a `candle_index` dict keyed by UNIX timestamp for O(1) lookup per gap.
   d. For each gap in the group, look up the matching candle by timestamp. Log a warning and skip if no candle is found (Coinbase may not have data for that window).
   e. Build `agg_rows` and `heartbeat_rows` lists.
5. If `dry_run`, log a preview of the first row and move to the next product.
6. Otherwise, insert `agg_rows` into `trades_aggregated_1min` and `heartbeat_rows` into `pipeline_heartbeat` using `client.insert_rows_json`. Log any insertion errors returned by the BigQuery streaming API.
7. Log a final summary with `total_inserted`.

**Design decision — one `fetch_candles` call per product, not per gap:**
Gaps for a single product are almost always contiguous or close together (a short outage). Fetching the entire span in one call (with auto-pagination) is much more efficient than N calls for N individual gaps. The candle index then filters down to exactly the windows that were missing.

**Design decision — heartbeat insert after aggregation insert:**
If the aggregation insert succeeds but the heartbeat insert fails, the gap will be re-detected and re-inserted on the next run. BigQuery streaming inserts are idempotent at the row level only if `insertId` is provided — this implementation does not set `insertId`, so a partial re-run can produce duplicate aggregation rows. See Known Gaps below.

**Design decision — `insert_rows_json` vs. load jobs:**
`client.insert_rows_json` uses the BigQuery streaming insert API, which has low latency (rows are queryable within seconds). A load job would be more efficient for large batches but adds complexity. For the expected scale of backfill (tens to hundreds of rows per run), streaming inserts are appropriate.

---

#### CLI entry point

The `if __name__ == "__main__":` block at the bottom of `backfill_runner.py` parses two arguments and calls `run_backfill`:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--lookback-hours` | `int` | `24` | Hours to search for gaps |
| `--dry-run` | flag | `False` | Preview mode — no BigQuery writes |

---

## How to run

**Normal backfill (last 24 hours):**
```bash
python -m backfill.backfill_runner
```

**Extended backfill (last 48 hours):**
```bash
python -m backfill.backfill_runner --lookback-hours 48
```

**Dry run — preview gaps without writing to BigQuery:**
```bash
python -m backfill.backfill_runner --lookback-hours 48 --dry-run
```

**Environment requirements:**

The module reads from `config/settings.py`, which requires these environment variables (or `.env` file):

| Variable | Description |
|---|---|
| `GCP_PROJECT_ID` | GCP project hosting BigQuery and Pub/Sub |
| `BQ_DATASET` | BigQuery dataset name (default: `crypto_streaming`) |
| `BQ_TABLE_AGGREGATED` | Aggregated table name (default: `trades_aggregated_1min`) |
| `BQ_TABLE_HEARTBEAT` | Heartbeat table name (default: `pipeline_heartbeat`) |
| `TRADING_PAIRS` | Comma-separated list of pairs (default: `BTC-USD,ETH-USD,SOL-USD`) |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service account key JSON |

**Loading env vars on Windows (Git Bash):**
```bash
set -a && source .env && set +a
export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/sa-key.json
export PYTHONPATH=$(pwd)
python -m backfill.backfill_runner --lookback-hours 24
```

---

## Known gaps / limitations

| Gap | Impact | Recommendation |
|---|---|---|
| `trade_count`, `buy_count`, `sell_count` are always `0` in backfilled rows | Directional analysis and trade frequency metrics are unavailable for backfilled windows | Accept as a known limitation; annotate backfilled rows with a `source` column (`"stream"` vs. `"backfill"`) to make filtering explicit |
| `avg_price` is a time-weighted midpoint, not volume-weighted | Price average for backfilled windows will differ from streamed windows, especially during high-volatility periods | Document the approximation; consider using `(open + high + low + close) / 4` as a slightly better estimate |
| No `insertId` on streaming inserts | If the heartbeat insert fails after the aggregation insert succeeds, the next backfill run will re-insert duplicate aggregation rows | Set a deterministic `insertId` per row (e.g., `f"{product_id}_{window_start_ts}"`) to make inserts idempotent |
| HTTP errors abort the entire run | A single Coinbase API failure leaves remaining products unprocessed | Wrap `fetch_candles` in a try/except per product and continue to the next product rather than raising |
| No retry logic on Coinbase API calls | Transient network errors or 5xx responses cause the run to fail hard | Add exponential backoff retries around `requests.get`, similar to the pattern in `coinbase_producer.py` |
| Lookback window is always anchored to `CURRENT_TIMESTAMP()` | Re-running with the same `--lookback-hours` after a successful run will find no gaps (by design), but there is no way to target a specific historical date range | Add `--start` / `--end` CLI arguments to allow ad-hoc backfill of arbitrary date ranges |
| `TRADING_PAIRS` injected as a SQL string literal | If a trading pair name contained a single quote, it would break the SQL. Currently safe because the values are controlled via environment variable | Use parameterized queries or validate pair names at startup |
| No structured distinction between live and backfilled rows in BigQuery | Queries cannot tell whether a given `trades_aggregated_1min` row came from the stream or the backfill | Add a `source STRING` column to `trades_aggregated_1min` with values `"stream"` or `"backfill"` |
