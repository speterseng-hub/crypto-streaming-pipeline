# beam_pipeline/

Apache Beam streaming pipeline that reads trade events from Pub/Sub, validates and transforms them, and writes results to BigQuery. Runs locally with `DirectRunner` for development, and on Google Cloud Dataflow for production.

---

## Files

### `bq_schemas.py`

**Purpose:** BigQuery table schema definitions. Kept separate from transforms so the schema is the single source of truth — both for Dataflow's `WriteToBigQuery` and for any manual table creation.

#### Constants

---

##### `TRADES_RAW_SCHEMA`

Schema for the `trades_raw` table. One row per trade event.

| Field | BQ Type | Mode | Description |
|---|---|---|---|
| `trade_id` | `STRING` | REQUIRED | Unique ID from Coinbase |
| `product_id` | `STRING` | REQUIRED | Trading pair, e.g. `"BTC-USD"` |
| `price` | `FLOAT64` | REQUIRED | Execution price |
| `size` | `FLOAT64` | REQUIRED | Volume traded |
| `side` | `STRING` | REQUIRED | `"BUY"` or `"SELL"` |
| `timestamp` | `TIMESTAMP` | REQUIRED | ISO 8601 string; BigQuery parses it automatically |

**Design decision:** All fields are `REQUIRED` (not `NULLABLE`). This enforces the contract that only fully-validated trades reach BigQuery — the `ParseTradeFn` dead-letter mechanism prevents partial records.

---

##### `TRADES_AGGREGATED_SCHEMA`

Schema for the `trades_aggregated_1min` table. One row per trading pair per 1-minute window.

| Field | BQ Type | Mode | Description |
|---|---|---|---|
| `product_id` | `STRING` | REQUIRED | Trading pair |
| `window_start` | `TIMESTAMP` | REQUIRED | Window open time (UTC) |
| `window_end` | `TIMESTAMP` | REQUIRED | Window close time (UTC) |
| `trade_count` | `INT64` | REQUIRED | Number of trades in the window |
| `avg_price` | `FLOAT64` | REQUIRED | Mean execution price |
| `total_volume` | `FLOAT64` | REQUIRED | Sum of `size` across all trades |
| `min_price` | `FLOAT64` | REQUIRED | Lowest execution price |
| `max_price` | `FLOAT64` | REQUIRED | Highest execution price |
| `buy_count` | `INT64` | REQUIRED | Number of BUY-side trades |
| `sell_count` | `INT64` | REQUIRED | Number of SELL-side trades |

---

### `transforms.py`

**Purpose:** All Apache Beam custom transform classes. Each class handles one step in the pipeline. Keeping transforms in a separate file from `pipeline.py` makes them independently testable.

#### Key Beam Concepts Used

- **`DoFn`** — function applied to each element in a `PCollection` (like `map` with side outputs)
- **`CombineFn`** — distributed aggregation (like SQL `GROUP BY` + aggregate functions, but computed in parallel)
- **`WindowParam`** — lets a `DoFn` access the time window metadata of the element being processed
- **Side outputs** — a `DoFn` can emit to multiple named output collections (used for dead-letter routing)

---

#### Classes

---

##### `ParseTradeFn(beam.DoFn)`

**Step 1** in the pipeline. Parses raw Pub/Sub message bytes into a Python dict.

**Input:** `bytes` — raw Pub/Sub message payload
**Main output:** `dict` — validated trade with `price` and `size` as `float`
**Side output:** `"dead_letter"` tag — original bytes for any message that fails parsing or validation

```
process(element, timestamp)
├── json.loads(element.decode("utf-8"))
├── check required fields: {trade_id, product_id, price, size, side, timestamp}
├── convert price and size to float
└── yield trade dict  ← main output
    OR yield TaggedOutput("dead_letter", element)  ← side output
```

**Class attribute — `DEAD_LETTER_TAG`**
A `TaggedOutput` sentinel used to route bad messages. In `pipeline.py`, this tag is referenced as `"dead_letter"` in `.with_outputs("dead_letter", main="valid")`.

**Design decision:** Validation here is simpler than in `schemas.py` — just checks field presence and type coercion. This is intentional: the Beam pipeline handles messages that may have bypassed the producer's Pydantic validation (e.g., replayed messages, messages from other producers).

---

##### `KeyByProductFn(beam.DoFn)`

**Step 2** in Branch B. Transforms a flat trade dict into a `(key, value)` tuple required for `CombinePerKey`.

**Input:** `dict` — validated trade
**Output:** `(str, dict)` — `(product_id, trade)`

```python
yield (trade["product_id"], trade)
```

Beam's `CombinePerKey` requires keyed pairs. This step creates the key that determines how trades are grouped — all `BTC-USD` trades go to one aggregator, all `ETH-USD` trades to another.

---

##### `TradeAggregate` (dataclass)

Typed container for a completed window aggregation result. Used as documentation and for type hints — the actual accumulator in `AggregateTradesFn` is a plain `dict` for Beam serialization compatibility.

Fields mirror the `TRADES_AGGREGATED_SCHEMA` exactly (minus `window_start`/`window_end` which come from `WindowParam`).

---

##### `AggregateTradesFn(beam.CombineFn)`

**Step 3** in Branch B. Aggregates all trades within a 1-minute window for a single trading pair.

**Why `CombineFn` instead of `GroupByKey`?**
`GroupByKey` materializes all elements in memory before aggregating. For a high-volume trading pair (potentially thousands of trades/minute), this is expensive. `CombineFn` computes partial aggregates on each worker and merges them — much more efficient and parallelizable.

The four methods Beam calls:

**`create_accumulator() → dict`**
Returns a fresh zero-state accumulator:
```python
{"count": 0, "price_sum": 0.0, "volume_sum": 0.0,
 "min_price": inf, "max_price": -inf, "buy_count": 0, "sell_count": 0}
```
`min_price` starts at `+inf` so any real price is smaller. `max_price` starts at `-inf` so any real price is larger.

**`add_input(accumulator, trade) → dict`**
Folds one trade into the accumulator. Returns a copy (immutable pattern) with updated counts, sums, and min/max. Called for every trade in the window on a single worker.

**`merge_accumulators(accumulators) → dict`**
Combines partial accumulators from multiple workers into one. This is what makes `CombineFn` distributed — Beam can split the window's trades across workers, each produces a partial accumulator, then they're merged here.

**`extract_output(accumulator) → dict | None`**
Computes the final result from the accumulator:
- Returns `None` if `count == 0` (empty window — no rows written to BQ).
- Computes `avg_price = price_sum / count`.
- Returns the result dict (without `product_id`, `window_start`, `window_end` — those are added by `FormatForBigQueryFn`).

---

##### `FormatForBigQueryFn(beam.DoFn)`

**Step 4** in Branch B. Merges the aggregation result with window metadata into a BigQuery-ready row.

**Input:** `(product_id, agg_dict)` tuple
**Output:** `dict` matching `TRADES_AGGREGATED_SCHEMA`

```python
def process(self, element, window=beam.DoFn.WindowParam):
    product_id, agg = element
    if agg is None:  # empty window from extract_output
        return
    yield {
        "product_id": product_id,
        "window_start": window.start.to_utc_datetime().isoformat(),
        "window_end": window.end.to_utc_datetime().isoformat(),
        **agg,
    }
```

`beam.DoFn.WindowParam` is a special Beam parameter that injects the window boundary timestamps. Without it, you'd have no way to know which time window the aggregated row belongs to.

---

##### `FormatRawForBigQueryFn(beam.DoFn)`

**Branch A only.** Simple pass-through that reformats a trade dict into a BigQuery row. Ensures the dict keys exactly match the `TRADES_RAW_SCHEMA` field names — no extra fields, no missing fields.

---

### `pipeline.py`

**Purpose:** Wires all transforms into a complete streaming pipeline graph. Handles both local (`DirectRunner`) and cloud (`DataflowRunner`) execution.

#### Constants

| Name | Value | Description |
|---|---|---|
| `WINDOW_SIZE_SECONDS` | `60` | Fixed window size for Branch B aggregation. Hardcoded — should be a pipeline argument. |

---

#### Functions

---

##### `build_pipeline_options(argv=None) → PipelineOptions`

Constructs the Beam `PipelineOptions` object that configures how and where the pipeline runs.

**Argument parsing:**
- Uses `argparse` to extract `--runner` from argv before passing the rest to `PipelineOptions`.
- This allows `--runner=DirectRunner` or `--runner=DataflowRunner` to be specified at the command line.

**Options set:**

| Option | Source | Description |
|---|---|---|
| `runner` | CLI arg | `DirectRunner` (local) or `DataflowRunner` (GCP) |
| `project` | `settings.py` | GCP project ID |
| `region` | `settings.py` | GCP region |
| `temp_location` | `settings.py` | GCS path for Dataflow temp files |
| `staging_location` | `settings.py` | GCS path for worker package uploads |
| `setup_file` | hardcoded `"./setup.py"` | Tells Dataflow how to package this repo for workers |
| `streaming` | hardcoded `True` | **Critical** — without this, Beam treats Pub/Sub as a bounded source and the pipeline exits immediately |

**Why `setup_file`?** When Dataflow launches workers, they're fresh VMs with no knowledge of your code. `setup_file` points to `setup.py`, which Dataflow uses to pip-install your package and its dependencies on each worker before processing begins.

---

##### `run(argv=None)`

Defines and launches the pipeline graph using the `with beam.Pipeline(options=options) as p:` context manager. The pipeline runs until the context exits (for `DirectRunner`, this blocks; for `DataflowRunner`, this submits the job and returns).

**Pipeline graph:**

```
p
└── ReadFromPubSub(subscription)          → raw_bytes (PCollection[bytes])
    └── ParseTrades (ParseTradeFn)
        ├── .valid                         → valid_trades (PCollection[dict])
        │   ├── Branch A
        │   │   └── FormatRaw              → FormatRawForBigQueryFn
        │   │       └── WriteRawToBQ       → BigQuery: trades_raw
        │   └── Branch B
        │       └── ApplyWindow            → FixedWindows(60s)
        │           └── KeyByProduct       → KeyByProductFn
        │               └── AggregatePerWindow → CombinePerKey(AggregateTradesFn)
        │                   └── FormatAggregated → FormatForBigQueryFn
        │                       └── WriteAggToBQ → BigQuery: trades_aggregated_1min
        └── .dead_letter                   → LogDeadLetters (lambda, logs only)
```

**BigQuery write settings:**
- `WRITE_APPEND` — new rows are appended; existing rows are never overwritten.
- `CREATE_IF_NEEDED` — Dataflow creates the table if it doesn't exist, using the schema from `bq_schemas.py`.

**Dead-letter handling (current):**
```python
dead_letters | "LogDeadLetters" >> beam.Map(
    lambda x: log.warning("dead_letter: %s", x[:200] if x else "None")
)
```
Dead letters are logged and discarded. In production, they should be written to a GCS path or BigQuery table for inspection.

**TODO (Phase 1b):** Line 131 has a placeholder comment for a `WriteToParquet → GCS` branch on `valid_trades`. This would create a data lake backup of all raw trades in columnar format.

---

## Pipeline Execution Modes

| Mode | Command | Use case |
|---|---|---|
| `DirectRunner` | `python beam_pipeline/pipeline.py --runner=DirectRunner` | Local dev and testing. Runs in-process, no GCP needed (except live Pub/Sub). |
| `DataflowRunner` | `python beam_pipeline/pipeline.py --runner=DataflowRunner ...` | Production. Submits job to GCP Dataflow, auto-scales workers. |

---

## Known Gaps

| Gap | Impact | Recommendation |
|---|---|---|
| Dead letters only logged | Data loss, no audit trail | Write to dead-letter BQ table or GCS |
| `WINDOW_SIZE_SECONDS` hardcoded | Can't tune without code change | Move to `PipelineOptions` custom argument |
| No GCS Parquet branch | No data lake backup | Implement Phase 1b TODO |
| No deduplication | Pub/Sub delivers at-least-once; duplicate trades possible | Track `trade_id` in a side input or use Dataflow exactly-once mode |
