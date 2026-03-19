# producer/

WebSocket client that connects to the Coinbase public trades feed and publishes validated trade events to Google Cloud Pub/Sub. This is the entry point of the pipeline — all data originates here.

---

## Files

### `schemas.py`

**Purpose:** Pydantic data models that define the shape of a trade message. Validation happens here, at the system boundary, before anything enters Pub/Sub.

#### Classes

---

##### `CoinbaseTrade`

Represents a single trade event from the Coinbase WebSocket feed.

```python
class CoinbaseTrade(BaseModel):
    trade_id: str       # Unique ID assigned by Coinbase
    product_id: str     # Trading pair, e.g. "BTC-USD"
    price: float        # Execution price
    size: float         # Volume traded (in base currency)
    side: str           # "BUY" or "SELL" (normalized to uppercase)
    timestamp: str      # ISO 8601 string, e.g. "2024-01-15T10:30:00Z"
```

**Validator — `side_must_be_valid`**
- Accepts `"buy"`, `"BUY"`, `"Buy"` etc. and normalizes to uppercase.
- Raises `ValueError` for any other value — message goes to dead letter.
- Uses `@field_validator` (Pydantic v2 API).

**Method — `to_pubsub_bytes() → bytes`**
- Serializes the model to JSON and encodes as UTF-8 bytes.
- Used by the producer before calling `publisher.publish(data=...)`.
- Internally calls `model_dump_json()` (Pydantic v2).

**Class method — `from_pubsub_bytes(data: bytes) → CoinbaseTrade`**
- Deserializes bytes back into a `CoinbaseTrade` instance.
- Used on the consumer (Beam) side to reconstruct the object.
- Internally calls `model_validate_json()` (Pydantic v2).

**Design decisions:**
- `price` and `size` are `float`, not `Decimal`. Acceptable for analytics; would need `Decimal` for financial settlement.
- `timestamp` is kept as `str` to avoid serialization round-trip complexity. BigQuery accepts ISO 8601 strings natively.
- No validation that `timestamp` is a valid ISO 8601 string — only format, not value, is checked by Pydantic here.

---

##### `PubSubMessage`

Envelope model describing the full structure of a Pub/Sub message.

```python
class PubSubMessage(BaseModel):
    data: CoinbaseTrade
    attributes: dict = {}
```

**Note:** This class is not instantiated at runtime. The producer calls `trade.to_pubsub_bytes()` directly and sets attributes separately on the Pub/Sub client call. `PubSubMessage` exists as documentation — it describes the conceptual structure of what is published.

---

### `coinbase_producer.py`

**Purpose:** Connects to the Coinbase Advanced Trade WebSocket public feed, parses incoming trade events, validates them via `CoinbaseTrade`, and publishes to Pub/Sub. Runs indefinitely with automatic reconnection.

#### Module-level globals

| Name | Type | Description |
|---|---|---|
| `publisher` | `pubsub_v1.PublisherClient` | Shared Pub/Sub client. Initialized once at module load. Thread-safe. Configured with batch settings. |
| `TOPIC_PATH` | `str` | Full Pub/Sub topic path: `projects/<project>/topics/<topic>`. |
| `_stats` | `dict` | In-memory counters: `published`, `errors`, `start_time`. Not thread-safe (relies on CPython GIL). |

**Batch settings on the publisher:**
```python
BatchSettings(max_messages=100, max_latency=0.1)
```
Messages are buffered and sent in batches of up to 100, or every 0.1 seconds — whichever comes first. This reduces Pub/Sub API calls while keeping latency low.

---

#### Functions

---

##### `_publish_trade(trade: CoinbaseTrade) → None`

Publishes a validated trade to Pub/Sub asynchronously.

- Calls `publisher.publish(...)` which returns a `Future` immediately (non-blocking).
- Attaches `product_id` and `side` as Pub/Sub **message attributes** — these are metadata that subscribers can filter on server-side without deserializing the message body.
- Registers `_on_publish_done` as a callback on the future.

---

##### `_on_publish_done(future) → None`

Callback invoked when a Pub/Sub publish completes (success or failure).

- On success: increments `_stats["published"]`. Every 100 publishes, logs the cumulative count and publish rate (messages/sec).
- On failure: increments `_stats["errors"]` and logs the error. Message is **lost** — no retry, no dead-letter queue.

**Gap:** Failed publishes are counted and logged but not persisted. In production, failed messages should be retried or written to a dead-letter topic.

---

##### `on_open(ws: WebSocketApp) → None`

WebSocket callback fired when the connection is established.

Sends a Coinbase subscription message:
```json
{
  "type": "subscribe",
  "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD"],
  "channel": "market_trades"
}
```
This tells Coinbase which trading pairs and channel to stream. The `market_trades` channel delivers real-time trade executions (not order book updates).

---

##### `on_message(ws: WebSocketApp, raw: str) → None`

WebSocket callback fired for every incoming message.

1. Parses the raw string as JSON. If parsing fails, logs a warning and returns (dead letter, not persisted).
2. Checks `msg["channel"]`:
   - `"subscriptions"` — confirmation message from Coinbase on connect. Logged and ignored.
   - `"market_trades"` — iterates `msg["events"]`, then each event's `"trades"` list, calling `_process_trade()` for each.
3. Other channel types are silently ignored.

---

##### `_process_trade(raw_trade: dict) → None`

Validates and publishes a single raw trade dict.

1. Maps raw Coinbase fields to `CoinbaseTrade` fields (notably `"time"` → `timestamp`).
2. Explicit `float()` conversion for `price` and `size` — Coinbase sends these as strings.
3. Constructs a `CoinbaseTrade` — Pydantic validates here.
4. On success: calls `_publish_trade()`.
5. On any exception (missing field, validation error, etc.): logs a warning with the raw trade truncated to 200 chars. Message is dropped.

**Gap:** All exceptions are caught together. A missing `trade_id` (validation error) looks the same in logs as a network error. Distinguishing error types would help debugging.

---

##### `on_error(ws: WebSocketApp, error: Exception) → None`

Logs WebSocket-level errors (connection refused, TLS failure, etc.). No recovery logic here — recovery is handled by the reconnect loop in `run_producer`.

---

##### `on_close(ws: WebSocketApp, close_status_code, close_msg) → None`

Logs when the WebSocket connection closes. Normal part of the reconnect cycle.

---

##### `run_producer(max_retries: int = -1) → None`

Main entry point. Runs the WebSocket producer with exponential backoff reconnection.

**Arguments:**
- `max_retries=-1` — infinite retries (production default). Pass a positive integer to limit retries (useful in tests).

**Signal handling:**
- Registers `SIGINT` and `SIGTERM` handlers that call `sys.exit(0)` for graceful shutdown.
- This allows `Ctrl+C` or a `kill` command to cleanly stop the producer.

**Reconnection logic:**
```
attempt 1: connect → disconnect → wait 1s
attempt 2: connect → disconnect → wait 2s
attempt 3: connect → disconnect → wait 4s
...
attempt N: connect → disconnect → wait 60s (capped)
```
`backoff = min(backoff * 2, 60)` — doubles each time, never exceeds 60 seconds. Prevents hammering the Coinbase API if it's down.

**WebSocket config:**
- `ping_interval=30` — sends a WebSocket ping every 30 seconds to keep the connection alive through firewalls/NAT.
- `ping_timeout=10` — if no pong is received within 10 seconds, the connection is considered dead and closed, triggering reconnect.

---

## Data Flow

```
Coinbase WS (raw JSON string)
    ↓ on_message()
JSON parse
    ↓
channel == "market_trades"?
    ↓
iterate events → trades
    ↓ _process_trade()
CoinbaseTrade (Pydantic validation)
    ↓ _publish_trade()
Pub/Sub (async future)
    ↓ _on_publish_done()
_stats updated / error logged
```

---

## Known Gaps

| Gap | Impact | Recommendation |
|---|---|---|
| Dead letters logged only | Lost data in prod | Write to dead-letter Pub/Sub topic or GCS |
| `_stats` not thread-safe | Minor — CPython GIL protects simple int ops | Use `threading.Lock` or `atomic` counter for correctness |
| All exceptions caught together | Hard to debug error types | Distinguish `KeyError`, `ValidationError`, `TypeError` |
| Publish failures not retried | Potential data loss during Pub/Sub outages | The SDK retries transiently, but exhausted retries are silently dropped |
