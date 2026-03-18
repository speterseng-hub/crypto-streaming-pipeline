"""
Central config — reads from environment variables or .env file.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── GCP ──────────────────────────────────────────────────────────────────────
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCP_REGION = os.getenv("GCP_REGION", "us-central1")

# ── Pub/Sub ───────────────────────────────────────────────────────────────────
PUBSUB_TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "crypto-trades-raw")
PUBSUB_SUBSCRIPTION_ID = os.getenv("PUBSUB_SUBSCRIPTION_ID", "crypto-trades-beam-sub")

# ── BigQuery ──────────────────────────────────────────────────────────────────
BQ_DATASET = os.getenv("BQ_DATASET", "crypto_streaming")
BQ_TABLE_RAW = os.getenv("BQ_TABLE_RAW", "trades_raw")
BQ_TABLE_AGGREGATED = os.getenv("BQ_TABLE_AGGREGATED", "trades_aggregated_1min")

# ── GCS ───────────────────────────────────────────────────────────────────────
GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_TEMP_LOCATION = f"gs://{GCS_BUCKET}/temp"
GCS_STAGING_LOCATION = f"gs://{GCS_BUCKET}/staging"
GCS_PARQUET_PREFIX = f"gs://{GCS_BUCKET}/parquet/trades"

# ── Bigtable ──────────────────────────────────────────────────────────────────
BIGTABLE_INSTANCE_ID = os.getenv("BIGTABLE_INSTANCE_ID", "crypto-realtime")
BIGTABLE_TABLE_ID = os.getenv("BIGTABLE_TABLE_ID", "trade-metrics")

# ── Coinbase WebSocket ────────────────────────────────────────────────────────
COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"
TRADING_PAIRS = os.getenv("TRADING_PAIRS", "BTC-USD,ETH-USD,SOL-USD").split(",")
