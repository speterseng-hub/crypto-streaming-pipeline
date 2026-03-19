"""
Coinbase WebSocket Producer → Google Cloud Pub/Sub

Connects to Coinbase Advanced Trade WebSocket (no auth needed for public trades).
Publishes each trade event as a JSON message to a Pub/Sub topic.

Usage:
    python producer/coinbase_producer.py

Env vars required:
    GCP_PROJECT_ID, GCS_BUCKET (see .env.example)
"""
import json
import signal
import sys
import time
import threading
#from typing import Optional

import structlog
import websocket
from google.cloud import pubsub_v1

from config.settings import (
    GCP_PROJECT_ID,
    PUBSUB_TOPIC_ID,
    TRADING_PAIRS,
    COINBASE_WS_URL,
)
from producer.schemas import CoinbaseTrade

log = structlog.get_logger(__name__)

# ── Pub/Sub publisher (shared, thread-safe) ───────────────────────────────────
publisher = pubsub_v1.PublisherClient(
    # Batch settings: send up to 100 messages or every 0.1s — good for streaming
    batch_settings=pubsub_v1.types.BatchSettings(
        max_messages=100,
        max_latency=0.1,  # seconds
    )
)
TOPIC_PATH = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)

# ── Metrics (simple in-memory counters) ───────────────────────────────────────
_stats = {"published": 0, "errors": 0, "start_time": time.time()}


def _publish_trade(trade: CoinbaseTrade) -> None:
    """Publish a validated trade to Pub/Sub (non-blocking, uses futures)."""
    future = publisher.publish(
        TOPIC_PATH,
        data=trade.to_pubsub_bytes(),
        # Pub/Sub message attributes — useful for server-side filtering
        product_id=trade.product_id,
        side=trade.side,
    )
    future.add_done_callback(_on_publish_done)


def _on_publish_done(future) -> None:
    """Callback: log errors, count successes."""
    try:
        future.result()
        _stats["published"] += 1
        if _stats["published"] % 100 == 0:
            elapsed = time.time() - _stats["start_time"]
            rate = _stats["published"] / elapsed
            log.info("publish_stats", published=_stats["published"], rate_per_sec=f"{rate:.1f}")
    except Exception as e:
        _stats["errors"] += 1
        log.error("publish_failed", error=str(e))


# ── WebSocket callbacks ───────────────────────────────────────────────────────

def on_open(ws: websocket.WebSocketApp) -> None:
    """Subscribe to market_trades channel for the configured trading pairs."""
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": TRADING_PAIRS,
        "channel": "market_trades",
    }
    ws.send(json.dumps(subscribe_msg))
    log.info("subscribed", pairs=TRADING_PAIRS, topic=TOPIC_PATH)


def on_message(ws: websocket.WebSocketApp, raw: str) -> None:
    """Parse incoming WebSocket message and publish valid trades."""
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("invalid_json", raw=raw[:200])
        return

    msg_type = msg.get("channel")

    # Coinbase sends a 'subscriptions' confirmation on connect — ignore it
    if msg_type == "subscriptions":
        log.info("subscription_confirmed")
        return

    # market_trades channel sends events with a list of trades
    if msg_type == "market_trades":
        events = msg.get("events", [])
        for event in events:
            for raw_trade in event.get("trades", []):
                _process_trade(raw_trade)


def _process_trade(raw_trade: dict) -> None:
    """Validate and publish a single trade dict."""
    try:
        trade = CoinbaseTrade(
            trade_id=raw_trade["trade_id"],
            product_id=raw_trade["product_id"],
            price=float(raw_trade["price"]),
            size=float(raw_trade["size"]),
            side=raw_trade["side"],
            timestamp=raw_trade["time"],
        )
        _publish_trade(trade)
        log.debug("trade_received", pair=trade.product_id, price=trade.price, side=trade.side)
    except Exception as e:
        log.warning("trade_parse_error", error=str(e), raw=str(raw_trade)[:200])


def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
    log.error("websocket_error", error=str(error))


def on_close(ws: websocket.WebSocketApp, close_status_code, close_msg) -> None:
    log.info("websocket_closed", code=close_status_code, msg=close_msg)


# ── Main loop with auto-reconnect ─────────────────────────────────────────────

def run_producer(max_retries: int = -1) -> None:
    """
    Run the WebSocket producer with exponential backoff reconnect.
    max_retries=-1 means infinite retries (production default).
    """
    attempt = 0
    backoff = 1  # seconds

    def _signal_handler(sig, frame):
        log.info("shutdown_requested")
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    while max_retries == -1 or attempt < max_retries:
        attempt += 1
        log.info("connecting", attempt=attempt, url=COINBASE_WS_URL)

        ws = websocket.WebSocketApp(
            COINBASE_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        ws.run_forever(ping_interval=30, ping_timeout=10)

        log.warning("disconnected_will_retry", backoff_sec=backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)  # cap at 60s


if __name__ == "__main__":
    run_producer()
