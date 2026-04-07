"""
Fetch historical 1-minute OHLCV candles from the Coinbase Advanced Trade
public REST API.

Endpoint: GET /api/v3/brokerage/market/products/{product_id}/candles
No authentication required for this public endpoint.

Rate limit: ~10 req/s for public endpoints. We stay well under that.
Max candles per request: 300. Pagination is handled automatically.
"""

import time
from datetime import datetime, timezone

import requests

CANDLES_URL = (
    "https://api.coinbase.com/api/v3/brokerage/market/products/{product_id}/candles"
)
MAX_CANDLES_PER_REQUEST = 300  # Coinbase hard limit
RATE_LIMIT_DELAY = 0.2         # seconds between requests (~5 req/s)


def fetch_candles(product_id: str, start: datetime, end: datetime) -> list[dict]:
    """
    Fetch 1-minute candles for product_id between start and end (UTC datetimes).

    Paginates automatically if the range spans more than MAX_CANDLES_PER_REQUEST minutes.

    Returns:
        List of dicts with keys:
            product_id, window_start, window_end,
            open, high, low, close, volume
        window_start and window_end are timezone-aware UTC datetimes.
    """
    all_candles = []

    start_ts = int(start.timestamp())
    end_ts = int(end.timestamp())
    chunk_seconds = MAX_CANDLES_PER_REQUEST * 60

    current_start = start_ts
    while current_start < end_ts:
        current_end = min(current_start + chunk_seconds, end_ts)

        resp = requests.get(
            CANDLES_URL.format(product_id=product_id),
            params={
                "start": str(current_start),
                "end": str(current_end),
                "granularity": "ONE_MINUTE",
            },
            timeout=10,
        )
        resp.raise_for_status()

        for c in resp.json().get("candles", []):
            candle_ts = int(c["start"])
            all_candles.append({
                "product_id": product_id,
                "window_start": datetime.fromtimestamp(candle_ts, tz=timezone.utc),
                "window_end": datetime.fromtimestamp(candle_ts + 60, tz=timezone.utc),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": float(c["volume"]),
            })

        current_start = current_end
        time.sleep(RATE_LIMIT_DELAY)

    return all_candles
