"""
Phase 2 backfill orchestrator.

Steps:
  1. Query pipeline_heartbeat for missing windows (gap_detector).
  2. For each product, fetch the missing candles from Coinbase REST API.
  3. Map OHLCV candles → trades_aggregated_1min rows.
  4. Insert rows into trades_aggregated_1min.
  5. Insert synthetic heartbeat rows so the gaps are not re-processed on
     the next run.

Usage:
    python -m backfill.backfill_runner [--lookback-hours N] [--dry-run]

Notes on data fidelity:
    Coinbase REST candles provide OHLCV data, NOT individual trades.
    The following fields cannot be derived and are set to 0:
        trade_count, buy_count, sell_count
    avg_price is approximated as (open + close) / 2.
"""

import argparse
from datetime import timezone
from itertools import groupby

import structlog
from google.cloud import bigquery

from backfill.coinbase_rest_client import fetch_candles
from backfill.gap_detector import find_gaps
from config import settings

log = structlog.get_logger()


# ── Row builders ──────────────────────────────────────────────────────────────

def _to_aggregated_row(candle: dict) -> dict:
    """Map a Coinbase OHLCV candle to a trades_aggregated_1min BigQuery row."""
    return {
        "product_id": candle["product_id"],
        "window_start": candle["window_start"].isoformat(),
        "window_end": candle["window_end"].isoformat(),
        "trade_count": 0,
        "avg_price": round((candle["open"] + candle["close"]) / 2, 8),
        "total_volume": candle["volume"],
        "min_price": candle["low"],
        "max_price": candle["high"],
        "buy_count": 0,
        "sell_count": 0,
    }


def _to_heartbeat_row(candle: dict) -> dict:
    """Create a synthetic heartbeat row to mark a gap as backfilled."""
    return {
        "product_id": candle["product_id"],
        "window_start": candle["window_start"].isoformat(),
        "window_end": candle["window_end"].isoformat(),
        "trade_count": 0,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def run_backfill(lookback_hours: int = 24, dry_run: bool = False) -> None:
    client = bigquery.Client(project=settings.GCP_PROJECT_ID)
    agg_table = f"{settings.GCP_PROJECT_ID}.{settings.BQ_DATASET}.{settings.BQ_TABLE_AGGREGATED}"
    heartbeat_table = f"{settings.GCP_PROJECT_ID}.{settings.BQ_DATASET}.{settings.BQ_TABLE_HEARTBEAT}"

    log.info("backfill.start", lookback_hours=lookback_hours, dry_run=dry_run)

    gaps = find_gaps(lookback_hours=lookback_hours)
    if not gaps:
        log.info("backfill.no_gaps_found")
        return

    log.info("backfill.gaps_found", count=len(gaps))

    # Sort so groupby works correctly
    gaps.sort(key=lambda g: (g["product_id"], g["window_start"]))

    total_inserted = 0

    for product_id, group_iter in groupby(gaps, key=lambda g: g["product_id"]):
        group = list(group_iter)
        range_start = group[0]["window_start"]
        range_end = group[-1]["window_end"]

        log.info(
            "backfill.fetching_candles",
            product_id=product_id,
            start=range_start.isoformat(),
            end=range_end.isoformat(),
            gap_windows=len(group),
        )

        candles = fetch_candles(product_id, range_start, range_end)

        # Index candles by UNIX timestamp for reliable matching
        candle_index = {int(c["window_start"].timestamp()): c for c in candles}

        agg_rows = []
        heartbeat_rows = []

        for gap in group:
            ws = gap["window_start"]
            if ws.tzinfo is None:
                ws = ws.replace(tzinfo=timezone.utc)

            candle = candle_index.get(int(ws.timestamp()))
            if candle is None:
                log.warning(
                    "backfill.no_candle_for_gap",
                    product_id=product_id,
                    window_start=ws.isoformat(),
                )
                continue

            agg_rows.append(_to_aggregated_row(candle))
            heartbeat_rows.append(_to_heartbeat_row(candle))

        if not agg_rows:
            log.info("backfill.nothing_to_insert", product_id=product_id)
            continue

        if dry_run:
            log.info(
                "backfill.dry_run",
                product_id=product_id,
                would_insert=len(agg_rows),
                sample=agg_rows[0],
            )
            continue

        # Insert aggregated rows
        errors = client.insert_rows_json(agg_table, agg_rows)
        if errors:
            log.error("backfill.insert_errors", table=agg_table, errors=errors)
        else:
            log.info("backfill.inserted", table=agg_table, count=len(agg_rows), product_id=product_id)

        # Insert synthetic heartbeats to prevent re-processing on next run
        errors = client.insert_rows_json(heartbeat_table, heartbeat_rows)
        if errors:
            log.error("backfill.insert_errors", table=heartbeat_table, errors=errors)
        else:
            log.info("backfill.inserted", table=heartbeat_table, count=len(heartbeat_rows), product_id=product_id)

        total_inserted += len(agg_rows)

    log.info("backfill.complete", total_inserted=total_inserted)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill missing crypto trade windows from Coinbase REST API"
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=24,
        help="How many hours back to search for gaps (default: 24)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Find gaps and fetch candles but do not write to BigQuery",
    )
    args = parser.parse_args()

    run_backfill(lookback_hours=args.lookback_hours, dry_run=args.dry_run)
