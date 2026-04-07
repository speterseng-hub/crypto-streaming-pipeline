"""
Find missing 1-minute windows by querying the pipeline_heartbeat table.

A gap is any (product_id, window_start) combination that exists in the
expected time range but has no corresponding row in pipeline_heartbeat.
"""

from datetime import timezone

from google.cloud import bigquery

from config import settings


def find_gaps(lookback_hours: int = 24) -> list[dict]:
    """
    Query BigQuery for windows missing from pipeline_heartbeat.

    Args:
        lookback_hours: How far back to search (default 24h).

    Returns:
        List of dicts with keys: product_id, window_start, window_end
        (window_start and window_end are timezone-aware UTC datetimes).
    """
    client = bigquery.Client(project=settings.GCP_PROJECT_ID)

    pairs_literal = ", ".join(f"'{p}'" for p in settings.TRADING_PAIRS)

    query = f"""
    WITH
    expected AS (
        SELECT
            product_id,
            t AS window_start,
            TIMESTAMP_ADD(t, INTERVAL 60 SECOND) AS window_end
        FROM
            UNNEST([{pairs_literal}]) AS product_id,
            UNNEST(
                GENERATE_TIMESTAMP_ARRAY(
                    TIMESTAMP_TRUNC(
                        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_hours} HOUR),
                        MINUTE
                    ),
                    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE),
                    INTERVAL 1 MINUTE
                )
            ) AS t
    ),
    actual AS (
        SELECT product_id, window_start
        FROM `{settings.GCP_PROJECT_ID}.{settings.BQ_DATASET}.{settings.BQ_TABLE_HEARTBEAT}`
        WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_hours} HOUR)
    )
    SELECT e.product_id, e.window_start, e.window_end
    FROM expected e
    LEFT JOIN actual a USING (product_id, window_start)
    WHERE a.window_start IS NULL
    ORDER BY e.product_id, e.window_start
    """

    rows = client.query(query).result()

    return [
        {
            "product_id": row.product_id,
            "window_start": _ensure_utc(row.window_start),
            "window_end": _ensure_utc(row.window_end),
        }
        for row in rows
    ]


def _ensure_utc(dt):
    """Attach UTC timezone if the datetime is naive (BigQuery can return naive datetimes)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt
