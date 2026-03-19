"""
BigQuery table schemas.
Defined here so they match exactly what FormatForBigQueryFn produces.
"""

# Raw trades — every single trade event
TRADES_RAW_SCHEMA = {
    "fields": [
        {"name": "trade_id",    "type": "STRING",    "mode": "REQUIRED"},
        {"name": "product_id",  "type": "STRING",    "mode": "REQUIRED"},
        {"name": "price",       "type": "FLOAT64",   "mode": "REQUIRED"},
        {"name": "size",        "type": "FLOAT64",   "mode": "REQUIRED"},
        {"name": "side",        "type": "STRING",    "mode": "REQUIRED"},
        {"name": "timestamp",   "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

# Pipeline heartbeat — one row per (product, window); missing row = gap
PIPELINE_HEARTBEAT_SCHEMA = {
    "fields": [
        {"name": "product_id",   "type": "STRING",    "mode": "REQUIRED"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end",   "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "trade_count",  "type": "INT64",     "mode": "REQUIRED"},
    ]
}

# 1-minute aggregated window results
TRADES_AGGREGATED_SCHEMA = {
    "fields": [
        {"name": "product_id",   "type": "STRING",  "mode": "REQUIRED"},
        {"name": "window_start", "type": "TIMESTAMP","mode": "REQUIRED"},
        {"name": "window_end",   "type": "TIMESTAMP","mode": "REQUIRED"},
        {"name": "trade_count",  "type": "INT64",   "mode": "REQUIRED"},
        {"name": "avg_price",    "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "total_volume", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "min_price",    "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "max_price",    "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "buy_count",    "type": "INT64",   "mode": "REQUIRED"},
        {"name": "sell_count",   "type": "INT64",   "mode": "REQUIRED"},
    ]
}
