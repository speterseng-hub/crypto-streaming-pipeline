"""
Apache Beam custom transforms for the crypto trading pipeline.

Key Beam concepts used here:
- DoFn: a function applied to each element in a PCollection
- WindowedValue: element tagged with a time window
- CombineFn: aggregation function (like a groupBy + aggregate in SQL)
"""
import json
from dataclasses import dataclass
from typing import Iterator, Tuple

import apache_beam as beam
from apache_beam.transforms.window import SlidingWindows, FixedWindows


# ── Step 1: Parse raw Pub/Sub messages ───────────────────────────────────────

class ParseTradeFn(beam.DoFn):
    """
    Parses raw Pub/Sub message bytes → dict.
    Invalid messages go to a 'dead_letter' side output.
    """
    DEAD_LETTER_TAG = beam.pvalue.TaggedOutput("dead_letter", None)

    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        try:
            trade = json.loads(element.decode("utf-8"))
            # Validate required fields
            required = {"trade_id", "product_id", "price", "size", "side", "timestamp"}
            if not required.issubset(trade.keys()):
                yield beam.pvalue.TaggedOutput("dead_letter", element)
                return
            trade["price"] = float(trade["price"])
            trade["size"] = float(trade["size"])
            yield trade
        except Exception as e:
            yield beam.pvalue.TaggedOutput("dead_letter", element)


# ── Step 2: Key by product_id for grouping ────────────────────────────────────

class KeyByProductFn(beam.DoFn):
    """Emit (product_id, trade) pairs — needed for GroupByKey."""
    def process(self, trade):
        yield (trade["product_id"], trade)


# ── Step 3: Aggregate trades within a window ─────────────────────────────────

@dataclass
class TradeAggregate:
    product_id: str
    window_start: str
    window_end: str
    trade_count: int
    avg_price: float
    total_volume: float
    min_price: float
    max_price: float
    buy_count: int
    sell_count: int


class AggregateTradesFn(beam.CombineFn):
    """
    Aggregates trades within a window into a single TradeAggregate.
    CombineFn is more efficient than GroupByKey for large windows.
    """

    def create_accumulator(self):
        return {
            "count": 0,
            "price_sum": 0.0,
            "volume_sum": 0.0,
            "min_price": float("inf"),
            "max_price": float("-inf"),
            "buy_count": 0,
            "sell_count": 0,
        }

    def add_input(self, accumulator, trade):
        acc = accumulator.copy()
        acc["count"] += 1
        acc["price_sum"] += trade["price"]
        acc["volume_sum"] += trade["size"]
        acc["min_price"] = min(acc["min_price"], trade["price"])
        acc["max_price"] = max(acc["max_price"], trade["price"])
        if trade["side"] == "BUY":
            acc["buy_count"] += 1
        else:
            acc["sell_count"] += 1
        return acc

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["count"] += acc["count"]
            merged["price_sum"] += acc["price_sum"]
            merged["volume_sum"] += acc["volume_sum"]
            merged["min_price"] = min(merged["min_price"], acc["min_price"])
            merged["max_price"] = max(merged["max_price"], acc["max_price"])
            merged["buy_count"] += acc["buy_count"]
            merged["sell_count"] += acc["sell_count"]
        return merged

    def extract_output(self, accumulator):
        if accumulator["count"] == 0:
            return None
        return {
            "trade_count": accumulator["count"],
            "avg_price": accumulator["price_sum"] / accumulator["count"],
            "total_volume": accumulator["volume_sum"],
            "min_price": accumulator["min_price"],
            "max_price": accumulator["max_price"],
            "buy_count": accumulator["buy_count"],
            "sell_count": accumulator["sell_count"],
        }


class FormatForBigQueryFn(beam.DoFn):
    """
    Formats the windowed aggregate dict into a BigQuery-ready row.
    Receives window metadata via WindowParam.
    """
    def process(self, element, window=beam.DoFn.WindowParam):
        product_id, agg = element
        if agg is None:
            return
        yield {
            "product_id": product_id,
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            **agg,
        }


class FormatRawForBigQueryFn(beam.DoFn):
    """Formats a raw trade dict into a BigQuery row."""
    def process(self, trade):
        yield {
            "trade_id": trade["trade_id"],
            "product_id": trade["product_id"],
            "price": trade["price"],
            "size": trade["size"],
            "side": trade["side"],
            "timestamp": trade["timestamp"],
        }


class ParquetSink(beam.io.fileio.FileSink):
    """
    Streaming-compatible Parquet sink for use with beam.io.fileio.WriteToFiles.

    WriteToParquet uses the old-style Write sink API which re-applies global
    windowing internally, making it incompatible with streaming pipelines.
    WriteToFiles respects the existing window and calls open/write/flush per file.
    """
    def __init__(self, schema):
        self._schema = schema

    def open(self, fh):
        self._fh = fh
        self._rows = []

    def write(self, element):
        self._rows.append(element)

    def flush(self):
        import io
        import pyarrow
        import pyarrow.parquet as pq
        if self._rows:
            table = pyarrow.Table.from_pylist(self._rows, schema=self._schema)
            buf = io.BytesIO()
            pq.write_table(table, buf)
            self._fh.write(buf.getvalue())


class HeartbeatFn(beam.DoFn):
    """
    Emits one heartbeat row per (product_id, window) from the already-aggregated result.
    Reuses the output of AggregateTradesFn so no extra work is done.

    A missing row in pipeline_heartbeat for a given minute signals a gap —
    either the producer was down or no trades arrived for that product.
    """
    def process(self, element, window=beam.DoFn.WindowParam):
        product_id, agg = element
        if agg is None:
            return
        yield {
            "product_id": product_id,
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "trade_count": agg["trade_count"],
        }
