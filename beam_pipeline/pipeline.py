"""
Dataflow Streaming Pipeline — Apache Beam

Flow:
  Pub/Sub → parse → Branch A: raw trades      → BigQuery: trades_raw
                  → Branch B: 1-min window    → aggregate → BigQuery: trades_aggregated_1min
                                                           → BigQuery: pipeline_heartbeat
                  → Branch C: raw trades      → WriteToParquet → GCS

Run locally (DirectRunner):
    python beam_pipeline/pipeline.py --runner=DirectRunner

Run on Dataflow:
    python beam_pipeline/pipeline.py \
        --runner=DataflowRunner \
        --project=$GCP_PROJECT_ID \
        --region=$GCP_REGION \
        --temp_location=$GCS_TEMP_LOCATION \
        --staging_location=$GCS_STAGING_LOCATION \
        --streaming
"""
import argparse
import logging

import apache_beam as beam
import pyarrow
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, Repeatedly
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io import fileio

from config.settings import (
    GCP_PROJECT_ID,
    GCP_REGION,
    PUBSUB_SUBSCRIPTION_ID,
    BQ_DATASET,
    BQ_TABLE_RAW,
    BQ_TABLE_AGGREGATED,
    BQ_TABLE_HEARTBEAT,
    GCS_TEMP_LOCATION,
    GCS_STAGING_LOCATION,
    GCS_PARQUET_PREFIX,
)
from beam_pipeline.transforms import (
    ParseTradeFn,
    KeyByProductFn,
    AggregateTradesFn,
    FormatForBigQueryFn,
    FormatRawForBigQueryFn,
    HeartbeatFn,
    ParquetSink,
)
from beam_pipeline.bq_schemas import (
    TRADES_RAW_SCHEMA,
    TRADES_AGGREGATED_SCHEMA,
    PIPELINE_HEARTBEAT_SCHEMA,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

WINDOW_SIZE_SECONDS = 60  # 1-minute fixed windows

# Parquet schema for raw trades — must match what FormatRawForBigQueryFn produces
PARQUET_SCHEMA = pyarrow.schema([
    pyarrow.field("trade_id",   pyarrow.string()),
    pyarrow.field("product_id", pyarrow.string()),
    pyarrow.field("price",      pyarrow.float64()),
    pyarrow.field("size",       pyarrow.float64()),
    pyarrow.field("side",       pyarrow.string()),
    pyarrow.field("timestamp",  pyarrow.string()),
])


def build_pipeline_options(argv=None) -> PipelineOptions:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", default="DirectRunner")
    known, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(
        pipeline_args,
        runner=known.runner,
        project=GCP_PROJECT_ID,
        region=GCP_REGION,
        temp_location=GCS_TEMP_LOCATION,
        staging_location=GCS_STAGING_LOCATION,
        # Dataflow-specific: package this repo for workers
        setup_file="./setup.py",
    )
    # CRITICAL: must set streaming=True for Pub/Sub sources
    options.view_as(StandardOptions).streaming = True
    return options


def run(argv=None):
    options = build_pipeline_options(argv)
    subscription = f"projects/{GCP_PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION_ID}"
    bq_raw_table = f"{GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE_RAW}"
    bq_agg_table = f"{GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE_AGGREGATED}"
    bq_heartbeat_table = f"{GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE_HEARTBEAT}"

    log.info("Starting pipeline | subscription=%s", subscription)

    with beam.Pipeline(options=options) as p:

        # ── Read from Pub/Sub ─────────────────────────────────────────────────
        raw_bytes = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=subscription)
        )

        # ── Parse & validate ──────────────────────────────────────────────────
        parsed = raw_bytes | "ParseTrades" >> beam.ParDo(
            ParseTradeFn()
        ).with_outputs("dead_letter", main="valid")

        valid_trades = parsed.valid
        dead_letters = parsed.dead_letter

        # Log dead letters (in production: write to GCS or BQ for inspection)
        dead_letters | "LogDeadLetters" >> beam.Map(
            lambda x: log.warning("dead_letter: %s", x[:200] if x else "None")
        )

        # ── Branch A: Write raw trades to BigQuery ────────────────────────────
        (
            valid_trades
            | "FormatRaw" >> beam.ParDo(FormatRawForBigQueryFn())
            | "WriteRawToBQ" >> WriteToBigQuery(
                table=bq_raw_table,
                schema=TRADES_RAW_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # ── Shared windowed PCollection (used by branches B, C, D) ───────────
        windowed_keyed = (
            valid_trades
            | "ApplyWindow" >> beam.WindowInto(FixedWindows(WINDOW_SIZE_SECONDS))
            | "KeyByProduct" >> beam.ParDo(KeyByProductFn())
        )

        aggregated = windowed_keyed | "AggregatePerWindow" >> beam.CombinePerKey(AggregateTradesFn())

        # ── Branch B: Aggregated window → BigQuery ────────────────────────────
        (
            aggregated
            | "FormatAggregated" >> beam.ParDo(FormatForBigQueryFn())
            | "WriteAggToBQ" >> WriteToBigQuery(
                table=bq_agg_table,
                schema=TRADES_AGGREGATED_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # ── Branch C: Raw trades → Parquet → GCS ─────────────────────────────
        # Uses WriteToFiles (streaming-safe) instead of WriteToParquet (batch-only).
        # WriteToParquet uses the old Write sink API which re-applies global windowing
        # internally, breaking streaming pipelines. WriteToFiles respects the existing
        # window and calls open/write/flush once per file per window.
        (
            valid_trades
            | "WindowForParquet" >> beam.WindowInto(
                FixedWindows(WINDOW_SIZE_SECONDS),
                trigger=Repeatedly(AfterProcessingTime(WINDOW_SIZE_SECONDS)),
                accumulation_mode=AccumulationMode.DISCARDING,
            )
            | "FormatRawForParquet" >> beam.ParDo(FormatRawForBigQueryFn())
            | "WriteToParquet" >> fileio.WriteToFiles(
                path=GCS_PARQUET_PREFIX,
                sink=lambda dest: ParquetSink(PARQUET_SCHEMA),
                file_naming=fileio.default_file_naming(prefix="trades", suffix=".parquet"),
            )
        )

        # ── Branch D: Heartbeat → BigQuery ───────────────────────────────────
        # One row per (product_id, window). A missing row means no trades arrived
        # for that product during that minute — used for gap detection in Phase 2.
        (
            aggregated
            | "FormatHeartbeat" >> beam.ParDo(HeartbeatFn())
            | "WriteHeartbeatToBQ" >> WriteToBigQuery(
                table=bq_heartbeat_table,
                schema=PIPELINE_HEARTBEAT_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
