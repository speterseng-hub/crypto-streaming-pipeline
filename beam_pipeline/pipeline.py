"""
Dataflow Streaming Pipeline — Apache Beam

Flow:
  Pub/Sub → parse → [raw → BQ] + [window → aggregate → BQ]
                                                       ↓
                                                   GCS (Parquet)

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
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

from config.settings import (
    GCP_PROJECT_ID,
    GCP_REGION,
    PUBSUB_SUBSCRIPTION_ID,
    BQ_DATASET,
    BQ_TABLE_RAW,
    BQ_TABLE_AGGREGATED,
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
)
from beam_pipeline.bq_schemas import TRADES_RAW_SCHEMA, TRADES_AGGREGATED_SCHEMA

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

WINDOW_SIZE_SECONDS = 60  # 1-minute fixed windows


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

        # ── Branch B: Window → aggregate → BigQuery ───────────────────────────
        (
            valid_trades
            | "ApplyWindow" >> beam.WindowInto(FixedWindows(WINDOW_SIZE_SECONDS))
            | "KeyByProduct" >> beam.ParDo(KeyByProductFn())
            | "AggregatePerWindow" >> beam.CombinePerKey(AggregateTradesFn())
            | "FormatAggregated" >> beam.ParDo(FormatForBigQueryFn())
            | "WriteAggToBQ" >> WriteToBigQuery(
                table=bq_agg_table,
                schema=TRADES_AGGREGATED_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # TODO Phase 1b: Add WriteToParquet → GCS branch here


if __name__ == "__main__":
    run()
