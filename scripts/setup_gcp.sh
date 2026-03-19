#!/bin/bash
# ============================================================
# setup_gcp.sh — Enable APIs and create GCP resources
# Run once before starting the pipeline.
#
# Usage:
#   export GCP_PROJECT_ID=your-project-id
#   export GCS_BUCKET=your-bucket-name
#   bash scripts/setup_gcp.sh
# ============================================================
set -euo pipefail

: "${GCP_PROJECT_ID:?Need to set GCP_PROJECT_ID}"
: "${GCS_BUCKET:?Need to set GCS_BUCKET}"

REGION="${GCP_REGION:-us-central1}"
PUBSUB_TOPIC="${PUBSUB_TOPIC_ID:-crypto-trades-raw}"
PUBSUB_SUB="${PUBSUB_SUBSCRIPTION_ID:-crypto-trades-beam-sub}"
BQ_DATASET="${BQ_DATASET:-crypto_streaming}"

echo "==> Setting project: $GCP_PROJECT_ID"
gcloud config set project "$GCP_PROJECT_ID"

# ── 1. Enable APIs ────────────────────────────────────────────────────────────
echo "==> Enabling GCP APIs..."
gcloud services enable \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  bigtable.googleapis.com \
  bigtableadmin.googleapis.com \
  dataproc.googleapis.com \
  composer.googleapis.com

# ── 2. Create GCS bucket ─────────────────────────────────────────────────────
echo "==> Creating GCS bucket: gs://$GCS_BUCKET"
gcloud storage buckets create "gs://$GCS_BUCKET" \
  --project="$GCP_PROJECT_ID" \
  --location="$REGION" \
  --uniform-bucket-level-access \
  2>/dev/null || echo "  Bucket already exists, skipping."

# ── 3. Create Pub/Sub topic & subscription ───────────────────────────────────
echo "==> Creating Pub/Sub topic: $PUBSUB_TOPIC"
gcloud pubsub topics create "$PUBSUB_TOPIC" 2>/dev/null || echo "  Topic already exists."

echo "==> Creating Pub/Sub subscription: $PUBSUB_SUB"
gcloud pubsub subscriptions create "$PUBSUB_SUB" \
  --topic="$PUBSUB_TOPIC" \
  --ack-deadline=60 \
  --message-retention-duration=7d \
  2>/dev/null || echo "  Subscription already exists."

# ── 4. Create BigQuery dataset ───────────────────────────────────────────────
echo "==> Creating BigQuery dataset: $BQ_DATASET"
bq --project_id="$GCP_PROJECT_ID" mk \
  --dataset \
  --location="$REGION" \
  "$BQ_DATASET" \
  2>/dev/null || echo "  Dataset already exists."

# ── 5. Create service account for the pipeline ───────────────────────────────
SA_NAME="crypto-pipeline-sa"
SA_EMAIL="$SA_NAME@$GCP_PROJECT_ID.iam.gserviceaccount.com"

echo "==> Creating service account: $SA_EMAIL"
gcloud iam service-accounts create "$SA_NAME" \
  --display-name="Crypto Pipeline Service Account" \
  2>/dev/null || echo "  Service account already exists."

echo "==> Granting roles to service account..."
for ROLE in \
  roles/pubsub.publisher \
  roles/pubsub.subscriber \
  roles/dataflow.worker \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser \
  roles/storage.objectAdmin \
  roles/bigtable.user; do
  gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="$ROLE" \
    --quiet
done

echo ""
echo "✅  GCP setup complete!"
echo ""
echo "Next steps:"
echo "  1. Create and download a key for the service account:"
echo "     gcloud iam service-accounts keys create sa-key.json --iam-account=$SA_EMAIL"
echo "  2. Export: export GOOGLE_APPLICATION_CREDENTIALS=\$(pwd)/sa-key.json"
echo "  3. Copy .env.example to .env and fill in your values"
echo "  4. Run the producer: python producer/coinbase_producer.py"
