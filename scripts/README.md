# scripts/

One-time GCP provisioning scripts. Run these before starting the pipeline for the first time.

---

## Files

### `setup_gcp.sh`

**Purpose:** Creates all required GCP resources for the pipeline. Designed to be run once per environment (dev, staging, prod). Safe to re-run — all operations are idempotent (skip if the resource already exists).

**Prerequisites:**
- `gcloud` CLI installed and authenticated (`gcloud auth login`)
- `bq` CLI available (part of Google Cloud SDK)
- `GCP_PROJECT_ID` and `GCS_BUCKET` environment variables set

```bash
export GCP_PROJECT_ID=your-project-id
export GCS_BUCKET=your-bucket-name
bash scripts/setup_gcp.sh
```

---

#### Shell options

```bash
set -euo pipefail
```

- `-e` — exit immediately on any command failure
- `-u` — treat unset variables as errors
- `-o pipefail` — a pipeline fails if any command in it fails (not just the last)

This prevents the script from silently continuing after a failed step.

---

#### Variable validation

```bash
: "${GCP_PROJECT_ID:?Need to set GCP_PROJECT_ID}"
: "${GCS_BUCKET:?Need to set GCS_BUCKET}"
```

The `:` command is a shell no-op. The `${VAR:?message}` syntax causes the shell to exit with an error message if `VAR` is unset or empty. This is the idiomatic bash way to require env vars without an `if` block.

---

#### Step 1 — Enable GCP APIs

```bash
gcloud services enable \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  bigtable.googleapis.com \
  bigtableadmin.googleapis.com \
  dataproc.googleapis.com \
  composer.googleapis.com
```

GCP APIs are disabled by default on new projects — you must explicitly enable each one before using it. All 8 APIs needed across current and planned phases are enabled upfront.

**Why enable Bigtable, Dataproc, and Composer now?** They're needed for Phase 2 but enabling them early avoids a surprise failure when Phase 2 work begins.

---

#### Step 2 — Create GCS bucket

```bash
gcloud storage buckets create "gs://$GCS_BUCKET" \
  --location="$REGION" \
  --uniform-bucket-level-access \
  2>/dev/null || echo "  Bucket already exists, skipping."
```

`--uniform-bucket-level-access` disables per-object ACLs and enforces IAM-only access control — the recommended security posture for GCP buckets.

`2>/dev/null || echo "..."` — if the bucket already exists, `gcloud` exits with an error. Redirecting stderr and using `||` converts that error into a skip message, keeping the script idempotent.

---

#### Step 3 — Create Pub/Sub topic and subscription

```bash
gcloud pubsub topics create "$PUBSUB_TOPIC"
gcloud pubsub subscriptions create "$PUBSUB_SUB" \
  --topic="$PUBSUB_TOPIC" \
  --ack-deadline=60 \
  --message-retention-duration=7d
```

**`--ack-deadline=60`** — Dataflow has 60 seconds to acknowledge a message before Pub/Sub redelivers it. Default is 10 seconds, which can cause redeliveries under high load or slow worker startup.

**`--message-retention-duration=7d`** — Pub/Sub keeps unacknowledged messages for 7 days. If the Beam pipeline is down for up to 7 days, it can catch up on missed messages when it restarts. Default is 7 days; minimum is 10 minutes.

**Why topic and subscription are separate resources:**
- The topic is the channel where the producer publishes.
- The subscription is a consumer's view of that channel. Multiple subscriptions can exist on one topic — each gets a full copy of every message.
- The Beam pipeline reads from the subscription, not the topic directly.

---

#### Step 4 — Create BigQuery dataset

```bash
bq --project_id="$GCP_PROJECT_ID" mk \
  --dataset \
  --location="$REGION" \
  "$BQ_DATASET"
```

Creates the `crypto_streaming` dataset. Tables (`trades_raw`, `trades_aggregated_1min`) are created automatically by Dataflow when the pipeline first runs (due to `CREATE_IF_NEEDED` in `pipeline.py`).

**Note:** Uses the `bq` CLI (part of Google Cloud SDK) rather than `gcloud` — BigQuery has its own CLI tool.

---

#### Step 5 — Create service account and grant IAM roles

```bash
gcloud iam service-accounts create "crypto-pipeline-sa" \
  --display-name="Crypto Pipeline Service Account"
```

Creates a dedicated service account `crypto-pipeline-sa@<project>.iam.gserviceaccount.com`. This is the identity the pipeline runs as.

**IAM roles granted:**

| Role | Why |
|---|---|
| `roles/pubsub.publisher` | Producer can publish to topics |
| `roles/pubsub.subscriber` | Beam pipeline can pull from subscriptions |
| `roles/dataflow.worker` | Dataflow workers can communicate with the Dataflow service |
| `roles/bigquery.dataEditor` | Pipeline can insert rows into BigQuery tables |
| `roles/bigquery.jobUser` | Pipeline can create and run BigQuery jobs |
| `roles/storage.objectAdmin` | Pipeline can read/write GCS objects (temp, staging, Parquet) |
| `roles/bigtable.user` | Phase 2: pipeline can read/write Bigtable |

**Principle of least privilege:** Only the roles actually needed are granted. The service account cannot, for example, delete BigQuery datasets or create new GCP projects.

---

#### Post-setup instructions (printed by the script)

```
1. gcloud iam service-accounts keys create sa-key.json --iam-account=<SA_EMAIL>
2. export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/sa-key.json
3. Copy .env.example to .env and fill in your values
4. python producer/coinbase_producer.py
```

**Security note:** `sa-key.json` is a long-lived credential. The `.gitignore` excludes `*-key.json` and `credentials*.json` — never commit this file.

---

## Idempotency Pattern

Every resource creation command follows the same pattern:

```bash
gcloud ... create RESOURCE 2>/dev/null || echo "  Already exists, skipping."
```

This makes the script safe to re-run (e.g., after adding a new resource, or to verify state). The `2>/dev/null` hides the error output from `gcloud` when the resource already exists; `||` catches the non-zero exit code and prints a friendly message instead.
