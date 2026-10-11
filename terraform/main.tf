# Terraform — Streaming ETL Pipeline on GCP
# Provisions: Pub/Sub, BigQuery, GCS, Dataflow SA, IAM

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  backend "gcs" {
    bucket = "YOUR_TERRAFORM_STATE_BUCKET"
    prefix = "streaming-etl/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── GCS buckets ────────────────────────────────────────────────────────────────
resource "google_storage_bucket" "pipeline_bucket" {
  name          = "${var.project_id}-streaming-etl"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = false

  lifecycle_rule {
    condition { age = 90 }
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  lifecycle_rule {
    condition { age = 365 }
    action    { type = "Delete" }
  }
}

# ── Pub/Sub ────────────────────────────────────────────────────────────────────
resource "google_pubsub_topic" "raw_events" {
  name                       = "raw-events"
  message_retention_duration = "86400s"  # 24h
}

resource "google_pubsub_topic" "dead_letter" {
  name = "dead-letter-events"
}

resource "google_pubsub_subscription" "dataflow_sub" {
  name                 = "dataflow-events-sub"
  topic                = google_pubsub_topic.raw_events.name
  ack_deadline_seconds = 60
  message_retention_duration = "86400s"

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# ── BigQuery ───────────────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "streaming_etl" {
  dataset_id  = "streaming_etl"
  location    = var.region
  description = "Streaming ETL pipeline — raw events, mart tables, audit log"
}

resource "google_bigquery_table" "events" {
  dataset_id          = google_bigquery_dataset.streaming_etl.dataset_id
  table_id            = "events"
  deletion_protection = false

  time_partitioning {
    type = "DAY"
    field = "event_ts"
  }
  clustering = ["event_type", "country"]

  schema = jsonencode([
    {name="event_id",type="STRING",mode="REQUIRED"},
    {name="event_type",type="STRING",mode="REQUIRED"},
    {name="user_id",type="STRING",mode="REQUIRED"},
    {name="session_id",type="STRING",mode="REQUIRED"},
    {name="event_ts",type="TIMESTAMP",mode="REQUIRED"},
    {name="properties",type="STRING",mode="NULLABLE"},
    {name="country",type="STRING",mode="NULLABLE"},
    {name="platform",type="STRING",mode="NULLABLE"},
    {name="is_valid",type="BOOLEAN",mode="REQUIRED"},
    {name="processed_at",type="TIMESTAMP",mode="REQUIRED"},
    {name="window_start",type="TIMESTAMP",mode="NULLABLE"},
    {name="window_end",type="TIMESTAMP",mode="NULLABLE"},
  ])
}

resource "google_bigquery_table" "dead_letter_queue" {
  dataset_id          = google_bigquery_dataset.streaming_etl.dataset_id
  table_id            = "dead_letter_queue"
  deletion_protection = false
  schema = jsonencode([
    {name="raw_message",type="STRING",mode="REQUIRED"},
    {name="error_reason",type="STRING",mode="REQUIRED"},
    {name="topic",type="STRING",mode="NULLABLE"},
    {name="failed_at",type="TIMESTAMP",mode="REQUIRED"},
  ])
}

resource "google_bigquery_table" "pipeline_audit_log" {
  dataset_id          = google_bigquery_dataset.streaming_etl.dataset_id
  table_id            = "pipeline_audit_log"
  deletion_protection = false
  schema = jsonencode([
    { name = "dag_run_id", type = "STRING", mode = "REQUIRED" },
    { name = "dag_id", type = "STRING", mode = "REQUIRED" },
    { name = "status", type = "STRING", mode = "REQUIRED" },
    { name = "run_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# ── Service account ────────────────────────────────────────────────────────────
resource "google_service_account" "dataflow_sa" {
  account_id   = "streaming-etl-dataflow"
  display_name = "Streaming ETL Dataflow Worker"
}

locals {
  dataflow_roles = [
    "roles/dataflow.worker",
    "roles/bigquery.dataEditor",
    "roles/storage.objectAdmin",
    "roles/pubsub.subscriber",
    "roles/pubsub.publisher",
  ]
}

resource "google_project_iam_member" "dataflow_roles" {
  for_each = toset(local.dataflow_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.dataflow_sa.email}"
}
