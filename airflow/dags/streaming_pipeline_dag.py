"""
Airflow DAG: Streaming ETL Pipeline Orchestration.

Daily batch layer on top of the streaming pipeline:
  1. Trigger a new Dataflow job (or verify existing is healthy)
  2. Run dbt transformations (staging → intermediate → mart)
  3. Run dbt data-quality tests
  4. Update pipeline metadata in BigQuery audit log
  5. Alert on any failures

Schedule: Daily at 02:00 UTC (off-peak, after midnight UTC aggregations settle).
"""
from __future__ import annotations
import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.utils.dates import days_ago

PROJECT_ID  = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
REGION      = os.getenv("DATAFLOW_REGION", "us-central1")
GCS_BUCKET  = os.getenv("GCS_BUCKET", f"gs://{PROJECT_ID}-streaming-etl")
DBT_DIR     = "/opt/airflow/dbt"
DBT_CMD     = f"dbt --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"

DEFAULT_ARGS = {
    "owner":            "jaimin.babariya",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "execution_timeout":timedelta(hours=3),
}


def _check_dataflow_health(**ctx) -> str:
    """Branch: restart Dataflow job if not running, else go straight to dbt."""
    from googleapiclient import discovery
    df = discovery.build("dataflow", "v1b3")
    jobs = df.projects().locations().jobs().list(
        projectId=PROJECT_ID, location=REGION,
        filter="ACTIVE"
    ).execute().get("jobs", [])
    running = any(j.get("name", "").startswith("streaming-etl") for j in jobs)
    return "dataflow_running_ok" if running else "restart_dataflow"


with DAG(
    dag_id="streaming_etl_pipeline",
    description="Streaming ETL: Dataflow health check + dbt transformations",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "etl", "dataflow", "dbt", "bigquery"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Check Dataflow job health
    check_df = BranchPythonOperator(
        task_id="check_dataflow_health",
        python_callable=_check_dataflow_health,
    )

    df_ok = EmptyOperator(task_id="dataflow_running_ok")

    restart_df = DataflowStartFlexTemplateOperator(
        task_id="restart_dataflow",
        project_id=PROJECT_ID,
        location=REGION,
        body={
            "launchParameter": {
                "jobName":              "streaming-etl-pipeline",
                "containerSpecGcsPath": f"{GCS_BUCKET}/templates/streaming-etl.json",
                "parameters": {
                    "pubsubSubscription": f"projects/{PROJECT_ID}/subscriptions/dataflow-events-sub",
                    "bqDataset":         "streaming_etl",
                    "gcsBucket":         GCS_BUCKET,
                },
            }
        },
        wait_until_finished=False,
    )

    rejoin = EmptyOperator(task_id="rejoin", trigger_rule="none_failed_min_one_success")

    # 2. dbt transformations
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging",
    )
    dbt_staging_test = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"{DBT_CMD} test --select staging source:streaming_etl",
    )
    dbt_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=f"{DBT_CMD} run --select mart",   # ephemeral intermediate models compile in
    )
    dbt_mart_test = BashOperator(
        task_id="dbt_test_mart",
        bash_command=f"{DBT_CMD} test --select mart",
    )

    # 3. Log audit record
    def _write_audit(**ctx):
        from google.cloud import bigquery
        from datetime import datetime, timezone
        client = bigquery.Client(project=PROJECT_ID)
        rows = [{"dag_run_id": ctx["run_id"], "dag_id": "streaming_etl_pipeline",
                 "status": "success", "run_at": datetime.now(timezone.utc).isoformat()}]
        client.insert_rows_json(f"{PROJECT_ID}.streaming_etl.pipeline_audit_log", rows)

    audit = PythonOperator(task_id="write_audit_log", python_callable=_write_audit)
    end   = EmptyOperator(task_id="end")

    # DAG topology
    (start >> check_df >> [df_ok, restart_df] >> rejoin
     >> dbt_staging >> dbt_staging_test
     >> dbt_mart >> dbt_mart_test
     >> audit >> end)
