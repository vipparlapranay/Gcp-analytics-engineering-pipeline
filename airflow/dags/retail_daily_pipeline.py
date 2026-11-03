from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from gcp_analytics.bigquery_load import build_daily_load_config


def load_daily_partition(ds: str, **_context) -> None:
    from google.cloud import bigquery

    project = os.environ.get("GCP_PROJECT_ID")
    raw_dataset = os.environ.get("BQ_RAW_DATASET")
    location = os.environ.get("BIGQUERY_LOCATION", "US")
    bucket = os.environ.get("GCS_LANDING_BUCKET")
    if not project or not raw_dataset or not bucket:
        raise RuntimeError(
            "Set GCP_PROJECT_ID, BQ_RAW_DATASET and GCS_LANDING_BUCKET in the "
            "Airflow worker environment."
        )

    prefix = f"gs://{bucket}/landing/orders/"
    source_uri = f"{prefix}order_date={ds}/*.parquet"
    client = bigquery.Client(project=project, location=location)
    table_id = f"{project}.{raw_dataset}.orders"
    raw_table = client.get_table(table_id)
    if (
        raw_table.time_partitioning is None
        or raw_table.time_partitioning.field != "order_date"
    ):
        raise ValueError(
            f"{table_id} must be partitioned by order_date before daily loads."
        )
    if not {"region", "category"}.issubset(raw_table.clustering_fields or []):
        raise ValueError(
            f"{table_id} must be clustered by region and category before daily loads."
        )

    partition_date = date.fromisoformat(ds)
    staging_table_id = (
        f"{project}.{raw_dataset}._orders_stage_{partition_date:%Y%m%d}"
    )
    client.delete_table(staging_table_id, not_found_ok=True)
    staging_table = bigquery.Table(staging_table_id, schema=raw_table.schema)
    staging_table.time_partitioning = raw_table.time_partitioning
    staging_table.clustering_fields = raw_table.clustering_fields
    staging_table.expires = datetime.now(timezone.utc) + timedelta(days=1)
    client.create_table(staging_table)

    config = build_daily_load_config(bucket)
    config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_uri(
        source_uri,
        staging_table_id,
        job_config=config,
        location=location,
    )
    job.result()
    if job.errors:
        raise RuntimeError(f"BigQuery daily partition load failed: {job.errors}")
    if not job.output_rows:
        raise ValueError(f"No order rows were loaded from {source_uri}.")

    columns = ", ".join(f"`{field.name}`" for field in raw_table.schema)
    replace_partition = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{table_id}` WHERE order_date = @run_date;
        INSERT INTO `{table_id}` ({columns})
        SELECT {columns}
        FROM `{staging_table_id}`
        WHERE order_date = @run_date;
        COMMIT TRANSACTION;
    """
    replace_job = client.query(
        replace_partition,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_date", "DATE", partition_date)
            ],
            labels={"pipeline": "retail-dbt-daily"},
        ),
        location=location,
    )
    replace_job.result()
    client.delete_table(staging_table_id, not_found_ok=True)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = PROJECT_ROOT / "dbt"

with DAG(
    dag_id="gcp_retail_dbt_daily",
    description=(
        "Load the arriving daily GCS Parquet partition, enforce dbt source freshness, "
        "then run tested incremental marts."
    ),
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "analytics-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bigquery", "dbt", "retail"],
) as dag:
    load_partition = PythonOperator(
        task_id="load_daily_gcs_partition",
        python_callable=load_daily_partition,
    )

    check_freshness = BashOperator(
        task_id="check_source_freshness",
        bash_command=(
            "dbt source freshness "
            f"--project-dir '{DBT_DIR}' --profiles-dir '{DBT_DIR}' --target bigquery"
        ),
        append_env=True,
    )

    build_and_test = BashOperator(
        task_id="dbt_build_and_test",
        bash_command=(
            "dbt build --fail-fast "
            "--vars '{\"quality_check_date\": \"{{ ds }}\"}' "
            f"--project-dir '{DBT_DIR}' --profiles-dir '{DBT_DIR}' --target bigquery"
        ),
        append_env=True,
    )

    load_partition >> check_freshness >> build_and_test
