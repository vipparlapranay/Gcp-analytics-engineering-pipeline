from __future__ import annotations

import re


_BUCKET_NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$")


def build_daily_load_config(bucket: str):
    if not _BUCKET_NAME.fullmatch(bucket):
        raise ValueError(f"Invalid Google Cloud Storage bucket name: {bucket!r}")

    from google.cloud import bigquery

    prefix = f"gs://{bucket}/landing/orders/"
    hive = bigquery.HivePartitioningOptions()
    hive.mode = "AUTO"
    hive.source_uri_prefix = prefix
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        hive_partitioning=hive,
        labels={"pipeline": "retail-dbt-daily"},
    )
