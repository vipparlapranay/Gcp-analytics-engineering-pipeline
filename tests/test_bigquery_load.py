import pytest

from gcp_analytics.bigquery_load import build_daily_load_config


def test_daily_bigquery_load_is_hive_partitioned_and_cost_aware() -> None:
    pytest.importorskip("google.cloud.bigquery")
    config = build_daily_load_config("retail-landing-123")

    assert config.source_format == "PARQUET"
    assert config.write_disposition == "WRITE_APPEND"
    assert config.create_disposition == "CREATE_NEVER"
    assert config.hive_partitioning.mode == "AUTO"
    assert config.hive_partitioning.source_uri_prefix == (
        "gs://retail-landing-123/landing/orders/"
    )
    assert config.time_partitioning is None
    assert config.clustering_fields is None
    assert config.labels == {"pipeline": "retail-dbt-daily"}


def test_daily_bigquery_load_rejects_invalid_bucket_name() -> None:
    with pytest.raises(ValueError, match="Invalid Google Cloud Storage bucket"):
        build_daily_load_config("gs://not-a-bucket/path")
