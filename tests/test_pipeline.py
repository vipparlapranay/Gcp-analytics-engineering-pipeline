from datetime import date

from gcp_analytics.pipeline import run


def test_local_dbt_pipeline_checks_quality_and_measures_partition(tmp_path) -> None:
    metrics = run(
        output_dir=tmp_path / "results",
        orders_per_day=10,
        focus_date=date(2025, 11, 28),
        data_dir=tmp_path / "data" / "landing",
    )

    assert metrics["order_count"] > 5_000
    assert metrics["daily_partition_count"] > 700
    assert metrics["dbt_models_total"] == 4
    assert metrics["dbt_models_succeeded"] == 4
    assert metrics["dbt_tests_total"] >= 20
    assert metrics["dbt_tests_passed"] == metrics["dbt_tests_total"]
    assert metrics["partition_pruning_estimate"]["local_object_volume_reduction_pct"] > 90
    assert metrics["focus_month_sql_evidence"]["monthly_sales"]["month"] == "November 2025"
    assert (tmp_path / "results" / "monthly_kpis.csv").is_file()
    assert (tmp_path / "results" / "evidence.json").is_file()
