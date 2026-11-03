import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = ROOT / "airflow" / "dags" / "retail_daily_pipeline.py"


def test_daily_dag_has_load_freshness_and_fail_fast_build_steps() -> None:
    source = DAG_FILE.read_text(encoding="utf-8")
    module = ast.parse(source)
    task_ids = {
        node.value.value
        for node in ast.walk(module)
        if isinstance(node, ast.keyword)
        and node.arg == "task_id"
        and isinstance(node.value, ast.Constant)
    }

    assert {
        "load_daily_gcs_partition",
        "check_source_freshness",
        "dbt_build_and_test",
    }.issubset(task_ids)
    assert "schedule=\"@daily\"" in source
    assert "catchup=False" in source
    assert "dbt source freshness" in source
    assert "dbt build --fail-fast" in source
    assert "quality_check_date" in source
    assert "build_daily_load_config" in source
    assert "WRITE_TRUNCATE" in source
    assert "BEGIN TRANSACTION" in source
    assert "INSERT INTO" in source
    assert "DELETE FROM" in source
    assert "order_date = @run_date" in source
