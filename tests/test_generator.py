from datetime import date

import pandas as pd
import pytest

from gcp_analytics.generator import create_orders, create_products, write_landing_data


def test_order_generator_is_deterministic_and_campaign_is_scoped() -> None:
    first = create_orders(
        start=date(2025, 11, 1),
        end=date(2025, 11, 30),
        average_daily_orders=20,
        seed=7,
    )
    second = create_orders(
        start=date(2025, 11, 1),
        end=date(2025, 11, 30),
        average_daily_orders=20,
        seed=7,
    )

    pd.testing.assert_frame_equal(
        first.drop(columns="ingested_at"),
        second.drop(columns="ingested_at"),
    )
    campaign = first[first["campaign_id"] == "NORTH-NOV-WEEK"]
    assert not campaign.empty
    assert campaign["region"].eq("North").all()
    assert campaign["order_date"].apply(
        lambda value: date(2025, 11, 24) <= value <= date(2025, 11, 30)
    ).all()
    assert first["net_sales"].gt(0).all()
    assert "order_month" not in first.columns


def test_generator_rejects_bad_dates_and_daily_volume() -> None:
    with pytest.raises(ValueError, match="on or before"):
        create_orders(start=date(2025, 12, 1), end=date(2025, 11, 1))
    with pytest.raises(ValueError, match="at least 10"):
        create_orders(average_daily_orders=9)


def test_product_dimension_is_complete_and_stable() -> None:
    products = create_products()
    assert len(products) == 40
    assert products["product_id"].is_unique
    assert products["standard_unit_price"].gt(0).all()


def test_partition_writer_creates_daily_hive_paths(tmp_path) -> None:
    orders = create_orders(
        start=date(2025, 11, 27),
        end=date(2025, 11, 29),
        average_daily_orders=20,
        seed=8,
    )
    partitioned_dir, baseline = write_landing_data(orders, tmp_path)

    assert (partitioned_dir / "order_date=2025-11-27").is_dir()
    assert (partitioned_dir / "order_date=2025-11-28").is_dir()
    assert (partitioned_dir / "order_date=2025-11-29").is_dir()
    assert baseline.is_file()
