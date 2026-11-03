from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

REGIONS = ("North", "South", "East", "West")
CATEGORIES = ("Home", "Technology", "Wellness", "Outdoor")
CHANNELS = ("Online", "Store", "Partner")
PRODUCTS = tuple(f"SKU-{number:03d}" for number in range(1, 41))


def create_orders(
    start: date = date(2024, 1, 1),
    end: date = date(2025, 12, 31),
    average_daily_orders: int = 170,
    seed: int = 20261103,
) -> pd.DataFrame:
    if start > end:
        raise ValueError("The start date must be on or before the end date.")
    if average_daily_orders < 10:
        raise ValueError("average_daily_orders must be at least 10.")

    rng = np.random.default_rng(seed)
    days = pd.date_range(start=start, end=end, freq="D")
    seasonality = (
        1
        + 0.16 * np.sin(2 * np.pi * (days.month.to_numpy() - 1) / 12)
        + 0.05 * np.cos(4 * np.pi * (days.month.to_numpy() - 1) / 12)
    )
    day_effect = np.where(days.dayofweek.to_numpy() >= 5, 1.18, 0.93)
    orders_per_day = rng.poisson(average_daily_orders * seasonality * day_effect)
    order_dates = np.repeat(days.to_numpy(), orders_per_day)
    count = len(order_dates)
    if count == 0:
        raise ValueError("The selected period produced no orders.")

    dates = pd.to_datetime(order_dates)
    product_indices = rng.integers(0, len(PRODUCTS), size=count)
    product_ids = np.asarray(PRODUCTS)[product_indices]
    categories = np.asarray(CATEGORIES)[product_indices % len(CATEGORIES)]
    regions = rng.choice(REGIONS, count, p=[0.27, 0.23, 0.28, 0.22])
    channels = rng.choice(CHANNELS, count, p=[0.48, 0.40, 0.12])
    customer_ids = rng.integers(1, max(count // 3, 500), size=count)
    unit_prices = 18 + product_indices * 4.1 + rng.uniform(-3.5, 9.0, count)
    ordinary_units = rng.poisson(1.35, count) + 1

    promo_mask = (
        (dates.year == 2025)
        & (dates.month == 11)
        & np.isin(dates.day, [24, 25, 26, 27, 28, 29, 30])
        & (regions == "North")
    )
    promo_units = rng.poisson(1.7, count)
    units = ordinary_units + np.where(promo_mask, promo_units, 0)
    gross_sales = units * unit_prices
    regular_discount_rate = rng.uniform(0.01, 0.09, count)
    promo_discount_rate = rng.uniform(0.16, 0.27, count)
    discount_rate = np.where(promo_mask, promo_discount_rate, regular_discount_rate)
    discounts = gross_sales * discount_rate
    net_sales = gross_sales - discounts
    cost_rate = 0.43 + (product_indices % 8) * 0.034
    cost = gross_sales * cost_rate

    ingested_at = pd.Timestamp.now(tz="UTC").floor("s")
    return pd.DataFrame(
        {
            "order_id": [f"ORD-{number:010d}" for number in range(1, count + 1)],
            "customer_id": [f"CUS-{number:08d}" for number in customer_ids],
            "product_id": product_ids,
            "category": categories,
            "region": regions,
            "channel": channels,
            "campaign_id": np.where(promo_mask, "NORTH-NOV-WEEK", "STANDARD"),
            "order_date": dates.date,
            "units": units.astype("int32"),
            "gross_sales": gross_sales.round(2),
            "discount_amount": discounts.round(2),
            "net_sales": net_sales.round(2),
            "cost": cost.round(2),
            "ingested_at": ingested_at,
        }
    )


def create_products() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_id": PRODUCTS,
            "category": [CATEGORIES[index % len(CATEGORIES)] for index in range(len(PRODUCTS))],
            "standard_unit_price": [
                round(18 + index * 4.1 + 2.75, 2) for index in range(len(PRODUCTS))
            ],
        }
    )


def write_landing_data(
    orders: pd.DataFrame,
    output_dir: Path,
) -> tuple[Path, Path]:
    if orders.empty:
        raise ValueError("Cannot write an empty order dataset.")
    output_dir.mkdir(parents=True, exist_ok=True)
    partitioned_dir = output_dir / "orders"
    partitioned_dir.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(orders, preserve_index=False)
    ds.write_dataset(
        table,
        base_dir=str(partitioned_dir),
        format="parquet",
        partitioning=["order_date"],
        partitioning_flavor="hive",
        existing_data_behavior="delete_matching",
        file_options=ds.ParquetFileFormat().make_write_options(
            compression="zstd",
            use_dictionary=True,
        ),
        max_rows_per_file=100_000,
        max_rows_per_group=16_384,
    )
    baseline_path = output_dir / "orders_unpartitioned.parquet"
    pq.write_table(
        table,
        baseline_path,
        compression="zstd",
        use_dictionary=True,
        row_group_size=16_384,
    )
    return partitioned_dir, baseline_path
