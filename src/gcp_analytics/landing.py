from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from gcp_analytics.generator import create_orders, write_landing_data
from gcp_analytics.pipeline import PROJECT_ROOT


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate one deterministic Hive-partitioned synthetic GCS load."
    )
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--orders-per-day", type=int, default=170)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "landing",
    )
    args = parser.parse_args()
    orders = create_orders(
        start=args.date,
        end=args.date,
        average_daily_orders=args.orders_per_day,
    )
    partitioned_dir, _ = write_landing_data(orders, args.output_dir)
    partition = partitioned_dir / f"order_date={args.date.isoformat()}"
    if not partition.is_dir() or not list(partition.glob("*.parquet")):
        raise FileNotFoundError(f"Daily Parquet partition was not created: {partition}")
    print(f"Generated {len(orders):,} synthetic orders in {partition}")


if __name__ == "__main__":
    main()
