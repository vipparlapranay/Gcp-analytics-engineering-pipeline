-- Replace the project/dataset placeholders and choose the correct region.
-- Run this DDL before the scheduled Airflow DAG. The loader validates this
-- partitioning and clustering contract before staging incoming data.

CREATE SCHEMA IF NOT EXISTS `REPLACE_WITH_PROJECT.REPLACE_WITH_RAW_DATASET`
OPTIONS (location = "US");

CREATE TABLE IF NOT EXISTS
  `REPLACE_WITH_PROJECT.REPLACE_WITH_RAW_DATASET.orders` (
    order_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    product_id STRING NOT NULL,
    category STRING NOT NULL,
    region STRING NOT NULL,
    channel STRING NOT NULL,
    campaign_id STRING NOT NULL,
    order_date DATE NOT NULL,
    units INT64 NOT NULL,
    gross_sales NUMERIC NOT NULL,
    discount_amount NUMERIC NOT NULL,
    net_sales NUMERIC NOT NULL,
    cost NUMERIC NOT NULL,
    ingested_at TIMESTAMP NOT NULL
  )
PARTITION BY order_date
CLUSTER BY region, category
OPTIONS (
  description = "Synthetic retail order facts; daily partitioned and region/category clustered."
);
