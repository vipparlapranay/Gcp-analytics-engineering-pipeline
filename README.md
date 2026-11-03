# NORTHSTAR: GCP Analytics Engineering Pipeline

A cost-aware, end-to-end analytics engineering portfolio project built around **BigQuery, dbt, Apache Airflow, SQL, and Looker Studio**. Run the exact transformations and quality gates locally with DuckDB for zero-GCP-cost validation; install the BigQuery adapter and configure credentials when you are ready to deploy.

> **Portfolio data is synthetic.** The simulated promotions and trends are deliberately generated examples, not customer data, causal evidence, or company results. The local Parquet byte comparison is not a BigQuery bytes-billed result. No GCP resources have been created by this project.

## What makes the project useful

- **A reproducible, seeded source:** two years of synthetic daily e-commerce orders, 40 product SKUs, customer identifiers, seasonal patterns, channels, regions, and a documented November promotion.
- **Cost-aware warehouse shape:** daily source partitions; dbt incremental monthly marts; BigQuery `DATE` partitioning, region/category clustering, query byte caps, partition-aware models, and filtered dashboard guidance.
- **Test gates that stop bad data:** dbt uniqueness, null, accepted-value, product relationship, source freshness, and monthly KPI checks. The daily DAG runs source freshness and `dbt build --fail-fast`.
- **Same SQL in two environments:** DuckDB validates generated Parquet locally; adapter-dispatched date functions and write strategies target BigQuery.
- **Business-ready handoff:** interactive Streamlit reviewer dashboard, a Looker Studio page/field blueprint, SQL-evidence summaries, and an Airflow daily-load DAG.

## Reproduce the local benchmark

Requires Python 3.10+.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python -m gcp_analytics.pipeline
python -m pytest -q
streamlit run dashboard\app.py
```

Default run: seeded daily orders from **2024-01-01 through 2025-12-31**, approximately **124k orders**, daily Hive-partitioned Parquet at `data\landing\orders\order_date=YYYY-MM-DD\`, and corresponding dbt marts in `results\analytics.duckdb`.

Use `python -m gcp_analytics.pipeline --orders-per-day 30` for a faster development run, or choose another date present in the generated dataset with `--focus-date YYYY-MM-DD`. A pipeline run creates measured results in `results\metrics.json`, `results\scan_efficiency.json`, `results\monthly_kpis.csv`, `results\monthly_segments.csv`, and `results\evidence.json`. The locally generated database, raw Parquet, and Airflow state are ignored by Git.

To generate just one day's partition for a dry run of the Airflow handoff:

```powershell
python -m gcp_analytics.landing --date 2025-11-28 --orders-per-day 30
```

This writes only that date's Hive partition; the command does not upload data to a cloud bucket.

### Results snapshot

Metrics are produced by executing the pipeline, not by inserting resume-style estimates:

| Metric | Measurement |
|---|---:|
| Generated daily orders, 2024–2025 | 124,575 |
| dbt models | 4 (staging, intermediate, monthly KPI, monthly segments) |
| dbt quality tests | 26/26 passed |
| Selected day vs unpartitioned history | 12,468 B vs 2,999,749 B |
| Local Parquet volume reduction proxy | 99.58% |

`results/scan_efficiency.json` stores exact byte counts and the comparison method. This proxy helps verify that date-based partitioning materially reduces the relevant local input. It is **not** a BigQuery query-plan statistic. Once deployed, compare actual `total_bytes_processed` from BigQuery job metadata for equivalent bounded and unbounded queries before making a production cost claim.

The focus-period dashboard is also derived from SQL: monthly net-sales change, gross-margin movement, largest regional contribution, and seeded campaign context. All narrative is descriptive; campaign participation is not a causal lift analysis.

## dbt model layers

```text
GCS daily Parquet → raw.orders (daily DATE partition; region/category clustering)
       ├── stg_orders                  typed, tested source view
       ├── products                    dbt seed dimension
       └── int_order_economics         order-level sales, costs and margin
             ├── mart_monthly_kpis     date-partitioned executive scorecard
             └── mart_monthly_segments date-partitioned and region/category clustered
```

BigQuery marts use `insert_overwrite` on the two most recent affected month partitions, so late-arriving rows can update recent aggregates without replacing the full mart. The local DuckDB target uses the corresponding `delete+insert` incremental strategy. The BigQuery profile sets `maximum_bytes_billed`; raw BigQuery loads cluster by region and category. Report/dashboard queries should constrain the `order_month` field.

## Airflow daily DAG

`airflow\dags\retail_daily_pipeline.py` performs these ordered steps:

1. Validate the daily partitioned/clustered raw table, load that date into a temporary table, then atomically replace only the matching logical date in `raw.orders`. This makes retries idempotent and keeps existing rows intact if the staging load fails.
2. Run `dbt source freshness`; stale input fails its configured error threshold.
3. Run `dbt build --fail-fast` for seeds, staging/intermediate models, marts, and tests.

There are retries for transient task failures and only one active run. Missing input for a scheduled date fails explicitly; late arriving/backfilled data replaces only its affected partition and reruns the dbt DAG.

The DAG file is ready for an Airflow 2.10.x environment. Install Airflow with its matching official constraints file, then install this project with `.[gcp]` in the same deployment environment. Provide these variables to Airflow workers:

- `GCP_PROJECT_ID`
- `BIGQUERY_LOCATION` (for example `US`)
- `BQ_RAW_DATASET`
- `DBT_DATASET`
- `GCS_LANDING_BUCKET`
- `BIGQUERY_MAX_BYTES_BILLED`

Grant the worker identity only the required GCS read and BigQuery load/job/create/update permissions. Set up an Airflow connection or Application Default Credentials using your platform's supported identity mechanism; do not commit service-account JSON or secrets.

## BigQuery deployment (not executed here)

This repository did not authenticate to, or provision resources in, Google Cloud. These steps can incur storage, query, orchestration, and egress charges; review your project, region, permissions, quotas, and budget before using them.

1. Enable billing and the BigQuery and Cloud Storage APIs in a project you control. Create a region-matched GCS bucket, raw dataset, and dbt output dataset; set a billing budget and alerts.
2. Install the optional adapter and authenticate with Application Default Credentials:

   ```powershell
   python -m pip install -e ".[gcp]"
   gcloud auth application-default login
   ```

3. Set `GCP_PROJECT_ID`, `BIGQUERY_LOCATION`, `BQ_RAW_DATASET`, `DBT_DATASET`, `GCS_LANDING_BUCKET`, and `BIGQUERY_MAX_BYTES_BILLED` from `.env.example`.
4. Generate the same source and upload the initial history **once**:

   ```powershell
   python -m gcp_analytics.pipeline
   gcloud storage rsync --recursive data\landing\orders gs://$env:GCS_LANDING_BUCKET/landing/orders
   ```

5. Create the raw daily-partitioned/clustered table with
   `sql\bigquery_bootstrap.sql` after replacing project and dataset placeholders, then load the initial Parquet history using a reviewed BigQuery load job with Hive partition discovery. The daily DAG stages and atomically replaces one logical date, so retries and backfills do not duplicate or partially delete production rows.
   The BigQuery CLI load job can use `--source_format=PARQUET`,
   `--hive_partitioning_mode=AUTO`,
   `--hive_partitioning_source_uri_prefix=gs://BUCKET/landing/orders`,
   `--time_partitioning_field=order_date`,
   `--time_partitioning_type=DAY`, and
   `--clustering_fields=region,category` with the full-history glob
   `gs://BUCKET/landing/orders/order_date=*/*.parquet`. Check your target
   project, dataset, region, and BigQuery CLI version before running the load.
6. Install/configure Airflow, sync the DAG and repository onto workers, set the required variables, and verify one manually triggered date has source files before enabling the daily schedule.
7. Build the first BigQuery models with `dbt source freshness --project-dir dbt --profiles-dir dbt --target bigquery` followed by `dbt build --project-dir dbt --profiles-dir dbt --target bigquery`. Confirm the configured bytes billed cap and inspect BigQuery job history.
8. Connect the resulting marts to Looker Studio using [the dashboard hand-off](looker_studio/README.md).

To simulate a newly arriving single day for the DAG, generate and upload only
that partition:

```powershell
python -m gcp_analytics.landing --date 2026-11-04
gcloud storage cp --recursive data\landing\orders\order_date=2026-11-04 gs://$env:GCS_LANDING_BUCKET/landing/orders/
```

Confirm that the DAG's logical date matches the partition and that the object
exists before triggering the daily run.

The `raw.orders` table's production schema is described in the bootstrap SQL. Keep data region settings consistent across GCS, datasets, and query jobs. Validate table partition/clustering and the compiled dbt SQL in a non-production project before loading sensitive or business-critical data.

## Looker Studio

`looker_studio\README.md` documents the two BigQuery mart sources, page design, metric definitions, date controls, and partition-aware reporting practices. A published report requires your authenticated Google project and is not claimed to exist. The included Streamlit dashboard is the runnable, local visual review; it is not a substitute for or a claim of a hosted Looker Studio report.

## Repository map

- `src/gcp_analytics/` — deterministic source generation, Hive-partitioned Parquet, local dbt runner, business evidence, and measured volume comparison.
- `dbt/` — local and BigQuery profiles, adapter-aware macros, staged/intermediate/mart layers, product seed, tests, and source freshness.
- `airflow/dags/` — scheduled BigQuery incremental loader and tested dbt dependency chain.
- `sql/` — BigQuery table contract and reviewer query example.
- `dashboard/` — local, interactive reviewer dashboard.
- `looker_studio/` — BigQuery report setup handoff.
- `tests/` — generation, partitioning, dbt pipeline, and DAG structure checks.
