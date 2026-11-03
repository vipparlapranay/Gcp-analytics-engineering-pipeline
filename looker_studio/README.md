# Looker Studio dashboard hand-off

The `Monthly KPIs` and `Monthly Segments` dbt marts are the intended BigQuery
sources for a Looker Studio dashboard. This folder contains a build-ready field
map and report plan, **not a published Looker Studio URL or an exported native
report**; creating/publishing a Looker report requires a Google account and
access to the deployed BigQuery project.

## Connect the report

1. In Looker Studio, add a BigQuery data source for
   `PROJECT_ID.ANALYTICS_DATASET.MART_MONTHLY_KPIS`.
2. Add a second source for
   `PROJECT_ID.ANALYTICS_DATASET.MART_MONTHLY_SEGMENTS`.
3. Use `order_month` as a date-range dimension. Both marts are month-partitioned.
4. Build the dashboard with the following pages:
   - **Executive pulse:** net sales, gross profit, gross margin, orders, active customers.
   - **Where it moved:** monthly net sales trend and month-over-month change.
   - **Mix & margin:** region × product-category drill-down with discount and gross-margin rates.
5. Add a date-range control and default it to the latest complete month.
6. Avoid blending on raw orders. The aggregated marts reduce query volume and
   use partition pruning; charts should include a bounded `order_month` filter.

## Calculated fields

- **MoM Net Sales:** `(SUM(Net Sales) - Previous Month(SUM(Net Sales))) / Previous Month(SUM(Net Sales))`
- **Gross Margin:** `SUM(Gross Profit) / SUM(Net Sales)`
- **Average Order Value:** `SUM(Net Sales) / SUM(Order Count)`
- **Discount Rate:** `SUM(Discounts) / SUM(Gross Sales)`

Create time comparisons using Looker Studio's date-range comparison, not a
raw-table self-join. The query-per-chart bytes processed and dashboard refresh
behavior must be measured in the target BigQuery project after deployment.
