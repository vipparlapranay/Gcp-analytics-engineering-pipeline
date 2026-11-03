-- Parameterize @start_month / @end_month in Looker Studio.
-- Do not remove the order_month predicate: mart tables are month-partitioned.

SELECT
  order_month,
  region,
  category,
  SUM(order_count) AS order_count,
  SUM(active_customers) AS active_customers,
  SUM(units_sold) AS units_sold,
  SUM(gross_sales) AS gross_sales,
  SUM(discounts) AS discounts,
  SUM(net_sales) AS net_sales,
  SUM(gross_profit) AS gross_profit,
  SAFE_DIVIDE(SUM(discounts), SUM(gross_sales)) AS discount_rate,
  SAFE_DIVIDE(SUM(gross_profit), SUM(net_sales)) AS gross_margin_rate
FROM `REPLACE_WITH_PROJECT.ANALYTICS_DATASET.MART_MONTHLY_SEGMENTS`
WHERE order_month BETWEEN @start_month AND @end_month
GROUP BY order_month, region, category
ORDER BY order_month, net_sales DESC;
