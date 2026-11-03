{{
  config(
    materialized='incremental',
    unique_key='order_month',
    incremental_strategy=mart_incremental_strategy(),
    partition_by=mart_partition_config(),
    on_schema_change='sync_all_columns'
  )
}}

select
    order_month,
    count(*) as order_count,
    count(distinct customer_id) as active_customers,
    sum(units) as units_sold,
    sum(gross_sales) as gross_sales,
    sum(discount_amount) as discounts,
    sum(net_sales) as net_sales,
    sum(cost) as cost_of_goods,
    sum(gross_profit) as gross_profit,
    sum(discount_amount) / nullif(sum(gross_sales), 0) as discount_rate,
    sum(gross_profit) / nullif(sum(net_sales), 0) as gross_margin_rate
from {{ ref('int_order_economics') }}
{% if is_incremental() %}
where order_date >= {{ incremental_lookback_start() }}
{% endif %}
group by order_month
