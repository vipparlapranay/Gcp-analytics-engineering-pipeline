{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    product_id,
    category,
    region,
    channel,
    campaign_id,
    order_date,
    {{ month_start('order_date') }} as order_month,
    units,
    gross_sales,
    discount_amount,
    net_sales,
    cost,
    net_sales - cost as gross_profit,
    discount_amount / nullif(gross_sales, 0) as discount_rate,
    (net_sales - cost) / nullif(net_sales, 0) as gross_margin_rate
from {{ ref('stg_orders') }}
