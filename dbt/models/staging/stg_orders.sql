{{ config(materialized='view') }}

select
    cast(order_id as varchar) as order_id,
    cast(customer_id as varchar) as customer_id,
    cast(product_id as varchar) as product_id,
    cast(category as varchar) as category,
    cast(region as varchar) as region,
    cast(channel as varchar) as channel,
    cast(campaign_id as varchar) as campaign_id,
    cast(order_date as date) as order_date,
    cast(units as integer) as units,
    cast(gross_sales as double) as gross_sales,
    cast(discount_amount as double) as discount_amount,
    cast(net_sales as double) as net_sales,
    cast(cost as double) as cost,
    cast(ingested_at as timestamp) as ingested_at
from {{ source('raw', 'orders') }}
