-- models/marts/mart_top_products.sql
{{ config(materialized='table') }}

select
    product_id,
    category,
    count(*)                as nb_purchases,
    round(sum(amount), 2)   as total_revenue,
    count(distinct user_id) as nb_buyers,
    round(avg(amount), 2)   as avg_price
from {{ ref('stg_events') }}
where event_type = 'purchase'
  and amount is not null
group by product_id, category
order by total_revenue desc