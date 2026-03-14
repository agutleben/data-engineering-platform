-- models/marts/mart_daily_revenue.sql
{{ config(materialized='table') }}

select
    event_date,
    count(*)                                    as nb_purchases,
    round(sum(amount), 2)                       as revenue,
    count(distinct user_id)                     as nb_buyers,
    round(avg(amount), 2)                       as avg_order_value
from {{ ref('stg_events') }}
where event_type = 'purchase'
  and amount is not null
group by event_date
order by event_date desc