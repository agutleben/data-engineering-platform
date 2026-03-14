-- models/marts/mart_funnel_conversion.sql
{{ config(materialized='table') }}

select
    event_date,
    countif(event_type = 'view')        as nb_views,
    countif(event_type = 'add_to_cart') as nb_add_to_cart,
    countif(event_type = 'purchase')    as nb_purchases,
    round(
        countif(event_type = 'purchase')
        / nullif(countif(event_type = 'view'), 0) * 100
    , 2)                                as conversion_pct
from {{ ref('stg_events') }}
group by event_date
order by event_date desc