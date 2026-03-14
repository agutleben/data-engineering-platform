-- models/staging/stg_events.sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'events') }}
),

cleaned as (
    select
        event_id,
        event_type,
        user_id,
        product_id,
        category,
        device,
        country,

        -- nettoyage montant
        case
            when event_type = 'purchase' and amount > 0 then amount
            else null
        end as amount,

        -- dates propres
        cast(event_date as date)      as event_date,
        cast(event_ts as timestamp)   as event_ts,
        date_trunc(event_ts, hour)    as event_hour,

        -- colonnes dérivées
        extract(hour from event_ts)   as hour_of_day,
        extract(dayofweek from event_ts) as day_of_week,

        session_id,
        current_timestamp()           as _loaded_at

    from source
    where event_id is not null
      and event_type in ('view', 'add_to_cart', 'purchase')
      and user_id is not null
)

select * from cleaned