{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    partition_by={
        "field": "event_date",
        "data_type": "date"
    },
    cluster_by=["event_type", "country"]
) }}

with source as (
    select *
    from {{ source('raw','events') }}
    {% if is_incremental() %}
    where event_date >= date_sub(current_date(), interval 2 day)
    {% endif %}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by event_id
            order by event_ts desc
        ) as rn
    from source
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

        case
            when event_type = 'purchase' and amount > 0 then amount
        end as amount,

        cast(event_date as date)        as event_date,
        TIMESTAMP_MICROS(event_ts)      as event_ts,
        session_id,
        current_timestamp()             as _loaded_at

    from deduplicated
    where rn = 1
),

enriched as (
    select
        *,
        date_trunc(event_ts, hour)       as event_hour,
        extract(hour from event_ts)      as hour_of_day,
        extract(dayofweek from event_ts) as day_of_week
    from cleaned
)

select * from enriched