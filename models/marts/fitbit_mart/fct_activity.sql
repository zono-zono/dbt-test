{{ config(schema='mart_fitbit', tags=['daily-0900']) }}

select
    user_id,
    cast(activity_date as date) as date,
    total_steps,
    total_distance,
    calories
from {{ ref('stg_daily_activity') }}