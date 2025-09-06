{{ config(
    materialized='table'
) }}

with daily_sessions as (
    select
        event_date,
        count(distinct ga_session_id) as total_sessions,
        count(distinct user_pseudo_id) as unique_users,
        sum(page_views) as total_page_views,
        sum(purchases) as total_purchases,
        sum(add_to_carts) as total_add_to_carts,
        sum(item_views) as total_item_views,
        sum(total_event_value) as total_revenue,
        sum(session_duration_seconds) as total_session_duration,
        sum(is_bounce) as bounce_sessions,
        avg(session_duration_seconds) as avg_session_duration
    from {{ ref('stg_ga__sessions') }}
    group by event_date
),

daily_traffic_source as (
    select
        event_date,
        session_traffic_source_medium,
        count(distinct ga_session_id) as sessions_by_medium,
        count(distinct user_pseudo_id) as users_by_medium,
        sum(total_event_value) as revenue_by_medium
    from {{ ref('stg_ga__sessions') }}
    where session_traffic_source_medium is not null
    group by event_date, session_traffic_source_medium
),

daily_geographic as (
    select
        event_date,
        session_country,
        count(distinct ga_session_id) as sessions_by_country,
        count(distinct user_pseudo_id) as users_by_country,
        sum(total_event_value) as revenue_by_country
    from {{ ref('stg_ga__sessions') }}
    where session_country is not null
    group by event_date, session_country
),

daily_device as (
    select
        event_date,
        session_device_category,
        count(distinct ga_session_id) as sessions_by_device,
        count(distinct user_pseudo_id) as users_by_device,
        sum(total_event_value) as revenue_by_device
    from {{ ref('stg_ga__sessions') }}
    where session_device_category is not null
    group by event_date, session_device_category
),

daily_metrics as (
    select
        ds.event_date,
        ds.total_sessions,
        ds.unique_users,
        ds.total_page_views,
        ds.total_purchases,
        ds.total_add_to_carts,
        ds.total_item_views,
        ds.total_revenue,
        ds.total_session_duration,
        ds.bounce_sessions,
        ds.avg_session_duration,
        
        -- Calculate rates
        case 
            when ds.total_sessions > 0 
            then ds.bounce_sessions / ds.total_sessions 
            else 0 
        end as bounce_rate,
        
        case 
            when ds.total_sessions > 0 
            then ds.total_purchases / ds.total_sessions 
            else 0 
        end as conversion_rate,
        
        case 
            when ds.total_page_views > 0 
            then ds.total_purchases / ds.total_page_views 
            else 0 
        end as purchase_rate_per_page_view,
        
        case 
            when ds.unique_users > 0 
            then ds.total_sessions / ds.unique_users 
            else 0 
        end as sessions_per_user,
        
        case 
            when ds.total_sessions > 0 
            then ds.total_revenue / ds.total_sessions 
            else 0 
        end as revenue_per_session,
        
        case 
            when ds.total_sessions > 0 
            then ds.total_session_duration / ds.total_sessions 
            else 0 
        end as avg_session_duration_calculated
        
    from daily_sessions ds
)

select 
    dm.*,
    current_timestamp() as created_at
from daily_metrics dm
