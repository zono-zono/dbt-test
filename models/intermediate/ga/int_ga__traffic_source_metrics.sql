{{ config(
    materialized='ephemeral'
) }}

with traffic_source_sessions as (
    select
        event_date,
        session_traffic_source_name,
        session_traffic_source_medium,
        session_traffic_source_source,
        session_campaign,
        count(distinct ga_session_id) as sessions_count,
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
    where session_traffic_source_name is not null
    group by 
        event_date,
        session_traffic_source_name,
        session_traffic_source_medium,
        session_traffic_source_source,
        session_campaign
),

traffic_source_aggregated as (
    select
        session_traffic_source_name,
        session_traffic_source_medium,
        session_traffic_source_source,
        session_campaign,
        min(event_date) as first_seen_date,
        max(event_date) as last_seen_date,
        count(distinct event_date) as active_days,
        sum(sessions_count) as total_sessions,
        sum(unique_users) as total_unique_users,
        sum(total_page_views) as total_page_views,
        sum(total_purchases) as total_purchases,
        sum(total_add_to_carts) as total_add_to_carts,
        sum(total_item_views) as total_item_views,
        sum(total_revenue) as total_revenue,
        sum(total_session_duration) as total_session_duration,
        sum(bounce_sessions) as total_bounce_sessions,
        avg(avg_session_duration) as avg_session_duration,
        
        -- Calculate rates
        case 
            when sum(sessions_count) > 0 
            then sum(bounce_sessions) / sum(sessions_count) 
            else 0 
        end as bounce_rate,
        
        case 
            when sum(sessions_count) > 0 
            then sum(total_purchases) / sum(sessions_count) 
            else 0 
        end as conversion_rate,
        
        case 
            when sum(total_page_views) > 0 
            then sum(total_purchases) / sum(total_page_views) 
            else 0 
        end as purchase_rate_per_page_view,
        
        case 
            when sum(unique_users) > 0 
            then sum(total_sessions) / sum(unique_users) 
            else 0 
        end as sessions_per_user,
        
        case 
            when sum(total_sessions) > 0 
            then sum(total_revenue) / sum(total_sessions) 
            else 0 
        end as revenue_per_session
        
    from traffic_source_sessions
    group by 
        session_traffic_source_name,
        session_traffic_source_medium,
        session_traffic_source_source,
        session_campaign
)

select * from traffic_source_aggregated
