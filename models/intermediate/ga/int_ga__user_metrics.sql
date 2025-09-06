{{ config(
    materialized='ephemeral'
) }}

with user_sessions as (
    select
        user_pseudo_id,
        event_date,
        count(distinct ga_session_id) as sessions_count,
        sum(page_views) as total_page_views,
        sum(purchases) as total_purchases,
        sum(add_to_carts) as total_add_to_carts,
        sum(item_views) as total_item_views,
        sum(total_event_value) as total_revenue,
        sum(session_duration_seconds) as total_session_duration,
        sum(is_bounce) as bounce_sessions,
        count(distinct landing_page) as unique_landing_pages,
        count(distinct session_country) as unique_countries,
        count(distinct session_device_category) as unique_device_categories
    from {{ ref('stg_ga__sessions') }}
    group by user_pseudo_id, event_date
),

user_aggregated as (
    select
        user_pseudo_id,
        min(event_date) as first_seen_date,
        max(event_date) as last_seen_date,
        count(distinct event_date) as active_days,
        sum(sessions_count) as total_sessions,
        sum(total_page_views) as total_page_views,
        sum(total_purchases) as total_purchases,
        sum(total_add_to_carts) as total_add_to_carts,
        sum(total_item_views) as total_item_views,
        sum(total_revenue) as total_revenue,
        sum(total_session_duration) as total_session_duration,
        sum(bounce_sessions) as total_bounce_sessions,
        max(unique_landing_pages) as max_unique_landing_pages,
        max(unique_countries) as max_unique_countries,
        max(unique_device_categories) as max_unique_device_categories,
        
        -- Calculate averages
        avg(sessions_count) as avg_sessions_per_day,
        avg(total_page_views) as avg_page_views_per_day,
        avg(total_session_duration) as avg_session_duration_per_day,
        
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
        end as purchase_rate_per_page_view
        
    from user_sessions
    group by user_pseudo_id
)

select * from user_aggregated
