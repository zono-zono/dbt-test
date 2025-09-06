{{ config(
    materialized='table'
) }}

with user_metrics as (
    select * from {{ ref('int_ga__user_metrics') }}
),

user_segments as (
    select
        *,
        case 
            when total_revenue > 0 then 'Customer'
            when total_add_to_carts > 0 then 'Potential Customer'
            when total_item_views > 0 then 'Interested User'
            else 'Casual User'
        end as user_segment,
        
        case 
            when total_sessions >= 10 then 'High Engagement'
            when total_sessions >= 5 then 'Medium Engagement'
            when total_sessions >= 2 then 'Low Engagement'
            else 'Single Session'
        end as engagement_level,
        
        case 
            when total_revenue >= 100 then 'High Value'
            when total_revenue >= 50 then 'Medium Value'
            when total_revenue > 0 then 'Low Value'
            else 'No Purchase'
        end as value_segment,
        
        case 
            when active_days >= 7 then 'Frequent User'
            when active_days >= 3 then 'Regular User'
            when active_days >= 2 then 'Occasional User'
            else 'One-time User'
        end as frequency_segment
        
    from user_metrics
)

select 
    user_pseudo_id,
    first_seen_date,
    last_seen_date,
    active_days,
    total_sessions,
    total_page_views,
    total_purchases,
    total_add_to_carts,
    total_item_views,
    total_revenue,
    total_session_duration,
    total_bounce_sessions,
    max_unique_landing_pages,
    max_unique_countries,
    max_unique_device_categories,
    avg_sessions_per_day,
    avg_page_views_per_day,
    avg_session_duration_per_day,
    bounce_rate,
    conversion_rate,
    purchase_rate_per_page_view,
    user_segment,
    engagement_level,
    value_segment,
    frequency_segment,
    current_timestamp() as created_at
from user_segments
