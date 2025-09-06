{{ config(
    materialized='table'
) }}

with traffic_source_metrics as (
    select * from {{ ref('int_ga__traffic_source_metrics') }}
),

traffic_source_ranked as (
    select
        *,
        row_number() over (order by total_revenue desc) as revenue_rank,
        row_number() over (order by total_sessions desc) as sessions_rank,
        row_number() over (order by conversion_rate desc) as conversion_rank,
        row_number() over (order by bounce_rate asc) as bounce_rank,
        
        -- Calculate performance scores (0-100 scale)
        case 
            when max(total_revenue) over () > 0 
            then (total_revenue / max(total_revenue) over ()) * 100 
            else 0 
        end as revenue_score,
        
        case 
            when max(total_sessions) over () > 0 
            then (total_sessions / max(total_sessions) over ()) * 100 
            else 0 
        end as sessions_score,
        
        case 
            when max(conversion_rate) over () > 0 
            then (conversion_rate / max(conversion_rate) over ()) * 100 
            else 0 
        end as conversion_score,
        
        case 
            when max(bounce_rate) over () > 0 
            then (1 - (bounce_rate / max(bounce_rate) over ())) * 100 
            else 100 
        end as bounce_score,
        
        -- Overall performance score (weighted average)
        (
            (case when max(total_revenue) over () > 0 then (total_revenue / max(total_revenue) over ()) * 100 else 0 end) * 0.3 +
            (case when max(total_sessions) over () > 0 then (total_sessions / max(total_sessions) over ()) * 100 else 0 end) * 0.2 +
            (case when max(conversion_rate) over () > 0 then (conversion_rate / max(conversion_rate) over ()) * 100 else 0 end) * 0.3 +
            (case when max(bounce_rate) over () > 0 then (1 - (bounce_rate / max(bounce_rate) over ())) * 100 else 100 end) * 0.2
        ) as overall_performance_score
        
    from traffic_source_metrics
),

traffic_source_segmented as (
    select
        *,
        case 
            when overall_performance_score >= 80 then 'Top Performer'
            when overall_performance_score >= 60 then 'Good Performer'
            when overall_performance_score >= 40 then 'Average Performer'
            when overall_performance_score >= 20 then 'Below Average'
            else 'Poor Performer'
        end as performance_tier,
        
        case 
            when total_revenue >= 1000 then 'High Revenue'
            when total_revenue >= 500 then 'Medium Revenue'
            when total_revenue > 0 then 'Low Revenue'
            else 'No Revenue'
        end as revenue_tier,
        
        case 
            when total_sessions >= 100 then 'High Volume'
            when total_sessions >= 50 then 'Medium Volume'
            when total_sessions >= 10 then 'Low Volume'
            else 'Minimal Volume'
        end as volume_tier
        
    from traffic_source_ranked
)

select 
    session_traffic_source_name,
    session_traffic_source_medium,
    session_traffic_source_source,
    session_campaign,
    first_seen_date,
    last_seen_date,
    active_days,
    total_sessions,
    total_unique_users,
    total_page_views,
    total_purchases,
    total_add_to_carts,
    total_item_views,
    total_revenue,
    total_session_duration,
    total_bounce_sessions,
    avg_session_duration,
    bounce_rate,
    conversion_rate,
    purchase_rate_per_page_view,
    sessions_per_user,
    revenue_per_session,
    revenue_rank,
    sessions_rank,
    conversion_rank,
    bounce_rank,
    revenue_score,
    sessions_score,
    conversion_score,
    bounce_score,
    overall_performance_score,
    performance_tier,
    revenue_tier,
    volume_tier,
    current_timestamp() as created_at
from traffic_source_segmented
