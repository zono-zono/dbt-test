{{ config(
    materialized='view'
) }}

with session_events as (
    select
        user_pseudo_id,
        ga_session_id,
        event_date,
        event_timestamp,
        event_name,
        page_location,
        page_title,
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,
        event_campaign,
        event_content,
        event_term,
        geo_country,
        geo_region,
        geo_city,
        device_category,
        device_os,
        device_browser,
        platform,
        engagement_time_msec,
        event_value,
        currency,
        transaction_id
    from {{ ref('stg_ga__events') }}
    where ga_session_id is not null
),

session_aggregated as (
    select
        user_pseudo_id,
        ga_session_id,
        event_date,
        min(event_timestamp) as session_start_timestamp,
        max(event_timestamp) as session_end_timestamp,
        count(*) as total_events,
        count(distinct event_name) as unique_events,
        countif(event_name = 'page_view') as page_views,
        countif(event_name = 'purchase') as purchases,
        countif(event_name = 'add_to_cart') as add_to_carts,
        countif(event_name = 'view_item') as item_views,
        sum(engagement_time_msec) as total_engagement_time_msec,
        sum(event_value) as total_event_value,
        max(currency) as currency,
        max(transaction_id) as transaction_id,
        
        -- First and last page info
        first_value(page_location) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as landing_page,
        last_value(page_location) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as exit_page,
        
        -- Traffic source (from first event)
        first_value(traffic_source_name) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as session_traffic_source_name,
        first_value(traffic_source_medium) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as session_traffic_source_medium,
        first_value(traffic_source_source) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as session_traffic_source_source,
        first_value(event_campaign) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as session_campaign,
        
        -- Geographic info
        first_value(geo_country) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as session_country,
        first_value(geo_region) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as session_region,
        first_value(geo_city) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as session_city,
        
        -- Device info
        first_value(device_category) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as session_device_category,
        first_value(device_os) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as session_device_os,
        first_value(device_browser) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as session_device_browser,
        first_value(platform) over (
            partition by user_pseudo_id, ga_session_id 
            order by event_timestamp 
            rows between unbounded preceding and unbounded following
        ) as session_platform
        
    from session_events
),

session_deduped as (
    select
        user_pseudo_id,
        ga_session_id,
        event_date,
        session_start_timestamp,
        session_end_timestamp,
        total_events,
        unique_events,
        page_views,
        purchases,
        add_to_carts,
        item_views,
        total_engagement_time_msec,
        total_event_value,
        currency,
        transaction_id,
        landing_page,
        exit_page,
        session_traffic_source_name,
        session_traffic_source_medium,
        session_traffic_source_source,
        session_campaign,
        session_country,
        session_region,
        session_city,
        session_device_category,
        session_device_os,
        session_device_browser,
        session_platform,
        
        -- Calculate session duration
        (session_end_timestamp - session_start_timestamp) / 1000000 as session_duration_seconds,
        
        -- Calculate bounce rate (single page view sessions)
        case when page_views = 1 then 1 else 0 end as is_bounce
        
    from session_aggregated
    qualify row_number() over (partition by user_pseudo_id, ga_session_id order by event_timestamp) = 1
)

select * from session_deduped
