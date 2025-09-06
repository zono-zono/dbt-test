{{ config(
    materialized='view'
) }}

with events as (
    select
        -- Event identification
        event_date,
        event_timestamp,
        event_name,
        event_bundle_sequence_id,
        event_previous_timestamp,
        event_value_in_usd,
        event_server_timestamp_offset,
        
        -- User identification
        user_pseudo_id,
        user_id,
        user_first_touch_timestamp,
        user_ltv,
        
        -- Device and platform info
        device.category as device_category,
        device.operating_system as device_os,
        device.operating_system_version as device_os_version,
        device.vendor_id as device_vendor,
        device.advertising_id as device_advertising_id,
        device.language as device_language,
        device.is_limited_ad_tracking as device_limited_ad_tracking,
        device.time_zone_offset_seconds as device_timezone_offset,
        device.browser as device_browser,
        device.browser_version as device_browser_version,
        device.web_info.browser as web_browser,
        device.web_info.browser_version as web_browser_version,
        device.web_info.hostname as web_hostname,
        
        -- Geographic info
        geo.continent as geo_continent,
        geo.country as geo_country,
        geo.region as geo_region,
        geo.city as geo_city,
        geo.sub_continent as geo_sub_continent,
        geo.metro as geo_metro,
        
        -- Traffic source
        traffic_source.name as traffic_source_name,
        traffic_source.medium as traffic_source_medium,
        traffic_source.source as traffic_source_source,
        
        -- App info
        app_info.id as app_id,
        app_info.version as app_version,
        app_info.install_store as app_install_store,
        app_info.firebase_app_id as app_firebase_id,
        app_info.install_source as app_install_source,
        
        -- Stream and platform
        stream_id,
        platform,
        
        -- Event parameters (extract common ones)
        (select value.string_value from unnest(event_params) where key = 'page_location') as page_location,
        (select value.string_value from unnest(event_params) where key = 'page_title') as page_title,
        (select value.string_value from unnest(event_params) where key = 'page_referrer') as page_referrer,
        (select value.string_value from unnest(event_params) where key = 'source') as event_source,
        (select value.string_value from unnest(event_params) where key = 'medium') as event_medium,
        (select value.string_value from unnest(event_params) where key = 'campaign') as event_campaign,
        (select value.string_value from unnest(event_params) where key = 'content') as event_content,
        (select value.string_value from unnest(event_params) where key = 'term') as event_term,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id,
        (select value.int_value from unnest(event_params) where key = 'ga_session_number') as ga_session_number,
        (select value.double_value from unnest(event_params) where key = 'engagement_time_msec') as engagement_time_msec,
        
        -- Ecommerce parameters
        (select value.string_value from unnest(event_params) where key = 'currency') as currency,
        (select value.double_value from unnest(event_params) where key = 'value') as event_value,
        (select value.string_value from unnest(event_params) where key = 'transaction_id') as transaction_id,
        (select value.string_value from unnest(event_params) where key = 'item_id') as item_id,
        (select value.string_value from unnest(event_params) where key = 'item_name') as item_name,
        (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
        (select value.string_value from unnest(event_params) where key = 'item_brand') as item_brand,
        (select value.double_value from unnest(event_params) where key = 'price') as item_price,
        (select value.int_value from unnest(event_params) where key = 'quantity') as item_quantity,
        
        -- User properties (extract common ones)
        (select value.string_value from unnest(user_properties) where key = 'user_type') as user_type,
        (select value.string_value from unnest(user_properties) where key = 'customer_type') as customer_type,
        
        -- Items array (for ecommerce events)
        items
        
    from {{ source('ga4_sample', 'events') }}
    where _table_suffix between 
        format_date('%Y%m%d', date_sub(current_date(), interval 30 day))
        and format_date('%Y%m%d', date_sub(current_date(), interval 1 day))
)

select * from events
