{{ config(materialized="view") }}

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

    -- Event parameters (cast key to STRING) + limit 1
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'page_location'       limit 1) as page_location,
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'page_title'          limit 1) as page_title,
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'page_referrer'       limit 1) as page_referrer,
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'source'              limit 1) as event_source,
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'medium'              limit 1) as event_medium,
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'campaign'            limit 1) as event_campaign,
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'content'             limit 1) as event_content,
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'term'                limit 1) as event_term,
    (select value.int_value    from unnest(event_params) where CAST(key AS STRING) = 'ga_session_id'       limit 1) as ga_session_id,
    (select value.int_value    from unnest(event_params) where CAST(key AS STRING) = 'ga_session_number'   limit 1) as ga_session_number,
    (select value.double_value from unnest(event_params) where CAST(key AS STRING) = 'engagement_time_msec' limit 1) as engagement_time_msec,

    -- Ecommerce parameters
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'currency'            limit 1) as currency,
    (select value.double_value from unnest(event_params) where CAST(key AS STRING) = 'value'               limit 1) as event_value,
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'transaction_id'      limit 1) as transaction_id,
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'item_id'             limit 1) as item_id,
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'item_name'           limit 1) as item_name,
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'item_category'       limit 1) as item_category,
    (select value.string_value from unnest(event_params) where CAST(key AS STRING) = 'item_brand'          limit 1) as item_brand,
    (select value.double_value from unnest(event_params) where CAST(key AS STRING) = 'price'               limit 1) as item_price,
    (select value.int_value    from unnest(event_params) where CAST(key AS STRING) = 'quantity'            limit 1) as item_quantity,

    -- User properties
    (select value.string_value from unnest(user_properties) where CAST(key AS STRING) = 'user_type'        limit 1) as user_type,
    (select value.string_value from unnest(user_properties) where CAST(key AS STRING) = 'customer_type'    limit 1) as customer_type,

    -- Items array（必要なら別モデルで UNNEST）
    items
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  where SAFE_CAST(_TABLE_SUFFIX AS INT64) between 20201101 and 20201231
)
select * from events
