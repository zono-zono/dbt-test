{{ config(schema='stg_fitbit') }}

with source as (

    select * 
    from {{ source('raw', 'weightLogInfo') }}

),

stg_weight_log_info as (

    select
        *
    from source
)

select * from stg_weight_log_info
