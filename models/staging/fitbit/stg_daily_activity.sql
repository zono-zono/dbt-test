{{ config(schema='stg_fitbit') }}

with source as (

    select * 
    from {{ source('raw', 'dailyActivity_merged') }}

),

stg_daily_activity as (

    select
        cast(Id as string) as user_id,
        cast(ActivityDate as date) as activity_date,
        cast(TotalSteps as int64) as total_steps,
        cast(TotalDistance as float64) as total_distance,
        cast(Calories as int64) as calories
    from source
)

select * from stg_daily_activity
