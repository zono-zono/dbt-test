{{ config(schema='mart_fitbit', tags=['daily-0900']) }}

select
    id,
    parse_date('%m/%d/%Y', regexp_extract(Date, r'^\d+/\d+/\d+')) as date,
    Weightkg,
    BMI
from {{ ref('stg_weight_log_info') }}

