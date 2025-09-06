{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id',
    on_schema_change = 'sync_all_columns'
) }}

select
  *
from dbt_zonozono.raw_customers
