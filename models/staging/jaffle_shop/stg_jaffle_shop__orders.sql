
-- Use the `ref` function to select from other models

select *
from {{ ref('stg_jaffle_shop__customers') }}
where id = 1
