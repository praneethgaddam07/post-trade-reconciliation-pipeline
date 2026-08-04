select distinct
    tick_date as date_day,
    date_part('year', tick_date) as year,
    date_part('month', tick_date) as month,
    date_part('day', tick_date) as day,
    date_part('dow', tick_date) as day_of_week
from {{ ref('stg_ticks') }}
