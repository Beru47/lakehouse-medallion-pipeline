with bounds as (
    select min(pickup_date) as d0, max(pickup_date) as d1 from {{ ref('stg_trips') }}
),
days as (
    select cast(unnest(generate_series(d0, d1, interval 1 day)) as date) as date_day from bounds
)
select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    dayname(date_day) as day_name,
    isodow(date_day) in (6, 7) as is_weekend
from days
