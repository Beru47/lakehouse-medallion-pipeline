select
    cast(strftime(pickup_date, '%Y%m%d') as integer) as date_key,
    pu_location_id as pickup_zone_key,
    trips,
    total_revenue,
    avg_fare,
    avg_distance,
    total_tips
from {{ ref('stg_daily_zone_metrics') }}
