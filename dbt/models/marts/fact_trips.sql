select
    trip_id,
    cast(strftime(pickup_date, '%Y%m%d') as integer) as date_key,
    pu_location_id as pickup_zone_key,
    do_location_id as dropoff_zone_key,
    vendor_id,
    payment_type,
    pickup_ts,
    dropoff_ts,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount
from {{ ref('stg_trips') }}
