select
    trip_id,
    vendor_id,
    pickup_ts,
    dropoff_ts,
    pickup_date,
    passenger_count,
    trip_distance,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
from read_parquet('{{ var("export_dir") }}/trips/*.parquet')
