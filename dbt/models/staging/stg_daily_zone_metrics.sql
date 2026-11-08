select
    pickup_date,
    pu_location_id,
    trips,
    total_revenue,
    avg_fare,
    avg_distance,
    total_tips
from read_parquet('{{ var("export_dir") }}/daily_zone_metrics/*.parquet')
