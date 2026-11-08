select
    zone_id as zone_key,
    borough,
    zone_name,
    service_zone
from {{ ref('stg_zones') }}
