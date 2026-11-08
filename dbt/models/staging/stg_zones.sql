-- current version of each zone from the SCD2 snapshot
select
    "LocationID" as zone_id,
    "Borough" as borough,
    "Zone" as zone_name,
    service_zone,
    dbt_valid_from
from {{ ref('zone_snapshot') }}
where dbt_valid_to is null
