-- the Spark gold aggregate and the dbt fact table must agree on trip counts per day
select f.date_key, f.n as fact_trips, a.n as gold_trips
from (select date_key, count(*) as n from {{ ref('fact_trips') }} group by 1) f
join (select date_key, sum(trips) as n from {{ ref('agg_daily_zone_metrics') }} group by 1) a
  using (date_key)
where f.n <> a.n
