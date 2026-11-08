{% snapshot zone_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='LocationID',
        strategy='check',
        check_cols=['Borough', 'Zone', 'service_zone'],
    )
}}
select * from {{ ref('taxi_zone_lookup') }}
{% endsnapshot %}
