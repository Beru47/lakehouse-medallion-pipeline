from datetime import datetime

import pytest
from pyspark.sql import functions as F

from lakehouse import gold, quality, silver

MONTH = "2024-01"


def trip(**over):
    base = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2024, 1, 5, 10, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 5, 10, 20),
        "passenger_count": 1.0,
        "trip_distance": 3.2,
        "PULocationID": 161,
        "DOLocationID": 236,
        "payment_type": 1,
        "fare_amount": 15.0,
        "tip_amount": 3.0,
        "total_amount": 21.0,
        "_source_file": "t.parquet",
        "_ingested_at": datetime(2024, 2, 1),
    }
    return {**base, **over}


SCHEMA = (
    "VendorID int, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, "
    "passenger_count double, trip_distance double, PULocationID int, DOLocationID int, "
    "payment_type int, fare_amount double, tip_amount double, total_amount double, "
    "_source_file string, _ingested_at timestamp"
)


def bronze(spark, rows):
    cols = list(trip())
    return spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], SCHEMA)


def reasons(quarantine):
    return {r.trip_distance: r.failed_rules for r in quarantine.collect()}


def test_bad_rows_are_quarantined_with_reasons(spark):
    rows = [
        trip(),
        trip(trip_distance=-1.0),
        trip(trip_distance=500.0),
        trip(PULocationID=999, trip_distance=7.0),
        trip(tpep_dropoff_datetime=datetime(2024, 1, 5, 9, 0), trip_distance=8.0),
        trip(tpep_pickup_datetime=datetime(2002, 12, 31), trip_distance=9.0),
        trip(tpep_pickup_datetime=None, trip_distance=10.0),
    ]
    clean, quarantine, dupes = silver.transform(bronze(spark, rows), MONTH)
    assert clean.count() == 1
    assert dupes == 0
    r = reasons(quarantine)
    assert r[-1.0] == ["trip_distance_out_of_range"]
    assert r[500.0] == ["trip_distance_out_of_range"]
    assert r[7.0] == ["pu_zone_invalid"]
    assert "dropoff_before_pickup" in r[8.0]
    assert "pickup_outside_month" in r[9.0]
    assert "pickup_ts_missing" in r[10.0]


def test_duplicates_are_dropped_and_counted(spark):
    rows = [trip(), trip(), trip(fare_amount=20.0, total_amount=26.0)]
    clean, quarantine, dupes = silver.transform(bronze(spark, rows), MONTH)
    assert (clean.count(), quarantine.count(), dupes) == (2, 0, 1)
    assert clean.select("trip_id").distinct().count() == 2


def test_null_passenger_count_is_allowed(spark):
    clean, quarantine, _ = silver.transform(bronze(spark, [trip(passenger_count=None)]), MONTH)
    assert (clean.count(), quarantine.count()) == (1, 0)


def test_silver_merge_is_idempotent(spark, tmp_path):
    target = str(tmp_path / "silver")
    clean, _, _ = silver.transform(bronze(spark, [trip(), trip(fare_amount=9.0)]), MONTH)
    silver.upsert(spark, clean, target)
    silver.upsert(spark, clean, target)
    assert spark.read.format("delta").load(target).count() == 2


def test_silver_merge_updates_existing_key(spark, tmp_path):
    target = str(tmp_path / "silver")
    first, _, _ = silver.transform(bronze(spark, [trip()]), MONTH)
    silver.upsert(spark, first, target)
    reingested = bronze(spark, [trip(_ingested_at=datetime(2024, 3, 1))])
    second, _, _ = silver.transform(reingested, MONTH)
    silver.upsert(spark, second, target)
    out = spark.read.format("delta").load(target)
    assert out.count() == 1
    assert out.first()._ingested_at == datetime(2024, 3, 1)


def test_gold_aggregates_and_merge(spark, tmp_path):
    rows = [trip(total_amount=10.0), trip(fare_amount=8.0, total_amount=20.0)]
    clean, _, _ = silver.transform(bronze(spark, rows), MONTH)
    agg = gold.daily_zone_metrics(clean)
    row = agg.first()
    assert (row.trips, row.total_revenue) == (2, 30.0)
    target = str(tmp_path / "gold")
    gold.upsert(spark, agg, target)
    gold.upsert(spark, agg, target)
    assert spark.read.format("delta").load(target).count() == 1


def test_quality_checks_fail_loudly_and_log(spark, tmp_path):
    clean, _, _ = silver.transform(bronze(spark, [trip()]), MONTH)
    log = tmp_path / "dq.jsonl"
    quality.enforce("silver", quality.silver_checks(clean), log)
    assert log.read_text().count("\n") == len(quality.silver_checks(clean))

    broken = clean.withColumn("trip_id", F.lit("same")).union(
        clean.withColumn("trip_id", F.lit("same"))
    )
    with pytest.raises(quality.DataQualityError, match="unique:trip_id"):
        quality.enforce("silver", quality.silver_checks(broken), log)


def test_reconciliation_detects_lost_rows():
    assert quality.check_reconciliation(10, 6, 3, 1).passed
    assert not quality.check_reconciliation(10, 6, 2, 1).passed
