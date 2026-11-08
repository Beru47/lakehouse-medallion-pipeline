"""Silver: typed, deduplicated, quality-filtered trips. Bad rows go to a quarantine table."""

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def rules(month: str) -> dict[str, Column]:
    """Rule name -> predicate that must hold for a row to be accepted.

    A null predicate result counts as a failure (see `failed_rules`).
    """
    return {
        "pickup_ts_missing": F.col("pickup_ts").isNotNull(),
        "pickup_outside_month": F.date_format("pickup_ts", "yyyy-MM") == F.lit(month),
        "dropoff_before_pickup": F.col("dropoff_ts") >= F.col("pickup_ts"),
        "trip_distance_out_of_range": F.col("trip_distance").between(0, 200),
        "fare_out_of_range": F.col("fare_amount").between(0, 1000),
        "total_amount_negative": F.col("total_amount") >= 0,
        "pu_zone_invalid": F.col("pu_location_id").between(1, 265),
        "do_zone_invalid": F.col("do_location_id").between(1, 265),
        "passenger_count_out_of_range": F.col("passenger_count").isNull()
        | F.col("passenger_count").between(0, 9),
    }


KEY_COLS = [
    "vendor_id",
    "pickup_ts",
    "dropoff_ts",
    "pu_location_id",
    "do_location_id",
    "fare_amount",
    "total_amount",
]


def typed(bronze: DataFrame) -> DataFrame:
    """Rename to snake_case, cast, and derive the natural trip_id (TLC data has no key)."""
    df = bronze.select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("PULocationID").cast("int").alias("pu_location_id"),
        F.col("DOLocationID").cast("int").alias("do_location_id"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
        "_source_file",
        "_ingested_at",
    )
    key = F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in KEY_COLS]), 256)
    return df.withColumn("trip_id", key).withColumn("pickup_date", F.to_date("pickup_ts"))


def failed_rules(rule_map: dict[str, Column]) -> Column:
    """Array with the name of every rule the row violates (empty array = clean)."""
    checks = [F.when(~F.coalesce(p, F.lit(False)), F.lit(n)) for n, p in rule_map.items()]
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def transform(bronze: DataFrame, month: str) -> tuple[DataFrame, DataFrame, int]:
    """Return (clean, quarantine, duplicates_dropped). Pure: no I/O, easy to unit-test."""
    df = typed(bronze).withColumn("failed_rules", failed_rules(rules(month)))
    quarantine = df.where(F.size("failed_rules") > 0).withColumn(
        "quarantined_at", F.current_timestamp()
    )
    valid = df.where(F.size("failed_rules") == 0).drop("failed_rules")
    latest = Window.partitionBy("trip_id").orderBy(F.col("_ingested_at").desc())
    clean = valid.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")
    return clean, quarantine, valid.count() - clean.count()


def _ensure_table(spark: SparkSession, df: DataFrame, target: str) -> None:
    if not DeltaTable.isDeltaTable(spark, target):
        df.limit(0).write.format("delta").partitionBy("pickup_date").save(target)


def upsert(spark: SparkSession, clean: DataFrame, target: str) -> None:
    """Idempotent MERGE on trip_id; the partition predicate lets Delta prune files."""
    _ensure_table(spark, clean, target)
    (
        DeltaTable.forPath(spark, target)
        .alias("t")
        .merge(clean.alias("s"), "t.trip_id = s.trip_id AND t.pickup_date = s.pickup_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_quarantine(
    spark: SparkSession, quarantine: DataFrame, source_file: str, target: str
) -> None:
    """Replace the quarantine rows of this source file (idempotent per batch)."""
    writer = quarantine.write.format("delta").mode("overwrite")
    if DeltaTable.isDeltaTable(spark, target):
        writer = writer.option("replaceWhere", f"_source_file = '{source_file}'")
    writer.save(target)
