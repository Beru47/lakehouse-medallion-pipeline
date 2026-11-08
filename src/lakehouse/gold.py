"""Gold: daily x pickup-zone aggregates, upserted with MERGE."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def daily_zone_metrics(silver: DataFrame) -> DataFrame:
    return silver.groupBy("pickup_date", "pu_location_id").agg(
        F.count("*").alias("trips"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.sum("tip_amount"), 2).alias("total_tips"),
    )


def upsert(spark: SparkSession, agg: DataFrame, target: str) -> None:
    if not DeltaTable.isDeltaTable(spark, target):
        agg.limit(0).write.format("delta").partitionBy("pickup_date").save(target)
    (
        DeltaTable.forPath(spark, target)
        .alias("t")
        .merge(
            agg.alias("s"),
            "t.pickup_date = s.pickup_date AND t.pu_location_id = s.pu_location_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
