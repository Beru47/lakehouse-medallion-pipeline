"""Export silver trips and gold metrics as plain Parquet for dbt-duckdb."""

from pyspark.sql import SparkSession


def export(spark: SparkSession, lake_dir: str, export_dir: str) -> None:
    for name, layer in [("trips", "silver"), ("daily_zone_metrics", "gold")]:
        df = spark.read.format("delta").load(f"{lake_dir}/{layer}")
        df.coalesce(1).write.mode("overwrite").parquet(f"{export_dir}/{name}")
