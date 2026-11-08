"""Bronze: raw trips as delivered, plus ingestion metadata. No cleaning here."""

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def with_metadata(df: DataFrame, source: Path) -> DataFrame:
    return df.withColumn("_source_file", F.lit(source.name)).withColumn(
        "_ingested_at", F.current_timestamp()
    )


def ingest(spark: SparkSession, source: Path, target: str) -> int:
    """Append one source file to bronze.

    Re-running the same file replaces its own rows (replaceWhere on _source_file), so the
    task is idempotent while bronze stays append-only across distinct files.
    """
    df = with_metadata(spark.read.parquet(str(source)), source)
    writer = df.write.format("delta").mode("overwrite")
    if DeltaTable.isDeltaTable(spark, target):
        writer = writer.option("replaceWhere", f"_source_file = '{source.name}'")
    writer.save(target)
    return df.count()
