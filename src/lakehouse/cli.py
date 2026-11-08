"""Pipeline entrypoint: python -m lakehouse.cli {bronze,silver,gold,export,all}."""

import argparse
import logging

from lakehouse import bronze, export, gold, quality, silver
from lakehouse.config import Settings
from lakehouse.spark import get_spark

log = logging.getLogger("lakehouse")


def run_bronze(spark, s: Settings) -> None:
    n = bronze.ingest(spark, s.source, s.table("bronze"))
    log.info("bronze: %d rows from %s", n, s.source.name)


def run_silver(spark, s: Settings) -> None:
    raw = (
        spark.read.format("delta")
        .load(s.table("bronze"))
        .where(f"_source_file = '{s.source.name}'")
    )
    clean, quarantine, dupes = silver.transform(raw, s.month)
    clean.cache()
    n_bronze, n_clean, n_quarantine = raw.count(), clean.count(), quarantine.count()
    results = [
        quality.check_reconciliation(n_bronze, n_clean, n_quarantine, dupes),
        *quality.silver_checks(clean),
    ]
    quality.enforce("silver", results, s.dq_log)  # fail loudly before touching silver
    silver.write_quarantine(spark, quarantine, s.source.name, s.table("quarantine"))
    silver.upsert(spark, clean, s.table("silver"))
    log.info("silver: %d clean, %d quarantined, %d duplicates", n_clean, n_quarantine, dupes)


def run_gold(spark, s: Settings) -> None:
    agg = gold.daily_zone_metrics(spark.read.format("delta").load(s.table("silver")))
    gold.upsert(spark, agg, s.table("gold"))
    log.info("gold: upserted %d daily/zone rows", agg.count())


def run_export(spark, s: Settings) -> None:
    export.export(spark, str(s.lake_dir), str(s.export_dir))
    log.info("export: parquet written to %s", s.export_dir)


STAGES = {"bronze": run_bronze, "silver": run_silver, "gold": run_gold, "export": run_export}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=[*STAGES, "all"])
    stage = parser.parse_args().stage
    settings, spark = Settings(), get_spark()
    for name in STAGES if stage == "all" else [stage]:
        STAGES[name](spark, settings)


if __name__ == "__main__":
    main()
