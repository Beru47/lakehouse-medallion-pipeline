"""End-to-end run on the committed sample, including a re-run to prove idempotency."""

from pathlib import Path

from lakehouse import cli
from lakehouse.config import Settings

SAMPLE = Path(__file__).parent.parent / "data" / "sample" / "yellow_sample.parquet"


def counts(spark, s):
    return {
        layer: spark.read.format("delta").load(s.table(layer)).count()
        for layer in ("bronze", "silver", "quarantine", "gold")
    }


def test_full_pipeline_is_idempotent(spark, tmp_path):
    s = Settings(
        lake_dir=tmp_path / "lake",
        export_dir=tmp_path / "export",
        source=SAMPLE,
        dq_log=tmp_path / "dq.jsonl",
    )
    for stage in ("bronze", "silver", "gold", "export"):
        cli.STAGES[stage](spark, s)
    first = counts(spark, s)

    assert first["bronze"] == 3030  # 3000 sampled + 30 deliberate duplicates
    # valid duplicates are dropped; duplicated bad rows stay in quarantine
    assert first["bronze"] - 30 <= first["silver"] + first["quarantine"] <= first["bronze"]
    assert first["silver"] > 2500
    assert (tmp_path / "export" / "trips").exists()

    for stage in ("bronze", "silver", "gold", "export"):
        cli.STAGES[stage](spark, s)
    assert counts(spark, s) == first
