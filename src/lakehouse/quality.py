"""Explicit data-quality checks. Every result is logged; any failure raises."""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger("lakehouse.dq")


class DataQualityError(RuntimeError):
    pass


@dataclass
class Result:
    check: str
    passed: bool
    detail: str


def check_not_null(df: DataFrame, cols: list[str]) -> list[Result]:
    row = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first()
    return [Result(f"not_null:{c}", (row[c] or 0) == 0, f"{row[c] or 0} nulls") for c in cols]


def check_range(df: DataFrame, col: str, lo: float, hi: float) -> Result:
    bad = df.where(~F.col(col).between(lo, hi)).count()
    return Result(f"range:{col}[{lo},{hi}]", bad == 0, f"{bad} rows out of range")


def check_unique(df: DataFrame, col: str) -> Result:
    dupes = df.groupBy(col).count().where("count > 1").count()
    return Result(f"unique:{col}", dupes == 0, f"{dupes} duplicated keys")


def check_reconciliation(bronze: int, clean: int, quarantined: int, dupes: int) -> Result:
    """Every bronze row must be accounted for: clean, quarantined, or a dropped duplicate."""
    unaccounted = bronze - (clean + quarantined + dupes)
    return Result("reconciliation", unaccounted == 0, f"unaccounted rows: {unaccounted}")


def enforce(stage: str, results: list[Result], log_path: Path) -> None:
    """Log all results (JSON lines), then raise if any check failed."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    with log_path.open("a") as f:
        for r in results:
            f.write(json.dumps({"ts": now, "stage": stage, **asdict(r)}) + "\n")
            level = logging.INFO if r.passed else logging.ERROR
            log.log(level, "[%s] %s passed=%s (%s)", stage, r.check, r.passed, r.detail)
    failed = [r for r in results if not r.passed]
    if failed:
        raise DataQualityError(f"{stage}: " + "; ".join(f"{r.check} ({r.detail})" for r in failed))


def silver_checks(clean: DataFrame) -> list[Result]:
    return [
        *check_not_null(clean, ["trip_id", "pickup_ts", "pickup_date", "pu_location_id"]),
        check_range(clean, "trip_distance", 0, 200),
        check_range(clean, "fare_amount", 0, 1000),
        check_range(clean, "pu_location_id", 1, 265),
        check_unique(clean, "trip_id"),
    ]
