"""Paths and run parameters, all overridable through environment variables."""

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    lake_dir: Path = field(default_factory=lambda: Path(os.getenv("LAKE_DIR", "data/lake")))
    export_dir: Path = field(default_factory=lambda: Path(os.getenv("EXPORT_DIR", "data/export")))
    source: Path = field(
        default_factory=lambda: Path(
            os.getenv("SOURCE", "data/raw/yellow_tripdata_2024-01.parquet")
        )
    )
    # Expected pickup month (YYYY-MM): rows outside it are quarantined.
    month: str = field(default_factory=lambda: os.getenv("MONTH", "2024-01"))
    dq_log: Path = field(default_factory=lambda: Path(os.getenv("DQ_LOG", "data/dq_log.jsonl")))

    def table(self, layer: str) -> str:
        return str(self.lake_dir / layer)
