"""Build the committed test fixture: a seeded random sample of the raw file.

A few rows are duplicated on purpose so the dedupe path is exercised in tests and CI.
"""

import pandas as pd

raw = pd.read_parquet("data/raw/yellow_tripdata_2024-01.parquet")
sample = raw.sample(3000, random_state=42)
sample = pd.concat([sample, sample.head(30)], ignore_index=True)
sample.to_parquet("data/sample/yellow_sample.parquet", index=False, coerce_timestamps="us")
print(len(sample), "rows written")
