PY ?= python
export LAKE_DIR ?= data/lake
export EXPORT_DIR ?= data/export
export SOURCE ?= data/raw/yellow_tripdata_2024-01.parquet
export MONTH ?= 2024-01
export DBT_PROFILES_DIR ?= dbt

.PHONY: setup ingest run dbt sample-run test lint

setup:
	$(PY) -m pip install -e ".[dev]"

ingest:  ## download one month of NYC TLC yellow taxi trips (not committed)
	$(PY) scripts/download.py $(MONTH)

run:  ## bronze -> silver -> gold -> export, then dbt build, on the downloaded month
	$(PY) -m lakehouse.cli all
	cd dbt && dbt build

sample-run:  ## same pipeline on the committed sample (no download needed)
	$(MAKE) run SOURCE=data/sample/yellow_sample.parquet

test:
	$(PY) -m pytest

lint:
	ruff check . && ruff format --check .
