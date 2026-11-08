"""ingest -> silver -> gold -> export -> dbt build.

Every task is idempotent (replaceWhere on bronze, MERGE on silver/gold, overwrite on export,
dbt rebuilds its tables), so a retry or a manual re-run of a month never duplicates data.
Requires Airflow 2.4+ and the project installed in the worker environment (`make setup`).
"""

from datetime import timedelta

import pendulum
from airflow.operators.bash import BashOperator

from airflow import DAG

PROJECT = "{{ var.value.get('lakehouse_home', '/opt/lakehouse') }}"
ENV = "MONTH={{ params.month }} SOURCE=data/raw/yellow_tripdata_{{ params.month }}.parquet "

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}


def step(task_id: str, command: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT} && {ENV}{command}",
    )


with DAG(
    dag_id="lakehouse_medallion",
    description="NYC TLC yellow taxi: bronze -> silver -> gold -> dbt star schema",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"month": "2024-01"},
    tags=["lakehouse", "dbt"],
) as dag:
    ingest = step("ingest", "python scripts/download.py $MONTH && python -m lakehouse.cli bronze")
    silver = step("silver", "python -m lakehouse.cli silver")
    gold = step("gold", "python -m lakehouse.cli gold")
    export = step("export", "python -m lakehouse.cli export")
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT}/dbt && DBT_PROFILES_DIR=. dbt build",
    )

    ingest >> silver >> gold >> export >> dbt_build
