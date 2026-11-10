"""End-to-end NYC taxi pipeline DAG.

Each task shells out to the project's main venv (not the Airflow venv --
PySpark/DuckDB stay isolated from Airflow's own dependency set) and reruns
one of the standalone scripts. Every step is idempotent (overwrite-mode
writes, truncate-and-reload loads, upserts), so a re-run after a failure
never double-counts data.

Assumes MinIO, Postgres and MongoDB are already running -- this DAG
orchestrates the data pipeline, not the infrastructure underneath it,
mirroring how a real Airflow DAG orchestrates jobs against S3/Redshift/
DynamoDB without provisioning them itself.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/Users/sahanaagadi/de"
VENV_ACTIVATE = f"source {PROJECT_ROOT}/.venv/bin/activate"
SPARK_ENV = f"source {PROJECT_ROOT}/.envrc && export SPARK_LOCAL_IP=127.0.0.1"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="Raw zone -> lake -> warehouse -> metrics store, end to end",
    schedule=None,  # triggered manually / via `airflow dags test`
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["nyc-taxi"],
) as dag:

    download_data = BashOperator(
        task_id="download_data",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_ACTIVATE} && python scripts/download_data.py",
    )

    load_to_minio = BashOperator(
        task_id="load_to_minio",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_ACTIVATE} && python scripts/load_to_minio.py",
    )

    clean_trips = BashOperator(
        task_id="clean_trips",
        bash_command=(
            f"cd {PROJECT_ROOT} && {VENV_ACTIVATE} && {SPARK_ENV} && "
            f"python spark_jobs/clean_trips.py"
        ),
    )

    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_ACTIVATE} && python warehouse/load_warehouse.py",
    )

    data_quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_ACTIVATE} && python -m pytest dq/test_data_quality.py -q",
    )

    load_metrics_store = BashOperator(
        task_id="load_metrics_store",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_ACTIVATE} && python metrics_store/load_latest_metrics.py",
    )

    (
        download_data
        >> load_to_minio
        >> clean_trips
        >> load_warehouse
        >> data_quality_checks
        >> load_metrics_store
    )
