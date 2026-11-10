"""Loads the cleaned lake zone into the Postgres star schema warehouse.

Uses DuckDB (not Spark) for this step: the plan's RAM guidance says to lean
on DuckDB over Spark on an 8GB machine, and DuckDB's postgres extension can
write directly into Postgres tables without round-tripping through pandas.

Source: s3://nyc-taxi/lake/trips_clean/  (MinIO, via DuckDB's httpfs)
        s3://nyc-taxi/reference/taxi_zone_lookup.csv
Target: dim_date, dim_zone, fact_trips in the `taxi_warehouse` Postgres db

Idempotent: truncates and reloads every table each run, so re-running after
a pipeline failure or a new month of data never double-counts.

Run:
  source .venv/bin/activate
  python warehouse/load_warehouse.py
"""
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

BUCKET = os.environ["MINIO_BUCKET"]
PG_CONN = (
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']} "
    f"host=localhost port={os.environ['POSTGRES_PORT']}"
)


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"""
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='{os.environ["MINIO_ROOT_USER"]}';
        SET s3_secret_access_key='{os.environ["MINIO_ROOT_PASSWORD"]}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    con.execute(f"ATTACH '{PG_CONN}' AS pg (TYPE POSTGRES)")
    return con


def load_dim_zone(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE stg_zone AS
        SELECT
            LocationID AS zone_key,
            Borough AS borough,
            Zone AS zone,
            service_zone
        FROM read_csv('s3://{BUCKET}/reference/taxi_zone_lookup.csv', header=true)
    """)
    con.execute("DELETE FROM pg.dim_zone")
    con.execute("INSERT INTO pg.dim_zone SELECT * FROM stg_zone")
    n = con.execute("SELECT count(*) FROM pg.dim_zone").fetchone()[0]
    print(f"dim_zone: {n} rows")


def load_dim_date(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE stg_date AS
        SELECT DISTINCT
            CAST(strftime(pickup_date, '%Y%m%d') AS INTEGER) AS date_key,
            pickup_date AS full_date,
            year(pickup_date) AS year,
            month(pickup_date) AS month,
            day(pickup_date) AS day,
            dayofweek(pickup_date) AS day_of_week,
            dayname(pickup_date) AS day_name,
            dayofweek(pickup_date) IN (0, 6) AS is_weekend
        FROM read_parquet(
            's3://{BUCKET}/lake/trips_clean/*/*/*.parquet',
            hive_partitioning=true
        )
    """)
    con.execute("DELETE FROM pg.dim_date")
    con.execute("INSERT INTO pg.dim_date SELECT * FROM stg_date")
    n = con.execute("SELECT count(*) FROM pg.dim_date").fetchone()[0]
    print(f"dim_date: {n} rows")


def load_fact_trips(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE stg_fact AS
        SELECT
            CAST(strftime(pickup_date, '%Y%m%d') AS INTEGER) AS pickup_date_key,
            PULocationID AS pickup_zone_key,
            DOLocationID AS dropoff_zone_key,
            CASE WHEN payment_type BETWEEN 1 AND 6 THEN payment_type ELSE NULL END AS payment_type_key,
            VendorID AS vendor_id,
            tpep_pickup_datetime AS pickup_datetime,
            tpep_dropoff_datetime AS dropoff_datetime,
            passenger_count,
            trip_distance,
            trip_duration_min,
            fare_amount,
            tip_amount,
            tolls_amount,
            total_amount
        FROM read_parquet(
            's3://{BUCKET}/lake/trips_clean/*/*/*.parquet',
            hive_partitioning=true
        )
    """)
    con.execute("DELETE FROM pg.fact_trips")
    con.execute("""
        INSERT INTO pg.fact_trips (
            pickup_date_key, pickup_zone_key, dropoff_zone_key, payment_type_key,
            vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
            trip_distance, trip_duration_min, fare_amount, tip_amount,
            tolls_amount, total_amount
        )
        SELECT * FROM stg_fact
    """)
    n = con.execute("SELECT count(*) FROM pg.fact_trips").fetchone()[0]
    print(f"fact_trips: {n} rows")


def main() -> None:
    con = connect()
    load_dim_zone(con)
    load_dim_date(con)
    load_fact_trips(con)
    con.close()
    print("warehouse load complete.")


if __name__ == "__main__":
    main()
