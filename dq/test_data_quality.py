"""Data-quality checks against the Postgres star schema warehouse.

Run after warehouse/load_warehouse.py:
  source .venv/bin/activate
  pytest dq/test_data_quality.py -v
"""
import os
from pathlib import Path

import psycopg2
import pytest
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


@pytest.fixture(scope="module")
def conn():
    connection = psycopg2.connect(
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host="localhost",
        port=os.environ["POSTGRES_PORT"],
    )
    yield connection
    connection.close()


def scalar(conn, query: str):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0]


def test_fact_trips_is_not_empty(conn):
    assert scalar(conn, "SELECT count(*) FROM fact_trips") > 0


def test_no_nulls_in_required_fact_columns(conn):
    required = [
        "pickup_date_key",
        "pickup_zone_key",
        "dropoff_zone_key",
        "pickup_datetime",
        "dropoff_datetime",
    ]
    for col in required:
        n = scalar(conn, f"SELECT count(*) FROM fact_trips WHERE {col} IS NULL")
        assert n == 0, f"{col} has {n} null values"


def test_dropoff_after_pickup(conn):
    n = scalar(
        conn,
        "SELECT count(*) FROM fact_trips WHERE dropoff_datetime <= pickup_datetime",
    )
    assert n == 0, f"{n} trips have dropoff <= pickup"


def test_fare_and_total_amount_non_negative(conn):
    n = scalar(
        conn,
        "SELECT count(*) FROM fact_trips WHERE fare_amount < 0 OR total_amount < 0",
    )
    assert n == 0, f"{n} trips have negative fare_amount or total_amount"


def test_trip_distance_within_plausible_range(conn):
    n = scalar(
        conn,
        "SELECT count(*) FROM fact_trips WHERE trip_distance < 0 OR trip_distance > 200",
    )
    assert n == 0, f"{n} trips have an implausible trip_distance"


def test_fact_trips_references_valid_zones(conn):
    n = scalar(
        conn,
        """
        SELECT count(*) FROM fact_trips f
        LEFT JOIN dim_zone pz ON f.pickup_zone_key = pz.zone_key
        WHERE pz.zone_key IS NULL
        """,
    )
    assert n == 0, f"{n} fact rows reference a pickup_zone_key missing from dim_zone"

    n = scalar(
        conn,
        """
        SELECT count(*) FROM fact_trips f
        LEFT JOIN dim_zone dz ON f.dropoff_zone_key = dz.zone_key
        WHERE dz.zone_key IS NULL
        """,
    )
    assert n == 0, f"{n} fact rows reference a dropoff_zone_key missing from dim_zone"


def test_fact_trips_references_valid_dates(conn):
    n = scalar(
        conn,
        """
        SELECT count(*) FROM fact_trips f
        LEFT JOIN dim_date d ON f.pickup_date_key = d.date_key
        WHERE d.date_key IS NULL
        """,
    )
    assert n == 0, f"{n} fact rows reference a pickup_date_key missing from dim_date"


def test_dim_zone_matches_source_lookup(conn):
    n = scalar(conn, "SELECT count(*) FROM dim_zone")
    assert n == 265, f"expected 265 TLC zones, found {n}"


def test_payment_type_keys_are_known(conn):
    n = scalar(
        conn,
        """
        SELECT count(*) FROM fact_trips
        WHERE payment_type_key IS NOT NULL
          AND payment_type_key NOT IN (SELECT payment_type_key FROM dim_payment_type)
        """,
    )
    assert n == 0, f"{n} fact rows have an unrecognized payment_type_key"
