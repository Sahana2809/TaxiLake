"""Computes "latest metrics per zone" from the warehouse and upserts them
into MongoDB -- the free stand-in for a DynamoDB-style serving layer that
an app would read for low-latency lookups, instead of hitting the
warehouse directly.

Run after warehouse/load_warehouse.py:
  source .venv/bin/activate
  python metrics_store/load_latest_metrics.py
"""
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

QUERY = """
WITH latest_period AS (
    SELECT max(year * 100 + month) AS ym FROM dim_date
),
zone_metrics AS (
    SELECT
        z.zone_key,
        z.zone,
        z.borough,
        d.year,
        d.month,
        COUNT(*) AS trips,
        ROUND(SUM(f.total_amount), 2) AS revenue,
        ROUND(AVG(f.fare_amount), 2) AS avg_fare,
        ROUND(AVG(f.trip_distance)::numeric, 2) AS avg_trip_distance,
        ROUND(AVG(f.trip_duration_min)::numeric, 2) AS avg_trip_duration_min
    FROM fact_trips f
    JOIN dim_zone z ON f.pickup_zone_key = z.zone_key
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    WHERE (d.year * 100 + d.month) = (SELECT ym FROM latest_period)
    GROUP BY z.zone_key, z.zone, z.borough, d.year, d.month
)
SELECT * FROM zone_metrics
"""


def fetch_latest_metrics():
    conn = psycopg2.connect(
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host="localhost",
        port=os.environ["POSTGRES_PORT"],
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(QUERY)
            return cur.fetchall()
    finally:
        conn.close()


def upsert_into_mongo(rows):
    client = MongoClient(
        "localhost",
        int(os.environ["MONGO_PORT"]),
        username=os.environ["MONGO_INITDB_ROOT_USERNAME"],
        password=os.environ["MONGO_INITDB_ROOT_PASSWORD"],
        authSource="admin",
    )
    db = client["taxi_metrics"]
    collection = db["zone_metrics"]
    collection.create_index("zone_key", unique=True)

    now = datetime.now(timezone.utc)
    ops = []
    for row in rows:
        doc = {
            k: float(v) if isinstance(v, Decimal) else v
            for k, v in row.items()
        }
        doc["last_updated"] = now
        ops.append(UpdateOne({"zone_key": doc["zone_key"]}, {"$set": doc}, upsert=True))

    if ops:
        result = collection.bulk_write(ops)
        print(
            f"upserted {result.upserted_count} new, "
            f"modified {result.modified_count} existing zone_metrics docs"
        )
    client.close()


def main() -> None:
    rows = fetch_latest_metrics()
    print(f"fetched metrics for {len(rows)} zones (latest period in the warehouse)")
    upsert_into_mongo(rows)


if __name__ == "__main__":
    main()
