"""Uploads data/raw/* into MinIO under a date-partitioned raw-zone layout:

  s3://nyc-taxi/raw/trip_type=yellow/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet
  s3://nyc-taxi/reference/taxi_zone_lookup.csv

Mirrors how you'd land files in a real S3 data lake before Glue/EMR picks
them up for cleaning.
"""
import os
import re
from pathlib import Path

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

RAW_DIR = ROOT / "data" / "raw"
BUCKET = os.environ["MINIO_BUCKET"]

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
)

TRIP_FILE_RE = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet")


def upload_trip_files() -> None:
    for path in sorted(RAW_DIR.glob("yellow_tripdata_*.parquet")):
        m = TRIP_FILE_RE.match(path.name)
        if not m:
            continue
        year, month = m.groups()
        key = f"raw/trip_type=yellow/year={year}/month={month}/{path.name}"
        print(f"uploading {path.name} -> s3://{BUCKET}/{key}")
        s3.upload_file(str(path), BUCKET, key)


def upload_reference_files() -> None:
    zone_file = RAW_DIR / "taxi_zone_lookup.csv"
    if zone_file.exists():
        key = "reference/taxi_zone_lookup.csv"
        print(f"uploading {zone_file.name} -> s3://{BUCKET}/{key}")
        s3.upload_file(str(zone_file), BUCKET, key)


def list_bucket() -> None:
    print(f"\ncontents of s3://{BUCKET}:")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            print(f"  {obj['Key']}  ({obj['Size'] / 1e6:.1f} MB)")


if __name__ == "__main__":
    upload_trip_files()
    upload_reference_files()
    list_bucket()
