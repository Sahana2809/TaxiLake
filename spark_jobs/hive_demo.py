"""Runs real HiveQL against the lake using Spark's embedded Hive support.

Spark's `enableHiveSupport()` pulls in the actual Hive metastore client
+ HiveQL parser (no separate Hive server needed, so it stays cheap on an
8GB laptop). The metastore is a local Derby DB under data/metastore_db/
-- genuinely Hive's metastore, just embedded.

Run:
  source .venv/bin/activate && source .envrc
  python spark_jobs/hive_demo.py
"""
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

BUCKET = os.environ["MINIO_BUCKET"]
LAKE_PATH = f"s3a://{BUCKET}/lake/trips_clean/"
WAREHOUSE_DIR = str(ROOT / "data" / "hive_warehouse")
DERBY_HOME = str(ROOT / "data" / "metastore_db")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("hive_demo")
        .master("local[*]")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", f"file://{WAREHOUSE_DIR}")
        .config("spark.hadoop.hive.metastore.warehouse.dir", f"file://{WAREHOUSE_DIR}")
        # Hadoop's default NameNode RPC port is 9000 -- the same port MinIO's
        # S3 API uses. Without this, the metastore falls back to
        # hdfs://localhost:9000 and gets garbled responses from MinIO.
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={DERBY_HOME}")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def main() -> None:
    Path(WAREHOUSE_DIR).mkdir(parents=True, exist_ok=True)
    Path(DERBY_HOME).mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS taxi")
    spark.sql("USE taxi")

    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS trips_clean (
            VendorID INT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count BIGINT,
            trip_distance DOUBLE,
            PULocationID INT,
            DOLocationID INT,
            payment_type BIGINT,
            fare_amount DOUBLE,
            total_amount DOUBLE,
            trip_duration_min DOUBLE,
            pickup_date DATE
        )
        PARTITIONED BY (year INT, month INT)
        STORED AS PARQUET
        LOCATION '{LAKE_PATH}'
        """
    )
    spark.sql("MSCK REPAIR TABLE trips_clean")

    print("\n-- partitions registered in the Hive metastore --")
    spark.sql("SHOW PARTITIONS trips_clean").show()

    print("\n-- HiveQL: CTE + window function -- top 5 pickup zones by "
          "revenue per month, ranked within month --")
    spark.sql(
        """
        WITH monthly_zone_revenue AS (
            SELECT
                year,
                month,
                PULocationID,
                ROUND(SUM(total_amount), 2) AS revenue,
                COUNT(*) AS trips
            FROM trips_clean
            GROUP BY year, month, PULocationID
        ),
        ranked AS (
            SELECT
                *,
                RANK() OVER (PARTITION BY year, month ORDER BY revenue DESC) AS rev_rank
            FROM monthly_zone_revenue
        )
        SELECT year, month, PULocationID, revenue, trips, rev_rank
        FROM ranked
        WHERE rev_rank <= 5
        ORDER BY year, month, rev_rank
        """
    ).show(20)

    spark.stop()


if __name__ == "__main__":
    main()
