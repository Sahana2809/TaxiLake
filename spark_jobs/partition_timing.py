"""Measures the effect of partitioning on a representative filtered query.

Writes a second copy of the cleaned lake data with no partitioning (one
flat directory), then runs the same filter+aggregate query against both
the partitioned and flat copies and prints the timings.

Run:
  source .venv/bin/activate && source .envrc
  python spark_jobs/partition_timing.py
"""
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

BUCKET = os.environ["MINIO_BUCKET"]
PARTITIONED_PATH = f"s3a://{BUCKET}/lake/trips_clean/"
FLAT_PATH = f"s3a://{BUCKET}/lake/trips_clean_flat/"

QUERY_YEAR = 2024
QUERY_MONTH = 2


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("partition_timing")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
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


def ensure_flat_copy(spark: SparkSession) -> None:
    """Writes the same cleaned data as one flat (non Hive-partitioned)
    directory, so the comparison isolates the effect of partitioning
    rather than comparing different datasets."""
    try:
        spark.read.parquet(FLAT_PATH).limit(1).count()
        print("flat copy already exists, skipping write")
        return
    except Exception:
        pass

    print("writing flat (unpartitioned) copy for comparison...")
    df = spark.read.parquet(PARTITIONED_PATH)
    df.coalesce(1).write.mode("overwrite").parquet(FLAT_PATH)


def time_query(spark: SparkSession, path: str, label: str) -> float:
    def run() -> float:
        start = time.perf_counter()
        result = (
            spark.read.parquet(path)
            .filter((F.col("year") == QUERY_YEAR) & (F.col("month") == QUERY_MONTH))
            .agg(
                F.count("*").alias("trips"),
                F.round(F.sum("total_amount"), 2).alias("revenue"),
            )
            .collect()
        )
        elapsed = time.perf_counter() - start
        return elapsed, result[0]

    # warm-up run (JIT / S3 connection warmup), then the measured run
    run()
    elapsed, row = run()
    print(f"{label}: {elapsed:.2f}s  (trips={row['trips']}, revenue={row['revenue']})")
    return elapsed


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    ensure_flat_copy(spark)

    print(f"\nquery: WHERE year={QUERY_YEAR} AND month={QUERY_MONTH}, count + sum(total_amount)\n")
    flat_time = time_query(spark, FLAT_PATH, "flat (unpartitioned)")
    part_time = time_query(spark, PARTITIONED_PATH, "partitioned by year/month")

    speedup = flat_time / part_time if part_time else float("inf")
    print(f"\nspeedup: {speedup:.1f}x")

    print("\n-- physical plan on the partitioned path (look for PartitionFilters) --")
    (
        spark.read.parquet(PARTITIONED_PATH)
        .filter((F.col("year") == QUERY_YEAR) & (F.col("month") == QUERY_MONTH))
        .explain()
    )

    spark.stop()


if __name__ == "__main__":
    main()
