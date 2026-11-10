# NYC Taxi Data Lake & Warehouse (local, zero-cost)

A local data-engineering pipeline that maps 1:1 onto a typical AWS data
stack (S3, Glue/EMR, Athena/Hive, Redshift, DynamoDB, Lambda, QuickSight) —
built entirely on free, open-source tools, with no AWS account and no card
on file. Runs on an 8GB Mac laptop.

## Architecture

```
NYC TLC (public)        Raw zone              Curated zone            Warehouse / stores
┌───────────────┐      ┌─────────────┐       ┌──────────────┐       ┌───────────────────┐
│ yellow_trip-   │      │  MinIO      │       │  MinIO       │       │ Postgres (star     │
│ data_*.parquet │─────▶│  s3://.../  │──────▶│  s3://.../   │──────▶│ schema warehouse)  │
│ taxi_zone_     │ load │  raw/       │ Spark │  lake/       │ load  │                    │
│ lookup.csv     │      │  (date-     │ clean │  trips_clean/│       │ MongoDB (latest    │
└───────────────┘      │  partitioned)│       │  (partitioned│       │ per-zone metrics)  │
                        └─────────────┘       │  by year/mo) │       └───────────────────┘
                                               └──────────────┘                │
                                                      │                        ▼
                                                      ▼                 Streamlit / Metabase
                                               Spark SQL + Hive          dashboard
                                               metastore (HiveQL,
                                               window fns, CTEs)

Orchestrated by: Airflow DAG (idempotent, runs the above end-to-end)
Checked by:      pytest data-quality assertions
```

## Tool mapping

| AWS service (the JD) | Free local equivalent | Where |
|---|---|---|
| S3 (data lake) | **MinIO** — native Homebrew binary, S3 API-compatible | `scripts/start_minio.sh` |
| Glue / EMR (Spark ETL) | **PySpark**, local mode, running natively on the host | `spark_jobs/clean_trips.py` |
| Athena / Hive / HadoopSQL | **Spark SQL with `enableHiveSupport()`** — a real embedded Hive metastore (Derby) + genuine HiveQL parser, no separate server | `spark_jobs/hive_demo.py` |
| Hadoop (preferred qual) | Spark's bundled Hadoop client (`hadoop-aws`, S3A connector) is literally Hadoop code talking to MinIO. For hands-on HDFS/`hdfs dfs` practice, an optional single-node HDFS profile is included | `docker-compose.yml` (`--profile hadoop`) |
| EMR cluster concepts (preferred qual) | Optional 1-master/1-worker **Spark standalone cluster** profile, for practicing cluster-mode submission (`spark-submit --master spark://...`) | `docker-compose.yml` (`--profile spark-cluster`) |
| Redshift (warehouse) | **PostgreSQL** in Docker, star schema | `warehouse/` |
| DynamoDB / NoSQL | **MongoDB** in Docker — "latest metrics per zone" store | `metrics_store/` |
| Lambda / orchestration | **Apache Airflow**, installed natively (pip), LocalExecutor | `orchestration/dags/` |
| QuickSight (dashboards) | **Streamlit**, native; optional **Metabase** profile for a QuickSight-like GUI BI tool | `dashboard/` |
| Informatica / ODI / SSIS / BODI / Datastage (preferred qual, GUI ETL) | **Apache NiFi** — free, visual, drag-and-drop dataflow tool, same category as those products | `docker-compose.yml` (`--profile nifi`) |
| Data-quality checks | **pytest** + plain SQL assertions | `dq/` |

## Why some things run natively instead of in Docker

- **MinIO**: as of 2024/2025, MinIO's Docker Hub and Quay container images
  require a logged-in account to pull — that conflicts with this project's
  "no account, no card" goal. The native `minio`/`mc` binaries
  (`brew install minio minio-mc`) are unauthenticated and free, and lighter
  on RAM than a container.
- **Spark/PySpark and Airflow**: running these natively (not inside the
  Colima VM) means they aren't double-constrained by both the VM's memory
  cap and the container's — on an 8GB host that headroom matters.

## RAM budget (8GB host)

Colima's Linux VM is sized at **2 CPU / 4GB** — just enough for the
always-on containers:

| Always-on | ~RAM |
|---|---|
| Postgres | ~200MB |
| MongoDB | ~300MB |
| MinIO (native, outside the VM) | ~100MB |

Everything else is a Docker Compose **profile** you bring up only when
actively using it, and should generally run *in isolation* (stop other
profiles first) rather than all at once:

```bash
docker compose --profile dashboard up -d metabase      # ~700MB-1GB
docker compose --profile nifi up -d nifi                # ~1-2GB
docker compose --profile hadoop up -d namenode datanode  # ~1-2GB
docker compose --profile spark-cluster up -d             # ~1-2GB
```

PySpark local-mode jobs (the main ETL path) run natively via the project
venv and use whatever RAM is free on the host at the time — stop
non-essential profiles before a big Spark run.

## Setup (already done in this repo)

```bash
# 1. Container runtime
brew install colima docker docker-compose
colima start --cpu 2 --memory 4 --disk 40

# 2. MinIO (native, not Docker — see note above)
brew install minio minio-mc

# 3. Python 3.11 (PySpark doesn't yet support 3.13+)
brew install python@3.11
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 4. Java 8 for Spark (already present: Temurin 8)
source .envrc   # sets JAVA_HOME, PYSPARK_PYTHON
```

## Running the pipeline so far (Week 1)

```bash
source .venv/bin/activate && source .envrc
export SPARK_LOCAL_IP=127.0.0.1

./scripts/start_minio.sh                 # start MinIO + create bucket
docker compose up -d postgres mongo      # start warehouse + metrics store

python scripts/download_data.py          # NYC TLC parquet -> data/raw/
python scripts/load_to_minio.py          # raw/ -> s3://nyc-taxi (date-partitioned)
python spark_jobs/clean_trips.py         # raw -> lake/trips_clean (partitioned, cleaned)
python spark_jobs/hive_demo.py           # HiveQL: CTEs + window functions over the lake
```

**Status right now:** ~6M trip rows (Jan+Feb 2024) loaded into MinIO,
cleaned by Spark (1.34% dropped by DQ filters — bad timestamps, out-of-range
distances/fares, cross-month stragglers), partitioned by `year`/`month`,
and queryable via HiveQL with a working Hive metastore.

## Day-by-day (Week 1, for reference)

1. Docker/Colima + Python environment, verify PySpark + Java work together
2. MinIO up, bucket created, Postgres + Mongo up
3. Download NYC TLC data, land it in the raw zone (date-partitioned)
4. PySpark cleaning job: nulls, duplicates, invalid trips, cross-month
   stragglers, derived columns (`trip_duration_min`, `pickup_date`)
5. Spark Hive-support demo: register the lake as an external Hive table,
   run HiveQL with CTEs + window functions

## Next (Week 2 — not yet built)

- Star schema in Postgres: `fact_trips`, `dim_date`, `dim_zone`,
  `dim_payment_type` (`warehouse/schema.sql`)
- Loader: lake Parquet → warehouse tables (`warehouse/load_warehouse.py`)
- Analytical SQL: window functions, CTEs (`warehouse/queries/`)
- Timing comparison: a representative query before vs. after partitioning

## Next (Week 3 — not yet built)

- Data-quality tests (`dq/test_data_quality.py`)
- Airflow DAG running the whole pipeline idempotently
  (`orchestration/dags/taxi_pipeline_dag.py`)
- MongoDB "latest metrics per zone" loader (`metrics_store/`)
- Streamlit dashboard (`dashboard/streamlit_app.py`)
- Optional: NiFi flow, HDFS practice, Spark standalone cluster submission

## Honest resume wording

> Built a local data lake and warehouse pipeline (MinIO S3-compatible
> storage, PySpark, Spark SQL/Hive metastore, Postgres star schema) over
> 6M+ NYC taxi trip records; partitioned Parquet cut [query] from [X]s to
> [Y]s; added [N] data-quality checks catching [category] issues.
> Designed to map to S3, Glue/EMR, Athena/Hive and Redshift — architecture
> ports directly, numbers are measured locally, not run on AWS.

Fill in brackets only with measurements you've actually taken (Week 2
includes a before/after partitioning timing you can cite).

Don't claim AWS, Glue, EMR, Redshift, Informatica, SSIS, or Hadoop/HDFS
*usage* on the resume — name the real local tools, and say the
architecture maps to those AWS services. It's fine to say you *read
documentation and understand how it maps* for the preferred-but-unused
items (AWS services, Hadoop/HDFS cluster ops).
