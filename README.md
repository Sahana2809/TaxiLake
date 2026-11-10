# NYC Taxi Data Lake & Warehouse (local, zero-cost)

A local data-engineering pipeline that maps 1:1 onto a typical AWS data
stack (S3, Glue/EMR, Athena/Hive, Redshift, DynamoDB, Lambda, QuickSight) —
built entirely on free, open-source tools, with no AWS account and no card
on file. Runs end to end on an 8GB Mac laptop.

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
                                                      ▼                 Streamlit dashboard
                                               Spark SQL + Hive
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
| DynamoDB / NoSQL | **MongoDB** in Docker — "latest metrics per zone" serving store | `metrics_store/` |
| Lambda / orchestration | **Apache Airflow**, installed natively (pip), in its own venv | `orchestration/dags/` |
| QuickSight (dashboards) | **Streamlit**, native | `dashboard/` |
| Informatica / ODI / SSIS / BODI / Datastage (preferred qual, GUI ETL) | **Apache NiFi** — free, visual, drag-and-drop dataflow tool, same category as those products | `docker-compose.yml` (`--profile nifi`) |
| Data-quality checks | **pytest** + SQL assertions | `dq/` |

## Why some things run natively instead of in Docker

- **MinIO**: as of 2024/2025, MinIO's Docker Hub and Quay container images
  require a logged-in account to pull — that conflicts with this project's
  "no account, no card" goal. The native `minio`/`mc` binaries
  (`brew install minio minio-mc`) are unauthenticated and free, and lighter
  on RAM than a container.
- **Spark/PySpark and Airflow**: running these natively (not inside the
  Colima VM) means they aren't double-constrained by both the VM's memory
  cap and the container's — on an 8GB host that headroom matters. Airflow
  lives in its own venv (`.venv-airflow/`) so its pinned dependency set
  never collides with PySpark/DuckDB's.

## RAM budget (8GB host)

Colima's Linux VM is sized at **2 CPU / 4GB** — just enough for the
always-on containers:

| Always-on | ~RAM |
|---|---|
| Postgres | ~200MB-1GB under load |
| MongoDB | ~100MB |
| MinIO (native, outside the VM) | ~100MB |

Everything else is a Docker Compose **profile** you bring up only when
actively using it, and should generally run *in isolation* (stop other
profiles first) rather than all at once:

```bash
docker compose --profile nifi up -d nifi                 # ~1-2GB
docker compose --profile hadoop up -d namenode datanode   # ~1-2GB
docker compose --profile spark-cluster up -d              # ~1-2GB
```

PySpark local-mode jobs and the Airflow DAG run natively via their venvs
and use whatever RAM is free on the host — in practice, on a genuinely
8GB machine, running Postgres + a Spark job + an Airflow-launched
subprocess all at once pushes the host into real memory pressure (heavy
compression, degraded I/O). Close other apps before a full pipeline run,
or run the pipeline in stages rather than through the scheduler.

## Setup

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

# 4. Java 8 for Spark (Temurin 8 via brew/AdoptOpenJDK)
source .envrc   # sets JAVA_HOME, PYSPARK_PYTHON

# 5. Airflow, in its own venv
python3.11 -m venv .venv-airflow
source .venv-airflow/bin/activate
pip install "apache-airflow==2.10.3" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt"
export AIRFLOW_HOME="$(pwd)/orchestration/airflow_home"
airflow db migrate
```

## Running the pipeline

```bash
source .venv/bin/activate && source .envrc
export SPARK_LOCAL_IP=127.0.0.1

./scripts/start_minio.sh                 # start MinIO + create bucket
docker compose up -d postgres mongo      # start warehouse + metrics store

python scripts/download_data.py          # NYC TLC parquet -> data/raw/
python scripts/load_to_minio.py          # raw/ -> s3://nyc-taxi (date-partitioned)
python spark_jobs/clean_trips.py         # raw -> lake/trips_clean (partitioned, cleaned)
python spark_jobs/hive_demo.py           # HiveQL: CTEs + window functions over the lake
python spark_jobs/partition_timing.py    # partitioned vs. flat query timing
python warehouse/load_warehouse.py       # lake -> Postgres star schema
python -m pytest dq/test_data_quality.py -v
python metrics_store/load_latest_metrics.py   # warehouse -> Mongo "latest metrics per zone"
streamlit run dashboard/streamlit_app.py
```

Or run it all through Airflow:

```bash
source .venv-airflow/bin/activate
export AIRFLOW_HOME="$(pwd)/orchestration/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/orchestration/dags"
airflow dags test nyc_taxi_pipeline 2024-01-01
```

## Measured results (Jan+Feb 2024, 5,972,150 raw rows)

- **Cleaning**: 5,972,150 → 5,892,073 rows (1.34% dropped) — bad
  timestamps (dropoff ≤ pickup), out-of-range distance/fare, and rows
  whose actual pickup date didn't match their source file's month (a
  real quirk in the TLC files).
- **Partitioning**: filtering a single month (`year=2024 AND month=2`)
  against the Hive-partitioned lake vs. a flat, unpartitioned copy of the
  same data: **1.52s → 1.17s (1.3x)**, confirmed by `EXPLAIN` showing
  `PartitionFilters` pruning to one file. The gain is modest at this
  (2-month, 141MB) scale — the effect compounds with more partitions and
  more data per partition; see "Honest resume wording" below.
- **Warehouse load**: 5,892,073 fact rows + 265 zones + 60 dates loaded
  into Postgres in ~1 minute — after fixing a bulk-load performance bug
  (see "Problems hit and fixed").
- **Data quality**: 9/9 checks pass (null checks, dropoff-after-pickup,
  non-negative fares, plausible trip distance, zone/date referential
  integrity, known payment types).

## Problems hit and fixed

Real issues found while building this, kept here because they're good
interview material:

1. **MinIO's container images now require a Docker Hub/Quay login.**
   Switched MinIO to a native Homebrew binary instead of a container —
   see "Why some things run natively" above.
2. **Mongo port collision.** This machine already runs a native MongoDB
   on `127.0.0.1:27017` (unrelated to this project). The Compose Mongo
   container was silently being shadowed by it for host-side connections.
   Fixed by publishing the container on `27018` instead (`.env`).
3. **Hadoop's default NameNode RPC port is 9000 — the same port MinIO's
   S3 API uses.** Spark's embedded Hive metastore defaulted its warehouse
   lookup to `hdfs://localhost:9000` and got garbled responses from
   MinIO (`RPC response exceeds maximum data length`). Fixed by pinning
   `spark.hadoop.fs.defaultFS` to `file:///` in `spark_jobs/hive_demo.py`.
4. **FK-safe delete order.** The warehouse loader originally deleted
   `dim_zone` before `fact_trips`, which is fine on an empty database but
   violates the foreign key on every subsequent run once `fact_trips`
   has data. Fixed by truncating the child table first.
5. **Per-row FK-check triggers made a bulk reload of ~5.9M rows take
   15+ minutes** on this machine's 2-vCPU Colima VM. Disabling
   `fact_trips`'s triggers for the duration of the delete+reload (a
   standard bulk-load pattern) brought it down to about a minute.
6. **Postgres `ROUND(double precision, int)` doesn't exist** — only
   `ROUND(numeric, int)`. Needed an explicit `::numeric` cast when
   averaging `double precision` columns for the Mongo metrics loader.
7. **8GB is a real, binding constraint.** Running Postgres, a Spark job,
   and an Airflow-launched subprocess concurrently pushed this host into
   genuine memory pressure (heavy compression, "stuck" processes in
   `top`, degraded I/O) during this build. The Airflow DAG was validated
   by (a) `airflow dags list-import-errors` reporting a clean import and
   (b) every script it calls having already been run and verified
   successfully on its own — but a full unattended scheduler run
   wasn't pushed through while the host was thrashing. If you hit the
   same thing: close other apps, or run the pipeline in stages rather
   than through Airflow.

## Day-by-day, as built

**Week 1** — Colima + Python env, MinIO/Postgres/Mongo up, NYC TLC data
downloaded and landed in the raw zone (date-partitioned), PySpark
cleaning job, Spark Hive-support demo with CTEs + window functions.

**Week 2** — Star schema (`fact_trips`, `dim_date`, `dim_zone`,
`dim_payment_type`), a DuckDB-based warehouse loader, four analytical SQL
queries (ranked top-N, moving average, percent-of-total, top-N-per-group),
and a partitioned-vs-flat query timing comparison.

**Week 3** — Data-quality pytest suite, MongoDB "latest metrics per zone"
loader, an Airflow DAG chaining the whole pipeline idempotently, and a
Streamlit dashboard over the warehouse.

**Optional, not run by default** (documented, available as Compose
profiles): Apache NiFi for GUI-ETL practice, single-node HDFS for
hands-on Hadoop practice, a Spark standalone cluster for cluster-mode
practice.


