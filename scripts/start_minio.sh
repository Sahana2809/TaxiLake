#!/usr/bin/env bash
# Starts MinIO natively (not in Docker — see docker-compose.yml note) and
# ensures the lake bucket exists. Run this before any Spark/boto3 script
# that talks to s3://nyc-taxi.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

DATA_DIR="$(pwd)/data/minio"
mkdir -p "$DATA_DIR"

if lsof -i :9000 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "MinIO already running on :9000"
else
  echo "Starting MinIO on :9000 (console :9001), data dir: $DATA_DIR"
  MINIO_ROOT_USER="$MINIO_ROOT_USER" MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
    minio server --address ":9000" --console-address ":9001" "$DATA_DIR" \
    > "$(pwd)/data/minio.log" 2>&1 &
  disown
  sleep 3
fi

mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
mc mb -p "local/${MINIO_BUCKET}" 2>/dev/null || echo "Bucket ${MINIO_BUCKET} already exists"
echo "MinIO ready. Console: http://localhost:9001 (user: $MINIO_ROOT_USER)"
