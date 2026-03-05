#!/usr/bin/env bash
set -euo pipefail

DB_HOST="${POSTGRES_HOST:-db}"
DB_PORT="${POSTGRES_PORT:-5432}"
DB_USER="${POSTGRES_USER:-user}"
DB_PASSWORD="${POSTGRES_PASSWORD:-example}"
DB_NAME="${POSTGRES_DB:-weatherstations}"

export DATABASE_URL="${DATABASE_URL:-postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}}"
export DATA_DIR="${DATA_DIR:-data}"

python - << 'PY'
import os
import time
import psycopg

dsn = os.environ["DATABASE_URL"]
for attempt in range(1, 61):
    try:
        with psycopg.connect(dsn):
            print("Database is ready")
            break
    except Exception as exc:  # noqa: BLE001
        if attempt == 60:
            raise RuntimeError(f"Database not reachable after {attempt} attempts: {exc}") from exc
        print(f"Waiting for database ({attempt}/60): {exc}")
        time.sleep(2)
PY

if [ ! -f "data/stations.txt" ] || [ ! -f "data/inventory.txt" ]; then
  echo "Metadata missing -> downloading station and inventory files"
  python scripts/download_metadata.py
else
  echo "Metadata already present -> skip download"
fi

python - << 'PY'
import os
import psycopg
from app.db import ensure_schema

dsn = os.environ["DATABASE_URL"]
ensure_schema()
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM stations")
        stations_count = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT count(*) FROM station_coverage")
        coverage_count = int(cur.fetchone()[0] or 0)

if stations_count > 0 and coverage_count > 0:
    print(
        f"Metadata already imported -> skip import "
        f"(stations={stations_count}, coverage={coverage_count})"
    )
else:
    print(
        f"Metadata missing/incomplete -> import required "
        f"(stations={stations_count}, coverage={coverage_count})"
    )
    os.execvp("python", ["python", "scripts/import_metadata.py"])
PY

echo "Starting API server"
if [ "${UVICORN_RELOAD:-0}" = "1" ] || [ "${UVICORN_RELOAD:-false}" = "true" ]; then
  echo "Uvicorn reload mode enabled"
  exec uvicorn app.main:app \
    --host 0.0.0.0 \
    --port "${APP_PORT:-8000}" \
    --reload \
    --reload-dir /app/app \
    --reload-dir /app/scripts
fi

exec uvicorn app.main:app --host 0.0.0.0 --port "${APP_PORT:-8000}"
