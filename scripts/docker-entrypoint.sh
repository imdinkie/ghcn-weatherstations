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

echo "Importing metadata into database"
python scripts/import_metadata.py

echo "Starting API server"
exec uvicorn app.main:app --host 0.0.0.0 --port "${APP_PORT:-8000}"
