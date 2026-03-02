import os

import psycopg

def connect() -> psycopg.Connection:
    # DB-Verbindung aufbauen, anhand der .ENV
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL environment variable not set.")
    return psycopg.connect(dsn)


def ensure_schema() -> None:
    # Datenschema anlegen. 
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stations (
                  id TEXT PRIMARY KEY,
                  lat DOUBLE PRECISION NOT NULL,
                  lon DOUBLE PRECISION NOT NULL,
                  elev_m DOUBLE PRECISION NOT NULL,
                  state TEXT,
                  name TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS station_coverage (
                  id TEXT NOT NULL references stations(id) PRIMARY KEY,
                  tmin_first_year INTEGER,
                  tmin_last_year INTEGER,
                  tmax_first_year INTEGER,
                  tmax_last_year INTEGER
                );

                CREATE TABLE IF NOT EXISTS station_metric_cache (
                  station_id TEXT NOT NULL references stations(id),
                  metric TEXT NOT NULL,
                  year INTEGER NOT NULL,
                  value_c DOUBLE PRECISION,
                  present_months INTEGER NOT NULL,
                  expected_months INTEGER NOT NULL,
                  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  PRIMARY KEY (station_id, metric, year)
                );

                CREATE INDEX IF NOT EXISTS idx_stations_lat_lon
                  ON stations (lat, lon);

                DO $$
                BEGIN
                  IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'station_metric_cache'
                      AND column_name = 'present_days'
                  ) AND NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'station_metric_cache'
                      AND column_name = 'present_months'
                  ) THEN
                    ALTER TABLE station_metric_cache
                    RENAME COLUMN present_days TO present_months;
                  END IF;

                  IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'station_metric_cache'
                      AND column_name = 'expected_days'
                  ) AND NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'station_metric_cache'
                      AND column_name = 'expected_months'
                  ) THEN
                    ALTER TABLE station_metric_cache
                    RENAME COLUMN expected_days TO expected_months;
                  END IF;
                END$$;
                """
            )
        conn.commit()
