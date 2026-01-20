import os

import psycopg

def connect():
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
                """
            )
        conn.commit()