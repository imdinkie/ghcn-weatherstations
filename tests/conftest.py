from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.db import connect, ensure_schema
from app.main import app


def _ensure_database_url_from_env_file() -> None:
    # Für lokale Läufe (ohne exportierte ENV) lesen wir DATABASE_URL direkt aus .env.
    # In CI ist DATABASE_URL bereits gesetzt; dann greift diese Funktion nicht ein.
    if os.getenv("DATABASE_URL"):
        return
    env_path = Path(".env")
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("DATABASE_URL="):
            os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip()
            return


@pytest.fixture(scope="session")
def ensure_test_database() -> Iterator[None]:
    # Session-weit einmalig:
    # 1) DATABASE_URL sicherstellen
    # 2) Schema anlegen, falls noch nicht vorhanden
    # Damit starten alle Tests auf einer gültigen DB-Struktur.
    _ensure_database_url_from_env_file()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for tests")
    ensure_schema()
    yield


@pytest.fixture()
def clean_tables(ensure_test_database: None) -> Iterator[None]:
    # Vor jedem Test: harte Isolation durch TRUNCATE.
    # So beeinflussen sich Testfälle nicht gegenseitig über Restdaten.
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                TRUNCATE TABLE
                  station_metric_cache,
                  station_coverage,
                  stations
                RESTART IDENTITY CASCADE
                """
            )
        conn.commit()
    yield


@pytest.fixture()
def client(clean_tables: None) -> Iterator[TestClient]:
    # FastAPI TestClient für HTTP-Integrationstests.
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def seed_station(clean_tables: None) -> Callable[..., None]:
    # Hilfsfixture, um Stationen + optionale Coverage kompakt zu seeden.
    # Dadurch bleibt Testcode fokussiert auf Assertions statt Setup-Noise.
    def _seed(
        *,
        station_id: str,
        name: str,
        lat: float,
        lon: float,
        elev_m: float = 100.0,
        state: str | None = None,
        tmin_first_year: int | None = None,
        tmin_last_year: int | None = None,
        tmax_first_year: int | None = None,
        tmax_last_year: int | None = None,
    ) -> None:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stations (id, name, lat, lon, elev_m, state)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (station_id, name, lat, lon, elev_m, state),
                )
                if any(v is not None for v in (tmin_first_year, tmin_last_year, tmax_first_year, tmax_last_year)):
                    cur.execute(
                        """
                        INSERT INTO station_coverage (
                          id, tmin_first_year, tmin_last_year, tmax_first_year, tmax_last_year
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (station_id, tmin_first_year, tmin_last_year, tmax_first_year, tmax_last_year),
                    )
            conn.commit()

    return _seed
