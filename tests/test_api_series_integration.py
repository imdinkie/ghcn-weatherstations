from __future__ import annotations

import calendar
from pathlib import Path

from app.db import connect
import app.main as main_module


STATION_ID = "TEST0000001"


def _build_line(year: int, month: int, element: str, daily_values_tenths: list[int | None]) -> str:
    # Erzeugt pro Monat/Element eine synthetische DLY-Zeile.
    prefix = f"{STATION_ID:<11}{year:04d}{month:02d}{element:<4}"
    days = calendar.monthrange(year, month)[1]
    slots: list[str] = []
    for day in range(1, 32):
        value = daily_values_tenths[day - 1] if day <= days else None
        raw = -9999 if value is None else value
        slots.append(f"{raw:5d}" + " " + " " + " ")
    return prefix + "".join(slots) + "\n"


def _write_fixture_dly(path: Path) -> None:
    # Vollständige Fixture über mehrere Monate für TMIN/TMAX.
    # Damit können wir API + Cache-Verhalten deterministisch testen.
    lines = [
        _build_line(2020, 3, "TMIN", [100] * 31),
        _build_line(2020, 4, "TMIN", [110] * 30),
        _build_line(2020, 5, "TMIN", [120] * 31),
        _build_line(2020, 6, "TMIN", [130] * 30),
        _build_line(2020, 7, "TMIN", [140] * 31),
        _build_line(2020, 8, "TMIN", [150] * 31),
        _build_line(2020, 9, "TMIN", [160] * 30),
        _build_line(2020, 10, "TMIN", [170] * 31),
        _build_line(2020, 11, "TMIN", [180] * 30),
        _build_line(2020, 12, "TMIN", [190] * 31),
        _build_line(2020, 3, "TMAX", [200] * 31),
        _build_line(2020, 4, "TMAX", [210] * 30),
        _build_line(2020, 5, "TMAX", [220] * 31),
        _build_line(2020, 6, "TMAX", [230] * 30),
        _build_line(2020, 7, "TMAX", [240] * 31),
        _build_line(2020, 8, "TMAX", [250] * 31),
        _build_line(2020, 9, "TMAX", [260] * 30),
        _build_line(2020, 10, "TMAX", [270] * 31),
        _build_line(2020, 11, "TMAX", [280] * 30),
        _build_line(2020, 12, "TMAX", [290] * 31),
    ]
    path.write_text("".join(lines), encoding="utf-8")


def test_series_endpoint_writes_and_reuses_cache(client, seed_station, tmp_path, monkeypatch) -> None:
    # Integrationstest für /api/stations/{id}/series:
    # 1) erster Aufruf füllt Cache
    # 2) zweiter Aufruf nutzt vorhandene Cache-Zeilen wieder
    seed_station(
        station_id=STATION_ID,
        name="SeriesStation",
        lat=48.0,
        lon=8.0,
        tmin_first_year=1980,
        tmin_last_year=2025,
        tmax_first_year=1980,
        tmax_last_year=2025,
    )

    dly_path = tmp_path / f"{STATION_ID}.dly"
    _write_fixture_dly(dly_path)
    # NOAA-Download im Test gezielt umgehen:
    # Statt echter Netzwerkabfrage liefert ensure_station_dly unsere lokale Fixture.
    monkeypatch.setattr(main_module, "ensure_station_dly", lambda station_id: dly_path)

    params = {
        "start_year": 2020,
        "end_year": 2020,
        "metrics": "tmin_year,tmax_year,tmin_winter",
    }

    r1 = client.get(f"/api/stations/{STATION_ID}/series", params=params)
    assert r1.status_code == 200
    body = r1.json()
    keys = {series["key"] for series in body["series"]}
    assert keys == {"tmin_year", "tmax_year", "tmin_winter"}

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM station_metric_cache WHERE station_id=%s", (STATION_ID,))
            count_after_first = cur.fetchone()[0]
    # Der Cache speichert intern immer alle 10 Metriken für das angefragte Jahr.
    assert count_after_first == 10

    r2 = client.get(f"/api/stations/{STATION_ID}/series", params=params)
    assert r2.status_code == 200

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM station_metric_cache WHERE station_id=%s", (STATION_ID,))
            count_after_second = cur.fetchone()[0]
    assert count_after_second == 10


def test_series_rejects_invalid_metric(client, seed_station, tmp_path, monkeypatch) -> None:
    # API-Validierung: unbekannte Metric führt zu 400.
    seed_station(station_id=STATION_ID, name="SeriesStation", lat=48.0, lon=8.0)
    dly_path = tmp_path / f"{STATION_ID}.dly"
    _write_fixture_dly(dly_path)
    monkeypatch.setattr(main_module, "ensure_station_dly", lambda station_id: dly_path)

    r = client.get(
        f"/api/stations/{STATION_ID}/series",
        params={"start_year": 2020, "end_year": 2020, "metrics": "not_a_metric"},
    )
    assert r.status_code == 400
    assert "invalid metrics" in r.text
