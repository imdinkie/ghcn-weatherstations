from __future__ import annotations

import calendar
from pathlib import Path

from app.ghcn_dly import _season_key, compute_means


STATION_ID = "TEST0000001"


def _build_line(year: int, month: int, element: str, daily_values_tenths: list[int | None]) -> str:
    # Baut eine gültige GHCN-DLY-Zeile für genau ein Element/Monat.
    # Fehlende Werte werden als -9999 gesetzt (GHCN-Missing-Konvention).
    prefix = f"{STATION_ID:<11}{year:04d}{month:02d}{element:<4}"
    days = calendar.monthrange(year, month)[1]
    slots: list[str] = []
    for day in range(1, 32):
        value = daily_values_tenths[day - 1] if day <= days else None
        raw = -9999 if value is None else value
        # 5 chars value + mflag + qflag + sflag
        slots.append(f"{raw:5d}" + " " + " " + " ")
    return prefix + "".join(slots) + "\n"


def _write_dly(path: Path, lines: list[str]) -> None:
    # Kleine Fixture-Datei für deterministische Parser-/Aggregations-Tests.
    path.write_text("".join(lines), encoding="utf-8")


def test_season_key_december_and_janfeb() -> None:
    # Projektkonvention: DJF gehört zum Dezemberjahr.
    assert _season_key(2020, 12) == (2020, "winter")
    assert _season_key(2021, 1) == (2020, "winter")
    assert _season_key(2021, 2) == (2020, "winter")


def test_compute_means_assigns_djf_to_december_year(tmp_path: Path) -> None:
    # Gemischtes DJF-Beispiel über Jahresgrenze:
    # Dez 2020 + Jan/Feb 2021 werden als Winter 2020 aggregiert.
    dly = tmp_path / "station.dly"
    lines = [
        _build_line(2020, 12, "TMIN", [100] * 31),
        _build_line(2021, 1, "TMIN", [200] * 31),
        _build_line(2021, 2, "TMIN", [300] * 28),
        _build_line(2020, 12, "TMAX", [400] * 31),
        _build_line(2021, 1, "TMAX", [500] * 31),
        _build_line(2021, 2, "TMAX", [600] * 28),
    ]
    _write_dly(dly, lines)

    series = compute_means(dly, start_year=2020, end_year=2021, elements={"TMIN", "TMAX"})

    # Erwartung mit Monatslogik:
    # Winter 2020 = Mittel aus Dec 2020 (10.0), Jan 2021 (20.0), Feb 2021 (30.0)
    tmin_winter_2020 = series["tmin_winter"][0]
    assert tmin_winter_2020.year == 2020
    assert tmin_winter_2020.present_months == 3
    assert tmin_winter_2020.expected_months == 3
    assert abs(tmin_winter_2020.value_c - 20.0) < 1e-6

    tmin_winter_2021 = series["tmin_winter"][1]
    assert tmin_winter_2021.value_c is None
    assert tmin_winter_2021.present_months == 0

    # Jahresmittel 2020 basiert auf Monatsmitteln Jan..Dez 2020.
    # In der Fixture gibt es nur Dez 2020, also entspricht der Jahreswert 10.0.
    tmin_year_2020 = series["tmin_year"][0]
    assert abs(tmin_year_2020.value_c - 10.0) < 1e-6


def test_compute_means_missing_values_produce_none(tmp_path: Path) -> None:
    # Vollständig fehlende Saisonwerte müssen als None/0-Tage erscheinen.
    dly = tmp_path / "station_missing.dly"
    lines = [
        _build_line(2020, 3, "TMIN", [None] * 31),
        _build_line(2020, 4, "TMIN", [None] * 30),
        _build_line(2020, 5, "TMIN", [None] * 31),
    ]
    _write_dly(dly, lines)

    series = compute_means(dly, start_year=2020, end_year=2020, elements={"TMIN"})

    spring = series["tmin_spring"][0]
    assert spring.value_c is None
    assert spring.present_months == 0
    assert spring.expected_months == 3


def test_compute_means_year_uses_available_months_as_divisor(tmp_path: Path) -> None:
    # Wenn ein Monat komplett fehlt, teilt das Jahresmittel durch die vorhandenen Monate.
    dly = tmp_path / "station_year_divisor.dly"
    lines = []
    for month in range(1, 12):  # Jan..Nov vorhanden, Dez fehlt
        lines.append(_build_line(2020, month, "TMIN", [100] * calendar.monthrange(2020, month)[1]))
    _write_dly(dly, lines)

    series = compute_means(dly, start_year=2020, end_year=2020, elements={"TMIN"})
    year_point = series["tmin_year"][0]
    assert year_point.value_c == 10.0
    assert year_point.present_months == 11
    assert year_point.expected_months == 12
