from __future__ import annotations

import calendar
import os
from dataclasses import dataclass
from pathlib import Path

import requests


@dataclass(frozen=True)
class MeanPoint:
    year: int
    value_c: float | None
    present_months: int
    expected_months: int


def data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "data")).resolve()


def _dly_dir() -> Path:
    # Wichtig: `data/dly` kann durch Docker-Läufe root-owned sein.
    # Für lokale Entwicklung muss der Ordner schreibbar sein, sonst schlagen Downloads fehl.
    p = data_dir() / "dly_cache"
    p.mkdir(parents=True, exist_ok=True)
    return p


def dly_url(station_id: str) -> str:
    return f"https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{station_id}.dly"


def ensure_station_dly(station_id: str) -> Path:
    """
    Stellt sicher, dass `data/dly_cache/{station_id}.dly` existiert.
    Gibt den lokalen Dateipfad zurück.
    """
    dly_path = _dly_dir() / f"{station_id}.dly"
    if dly_path.exists():
        return dly_path

    r = requests.get(dly_url(station_id), timeout=180)
    if r.status_code == 404:
        raise FileNotFoundError(f"No .dly file for station {station_id}")
    r.raise_for_status()

    tmp = dly_path.with_suffix(".part")
    tmp.write_bytes(r.content)
    tmp.replace(dly_path)
    return dly_path


def _season_key(year: int, month: int) -> tuple[int, str] | None:
    if month in (3, 4, 5):
        return year, "spring"
    if month in (6, 7, 8):
        return year, "summer"
    if month in (9, 10, 11):
        return year, "autumn"
    if month == 12:
        return year, "winter"
    if month in (1, 2):
        return year - 1, "winter"
    return None


def compute_means(
    dly_path: Path,
    *,
    start_year: int,
    end_year: int,
    elements: set[str],
) -> dict[str, list[MeanPoint]]:
    """
    Berechnet Reihen auf Basis von Monatsdurchschnitten:
      - tmin_year / tmax_year
      - tmin_spring / tmax_spring
      - tmin_summer / tmax_summer
      - tmin_autumn / tmax_autumn
      - tmin_winter / tmax_winter

    Regeln:
    - Pro Monat: Mittel der gültigen Tageswerte
    - Jahr: Mittel der vorhandenen Monatsmittel (1..12)
    - Winter(Y): Mittel aus Dec(Y), Jan(Y+1), Feb(Y+1)
    """
    monthly_sum: dict[tuple[str, int, int], float] = {}
    monthly_count: dict[tuple[str, int, int], int] = {}

    with dly_path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if len(line) < 21:
                continue

            year = int(line[11:15])
            month = int(line[15:17])
            element = line[17:21]
            if element not in elements:
                continue

            # Für Winter bis end_year benötigen wir Jan/Feb von end_year+1.
            if year < start_year or year > end_year + 1:
                continue

            try:
                days_in_month = calendar.monthrange(year, month)[1]
            except calendar.IllegalMonthError:
                continue

            for day in range(1, 32):
                base = 21 + (day - 1) * 8
                if base + 8 > len(line):
                    break

                raw = int(line[base : base + 5])
                qflag = line[base + 6]
                if raw == -9999 or qflag != " ":
                    continue
                if day > days_in_month:
                    continue

                key = (element, year, month)
                monthly_sum[key] = monthly_sum.get(key, 0.0) + (raw / 10.0)
                monthly_count[key] = monthly_count.get(key, 0) + 1

    def monthly_mean(element: str, year: int, month: int) -> float | None:
        key = (element, year, month)
        cnt = monthly_count.get(key, 0)
        if cnt == 0:
            return None
        return monthly_sum[key] / cnt

    out: dict[str, list[MeanPoint]] = {}
    season_months = {
        "spring": lambda y: ((y, 3), (y, 4), (y, 5)),
        "summer": lambda y: ((y, 6), (y, 7), (y, 8)),
        "autumn": lambda y: ((y, 9), (y, 10), (y, 11)),
        "winter": lambda y: ((y, 12), (y + 1, 1), (y + 1, 2)),
    }

    for element in elements:
        el = element.lower()

        for season in ("spring", "summer", "autumn", "winter"):
            key = f"{el}_{season}"
            out[key] = []
            for y in range(start_year, end_year + 1):
                values: list[float] = []
                for ym, m in season_months[season](y):
                    mm = monthly_mean(element, ym, m)
                    if mm is not None:
                        values.append(mm)
                cnt = len(values)
                mean = (sum(values) / cnt) if cnt else None
                out[key].append(
                    MeanPoint(
                        year=y,
                        value_c=mean,
                        present_months=cnt,
                        expected_months=3,
                    )
                )

        key_year = f"{el}_year"
        out[key_year] = []
        for y in range(start_year, end_year + 1):
            values: list[float] = []
            for m in range(1, 13):
                mm = monthly_mean(element, y, m)
                if mm is not None:
                    values.append(mm)
            cnt = len(values)
            mean = (sum(values) / cnt) if cnt else None
            out[key_year].append(
                MeanPoint(
                    year=y,
                    value_c=mean,
                    present_months=cnt,
                    expected_months=12,
                )
            )

    return out
