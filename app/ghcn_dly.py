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
    present_days: int
    expected_days: int


def data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "data")).resolve()


def _dly_dir() -> Path:
    # Wichtig: `data/dly` kann durch Docker-Läufe root-owned sein.
    # Für lokale Entwicklung muss der Ordner schreibbar sein, sonst schlagen Downloads/Hashes fehl.
    # Darum nutzen wir bewusst einen eigenen Cache-Ordner unter `data/`.
    p = data_dir() / "dly_cache"
    p.mkdir(parents=True, exist_ok=True)
    return p


def dly_url(station_id: str) -> str:
    return f"https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{station_id}.dly"


def ensure_station_dly(station_id: str) -> Path:
    """
    Stellt sicher, dass `data/dly/{station_id}.dly` existiert.
    Gibt den lokalen Dateipfad zurück.
    """
    dly_path = _dly_dir() / f"{station_id}.dly"
    if dly_path.exists():
        return dly_path

    if not dly_path.exists():
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


def _expected_days_season(year: int, season: str) -> int:
    if season == "spring":
        months = (3, 4, 5)
        base_years = (year, year, year)
    elif season == "summer":
        months = (6, 7, 8)
        base_years = (year, year, year)
    elif season == "autumn":
        months = (9, 10, 11)
        base_years = (year, year, year)
    elif season == "winter":
        months = (12, 1, 2)
        base_years = (year, year + 1, year + 1)
    else:
        raise ValueError(f"Unknown season: {season}")

    total = 0
    for y, m in zip(base_years, months, strict=True):
        total += calendar.monthrange(y, m)[1]
    return total


def compute_means(
    dly_path: Path,
    *,
    start_year: int,
    end_year: int,
    elements: set[str],
) -> dict[str, list[MeanPoint]]:
    """
    Returns series keyed by:
      - tmin_year / tmax_year
      - tmin_spring / tmax_spring
      - tmin_summer / tmax_summer
      - tmin_autumn / tmax_autumn
      - tmin_winter / tmax_winter
    """
    seasonal_sum: dict[tuple[str, int, str], float] = {}
    seasonal_count: dict[tuple[str, int, str], int] = {}

    with dly_path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if len(line) < 21:
                continue
            year = int(line[11:15])
            month = int(line[15:17])
            element = line[17:21]
            if element not in elements:
                continue

            if year < start_year or year > end_year + 1:
                continue

            season = _season_key(year, month)
            for day in range(1, 32):
                base = 21 + (day - 1) * 8
                if base + 8 > len(line):
                    break
                raw = int(line[base : base + 5])
                qflag = line[base + 6]
                if raw == -9999 or qflag != " ":
                    continue
                try:
                    _ = calendar.monthrange(year, month)[1]
                    if day > _:
                        continue
                except calendar.IllegalMonthError:
                    continue

                value_c = raw / 10.0

                if season is not None:
                    season_year, season_name = season
                    if start_year <= season_year <= end_year:
                        k2 = (element, season_year, season_name)
                        seasonal_sum[k2] = seasonal_sum.get(k2, 0.0) + value_c
                        seasonal_count[k2] = seasonal_count.get(k2, 0) + 1

    out: dict[str, list[MeanPoint]] = {}
    for element in elements:
        el = element.lower()  # "tmin" / "tmax"

        for season in ("spring", "summer", "autumn", "winter"):
            key = f"{el}_{season}"
            out[key] = []
            for y in range(start_year, end_year + 1):
                k = (element, y, season)
                cnt = seasonal_count.get(k, 0)
                mean = (seasonal_sum[k] / cnt) if cnt else None
                out[key].append(
                    MeanPoint(year=y, value_c=mean, present_days=cnt, expected_days=_expected_days_season(y, season))
                )

        key_year = f"{el}_year"
        out[key_year] = []
        season_keys = [f"{el}_spring", f"{el}_summer", f"{el}_autumn", f"{el}_winter"]
        for index, y in enumerate(range(start_year, end_year + 1)):
            season_values = [out[key][index].value_c for key in season_keys]
            available = [value for value in season_values if value is not None]
            mean = (sum(available) / len(available)) if available else None
            out[key_year].append(
                MeanPoint(
                    year=y,
                    value_c=mean,
                    present_days=len(available),
                    expected_days=4,
                )
            )

    return out
