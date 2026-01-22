from __future__ import annotations

import calendar
import hashlib
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


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_station_dly(station_id: str) -> tuple[Path, str]:
    """
    Stellt sicher, dass `data/dly/{station_id}.dly` existiert.
    Gibt (Pfad, sha256) zurück.
    """
    dly_path = _dly_dir() / f"{station_id}.dly"
    sha_path = dly_path.with_suffix(".sha256")

    if dly_path.exists() and sha_path.exists():
        return dly_path, sha_path.read_text(encoding="utf-8").strip()

    if not dly_path.exists():
        r = requests.get(dly_url(station_id), timeout=180)
        if r.status_code == 404:
            raise FileNotFoundError(f"No .dly file for station {station_id}")
        r.raise_for_status()
        tmp = dly_path.with_suffix(".part")
        tmp.write_bytes(r.content)
        tmp.replace(dly_path)

    sha = sha256_file(dly_path)
    sha_path.write_text(sha + "\n", encoding="utf-8")
    return dly_path, sha


def _season_key(year: int, month: int) -> tuple[int, str] | None:
    if month in (3, 4, 5):
        return year, "spring"
    if month in (6, 7, 8):
        return year, "summer"
    if month in (9, 10, 11):
        return year, "autumn"
    if month == 12:
        return year + 1, "winter"
    if month in (1, 2):
        return year, "winter"
    return None


def _expected_days_year(year: int) -> int:
    return 366 if calendar.isleap(year) else 365


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
        base_years = (year - 1, year, year)
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
    yearly_sum: dict[tuple[str, int], float] = {}
    yearly_count: dict[tuple[str, int], int] = {}
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

            if year < start_year - 1 or year > end_year:
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

                if start_year <= year <= end_year:
                    k = (element, year)
                    yearly_sum[k] = yearly_sum.get(k, 0.0) + value_c
                    yearly_count[k] = yearly_count.get(k, 0) + 1

                if season is not None:
                    season_year, season_name = season
                    if start_year <= season_year <= end_year:
                        k2 = (element, season_year, season_name)
                        seasonal_sum[k2] = seasonal_sum.get(k2, 0.0) + value_c
                        seasonal_count[k2] = seasonal_count.get(k2, 0) + 1

    out: dict[str, list[MeanPoint]] = {}
    for element in elements:
        el = element.lower()  # "tmin" / "tmax"

        key_year = f"{el}_year"
        out[key_year] = []
        for y in range(start_year, end_year + 1):
            k = (element, y)
            cnt = yearly_count.get(k, 0)
            mean = (yearly_sum[k] / cnt) if cnt else None
            out[key_year].append(MeanPoint(year=y, value_c=mean, present_days=cnt, expected_days=_expected_days_year(y)))

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

    return out
