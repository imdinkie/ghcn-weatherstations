from __future__ import annotations

from app.db import connect
from app.ghcn_dly import MeanPoint, compute_means


ALL_METRICS = [
    "tmin_year",
    "tmax_year",
    "tmin_spring",
    "tmax_spring",
    "tmin_summer",
    "tmax_summer",
    "tmin_autumn",
    "tmax_autumn",
    "tmin_winter",
    "tmax_winter",
]


def ensure_cached_metrics(
    *,
    station_id: str,
    sha256: str,
    dly_path,
    start_year: int,
    end_year: int,
) -> None:
    years = end_year - start_year + 1
    expected_rows = years * len(ALL_METRICS)

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                FROM station_metric_cache
                WHERE station_id=%s AND sha256=%s AND year BETWEEN %s AND %s
                """,
                (station_id, sha256, start_year, end_year),
            )
            have = cur.fetchone()[0]
            if have >= expected_rows:
                return

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (station_id,))
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*)
                    FROM station_metric_cache
                    WHERE station_id=%s AND sha256=%s AND year BETWEEN %s AND %s
                    """,
                    (station_id, sha256, start_year, end_year),
                )
                have = cur.fetchone()[0]
                if have >= expected_rows:
                    return

            series = compute_means(dly_path, start_year=start_year, end_year=end_year, elements={"TMIN", "TMAX"})

            with conn.cursor() as cur:
                for metric in ALL_METRICS:
                    points: list[MeanPoint] = series[metric]
                    for p in points:
                        cur.execute(
                            """
                            INSERT INTO station_metric_cache
                              (station_id, metric, year, sha256, value_c, present_days, expected_days)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (station_id, metric, year) DO UPDATE SET
                              sha256 = EXCLUDED.sha256,
                              value_c = EXCLUDED.value_c,
                              present_days = EXCLUDED.present_days,
                              expected_days = EXCLUDED.expected_days,
                              computed_at = now()
                            """,
                            (
                                station_id,
                                metric,
                                p.year,
                                sha256,
                                p.value_c,
                                p.present_days,
                                p.expected_days,
                            ),
                        )
            conn.commit()
        finally:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (station_id,))


def load_cached_series(
    *,
    station_id: str,
    sha256: str,
    start_year: int,
    end_year: int,
    metrics: list[str],
) -> list[dict]:
    by_metric: dict[str, dict[int, dict]] = {m: {} for m in metrics}

    placeholders = ", ".join(["%s"] * len(metrics))
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT metric, year, value_c, present_days, expected_days
                FROM station_metric_cache
                WHERE station_id=%s AND sha256=%s AND year BETWEEN %s AND %s
                  AND metric IN ({placeholders})
                ORDER BY metric, year
                """,
                [station_id, sha256, start_year, end_year, *metrics],
            )
            for metric, year, value_c, present_days, expected_days in cur.fetchall():
                by_metric[metric][year] = {
                    "year": year,
                    "value_c": value_c,
                    "present_days": present_days,
                    "expected_days": expected_days,
                }

    out: list[dict] = []
    for metric in metrics:
        points = []
        for y in range(start_year, end_year + 1):
            points.append(by_metric[metric].get(y, {"year": y, "value_c": None, "present_days": 0, "expected_days": 0}))
        out.append({"key": metric, "sha256": sha256, "points": points})
    return out
