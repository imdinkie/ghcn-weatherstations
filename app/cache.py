from __future__ import annotations

from app.db import connect
from app.ghcn_dly import MeanPoint, compute_means

"""
Caching-Strategie (vereinfacht):
- Der Cache ist persistent in Postgres (Tabelle `station_metric_cache`).
- Es gibt bewusst keine automatische Invalidierung über Dateihashes.
- Primärschlüssel der Tabelle ist (station_id, metric, year).
  Dadurch gibt es pro Kombination aus Station + Kennzahl + Jahr genau eine Zeile.
- Ablauf:
  1) Für einen Zeitraum wird gezählt, wie viele Cache-Zeilen schon vorhanden sind.
  2) Sind genug Zeilen da, wird nur gelesen (keine Neuberechnung).
  3) Fehlen Zeilen, wird berechnet und per UPSERT gespeichert.
- UPSERT (`INSERT ... ON CONFLICT DO UPDATE`) hält bestehende Zeilen aktuell,
  ohne Duplikate zu erzeugen.
"""


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
    dly_path,
    start_year: int,
    end_year: int,
) -> None:
    years = end_year - start_year + 1
    # Erwartete Vollständigkeit des Caches für diesen Request:
    # Für jedes Jahr im Intervall soll jede Metrik genau eine Zeile haben.
    expected_rows = years * len(ALL_METRICS)

    # 1) Schneller Vorab-Check ohne Lock:
    #    Wenn bereits genug Zeilen vorhanden sind, können wir sofort zurück.
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                FROM station_metric_cache
                WHERE station_id=%s AND year BETWEEN %s AND %s
                """,
                (station_id, start_year, end_year),
            )
            have = cur.fetchone()[0]
            # `have >= expected_rows` bedeutet:
            # Der Cache ist für die angefragten Jahre/Metriken vollständig genug.
            if have >= expected_rows:
                return

    # 2) Lock pro Station, damit parallele Requests nicht doppelt berechnen.
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (station_id,))
        try:
            # 3) Double-Check nach dem Lock:
            #    Ein anderer Request könnte den Cache inzwischen gefüllt haben.
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*)
                    FROM station_metric_cache
                    WHERE station_id=%s AND year BETWEEN %s AND %s
                    """,
                    (station_id, start_year, end_year),
                )
                have = cur.fetchone()[0]
                if have >= expected_rows:
                    return

            # 4) Cache unvollständig -> Aggregation jetzt berechnen.
            #    Die eigentliche Berechnungslogik liegt in app/ghcn_dly.py (compute_means).
            series = compute_means(dly_path, start_year=start_year, end_year=end_year, elements={"TMIN", "TMAX"})

            # 5) Jede berechnete Reihe/Jahr per UPSERT persistieren:
            #    - neue Zeile einfügen
            #    - bestehende Zeile aktualisieren
            with conn.cursor() as cur:
                for metric in ALL_METRICS:
                    points: list[MeanPoint] = series[metric]
                    for p in points:
                        cur.execute(
                            """
                            INSERT INTO station_metric_cache
                              (station_id, metric, year, value_c, present_months, expected_months)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (station_id, metric, year) DO UPDATE SET
                              value_c = EXCLUDED.value_c,
                              present_months = EXCLUDED.present_months,
                              expected_months = EXCLUDED.expected_months,
                              computed_at = now()
                            """,
                            (
                                station_id,
                                metric,
                                p.year,
                                p.value_c,
                                p.present_months,
                                p.expected_months,
                            ),
                        )
            # 6) Alle Änderungen atomar committen.
            conn.commit()
        finally:
            # 7) Lock in jedem Fall wieder freigeben.
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (station_id,))


def load_cached_series(
    *,
    station_id: str,
    start_year: int,
    end_year: int,
    metrics: list[str],
) -> list[dict]:
    by_metric: dict[str, dict[int, dict]] = {m: {} for m in metrics}

    def expected_months_for_metric(metric: str) -> int:
        return 12 if metric.endswith("_year") else 3

    # Die Metrics-Liste ist dynamisch; dafür bauen wir die passende Anzahl Platzhalter.
    placeholders = ", ".join(["%s"] * len(metrics))
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT metric, year, value_c, present_months, expected_months
                FROM station_metric_cache
                WHERE station_id=%s AND year BETWEEN %s AND %s
                  AND metric IN ({placeholders})
                ORDER BY metric, year
                """,
                [station_id, start_year, end_year, *metrics],
            )
            # Ergebnis zunächst als Dict strukturieren:
            # by_metric[metric][year] -> Punktdaten
            for metric, year, value_c, present_months, expected_months in cur.fetchall():
                by_metric[metric][year] = {
                    "year": year,
                    "value_c": value_c,
                    "present_months": present_months,
                    "expected_months": expected_months,
                }

    out: list[dict] = []
    for metric in metrics:
        points = []
        for y in range(start_year, end_year + 1):
            # Fehlende Jahre im Cache werden explizit als leere Punkte ergänzt,
            # damit Frontend immer eine lückenlose Jahr-Achse erhält.
            points.append(
                by_metric[metric].get(
                    y,
                    {
                        "year": y,
                        "value_c": None,
                        "present_months": 0,
                        "expected_months": expected_months_for_metric(metric),
                    },
                )
            )
        out.append({"key": metric, "points": points})
    return out
