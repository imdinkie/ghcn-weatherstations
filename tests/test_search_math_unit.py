from __future__ import annotations

from app.main import _bounding_box, _haversine_km


def test_haversine_zero_distance() -> None:
    # Identische Koordinaten müssen exakt 0 km liefern.
    assert _haversine_km(48.0, 8.0, 48.0, 8.0) == 0.0


def test_haversine_one_degree_longitude_at_equator() -> None:
    # Plausibilitätscheck: 1° Längengrad am Äquator liegt bei ~111 km.
    dist = _haversine_km(0.0, 0.0, 0.0, 1.0)
    assert 110.0 < dist < 112.5


def test_bounding_box_expands_for_radius() -> None:
    # Die Bounding-Box muss den Mittelpunkt enthalten und um ihn herum aufspannen.
    min_lat, max_lat, min_lon, max_lon = _bounding_box(48.0, 8.0, 50)
    assert min_lat < 48.0 < max_lat
    assert min_lon < 8.0 < max_lon


def test_bounding_box_handles_high_latitudes() -> None:
    # Polnähe ist ein sensibler Fall wegen cos(latitude) im Longitude-Delta.
    # Der Test prüft nur stabile Grenzen (kein Crash, kein invertiertes Intervall).
    min_lat, max_lat, min_lon, max_lon = _bounding_box(89.99, 10.0, 10)
    assert min_lat < max_lat
    assert min_lon < max_lon
