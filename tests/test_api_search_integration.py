from __future__ import annotations


def test_search_sorted_and_limited(client, seed_station) -> None:
    # Drei Stationen: nah, weiter weg, außerhalb Radius.
    # Erwartung: nur Treffer im Radius, sortiert nach Distanz, limitiert auf 2.
    seed_station(
        station_id="A0000000001",
        name="Near",
        lat=48.0000,
        lon=8.0000,
        tmin_first_year=1990,
        tmin_last_year=2025,
        tmax_first_year=1990,
        tmax_last_year=2025,
    )
    seed_station(
        station_id="B0000000001",
        name="Farther",
        lat=48.1200,
        lon=8.1200,
        tmin_first_year=1990,
        tmin_last_year=2025,
        tmax_first_year=1990,
        tmax_last_year=2025,
    )
    seed_station(
        station_id="C0000000001",
        name="Outside",
        lat=49.0000,
        lon=9.0000,
        tmin_first_year=1990,
        tmin_last_year=2025,
        tmax_first_year=1990,
        tmax_last_year=2025,
    )

    r = client.get("/search", params={"lat": 48.0, "lon": 8.0, "radius_km": 30, "limit": 2})
    assert r.status_code == 200
    payload = r.json()
    assert len(payload) == 2
    assert payload[0]["id"] == "A0000000001"
    assert payload[1]["id"] == "B0000000001"
    assert payload[0]["dist_km"] <= payload[1]["dist_km"]
    assert payload[0]["coverage_first_year"] == 1990
    assert payload[0]["coverage_last_year"] == 2025


def test_search_rejects_invalid_year_range(client) -> None:
    # Fachliche Validierung in /search: start_year darf nicht > end_year sein.
    r = client.get(
        "/search",
        params={"lat": 48.0, "lon": 8.0, "radius_km": 10, "limit": 5, "start_year": 2022, "end_year": 2020},
    )
    assert r.status_code == 400
    assert "start_year must be <= end_year" in r.text


def test_search_filters_by_coverage(client, seed_station) -> None:
    # Eine Station deckt den Zeitraum ab, eine nicht.
    # Erwartung: nur die passende Station wird zurückgegeben.
    seed_station(
        station_id="D0000000001",
        name="MatchesCoverage",
        lat=48.0,
        lon=8.0,
        tmin_first_year=1980,
        tmin_last_year=2024,
        tmax_first_year=1985,
        tmax_last_year=2024,
    )
    seed_station(
        station_id="E0000000001",
        name="NoCoverage",
        lat=48.001,
        lon=8.001,
        tmin_first_year=2005,
        tmin_last_year=2010,
        tmax_first_year=2005,
        tmax_last_year=2010,
    )

    r = client.get(
        "/search",
        params={
            "lat": 48.0,
            "lon": 8.0,
            "radius_km": 20,
            "limit": 10,
            "start_year": 2000,
            "end_year": 2020,
        },
    )
    assert r.status_code == 200
    ids = [row["id"] for row in r.json()]
    assert "D0000000001" in ids
    assert "E0000000001" not in ids
