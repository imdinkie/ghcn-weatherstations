from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from app.db import connect, ensure_schema

app = FastAPI(title="GHCN Weatherstations (Learning API)")


@app.get("/")
def root():
    return {"message": "API is running. Open /docs for Swagger UI."}


@app.get("/health")
def health():
    return {"ok": True}


# Pydantic-Model:
# - definiert, wie eine Station "aussieht" (Felder + Typen)
# - FastAPI nutzt das für Validierung + automatische Doku in /docs
class StationOut(BaseModel):
    id: str
    name: str
    lat: float
    lon: float
    elev_m: float | None = None
    state: str | None = None



@app.on_event("startup")
def _startup():
    ensure_schema()


@app.post("/dev/seed")
def dev_seed():
    # Dev-Endpoint: legt eine Demo-Station an (nur einmal, dank ON CONFLICT).
    ensure_schema()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stations (id, lat, lon, elev_m, state, name)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                ("DEMO001", 48.062, 8.493, 0.0, None, "Demo Station"),
            )
        conn.commit()
    return {"seeded": True}


@app.get("/stations", response_model=list[StationOut])
def list_stations(
    # Query-Parameter (optional) zum Begrenzen der Ergebnisanzahl.
    # limit=20 ist Standard; FastAPI validiert automatisch (gt/le).
    limit: int = Query(20, gt=0, le=200),
):
    # WICHTIG: Liste zurückgeben
    # -------------------------
    # response_model=list[StationOut] bedeutet:
    # - Die Response ist eine JSON-LISTE.
    # - Jeder Eintrag in der Liste ist ein StationOut-Objekt (id/name/lat/lon).
    #
    # JSON sieht dann z.B. so aus:
    # [
    #   {"id":"DEMO001","name":"Demo Station","lat":48.062,"lon":8.493},
    #   {"id":"...","name":"...","lat":...,"lon":...}
    # ]
    with connect() as conn:
        with conn.cursor() as cur:
            # WICHTIG: Keine f-Strings für SQL mit Parametern verwenden!
            # Sonst baust du dir SQL-Injection-Bugs.
            # Stattdessen nutzt du Platzhalter (%s) und übergibst die Werte als Tuple.
            cur.execute(
                """
                SELECT id, name, lat, lon, elev_m, state
                FROM stations
                ORDER BY id
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

    # `rows` ist eine Python-Liste von Tupeln:
    # z.B. [("DEMO001", "Demo Station", 48.062, 8.493), (...), ...]
    #
    # Wir wandeln jedes Tupel in StationOut um und sammeln alles in einer Liste.
    stations: list[StationOut] = []
    for row in rows:
        stations.append(StationOut(id=row[0], name=row[1], lat=row[2], lon=row[3], elev_m=row[4], state=row[5]))

    return stations


@app.get("/stations/{station_id}", response_model=StationOut)
def get_station(station_id: str):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, lat, lon, elev_m, state
                FROM stations
                WHERE id = %s
                """,
                (station_id,),
            )
            row = cur.fetchone()

    # fetchone() liefert None, wenn es keinen Treffer gibt.
    if row is None:
        raise HTTPException(status_code=404, detail="Station not found")

    return StationOut(id=row[0], name=row[1], lat=row[2], lon=row[3], elev_m=row[4], state=row[5])


class StationSearchOut(StationOut):
    # Entfernung zum Standpunkt (in km) – damit du die "nächsten Stationen" sortieren kannst.
    dist_km: float

    # Coverage-Infos sind praktisch, um die Start/Endjahr-Filter zu debuggen/anzeigen.
    tmin_first_year: int | None = None
    tmin_last_year: int | None = None
    tmax_first_year: int | None = None
    tmax_last_year: int | None = None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # Great-circle distance (Haversine). Genau genug für unseren Zweck.
    import math

    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def _bounding_box(lat: float, lon: float, radius_km: float) -> tuple[float, float, float, float]:
    # Damit die DB nicht "alle Stationen weltweit" scannen muss, filtern wir zuerst grob:
    # Wir nehmen ein Rechteck (Bounding Box) um den Punkt herum.
    # Danach filtern wir in Python mit Haversine nochmal exakt auf den Kreis (Radius).
    import math

    lat_delta = radius_km / 111.0
    lon_delta = radius_km / (111.0 * math.cos(math.radians(max(min(lat, 89.9), -89.9))) or 1.0)
    return lat - lat_delta, lat + lat_delta, lon - lon_delta, lon + lon_delta


@app.get("/search", response_model=list[StationSearchOut])
def search_stations(
    lat: float = Query(..., ge=-90.0, le=90.0),
    lon: float = Query(..., ge=-180.0, le=180.0),
    radius_km: float = Query(50.0, gt=0.0, le=2000.0),
    limit: int = Query(20, gt=0, le=200),
    # Letztes mögliches Endjahr ist das aktuelle Vorjahr (heute: 2025).
    start_year: int | None = Query(None, ge=1700, le=2025),
    end_year: int | None = Query(None, ge=1700, le=2025),
):
    # 1) Eingaben prüfen
    if start_year is not None and end_year is not None and start_year > end_year:
        raise HTTPException(status_code=400, detail="start_year must be <= end_year")

    # 2) Bounding Box berechnen (schnelle Vorauswahl in SQL)
    min_lat, max_lat, min_lon, max_lon = _bounding_box(lat, lon, radius_km)

    # 3) SQL bauen: Kandidaten aus der Box holen + (optional) Coverage-Filter
    where = ["s.lat BETWEEN %s AND %s", "s.lon BETWEEN %s AND %s"]
    params: list[object] = [min_lat, max_lat, min_lon, max_lon]

    # Die Aufgabe verlangt später Jahres-/Saisonmittel für TMIN und TMAX.
    # Daher filtern wir hier streng: Station muss (wenn Filter gesetzt) beide Zeiträume abdecken.
    if start_year is not None:
        where.append("(sc.tmin_first_year IS NOT NULL AND sc.tmin_first_year <= %s)")
        where.append("(sc.tmax_first_year IS NOT NULL AND sc.tmax_first_year <= %s)")
        params.extend([start_year, start_year])
    if end_year is not None:
        where.append("(sc.tmin_last_year IS NOT NULL AND sc.tmin_last_year >= %s)")
        where.append("(sc.tmax_last_year IS NOT NULL AND sc.tmax_last_year >= %s)")
        params.extend([end_year, end_year])

    sql = f"""
        SELECT
          s.id, s.name, s.lat, s.lon, s.elev_m, s.state,
          sc.tmin_first_year, sc.tmin_last_year, sc.tmax_first_year, sc.tmax_last_year
        FROM stations s
        LEFT JOIN station_coverage sc ON sc.id = s.id
        WHERE {" AND ".join(where)}
        LIMIT 5000
    """

    # 4) Kandidaten laden + echte Distanz berechnen + auf Radius filtern
    candidates: list[StationSearchOut] = []
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                dist = _haversine_km(lat, lon, row[2], row[3])
                if dist > radius_km:
                    continue
                candidates.append(
                    StationSearchOut(
                        id=row[0],
                        name=row[1],
                        lat=row[2],
                        lon=row[3],
                        elev_m=row[4],
                        state=row[5],
                        dist_km=dist,
                        tmin_first_year=row[6],
                        tmin_last_year=row[7],
                        tmax_first_year=row[8],
                        tmax_last_year=row[9],
                    )
                )

    # 5) Sortieren & Limitieren ("nächste Stationen")
    candidates.sort(key=lambda s: s.dist_km)
    return candidates[:limit]
