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
