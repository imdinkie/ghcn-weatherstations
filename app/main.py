import datetime as dt
import json
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from app.cache import ALL_METRICS, ensure_cached_metrics, load_cached_series
from app.db import connect, ensure_schema
from app.ghcn_dly import ensure_station_dly
from fastapi.templating import Jinja2Templates

app = FastAPI(title="GHCN Weatherstations (Learning API)")
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.on_event("startup")
def _startup() -> None:
    ensure_schema()


MAX_YEAR = dt.date.today().year - 1


@app.get("/")
def root(request: Request):
    # Startseite: Suchformular + (optional) Suchergebnisse auf derselben Seite.
    # Die Suche wird erst ausgeführt, wenn Parameter in der URL vorhanden sind.
    qp = request.query_params
    defaults = {
        "lat": 48.062,
        "lon": 8.493,
        "radius_km": 50,
        "limit": 20,
        "start_year": "",
        "end_year": "",
    }

    has_search = "lat" in qp and "lon" in qp
    if not has_search:
        return templates.TemplateResponse(
            "index.html",
            {"request": request, "max_year": MAX_YEAR, "defaults": defaults, "stations": [], "stations_json": "[]", "params_json": "{}"},
        )

    lat = float(qp.get("lat"))
    lon = float(qp.get("lon"))
    radius_km = float(qp.get("radius_km", defaults["radius_km"]))
    limit = int(qp.get("limit", defaults["limit"]))
    start_year_s = qp.get("start_year", "")
    end_year_s = qp.get("end_year", "")
    start_year_i = None if start_year_s in ("", None) else int(start_year_s)
    end_year_i = None if end_year_s in ("", None) else int(end_year_s)

    stations = search_stations(
        lat=lat,
        lon=lon,
        radius_km=radius_km,
        limit=limit,
        start_year=start_year_i,
        end_year=end_year_i,
    )

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "max_year": MAX_YEAR,
            "defaults": {
                "lat": lat,
                "lon": lon,
                "radius_km": radius_km,
                "limit": limit,
                "start_year": start_year_s,
                "end_year": end_year_s,
            },
            "stations": stations,
            "stations_json": json.dumps(
                [{"id": s.id, "name": s.name, "lat": s.lat, "lon": s.lon, "dist_km": s.dist_km} for s in stations]
            ),
            "params_json": json.dumps(
                {
                    "lat": lat,
                    "lon": lon,
                    "radius_km": radius_km,
                    "limit": limit,
                    "start_year": start_year_i,
                    "end_year": end_year_i,
                }
            ),
        },
    )


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
    # Warum eine extra Klasse?
    # ------------------------
    # In der Stationssuche wollen wir *mehr* zurückgeben als nur die Station selbst.
    #
    # StationOut (Basisklasse) beschreibt nur die Station-Stammdaten:
    #   id, name, lat, lon, elev_m, state
    #
    # Für Suchergebnisse brauchen wir zusätzlich z.B.:
    #   - dist_km: Entfernung vom eingegebenen Standpunkt
    #   - Coverage-Infos: damit Start/Endjahr-Filter nachvollziehbar sind
    #
    # Pydantic-Model-Inheritance (Vererbung) bedeutet hier:
    #   StationSearchOut = StationOut + zusätzliche Felder
    # -> In /docs siehst du dadurch ein sauberes Schema für Suchergebnisse.

    # Entfernung zum Standpunkt (in km) – damit du die "nächsten Stationen" sortieren kannst.
    dist_km: float

    # Coverage-Infos sind praktisch, um die Start/Endjahr-Filter zu debuggen/anzeigen.
    tmin_first_year: int | None = None
    tmin_last_year: int | None = None
    tmax_first_year: int | None = None
    tmax_last_year: int | None = None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # Great-circle distance (Haversine)
    # ---------------------------------
    # Problem: Wir wollen die Entfernung zweier Punkte auf der Erdoberfläche.
    # Die Erde ist (vereinfacht) eine Kugel. Die kürzeste Strecke auf einer Kugel
    # ist ein "Großkreis" (great circle).
    #
    # Die Haversine-Formel liefert genau diese Großkreisentfernung.
    #
    # Eingabe:
    #   lat1, lon1: Standpunkt (User-Eingabe)
    #   lat2, lon2: Station
    #
    # Ausgabe:
    #   Distanz in Kilometern (km)
    #
    # Warum Haversine?
    #   - stabil auch für kleine Distanzen
    #   - gut genug für "Stationen im Radius X km" (Semesterprojekt)
    import math

    # Erdradius in km (Mittelwert). Je nach Modell schwankt der "echte" Radius leicht.
    r = 6371.0

    # WICHTIG: trigonometrische Funktionen erwarten Radiant, nicht Grad.
    # math.radians(x) macht Grad -> Radiant.
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)

    # Differenzen der Winkel (in Radiant)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    # Haversine:
    # a ist ein Hilfswert zwischen 0 und 1
    # (0 = gleiche Punkte, 1 = antipodale Punkte)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2

    # c ist der Zentralwinkel (Winkel im Erdmittelpunkt) zwischen den beiden Punkten
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Distanz = Radius * Winkel
    return r * c


def _bounding_box(lat: float, lon: float, radius_km: float) -> tuple[float, float, float, float]:
    # Bounding Box (grobe Vorauswahl)
    # -------------------------------
    # Unser Ziel ist eigentlich ein Kreis (Radius um den Standpunkt).
    # Aber SQL (ohne Geo-Extensions) kann "Kreis um Punkt" schwieriger/teurer machen.
    #
    # Trick:
    #  1) Wir filtern in SQL zuerst grob in einem Rechteck (Bounding Box).
    #  2) Danach machen wir in Python den exakten Kreis-Test via Haversine.
    #
    # Vorteil:
    #  - Die DB kann mit Index auf lat/lon schnell Kandidaten auswählen
    #  - Python rechnet Distanz nur für Kandidaten, nicht für alle Stationen weltweit
    import math

    # Näherung:
    # 1° Latitude (Breitengrad) ~ 111 km
    # -> Wie viele Grad entsprechen radius_km?
    lat_delta = radius_km / 111.0

    # Longitude (Längengrad) ist abhängig von der Latitude:
    # Je näher am Pol, desto "enger" liegen die Meridiane zusammen.
    # 1° Longitude ~ 111 km * cos(latitude)
    #
    # cos(90°) = 0 -> Division durch 0, darum clampen wir lat auf +/-89.9
    lon_delta = radius_km / (111.0 * math.cos(math.radians(max(min(lat, 89.9), -89.9))) or 1.0)
    return lat - lat_delta, lat + lat_delta, lon - lon_delta, lon + lon_delta


@app.get("/search", response_model=list[StationSearchOut])
def search_stations(
    # FastAPI nimmt diese Funktionsparameter automatisch aus der URL als Query-Parameter.
    # Beispiel-Aufruf:
    #   /search?lat=48.06&lon=8.49&radius_km=50&limit=20&start_year=2000&end_year=2020
    #
    # Query(..., ge=..., le=...) ist Validierung:
    # - ge = "greater or equal" (>=)
    # - le = "less or equal" (<=)
    # - gt = "greater than" (>)
    # Wenn Werte nicht passen, gibt FastAPI automatisch HTTP 422 zurück (validations error).
    lat: float = Query(..., ge=-90.0, le=90.0),
    lon: float = Query(..., ge=-180.0, le=180.0),
    radius_km: float = Query(50.0, gt=0.0, le=2000.0),
    limit: int = Query(20, gt=0, le=200),
    # Letztes mögliches Endjahr ist das aktuelle Vorjahr.
    start_year: int | None = Query(None, ge=1700),
    end_year: int | None = Query(None, ge=1700),
):
    # Ziel dieser Funktion:
    # 1) Stationen im Umkreis (radius_km) um (lat, lon) finden
    # 2) optional nach Datenabdeckung filtern (start_year/end_year)
    # 3) nach Entfernung sortieren und auf limit begrenzen

    # 1) Eingaben prüfen (fachlicher Check, den FastAPI nicht automatisch macht)
    if start_year is not None and end_year is not None and start_year > end_year:
        raise HTTPException(status_code=400, detail="start_year must be <= end_year")
    if start_year is not None and start_year > MAX_YEAR:
        raise HTTPException(status_code=400, detail=f"start_year must be <= {MAX_YEAR}")
    if end_year is not None and end_year > MAX_YEAR:
        raise HTTPException(status_code=400, detail=f"end_year must be <= {MAX_YEAR}")

    # 2) Bounding Box berechnen (schnelle Vorauswahl in SQL)
    # Ergebnis: 4 Zahlen, die ein Rechteck definieren:
    #   s.lat BETWEEN min_lat AND max_lat
    #   s.lon BETWEEN min_lon AND max_lon
    min_lat, max_lat, min_lon, max_lon = _bounding_box(lat, lon, radius_km)

    # 3) SQL bauen: Kandidaten aus der Box holen + (optional) Coverage-Filter
    #
    # Wir bauen die WHERE-Klausel modular zusammen:
    # - `where` enthält Textstücke (ohne Benutzerwerte!)
    # - `params` enthält die konkreten Werte zu den %s Platzhaltern
    #
    # WICHTIG:
    # - Wir interpolieren keine Userwerte in SQL-Strings (keine f-strings mit lat/lon etc.)
    # - Stattdessen: SQL mit %s + params Liste -> psycopg setzt Werte sicher ein
    # - Das verhindert SQL Injection
    where = ["s.lat BETWEEN %s AND %s", "s.lon BETWEEN %s AND %s"]
    params: list[object] = [min_lat, max_lat, min_lon, max_lon]

    # Filterlogik: Station ist passend, wenn sie den Zeitraum für mindestens eines der Elemente abdeckt:
    # - entweder TMIN (first<=start und last>=end)
    # - oder TMAX (first<=start und last>=end)
    if start_year is not None and end_year is not None:
        where.append(
            """(
              (sc.tmin_first_year IS NOT NULL AND sc.tmin_first_year <= %s AND sc.tmin_last_year IS NOT NULL AND sc.tmin_last_year >= %s)
              OR
              (sc.tmax_first_year IS NOT NULL AND sc.tmax_first_year <= %s AND sc.tmax_last_year IS NOT NULL AND sc.tmax_last_year >= %s)
            )"""
        )
        params.extend([start_year, end_year, start_year, end_year])
    elif start_year is not None:
        where.append(
            """(
              (sc.tmin_first_year IS NOT NULL AND sc.tmin_first_year <= %s)
              OR
              (sc.tmax_first_year IS NOT NULL AND sc.tmax_first_year <= %s)
            )"""
        )
        params.extend([start_year, start_year])
    elif end_year is not None:
        where.append(
            """(
              (sc.tmin_last_year IS NOT NULL AND sc.tmin_last_year >= %s)
              OR
              (sc.tmax_last_year IS NOT NULL AND sc.tmax_last_year >= %s)
            )"""
        )
        params.extend([end_year, end_year])

    # `sql` ist ein f-string, aber:
    # - Wir fügen damit nur unsere fest definierten WHERE-Teile zusammen
    # - Wir interpolieren hier keine Benutzerwerte!
    # Benutzerwerte sind ausschließlich in `params`.
    sql = f"""
        SELECT
          s.id, s.name, s.lat, s.lon, s.elev_m, s.state,
          sc.tmin_first_year, sc.tmin_last_year, sc.tmax_first_year, sc.tmax_last_year
        FROM stations s
        -- LEFT JOIN: auch wenn Coverage fehlt, könnten Stationen erscheinen.
        -- In unserem Fall filtern wir bei start_year/end_year aber auf IS NOT NULL,
        -- d.h. ohne Coverage fallen sie bei gesetzten Filtern sowieso raus.
        LEFT JOIN station_coverage sc ON sc.id = s.id
        WHERE {" AND ".join(where)}
        -- Sicherheits-Limit: Bounding Box kann (je nach Radius) sehr viele Stationen liefern.
        -- Wir begrenzen erstmal hart, und schneiden danach in Python auf `limit` zu.
        LIMIT 5000
    """

    # 4) Kandidaten laden + echte Distanz berechnen + auf Radius filtern
    #
    # Ablauf:
    # - SQL liefert Kandidaten (viele sind noch außerhalb des Kreises, weil Bounding Box Rechteck ist)
    # - Für jeden Kandidaten:
    #   - Distanz (Haversine) berechnen
    #   - wenn dist > radius_km -> wegwerfen
    #   - sonst -> in Ergebnisliste aufnehmen
    candidates: list[StationSearchOut] = []
    with connect() as conn:
        with conn.cursor() as cur:
            # cur.execute(sql, params):
            # - SQL enthält %s Platzhalter
            # - params ist eine Liste in genau der passenden Reihenfolge
            cur.execute(sql, params)
            for row in cur.fetchall():
                # row ist ein Tupel in der Reihenfolge des SELECT:
                # 0 id
                # 1 name
                # 2 lat
                # 3 lon
                # 4 elev_m
                # 5 state
                # 6 tmin_first_year
                # 7 tmin_last_year
                # 8 tmax_first_year
                # 9 tmax_last_year
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
    # Sortieren nach der berechneten Distanz (kleinste zuerst).
    candidates.sort(key=lambda s: s.dist_km)

    # Dann auf die gewünschte Max-Anzahl beschneiden.
    return candidates[:limit]

@app.get("/ui/stations/{station_id}")
def ui_get_station(
    request: Request,
    station_id: str,
    start_year: str | None = Query(None),
    end_year: str | None = Query(None),
):
    station = get_station(station_id)

    start_year_i = None if start_year in (None, "") else int(start_year)
    end_year_i = None if end_year in (None, "") else int(end_year)
    if start_year_i is None:
        start_year_i = 2000
    if end_year_i is None:
        end_year_i = MAX_YEAR

    return templates.TemplateResponse(
        "station.html",
        {"request": request,
         "station": station,
         "start_year": start_year_i,
         "end_year": end_year_i,
         "max_year": MAX_YEAR}
    )


@app.get("/api/stations/{station_id}/series")
def api_station_series(
    station_id: str,
    start_year: int = Query(..., ge=1700),
    end_year: int = Query(..., ge=1700),
    metrics: str = Query(...),
):
    _ = get_station(station_id)
    if start_year > end_year:
        raise HTTPException(status_code=400, detail="start_year must be <= end_year")
    if end_year > MAX_YEAR:
        raise HTTPException(status_code=400, detail=f"end_year must be <= {MAX_YEAR}")

    requested = [m.strip() for m in metrics.split(",") if m.strip()]
    if not requested or any(m not in set(ALL_METRICS) for m in requested):
        raise HTTPException(status_code=400, detail="invalid metrics")

    dly_path, sha = ensure_station_dly(station_id)
    ensure_cached_metrics(
        station_id=station_id,
        sha256=sha,
        dly_path=dly_path,
        start_year=start_year,
        end_year=end_year,
    )
    series_out = load_cached_series(
        station_id=station_id,
        sha256=sha,
        start_year=start_year,
        end_year=end_year,
        metrics=requested,
    )
    return {"station_id": station_id, "start_year": start_year, "end_year": end_year, "series": series_out}
