from fastapi import FastAPI, Query
from pydantic import BaseModel
import psycopg

app = FastAPI(title="FastAPI Beispiele")


@app.get("/")
def root():
    return {"message": "Hello, World!"}


# 1) Query-Parameter: /add?a=1&b=2
@app.get("/add")
def add(a: float, b: float):
    return {"result": a + b}


# 2) Path-Parameter: /station/ABC123
@app.get("/station/{station_id}")
def station(station_id: str):
    return {"station_id": station_id}


# 3) Validation (Query): /search?lat=48.1&lon=8.4&radius_km=50&limit=20
@app.get("/search")
def search(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius_km: float = Query(50, gt=0, le=2000),
    limit: int = Query(20, gt=0, le=200),
):
    return {"lat": lat, "lon": lon, "radius_km": radius_km, "limit": limit}


# 4) Response-Model (Pydantic): /station-demo
class StationOut(BaseModel):
    id: str
    name: str
    lat: float
    lon: float

# 5) Database Integration: /station_list
@app.get("/station_list", response_model=StationOut)
def station_list():
    with psycopg.connect(
        dbname="weatherstations", user="user", password="example", host="localhost", port=5432
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM stations")
            rows = cur.fetchone()
            return StationOut(id=rows[0], name=rows[1], lat=rows[2], lon=rows[3])


def initialize_db():
    with psycopg.connect(
        dbname="weatherstations", user="user", password="example", host="localhost", port=5432) as conn:

        with conn.cursor() as cur:

            cur.execute("""
                        CREATE TABLE IF NOT EXISTS stations(
                            id VARCHAR PRIMARY KEY,
                            name VARCHAR,
                            lat FLOAT,
                            lon FLOAT
                        )
                        """)
            
            cur.execute("""
                        INSERT INTO stations (id, name, lat, lon) VALUES
                        ('DEMO001', 'Demo Station', 48.062, 8.493)
                        """)
            
            cur.execute("SELECT * FROM stations")
            rows = cur.fetchall()
            print(rows)

            conn.commit()