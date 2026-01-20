from app.db import connect, ensure_schema


ensure_schema()

with connect() as conn:
    with conn.cursor() as cur:
        with open("data/stations.txt", "r", encoding="utf-8") as f:
            for line in f:
                station_id = line[0:11].strip()
                lat = float(line[12:20].strip())
                lon = float(line[21:30].strip())
                elev_m = float(line[31:37].strip())
                state = line[38:40].strip() or None
                name = line[41:71].strip()

                cur.execute(
                    """
                    INSERT INTO stations (id, lat, lon, elev_m, state, name)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (station_id, lat, lon, elev_m, state, name),
                )

        with open("data/inventory.txt", "r", encoding="utf-8") as f:
            for line in f:
                station_id = line[0:11].strip()
                # lat / long sind in inventory.txt redundant, daher nicht geparst
                element = line[31:35].strip()
                first_year = int(line[36:40].strip())
                last_year = int(line[41:45].strip())

                if element == "TMIN":
                    tmin_first_year = first_year
                    tmin_last_year = last_year
                    tmax_first_year = None
                    tmax_last_year = None
                elif element == "TMAX":
                    tmin_first_year = None
                    tmin_last_year = None
                    tmax_first_year = first_year
                    tmax_last_year = last_year
                else:
                    continue  # Nur TMIN und TMAX berücksichtigen

                cur.execute(
                    """
                    INSERT INTO station_coverage (id, tmin_first_year, tmin_last_year, tmax_first_year, tmax_last_year)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                      tmin_first_year = COALESCE(station_coverage.tmin_first_year, EXCLUDED.tmin_first_year),
                      tmin_last_year = COALESCE(station_coverage.tmin_last_year, EXCLUDED.tmin_last_year),
                      tmax_first_year = COALESCE(station_coverage.tmax_first_year, EXCLUDED.tmax_first_year),
                      tmax_last_year = COALESCE(station_coverage.tmax_last_year, EXCLUDED.tmax_last_year)
                    """,
                    (
                        station_id,
                        tmin_first_year,
                        tmin_last_year,
                        tmax_first_year,
                        tmax_last_year,
                    ),
                )
