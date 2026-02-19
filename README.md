# GHCN Weatherstations

## Schnellstart (One-command)

```bash
docker compose up --build
```

Danach erreichbar unter:

- App: `http://localhost:8000`
- API Docs: `http://localhost:8000/docs`

Beim Start macht der App-Container automatisch:

1. auf die Datenbank warten
2. Metadaten laden (falls `data/stations.txt` oder `data/inventory.txt` fehlen)
3. Metadaten in Postgres importieren
4. FastAPI starten

## Optional: Adminer (DB-UI)

Adminer ist ein separates Debug-Tool für die Datenbank und läuft daher auf einem eigenen Port.

Start:

```bash
docker compose --profile debug up -d adminer
```

Zugriff:

- Adminer: `http://localhost:8080`

Hinweis:

- Port `8000` = App
- Port `8080` = Adminer

## Optionale Konfiguration per `.env`

Für den reinen Compose-Start ist keine `.env` zwingend nötig, da `docker-compose.yml` Defaults enthält.

Wenn du Defaults überschreiben willst:

```bash
cp .env.example .env
```

Beispiele für Override-Werte:

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `POSTGRES_PORT`
- `APP_PORT`

## Lokale Entwicklung ohne App-Container

DB im Container, App lokal:

```bash
docker compose up -d db
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
uvicorn app.main:app --reload --env-file .env
```

Wichtig: Beim lokalen App-Start muss `DATABASE_URL` auf `localhost` zeigen, z. B.:

```env
DATABASE_URL=postgresql://user:example@localhost:5432/weatherstations
```

## Tests

```bash
source .venv/bin/activate
pytest
```

- Unit-Tests: Parser/Mathematik/Fachlogik
- Integrations-Tests: API gegen echte Postgres-DB
- Coverage: Terminal + `coverage.xml`

## CI

Workflow: `.github/workflows/ci.yml`

- startet Postgres-Service
- installiert Dependencies
- führt `pytest` mit Coverage aus
- führt einen Container-Smoke-Test (`docker compose up` + `/health`) aus
- lädt `coverage.xml` als Artefakt hoch

## Lighthouse (manuell)

Reproduzierbarer Lauf mit Hilfsskript:

```bash
scripts/run_lighthouse.sh <STATION_ID>
```

Beispiel:

```bash
scripts/run_lighthouse.sh GME00121330
```

Details, Scope und Dokumentationsanforderungen:

- `docs/lighthouse-manual.md`
