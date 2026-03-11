# GHCN Weatherstations

## Schnellstart (Empfohlen: Release-Compose mit GHCR-Image)

Veröffentlichtes App-Image direkt per Compose starten:

```bash
docker compose -f docker-compose.release.yml up -d
```

Danach erreichbar unter:

- App: `http://localhost:8000`
- API Docs: `http://localhost:8000/docs`

Hinweise:

- Beim ersten Start zieht Compose das veröffentlichte App-Image aus GHCR.
- Beim ersten Start lädt die App Metadaten und importiert sie in Postgres.
- Der erste Start kann deshalb deutlich länger dauern als Folge-Starts.

Stoppen und aufräumen:

```bash
docker compose -f docker-compose.release.yml down -v
```

## Alternative: Lokaler Build per Compose

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

## Manuelle Alternative ohne Compose

Falls der Release-Weg ohne Compose benötigt wird, kann das veröffentlichte Image auch manuell gestartet werden:

```bash
docker network create ghcn-weatherstations-net
docker run -d --name ghcn-weatherstations-db \
  --network ghcn-weatherstations-net \
  -e POSTGRES_USER=user \
  -e POSTGRES_PASSWORD=example \
  -e POSTGRES_DB=weatherstations \
  postgres:16
docker run -d --name ghcn-weatherstations-app \
  --network ghcn-weatherstations-net \
  -p 8000:8000 \
  -e POSTGRES_HOST=ghcn-weatherstations-db \
  -e POSTGRES_PORT=5432 \
  -e POSTGRES_USER=user \
  -e POSTGRES_PASSWORD=example \
  -e POSTGRES_DB=weatherstations \
  -e DATABASE_URL=postgresql://user:example@ghcn-weatherstations-db:5432/weatherstations \
  -e APP_PORT=8000 \
  ghcr.io/imdinkie/ghcn-weatherstations:latest
```

## Docker Hot Reload (Entwicklung)

Für Entwicklung mit automatischem Reload bei Codeänderungen:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build
```

Hinweise:

- Die App läuft weiterhin auf `http://localhost:8000`.
- Änderungen in `app/` und `scripts/` werden im laufenden Container automatisch neu geladen.
- Für den normalen Betrieb ohne Reload weiter nur `docker compose up --build` verwenden.
- Wenn Port `8000` auf dem Host belegt ist, kannst du z. B. mit `APP_PORT=8001 docker compose up --build` auf einen anderen Host-Port ausweichen.

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
- `APP_PORT` (nur Host-Port der App; der Container selbst lauscht intern immer auf `8000`)

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

Die Test-Suite enthält Integrations-Tests gegen eine echte Postgres-DB.
Vor `pytest` muss daher die Datenbank laufen, z. B. so:

```bash
docker compose up -d db
source .venv/bin/activate
pytest
```

- Unit-Tests: Parser/Mathematik/Fachlogik
- Integrations-Tests: API gegen echte Postgres-DB
- Erwartet `DATABASE_URL`, standardmäßig aus `.env` mit `localhost:5432`
- Coverage: Terminal + `coverage.xml`

## CI

Workflow: `.github/workflows/ci.yml`

- startet Postgres-Service
- installiert Dependencies
- führt `pytest` mit Coverage aus
- führt einen Container-Smoke-Test (`docker compose up` + `/health`) aus
- veröffentlicht bei Push auf `master` ein Container-Image nach `ghcr.io/imdinkie/ghcn-weatherstations`
- lädt `coverage.xml` als Artefakt hoch

## Container Image

Bei erfolgreichen Pushes auf `master` wird das App-Image automatisch nach GHCR veröffentlicht:

- `ghcr.io/imdinkie/ghcn-weatherstations:latest`
- `ghcr.io/imdinkie/ghcn-weatherstations:sha-<commit>`

Beispiel:

```bash
docker pull ghcr.io/imdinkie/ghcn-weatherstations:latest
docker run --rm -p 8000:8000 ghcr.io/imdinkie/ghcn-weatherstations:latest
```

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
