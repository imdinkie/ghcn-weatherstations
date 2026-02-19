# GHCN Weatherstations

## Lokales Setup

### 1) Python-Umgebung

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

### 2) Datenbank starten

```bash
docker compose up -d db
```

Die App nutzt `DATABASE_URL` aus `.env`, z. B.:

```env
DATABASE_URL=postgresql://user:example@localhost:5432/weatherstations
```

### 3) App starten

```bash
uvicorn app.main:app --reload --env-file .env
```

## Tests

### Teststrategie

- Unit-Tests: Parser/Mathematik/Fachlogik auf Funktionsebene
- Integrations-Tests: API-Endpunkte gegen echte Postgres-DB
- Systemtests: dokumentierte manuelle Prüfungen mit Referenzfällen

### Testlauf lokal

```bash
pytest
```

Standardmäßig werden Coverage-Reports erzeugt:

- Terminal-Ausgabe (`term-missing`)
- `coverage.xml`

### Hinweise

- Für Tests muss `DATABASE_URL` gesetzt sein.
- Tests bereinigen die Tabellen `stations`, `station_coverage`, `station_metric_cache` vor jedem Testlauf.

## CI

GitHub Actions Workflow: `.github/workflows/ci.yml`

Enthalten:

- Postgres-Service
- Installation von Runtime- und Dev-Dependencies
- `pytest` mit Coverage
- Upload von `coverage.xml` als Artefakt

## Hinweise zu Lighthouse

Lighthouse (Performance + Accessibility) ist als separater Qualitätsnachweis geplant und wird im nächsten Schritt ergänzt.
