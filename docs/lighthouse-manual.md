# Lighthouse-Nachweis (manuell, reproduzierbar)

Dieses Dokument beschreibt, wie die geforderten Lighthouse-Nachweise für

- Performance
- Accessibility

erstellt werden.

## Scope

Es werden zwei Seiten gescannt:

1. Startseite: `http://localhost:8000/`
2. Stationsseite: `http://localhost:8000/ui/stations/<STATION_ID>?start_year=2000&end_year=2020&lat=48.062&lon=8.493&radius_km=50&limit=10`

## Voraussetzungen

1. App läuft lokal per Docker Compose:

```bash
docker compose up --build -d
```

2. Node.js + `npx` sind installiert.

## Reproduzierbarer Lauf (empfohlen)

```bash
scripts/run_lighthouse.sh <STATION_ID>
```

Beispiel:

```bash
scripts/run_lighthouse.sh GME00121330
```

Erzeugte Reports liegen unter:

- `artifacts/lighthouse/<timestamp>/index.report.html`
- `artifacts/lighthouse/<timestamp>/index.report.json`
- `artifacts/lighthouse/<timestamp>/station.report.html`
- `artifacts/lighthouse/<timestamp>/station.report.json`

Lighthouse-CLI-Protip:

- Mit `--view` kann der HTML-Report direkt im Browser geöffnet werden.
- Beispiel:

```bash
npx lighthouse http://localhost:8000 --view
```

## Manuelle Browser-Variante (Alternative)

1. Chrome öffnen
2. DevTools -> Lighthouse
3. Kategorie `Performance` und `Accessibility` auswählen
4. Für beide Seiten Report erzeugen
5. Reports speichern und Datum/Commit notieren

## Dokumentationspflicht

Für die Abgabe mindestens festhalten:

1. Datum/Uhrzeit
2. Commit-Hash
3. getestete URLs
4. Chrome-Version
5. Scores:
   - Performance (0-100)
   - Accessibility (0-100)
6. kurze Interpretation (wichtigste 2-3 Findings)

## Zielbezug

Die Nachweise decken die geforderten Lighthouse-Scans aus den Qualitätskriterien ab.
