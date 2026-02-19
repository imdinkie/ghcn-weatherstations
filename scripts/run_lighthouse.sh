#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <STATION_ID> [START_YEAR] [END_YEAR]"
  exit 1
fi

if ! command -v npx >/dev/null 2>&1; then
  echo "npx not found. Please install Node.js first."
  exit 1
fi

STATION_ID="$1"
START_YEAR="${2:-2000}"
END_YEAR="${3:-2020}"
BASE_URL="http://localhost:8000"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if ! curl -fsS "${BASE_URL}/health" >/dev/null 2>&1; then
  echo "App is not reachable at ${BASE_URL}. Start it first (docker compose up -d)."
  exit 1
fi

TS="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${REPO_ROOT}/artifacts/lighthouse/${TS}"
mkdir -p "${OUT_DIR}"

INDEX_URL="${BASE_URL}/"
STATION_URL="${BASE_URL}/ui/stations/${STATION_ID}?start_year=${START_YEAR}&end_year=${END_YEAR}&lat=48.062&lon=8.493&radius_km=50&limit=10"

run_scan() {
  local url="$1"
  local out_prefix="$2"

  npx --yes lighthouse "$url" \
    --only-categories=performance,accessibility \
    --preset=desktop \
    --chrome-flags="--headless --no-sandbox" \
    --output=html --output=json \
    --output-path="${OUT_DIR}/${out_prefix}"
}

run_scan "$INDEX_URL" "index"
run_scan "$STATION_URL" "station"

echo "Lighthouse reports written to ${OUT_DIR}"
