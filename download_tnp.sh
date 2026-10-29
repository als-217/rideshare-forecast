#!/usr/bin/env bash
# Download Chicago rideshare (TNP) trips for a date range as compressed CSV.
# API key can be obtained by creating an account at https://data.cityofchicago.org
#
# Usage:
#   ./download_tnp.sh START_DATE END_DATE [OUTPUT_FILE]
#
#   START_DATE  first day to include, YYYY-MM-DD
#   END_DATE    first day NOT included, YYYY-MM-DD (e.g. 2024-05-01 for all of April)
#   OUTPUT_FILE optional; defaults to ./data/tnp_START_to_END.csv.gz
#               A path ending in .gz is compressed during the download;
#               any other path (e.g. .csv) is saved as plain CSV.
#
# Optional: set COLUMNS to limit the columns (much smaller, faster downloads), e.g.
#   COLUMNS="trip_start_timestamp, trip_miles, fare" ./download_tnp.sh 2024-04-01 2024-05-01

set -euo pipefail

# Run from the script's own folder so .env and ./data are found wherever you call it from
cd "$(dirname "$0")"

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 START_DATE END_DATE [OUTPUT_FILE]" >&2
  exit 1
fi

START="$1"
END="$2"
COLUMNS="${COLUMNS:-*}"

# Load credentials from .env
if [[ ! -f .env ]]; then
  echo "Error: .env not found in $(pwd)" >&2
  exit 1
fi
set -a; source .env; set +a
: "${SOCRATA_KEY_ID:?SOCRATA_KEY_ID missing from .env}"
: "${SOCRATA_KEY_SECRET:?SOCRATA_KEY_SECRET missing from .env}"

# The data is split across three datasets; a range can't cross a boundary
for boundary in 2023-01-01 2025-01-01; do
  if [[ "$START" < "$boundary" && "$END" > "$boundary" ]]; then
    echo "Error: range crosses $boundary, where the data switches datasets." >&2
    echo "Split it into two runs, e.g. $START to $boundary and $boundary to $END." >&2
    exit 1
  fi
done

if   [[ "$START" < "2023-01-01" ]]; then DATASET="m6dm-c72p"   # 2018-2022
elif [[ "$START" < "2025-01-01" ]]; then DATASET="n26f-ihde"   # 2023-2024
else                                     DATASET="6dvr-xwnh"   # 2025-
fi

OUT="${3:-./data/raw/${DATASET}/tnp_${START}_to_${END}.csv.gz}"

QUERY="SELECT ${COLUMNS} WHERE trip_start_timestamp >= '${START}T00:00:00' AND trip_start_timestamp < '${END}T00:00:00'"

echo "Dataset: $DATASET"
echo "Range:   $START (inclusive) to $END (exclusive)"
echo "Output:  $OUT"

mkdir -p "$(dirname "$OUT")"

if ! : > "$OUT" 2>/dev/null; then
  echo "Error: cannot write to $OUT (check folder permissions)" >&2
  exit 1
fi

# Remove a partial output file if the download fails or is interrupted
trap 'rm -f "$OUT"; echo "Download failed; removed partial $OUT" >&2' ERR INT

fetch() {
  curl -u "$SOCRATA_KEY_ID:$SOCRATA_KEY_SECRET" \
    -X POST "https://data.cityofchicago.org/api/v3/views/${DATASET}/export.csv" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"${QUERY}\"}" \
    --compressed --fail
}

if [[ "$OUT" == *.gz ]]; then
  fetch | gzip -9 > "$OUT"
  lines=$(gzip -dc "$OUT" | wc -l)
else
  fetch > "$OUT"
  lines=$(wc -l < "$OUT")
fi

trap - ERR INT
echo "Done: $((lines)) lines saved to $OUT ($(du -h "$OUT" | cut -f1))"
