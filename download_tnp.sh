#!/usr/bin/env bash
# Download Chicago rideshare (TNP) trips for a date range as compressed CSV.
# API key can be obtained by creating an account at https://data.cityofchicago.org
#
# Usage:
#   ./download_tnp.sh START_DATE END_DATE
#
#   START_DATE  first day to include, YYYY-MM-DD
#   END_DATE    last day to include, YYYY-MM-DD (e.g. 2024-04-30 for all of April)
#
# Both dates must fall in the same month. Output is always saved to
#   ./data/raw/DATASET/tnp_START_to_END.csv.gz
#
# Optional: set COLUMNS to limit the columns (much smaller, faster downloads), e.g.
#   COLUMNS="trip_start_timestamp, trip_miles, fare" ./download_tnp.sh 2024-04-01 2024-04-30

set -euo pipefail

# Run from the script's own folder so .env and ./data are found wherever you call it from
cd "$(dirname "$0")"

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 START_DATE END_DATE   (both YYYY-MM-DD, same month, inclusive)" >&2
  exit 1
fi

START="$1"
END="$2"
COLUMNS="${COLUMNS:-*}"

# Check a date is YYYY-MM-DD and a real calendar day (rejects e.g. 2024-04-31).
# Tries macOS/BSD date first, then GNU/Linux date.
valid_date() {
  [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  local norm
  norm=$(date -j -f "%Y-%m-%d" "$1" "+%Y-%m-%d" 2>/dev/null \
      || date -d "$1" "+%Y-%m-%d" 2>/dev/null) || return 1
  [[ "$norm" == "$1" ]]
}

for d in "$START" "$END"; do
  if ! valid_date "$d"; then
    echo "Error: '$d' is not a valid date in YYYY-MM-DD format" >&2
    exit 1
  fi
done

if [[ "$START" > "$END" ]]; then
  echo "Error: START_DATE ($START) is after END_DATE ($END)" >&2
  exit 1
fi

# Both dates must be in the same month (this also keeps the range within one dataset)
if [[ "${START:0:7}" != "${END:0:7}" ]]; then
  echo "Error: START_DATE ($START) and END_DATE ($END) are in different months." >&2
  echo "Each download must cover a single month; split it into one run per month." >&2
  exit 1
fi

# Load credentials from .env
if [[ ! -f .env ]]; then
  echo "Error: .env not found in $(pwd)" >&2
  exit 1
fi
set -a; source .env; set +a
: "${SOCRATA_KEY_ID:?SOCRATA_KEY_ID missing from .env}"
: "${SOCRATA_KEY_SECRET:?SOCRATA_KEY_SECRET missing from .env}"

if   [[ "$START" < "2023-01-01" ]]; then DATASET="m6dm-c72p"   # 2018-2022
elif [[ "$START" < "2025-01-01" ]]; then DATASET="n26f-ihde"   # 2023-2024
else                                     DATASET="6dvr-xwnh"   # 2025-
fi

OUT="./data/raw/${DATASET}/tnp_${START}_to_${END}.csv.gz"

# END is inclusive: take every trip up to the last moment of END's day
QUERY="SELECT ${COLUMNS} WHERE trip_start_timestamp >= '${START}T00:00:00' AND trip_start_timestamp <= '${END}T23:59:59.999'"

echo "Dataset: $DATASET"
echo "Range:   $START to $END (both inclusive)"
echo "Output:  $OUT"

mkdir -p "$(dirname "$OUT")"

# Remove a partial output file if the download fails or is interrupted
trap 'rm -f "$OUT"; echo "Download failed; removed partial $OUT" >&2' ERR INT

curl -u "$SOCRATA_KEY_ID:$SOCRATA_KEY_SECRET" \
  -X POST "https://data.cityofchicago.org/api/v3/views/${DATASET}/export.csv" \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"${QUERY}\"}" \
  --compressed --fail \
  | gzip -9 > "$OUT"

trap - ERR INT
lines=$(gzip -dc "$OUT" | wc -l)
echo "Done: $((lines)) lines saved to $OUT ($(du -h "$OUT" | cut -f1))"
