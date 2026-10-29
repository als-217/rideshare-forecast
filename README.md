# Chicago Rideshare Demand Forecast

Short-term spatiotemporal demand forecasting on Chicago rideshare (TNP) trip data.

> **Status:** ingest implemented. The remaining stages are CLI stubs.

## The forecasting task

At each hour `t`, using only information available at `t`, forecast the number of
trip pickups in each Chicago community area for hours `t+1` through `t+6`.

- **Grain:** community area x hour
- **Horizons:** 1 to 6 hours ahead, evaluated separately
- **Target:** realized (fulfilled) trip counts. Unfulfilled requests are not in the data.
- **Headline metric:** WAPE. Quantile forecasts are also scored with pinball loss and interval coverage.
- **Evaluation:** rolling-origin backtests (train on the past, predict the next window), never a random split.

See [ADR 0001](docs/decisions/0001-stack-and-task-definition.md) for the reasoning.

## Setup

Requires [uv](https://docs.astral.sh/uv/) and git. uv installs the pinned Python version itself.

```bash
git clone https://github.com/als-217/rideshare-forecast
cd rideshare-forecast
make setup      # installs dependencies and git hooks
make check      # lint + type check + tests
```

## Pipeline

Each stage reads and writes files in a known location (set in `configs/base.yaml`),
so any stage can be rerun in isolation.

| Stage | Command | Reads | Writes | Status |
|---|---|---|---|---|
| Ingest | `uv run rideforecast ingest` | `data/raw` | `data/interim/trips` | done |
| Panel | `uv run rideforecast panel` | `data/interim` | `data/processed` | not implemented |
| Features | `uv run rideforecast features` | `data/processed` | `data/processed` | not implemented |
| Train | `uv run rideforecast train` | `data/processed` | models | not implemented |
| Backtest | `uv run rideforecast backtest` | models + features | metrics | not implemented |

Every stage accepts `--config / -c` to use a different YAML config.

## Data

Source: the City of Chicago Data Portal, Transportation Network Providers trip data,
published as three datasets:

| Period | Dataset ID |
|---|---|
| Nov 2018 - Dec 2022 | `m6dm-c72p` |
| 2023 - 2024 | `n26f-ihde` |
| 2025 onward | `6dvr-xwnh` |

Put gzipped CSV exports in a folder named after their dataset ID. Each file's name declares
the trips it contains, by start date (both dates inclusive, within one calendar month):

```
data/raw/n26f-ihde/tnp_2024-01-01_to_2024-01-31.csv.gz
```

A month may be split across several files, and overlapping files are fine: duplicate trips
are removed. Ingest rejects a file whose name breaks this pattern, spans two months, or
contains trips outside its declared dates.

`ingest` processes one month at a time, so disk use peaks at one month of staging. Each
column is parsed with the rules in `src/rideforecast/schema.py`, and each month is written
to `data/interim/trips/year=YYYY/month=MM/data.parquet`. A failed audit stops the run before
that month is published; earlier months stay published. Months with days missing from the
file names, or with declared days that have no trips, produce a warning.

Each run writes a manifest to `data/interim/_manifests/` with checksums, row counts, parse
failures, duplicates removed and coverage for every file and month.

To read the trips with the year and month folders as integer columns:

```sql
SELECT * FROM read_parquet(
    'data/interim/trips/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'year': INTEGER, 'month': INTEGER}
)
```

## Repository layout

```
configs/        YAML configuration
docs/decisions/ architecture decision records (ADRs)
sql/            DuckDB queries (planned)
src/rideforecast/
  config.py     typed config loading and validation
  cli.py        one command per pipeline stage
tests/unit/     unit tests
data/           raw, interim, processed (gitignored)
```

## Development

| Command | What it does |
|---|---|
| `make lint` | ruff lint and format check |
| `make format` | auto-fix lint issues and format |
| `make typecheck` | pyright |
| `make test` | pytest |
| `make check` | all of the above (what CI runs) |

## Known limitations

- Only fulfilled trips are observed, so the model forecasts realized demand, which is shaped by supply and pricing.
- The portal publishes data with a lag, so this simulates an operational setting on historical data rather than running in real time.
- Times are rounded to 15 minutes and some geography is suppressed for privacy.
