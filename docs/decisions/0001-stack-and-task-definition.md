# ADR 0001: Technology stack and forecasting task definition

- **Status:** Accepted
- **Date:** 2026-10-28

## Context

The project forecasts short-term rideshare demand from the Chicago TNP trip data.
The raw data is large (hundreds of millions of rows), but aggregated to community
area x hour it is a few million rows.

## Decision

**Task.** At each hour `t`, using only information available at `t`, forecast pickup
counts per community area for `t+1` through `t+6`.

- Hourly bins, even though the source times are rounded to 15 minutes. Hourly counts are less noisy, and 15-minute counts in small zones are mostly zeros.
- One global model across zones, with each horizon evaluated separately.
- Headline metric is WAPE (robust to zero counts, unlike MAPE).
- Rolling-origin backtesting. Every feature is computed as of the forecast origin.

**Stack.**

- **DuckDB** for aggregating raw Parquet into the zone x hour panel.
- **Polars** for feature engineering and modeling data.
- **uv** for environment and lockfile management.
- **ruff, pyright, pytest, pre-commit** as the local quality gate, with **GitHub Actions** running `make check` on every push.
- **pydantic + YAML** for typed, validated configuration.
- **Typer** for a CLI with one command per pipeline stage.

## Consequences

- Aggregation and modeling are separate steps joined by Parquet files, so each can be rerun alone.
- Leakage is the main risk of this design. A unit test that checks no feature for origin `t` uses data after `t` is required once the feature pipeline exists.
- Forecasts are of realized demand only (no unfulfilled requests in the data).

## Open questions (to be resolved in later ADRs)

- Timezone convention for the panel (UTC vs. local time, given daylight saving).
- Treatment of privacy-suppressed cells versus true zeros.
- Handling of 2020 as a structural break.
- Which weather and event data to include, and how to avoid leakage from them.
