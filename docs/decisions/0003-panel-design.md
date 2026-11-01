# ADR 0003: Panel design

- **Status:** Proposed
- **Date:** 2026-11-01

## Context

The panel turns interim trips into the series the model forecasts: pickups per community
area per hour (ADR 0001). ADR 0002 established that timestamps are naive America/Chicago
local time, that the fall-back 1 AM hour holds two real hours of trips, and that only trips
with a pickup community area count toward the target.

A plain aggregation only returns area-hours that had trips, so quiet hours in small areas
would be missing rather than zero. Missing data must also be distinguishable from zero
demand, or features and models will treat outages and undownloaded periods as real zeros.

## Decision

- **Pickups only.** The panel counts pickups per community area and hour. An
  origin-destination table is not built; it can be derived from interim later if needed.
- **Local clock time.** Hours are local wall-clock hours, because demand follows the local
  clock. No UTC column is kept. The time column, `start_hour`, is the start of each hour; the
  name avoids the SQL keyword `hour` and any confusion with an hour-of-day number.
- **Complete grid.** Every community area (1-77) has a row for every local clock hour from
  the first to the last day with trips.
- **Zero vs. missing.** `pickups = 0` means the area had no pickups in an hour with trips
  elsewhere in the city. An hour with no trips anywhere in the city is treated as missing
  data: `pickups` is NULL and `data_gap` is true. Chicago rideshare demand never falls to
  zero citywide, so this heuristic identifies outages and undownloaded periods.
- **Daylight saving time.** The spring-forward 2 AM hour does not exist in local time and
  has no row. The fall-back 1 AM hour is kept and flagged `dst_ambiguous`, to be excluded
  from training targets and evaluation.
- **Unknown area.** Trips without a pickup community area are counted per hour in a
  separate table, so the panel and this table together reconcile exactly with interim.
- **One file.** The panel is small (about 5 million rows for the full history), so it is
  written as a single Parquet file.

## Consequences

- Lag features computed by joining on time return NULL across gaps, so missing data cannot
  be mistaken for zero demand. Features and models must handle these NULLs explicitly.
- With sparse data, such as a few test months years apart, most of the grid is NULL gap
  hours. On the full history, gaps are rare.
- The panel build fails, publishing nothing, if any trip has an area outside 1-77, starts in
  an hour that does not exist in local time, or the counts do not reconcile with interim.
