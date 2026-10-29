# ADR 0002: Data details from exploration notebook 01

- **Status:** Accepted
- **Date:** 2026-10-28

## Context

This initial data exploration, contained in 'notebooks/01-explore-tnp-sample.ipynb', uses three samples of the data. The first is rides that start from Nov. 5, 2023 12:00 am to Nov. 5, 2023 4:00 am, the second is a sample of those that start between Sep. 30, 2026 9:00 am to Sep. 30, 2026 11:45 pm, and the third is from Mar. 08 2024 through Mar. 15 2024. The 2023 sample covers when clocks were adjusted backward for the end of Daylight Savings Time (DST), and the 2024 sample covers the start of DST.

## Findings

- Timestamps are rounded to nearest 15 minutes. The intervals are regularly distributed (none of the 15 minute intervals has markedly more observations than the others).
- Timestamps are in the following format: "%m/%d/%Y %I:%M:%S %p"
- Timestamps are in local time. Negative durations appear only in the fall-back sample (Nov 5, 2023), and the spring-forward date (Mar 10, 2024) has no start times between 2:00 and 2:59 AM.
- All dollar-valued columns are numeric once '$' is removed except for those that are NULL. All values in "Trip Seconds" are numeric once ',' is removed except for those that are NULL.
- Across the three samples, community area pickup has a missing rate of 8.74% and dropoff is 9.15%. Of the rows with a missing Pickup Community Area, 96.9% are also missing Pickup Centroid Location. Of the rows with a missing Dropoff Community Area, 97.4% are also missing Dropoff Centroid Location. There are no changes across time in the null rate.
- The columns missing from the 2018-2022 dataset are: "Percent Time Chicago", "Percent Distance Chicago", "Shared Trip Match". All shared columns have the same names and the same types.

## Questions

1. How to handle trips with missing elapsed time?
2. How to handle trips with missing dollar columns? Is this systematic?
3. How to handle shared trips?
4. Community pickup/dropoff are missing if the respective point is outside of Chicago or for other privacy reasons. How might dropping trips with missing pickup/dropoff community areas impact results?

## Decisions

1. Step one is to forecast ridership, so elapsed time is not relevant.
2. Same as (1).
3. Include shared trips. One ID represents a trip regardless of whether or not it's shared.
