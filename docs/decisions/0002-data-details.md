# ADR 0002: Data details from exploration notebook 01

## Context

This initial data exploration, contained in 'notebooks/01-explore-tnp-sample.ipynb', uses three samples of the data. The first is rides that start from Nov. 5, 2023 12:00 am to Nov. 5, 2023 4:00 am, the second is a sample of those that start between Sep 30, 2026 9:00 am to Sep 30, 2026 11:45 pm, and the third is all of March 2024. The 2023 sample covers when clocks were adjusted backward for the end of Daylight Savings Time (DST), and the 2024 sample covers the start of DST.

## Findings

- Timestamps are binned in 15 minute intervals. The intervals are regularly distributed (none of the 15 minute intervals has markedly more observations than the others).
- Timestamps are in the following format: "%m/%d/%Y %I:%M:%S %p"
- Timestamps are in local time. There are trips with end times before start times from the 2023 sample (start time between Nov 5 12 am and Nov 5 4 am) but none in the 2026 sample (start time between Sep 30 9 am to Sep 30 11:45 pm). Since negative times are only present in the sample that covers DST, this implies timestamps are in CT.
- All dollar-valued columns are numeric once '$' is removed except for those that are NULL. All values in "Trip Seconds" are numeric once ',' is removed except for those that are NULL.
- Community area pickup has a missing rate of 8.3% and dropoff is 10.5%.

## Questions

1. How to handle trips with missing elapsed time?
2. How to handle trips with missing dollar columns? Is this systematic?
3. How to handle shared trips?
4. Community pickup/dropoff are missing if the respective point is outside of Chicago or for other privacy reasons. How might dropping trips with missing pickup/dropoff community areas impact results?

## Decisions
