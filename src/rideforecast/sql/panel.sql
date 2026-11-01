-- Build the community area x hour pickup panel from the interim trips.
--
-- Variables, set by panel.py before this script runs:
--   trips_glob  Glob matching the interim trip Parquet files.
--   n_areas     Number of community areas, numbered 1 to n_areas.
--
-- Creates temporary tables, which panel.py audits before publishing:
--   trips                 The interim trip columns the panel needs, plus each trip's start hour.
--   local_hours           Every local clock hour from the first to the last day with trips.
--   city_hours            Trips per hour across the whole city, with or without an area.
--   panel                 Pickups per community area and hour, on the complete grid.
--   unknown_area_pickups  Pickups per hour for trips without a pickup community area.
--
-- Conventions (ADR 0003):
--   pickups = 0     the area had no pickups in an hour with trips elsewhere in the city.
--   pickups = NULL  no trips anywhere in the city that hour: missing data, not zero demand.

-- Timestamps are naive America/Chicago local time (ADR 0002). Generating hours as real
-- instants in this time zone and converting each back to local clock time gives the true
-- local clock: the spring-forward 2 AM hour never appears, and the fall-back 1 AM hour
-- appears twice, which is how it is detected.
SET TimeZone = 'America/Chicago';

CREATE OR REPLACE TEMP TABLE trips AS
SELECT
    trip_start_timestamp,
    pickup_community_area,
    date_trunc('hour', trip_start_timestamp) AS start_hour
FROM read_parquet(getvariable('trips_glob'));

CREATE OR REPLACE TEMP TABLE local_hours AS
WITH bounds AS (
    SELECT
        min(trip_start_timestamp)::DATE AS first_day,
        max(trip_start_timestamp)::DATE + 1 AS end_day
    FROM trips
),

instants AS (
    SELECT
        unnest(generate_series(
            first_day::TIMESTAMP::TIMESTAMPTZ,
            end_day::TIMESTAMP::TIMESTAMPTZ - INTERVAL 1 HOUR,
            INTERVAL 1 HOUR
        )) AS instant
    FROM bounds
)

SELECT
    instant::TIMESTAMP AS start_hour,
    count(*) > 1 AS dst_ambiguous
FROM instants
GROUP BY start_hour;

CREATE OR REPLACE TEMP TABLE city_hours AS
SELECT
    start_hour,
    count(*) AS trips
FROM trips
GROUP BY start_hour;

CREATE OR REPLACE TEMP TABLE panel AS
WITH areas AS (
    SELECT range::SMALLINT AS community_area
    FROM range(1, getvariable('n_areas') + 1)
),

area_counts AS (
    SELECT
        pickup_community_area AS community_area,
        start_hour,
        count(*) AS pickups
    FROM trips
    WHERE pickup_community_area IS NOT NULL
    GROUP BY pickup_community_area, start_hour
)

SELECT
    a.community_area,
    h.start_hour,
    CASE WHEN c.trips IS NULL THEN NULL ELSE coalesce(ac.pickups, 0) END::INTEGER AS pickups,
    c.trips IS NULL AS data_gap,
    h.dst_ambiguous
FROM areas AS a
CROSS JOIN local_hours AS h
LEFT JOIN city_hours AS c ON h.start_hour = c.start_hour
LEFT JOIN area_counts AS ac ON a.community_area = ac.community_area AND h.start_hour = ac.start_hour
ORDER BY a.community_area, h.start_hour;

CREATE OR REPLACE TEMP TABLE unknown_area_pickups AS
WITH unknown_counts AS (
    SELECT
        start_hour,
        count(*) AS pickups
    FROM trips
    WHERE pickup_community_area IS NULL
    GROUP BY start_hour
)

SELECT
    h.start_hour,
    CASE WHEN c.trips IS NULL THEN NULL ELSE coalesce(u.pickups, 0) END::INTEGER AS pickups,
    c.trips IS NULL AS data_gap,
    h.dst_ambiguous
FROM local_hours AS h
LEFT JOIN city_hours AS c ON h.start_hour = c.start_hour
LEFT JOIN unknown_counts AS u ON h.start_hour = u.start_hour
ORDER BY h.start_hour;
