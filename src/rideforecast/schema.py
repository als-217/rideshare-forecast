"""Column schema for the TNP trips data, from raw CSV header to interim Parquet column.

This module is the single source of truth for how ingest names, types, parses and keeps
each raw column. Changing how a column is handled means changing its entry here, so every
schema decision is visible and reviewable in one place.
"""

from dataclasses import dataclass
from typing import Literal

# How a raw string is turned into a typed value.
#
# - id: kept as text in staging only; replaced by a 64-bit key in interim.
# - timestamp: parsed with the configured strptime format.
# - number: "$" and "," stripped, then cast to the column's SQL type.
# - percent: "%" stripped, then cast. Values stay in percentage points ("85%" -> 85.0).
# - boolean: cast with DuckDB's boolean rules ("true"/"false", "t"/"f", "1"/"0").
# - text: not parsed (only used for dropped columns).
ParseKind = Literal["id", "timestamp", "number", "percent", "boolean", "text"]


@dataclass(frozen=True)
class Column:
    """How ingest handles one raw column.

    Attributes:
        raw_name: Column name in the portal's CSV header.
        name: snake_case name in interim.
        kind: How the raw string is parsed. See ParseKind.
        sql_type: DuckDB type the column is cast to.
        keep: Whether the column is written to interim.
        optional: True if some source datasets lack the column. Rows from those datasets
            get NULL. A missing non-optional column fails ingest.
        note: Why a column is dropped, or anything else unusual about it.
    """

    raw_name: str
    name: str
    kind: ParseKind
    sql_type: str
    keep: bool = True
    optional: bool = False
    note: str = ""


MONEY = "DECIMAL(10, 2)"
CENTROID_NOTE = "Dropped to save disk (ADR 0002): community areas are the spatial unit."

COLUMNS: tuple[Column, ...] = (
    Column(
        "Trip ID",
        "trip_id",
        "id",
        "VARCHAR",
        keep=False,
        note="Used for deduplication, then replaced by the 64-bit trip_key.",
    ),
    Column("Trip Start Timestamp", "trip_start_timestamp", "timestamp", "TIMESTAMP"),
    Column("Trip End Timestamp", "trip_end_timestamp", "timestamp", "TIMESTAMP"),
    Column("Trip Seconds", "trip_seconds", "number", "INTEGER"),
    Column("Trip Miles", "trip_miles", "number", "DOUBLE"),
    Column(
        "Percent Time Chicago",
        "percent_time_chicago",
        "percent",
        "DOUBLE",
        optional=True,
        note="Percentage points (0-100). Absent from the 2018-2022 dataset.",
    ),
    Column(
        "Percent Distance Chicago",
        "percent_distance_chicago",
        "percent",
        "DOUBLE",
        optional=True,
        note="Percentage points (0-100). Absent from the 2018-2022 dataset.",
    ),
    Column("Pickup Census Tract", "pickup_census_tract", "number", "BIGINT"),
    Column("Dropoff Census Tract", "dropoff_census_tract", "number", "BIGINT"),
    Column("Pickup Community Area", "pickup_community_area", "number", "SMALLINT"),
    Column("Dropoff Community Area", "dropoff_community_area", "number", "SMALLINT"),
    Column("Fare", "fare", "number", MONEY),
    Column("Tip", "tip", "number", MONEY),
    Column("Additional Charges", "additional_charges", "number", MONEY),
    Column("Trip Total", "trip_total", "number", MONEY),
    Column("Shared Trip Authorized", "shared_trip_authorized", "boolean", "BOOLEAN"),
    Column(
        "Shared Trip Match",
        "shared_trip_match",
        "boolean",
        "BOOLEAN",
        optional=True,
        note="Absent from the 2018-2022 dataset.",
    ),
    Column("Trips Pooled", "trips_pooled", "number", "SMALLINT"),
    Column(
        "Pickup Centroid Latitude",
        "pickup_centroid_latitude",
        "number",
        "DOUBLE",
        keep=False,
        note=CENTROID_NOTE,
    ),
    Column(
        "Pickup Centroid Longitude",
        "pickup_centroid_longitude",
        "number",
        "DOUBLE",
        keep=False,
        note=CENTROID_NOTE,
    ),
    Column(
        "Pickup Centroid Location",
        "pickup_centroid_location",
        "text",
        "VARCHAR",
        keep=False,
        note=CENTROID_NOTE,
    ),
    Column(
        "Dropoff Centroid Latitude",
        "dropoff_centroid_latitude",
        "number",
        "DOUBLE",
        keep=False,
        note=CENTROID_NOTE,
    ),
    Column(
        "Dropoff Centroid Longitude",
        "dropoff_centroid_longitude",
        "number",
        "DOUBLE",
        keep=False,
        note=CENTROID_NOTE,
    ),
    Column(
        "Dropoff Centroid Location",
        "dropoff_centroid_location",
        "text",
        "VARCHAR",
        keep=False,
        note=CENTROID_NOTE,
    ),
)

# The raw trip identifier, handled separately from the parsed columns.
TRIP_ID = COLUMNS[0]

# Columns parsed and written to interim, in output order. The position of a column in this
# tuple is its bit in the `parse_failures` bitmask that ingest records for each staged row.
PARSED: tuple[Column, ...] = tuple(c for c in COLUMNS if c.keep)

RAW_NAMES = frozenset(c.raw_name for c in COLUMNS)
REQUIRED_RAW_NAMES = frozenset(c.raw_name for c in COLUMNS if not c.optional)
