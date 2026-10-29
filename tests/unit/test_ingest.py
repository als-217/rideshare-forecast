import hashlib
import json
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from rideforecast.config import Config
from rideforecast.ingest import TRIP_KEY_SQL, IngestError, check_unique_keys, run_ingest
from rideforecast.schema import COLUMNS, PARSED

from .conftest import HEADER_OLD, trip_row


def read_trips(cfg: Config) -> list[dict]:
    """Read every published trip.

    Args:
        cfg: Config whose interim directory holds the published trips.

    Returns:
        One dictionary per trip, mapping column names to values, sorted by trip_key.
    """
    pattern = (cfg.paths.interim / "trips" / "**" / "*.parquet").as_posix()
    rel = duckdb.connect().sql(
        f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true) ORDER BY trip_key"
    )
    return [dict(zip(rel.columns, row, strict=True)) for row in rel.fetchall()]


def published_files(cfg: Config) -> list[Path]:
    """List the published monthly Parquet files.

    Args:
        cfg: Config whose interim directory holds the published trips.

    Returns:
        Paths of every published Parquet file, sorted.
    """
    return sorted((cfg.paths.interim / "trips").rglob("*.parquet"))


def test_values_parse_to_typed_columns(write_raw, make_cfg):
    """Dollar signs and thousands separators are stripped and values get their types."""
    write_raw([trip_row(Fare="$1,250.00", Trip_Seconds="1,020")])
    cfg = make_cfg()
    run_ingest(cfg)

    [trip] = read_trips(cfg)
    assert trip["fare"] == Decimal("1250.00")
    assert trip["trip_seconds"] == 1020
    assert trip["pickup_community_area"] == 8
    assert trip["shared_trip_authorized"] is False
    assert trip["trip_start_timestamp"].hour == 13
    assert trip["source_dataset"] == "n26f-ihde"


def test_percent_sign_stripped_and_kept_in_percentage_points(write_raw, make_cfg):
    """'85.5%' becomes 85.5, not 0.855."""
    write_raw([trip_row(Percent_Time_Chicago="85.5%", Percent_Distance_Chicago="100%")])
    cfg = make_cfg()
    run_ingest(cfg)

    [trip] = read_trips(cfg)
    assert trip["percent_time_chicago"] == 85.5
    assert trip["percent_distance_chicago"] == 100.0


def test_percent_sign_in_money_column_counted_as_failure(write_raw, make_cfg):
    """Each column accepts only its own format: '%' is stripped from percents only."""
    write_raw([trip_row(1), trip_row(2, Fare="12%")])
    cfg = make_cfg(max_failed_cast_rate=1.0)
    result = run_ingest(cfg)

    assert result.files[0].failed_casts["fare"] == 1


def test_output_column_types_match_schema(write_raw, make_cfg):
    """Each published column has the DuckDB type declared in schema.py."""
    write_raw([trip_row()])
    cfg = make_cfg()
    run_ingest(cfg)

    [path] = published_files(cfg)
    described = duckdb.connect().sql(f"DESCRIBE SELECT * FROM '{path.as_posix()}'").fetchall()
    types = {name: sql_type for name, sql_type, *_ in described}
    assert types["trip_key"] == "UBIGINT"
    for col in PARSED:
        assert types[col.name] == col.sql_type.replace(" ", ""), col.name


def test_dropped_columns_are_not_published(write_raw, make_cfg):
    """Trip ID and the centroid columns do not appear in interim."""
    write_raw([trip_row()])
    cfg = make_cfg()
    run_ingest(cfg)

    [trip] = read_trips(cfg)
    for col in COLUMNS:
        assert (col.name in trip) == col.keep, col.name


def test_trip_key_is_md5_prefix():
    """The key is the first 64 bits of MD5, so it never changes between runs or versions."""
    expected = int(hashlib.md5(b"abc").hexdigest()[:16], 16)
    [(key,)] = duckdb.connect().sql(f"SELECT {TRIP_KEY_SQL.format(raw="'abc'")}").fetchall()
    assert key == expected


def test_empty_values_stay_null_and_row_is_kept(write_raw, make_cfg):
    """Missing non-target values (fare, duration) do not drop the trip."""
    write_raw([trip_row(Fare="", Trip_Seconds="", Pickup_Community_Area="")])
    cfg = make_cfg()
    run_ingest(cfg)

    [trip] = read_trips(cfg)
    assert trip["fare"] is None
    assert trip["trip_seconds"] is None
    assert trip["pickup_community_area"] is None


def test_old_dataset_gets_null_optional_columns(write_raw, make_cfg):
    """The 2018-2022 header lacks three columns; they become NULL instead of failing."""
    write_raw([trip_row()], dataset="m6dm-c72p", header=HEADER_OLD)
    cfg = make_cfg()
    run_ingest(cfg)

    [trip] = read_trips(cfg)
    assert trip["percent_time_chicago"] is None
    assert trip["shared_trip_match"] is None
    assert trip["source_dataset"] == "m6dm-c72p"


def test_unexpected_column_rejected(write_raw, make_cfg):
    """A column the schema doesn't know about fails ingest instead of being ignored."""
    header = [*HEADER_OLD, "Surprise Column"]
    write_raw([trip_row()], header=header)
    with pytest.raises(IngestError, match="unexpected columns"):
        run_ingest(make_cfg())


def test_missing_required_column_rejected(write_raw, make_cfg):
    """A file without a column every dataset should have is rejected."""
    header = [c for c in HEADER_OLD if c != "Fare"]
    write_raw([trip_row()], header=header)
    with pytest.raises(IngestError, match="missing required columns"):
        run_ingest(make_cfg())


def test_duplicate_trip_ids_removed(write_raw, make_cfg):
    """Two rows with the same trip ID publish as one, and the manifest counts it."""
    write_raw([trip_row(1), trip_row(1), trip_row(2)])
    cfg = make_cfg()
    result = run_ingest(cfg)

    assert len(read_trips(cfg)) == 2
    [month] = result.files[0].months
    assert month.duplicates_removed == 1
    assert month.rows_published == 2


def test_rows_without_start_time_excluded_and_counted(write_raw, make_cfg):
    """A row with no start time is not published, but the manifest counts it."""
    write_raw([trip_row(1), trip_row(2, Trip_Start_Timestamp="")])
    cfg = make_cfg(max_failed_cast_rate=1.0)
    result = run_ingest(cfg)

    assert len(read_trips(cfg)) == 1
    assert result.files[0].rows_read == 2
    assert result.files[0].rows_without_start == 1


def test_failed_casts_above_threshold_rejected(write_raw, make_cfg):
    """An unparseable value above the allowed rate stops the file; nothing is published."""
    write_raw([trip_row(1), trip_row(2, Fare="twelve dollars")])
    cfg = make_cfg(max_failed_cast_rate=0.0)
    with pytest.raises(IngestError, match="fare"):
        run_ingest(cfg)
    assert published_files(cfg) == []


def test_failed_casts_within_threshold_become_null(write_raw, make_cfg):
    """Below the allowed rate, an unparseable value becomes NULL and is counted."""
    write_raw([trip_row(1), trip_row(2, Fare="twelve dollars")])
    cfg = make_cfg(max_failed_cast_rate=0.5)
    result = run_ingest(cfg)

    assert result.files[0].failed_casts["fare"] == 1
    fares = sorted(t["fare"] is None for t in read_trips(cfg))
    assert fares == [False, True]


def test_partitions_by_start_month(write_raw, make_cfg):
    """Trips are written to the folder of the month they started in."""
    write_raw(
        [
            trip_row(1, Trip_Start_Timestamp="03/31/2024 11:45:00 PM"),
            trip_row(2, Trip_Start_Timestamp="04/01/2024 12:00:00 AM"),
        ]
    )
    cfg = make_cfg()
    run_ingest(cfg)

    dirs = [
        p.parent.relative_to(cfg.paths.interim / "trips").as_posix() for p in published_files(cfg)
    ]
    assert dirs == ["year=2024/month=3", "year=2024/month=4"]


def test_rerun_gives_identical_output(write_raw, make_cfg):
    """Running ingest twice replaces each month rather than appending to it."""
    write_raw([trip_row(1), trip_row(1), trip_row(2)])
    cfg = make_cfg()
    run_ingest(cfg)
    first = read_trips(cfg)
    run_ingest(cfg)

    assert read_trips(cfg) == first
    assert len(published_files(cfg)) == 1


def test_month_split_across_files_rejected(write_raw, make_cfg):
    """Deduplication only works if one file holds all of a month's trips."""
    write_raw([trip_row(1)], name="a.csv.gz")
    write_raw([trip_row(2)], name="b.csv.gz")
    with pytest.raises(IngestError, match="already published"):
        run_ingest(make_cfg())


def test_key_collision_raises():
    """Two different trip IDs with the same key in one month fail the check."""
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE staged AS
        SELECT * FROM (VALUES ('id-a', 42::UBIGINT, 2024, 3), ('id-b', 42::UBIGINT, 2024, 3))
            AS t(trip_id, trip_key, year, month)
    """)
    with pytest.raises(IngestError, match="collision in 2024-03"):
        check_unique_keys(con, "staged")


def test_same_trip_same_key_is_not_a_collision():
    """Duplicates of one trip share a key legitimately."""
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE staged AS
        SELECT * FROM (VALUES ('id-a', 42::UBIGINT, 2024, 3), ('id-a', 42::UBIGINT, 2024, 3))
            AS t(trip_id, trip_key, year, month)
    """)
    check_unique_keys(con, "staged")


def test_manifest_records_run(write_raw, make_cfg):
    """The manifest records the status, checksum, rows read and months published."""
    raw_file = write_raw([trip_row(1), trip_row(2)])
    cfg = make_cfg()
    result = run_ingest(cfg)

    manifest = json.loads(result.manifest_path.read_text())
    assert manifest["status"] == "succeeded"
    [entry] = manifest["files"]
    assert entry["sha256"] == hashlib.sha256(raw_file.read_bytes()).hexdigest()
    assert entry["rows_read"] == 2
    assert entry["months"] == [
        {"year": 2024, "month": 3, "rows_published": 2, "duplicates_removed": 0}
    ]


def test_failed_run_still_writes_manifest(write_raw, make_cfg):
    """A failed run writes a manifest with status "failed" and the error message."""
    write_raw([trip_row(1, Fare="oops")])
    cfg = make_cfg()
    with pytest.raises(IngestError):
        run_ingest(cfg)

    [path] = (cfg.paths.interim / "_manifests").glob("*.json")
    manifest = json.loads(path.read_text())
    assert manifest["status"] == "failed"
    assert "fare" in manifest["error"]


def test_no_raw_files_raises(make_cfg):
    """Running ingest with no raw files raises IngestError."""
    with pytest.raises(IngestError, match="No raw files"):
        run_ingest(make_cfg())


def test_staging_is_cleaned_up(write_raw, make_cfg):
    """The staging directory is removed after the run."""
    write_raw([trip_row()])
    cfg = make_cfg()
    run_ingest(cfg)
    assert not (cfg.paths.interim / "_staging").exists()
