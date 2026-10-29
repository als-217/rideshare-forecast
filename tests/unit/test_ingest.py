import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

import rideforecast.ingest
from rideforecast.config import Config
from rideforecast.ingest import (
    TRIP_KEY_SQL,
    IngestError,
    check_unique_keys,
    parse_raw_file,
    run_ingest,
)
from rideforecast.schema import COLUMNS, PARSED

from .conftest import HEADER_OLD, trip_row

FIRST_HALF = "tnp_2024-03-01_to_2024-03-15.csv.gz"
SECOND_HALF = "tnp_2024-03-16_to_2024-03-31.csv.gz"


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


# --- Parsing -----------------------------------------------------------------------------


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


# --- Headers and parse failures ----------------------------------------------------------


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


# --- File names and declared dates -------------------------------------------------------


def test_file_name_dates_parsed():
    """The dataset comes from the folder and the dates from the file name."""
    raw_file = parse_raw_file(Path("data/raw/n26f-ihde/tnp_2024-01-01_to_2024-01-15.csv.gz"))
    assert raw_file.dataset == "n26f-ihde"
    assert (raw_file.start, raw_file.end) == (date(2024, 1, 1), date(2024, 1, 15))
    assert raw_file.month == (2024, 1)


@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("trips.csv.gz", "name must look like"),
        ("tnp_2024-01-01_to_2024-02-01.csv.gz", "more than one month"),
        ("tnp_2024-01-15_to_2024-01-01.csv.gz", "end date is before start date"),
        ("tnp_2024-02-30_to_2024-02-31.csv.gz", "invalid date"),
    ],
)
def test_invalid_file_name_rejected(write_raw, make_cfg, name, message):
    """A file whose name breaks the naming contract fails before anything is published."""
    write_raw([trip_row()], name=name)
    cfg = make_cfg()
    with pytest.raises(IngestError, match=message):
        run_ingest(cfg)
    assert published_files(cfg) == []


def test_rows_outside_declared_dates_rejected(write_raw, make_cfg):
    """A file containing trips outside the dates in its name is rejected."""
    write_raw(
        [trip_row(1), trip_row(2, Trip_Start_Timestamp="03/20/2024 09:00:00 AM")],
        name=FIRST_HALF,
    )
    cfg = make_cfg()
    with pytest.raises(IngestError, match="1 rows start outside the dates"):
        run_ingest(cfg)
    assert published_files(cfg) == []


# --- Months -------------------------------------------------------------------------------


def test_each_month_published_to_zero_padded_folder(write_raw, make_cfg):
    """Each month gets one file, in a folder with a two-digit month."""
    write_raw([trip_row(1)], name="tnp_2024-03-01_to_2024-03-31.csv.gz")
    write_raw(
        [trip_row(2, Trip_Start_Timestamp="12/05/2023 08:00:00 AM")],
        name="tnp_2023-12-01_to_2023-12-31.csv.gz",
    )
    cfg = make_cfg()
    result = run_ingest(cfg)

    dirs = [
        p.parent.relative_to(cfg.paths.interim / "trips").as_posix() for p in published_files(cfg)
    ]
    assert dirs == ["year=2023/month=12", "year=2024/month=03"]
    assert [(m.year, m.month) for m in result.months] == [(2023, 12), (2024, 3)]


def test_split_month_combined_into_one_file(write_raw, make_cfg):
    """Two files covering halves of a month publish as one complete month."""
    write_raw([trip_row(1, Trip_Start_Timestamp="03/02/2024 09:00:00 AM")], name=FIRST_HALF)
    write_raw([trip_row(2, Trip_Start_Timestamp="03/20/2024 09:00:00 AM")], name=SECOND_HALF)
    cfg = make_cfg()
    result = run_ingest(cfg)

    assert len(published_files(cfg)) == 1
    assert len(read_trips(cfg)) == 2
    [month] = result.months
    assert len(month.source_files) == 2
    assert month.missing_declared_days == []


def test_overlapping_files_deduplicated(write_raw, make_cfg):
    """A trip that appears in two overlapping files is published once."""
    shared = trip_row(1, Trip_Start_Timestamp="03/15/2024 09:00:00 AM")
    write_raw([shared], name=FIRST_HALF)
    write_raw([shared], name="tnp_2024-03-15_to_2024-03-31.csv.gz")
    cfg = make_cfg()
    result = run_ingest(cfg)

    assert len(read_trips(cfg)) == 1
    assert result.months[0].duplicates_removed == 1


def test_duplicate_trip_ids_removed(write_raw, make_cfg):
    """Two rows with the same trip ID in one file publish as one, and the manifest counts it."""
    write_raw([trip_row(1), trip_row(1), trip_row(2)])
    cfg = make_cfg()
    result = run_ingest(cfg)

    assert len(read_trips(cfg)) == 2
    [month] = result.months
    assert month.duplicates_removed == 1
    assert month.rows_published == 2


def test_failed_month_does_not_affect_earlier_months(write_raw, make_cfg):
    """A bad file stops its own month; months before it stay published."""
    write_raw(
        [trip_row(1, Trip_Start_Timestamp="02/10/2024 09:00:00 AM")],
        name="tnp_2024-02-01_to_2024-02-29.csv.gz",
    )
    write_raw([trip_row(2, Fare="oops")])
    cfg = make_cfg()
    with pytest.raises(IngestError, match="fare"):
        run_ingest(cfg)

    dirs = [p.parent.name for p in published_files(cfg)]
    assert dirs == ["month=02"]


def test_bad_file_blocks_whole_month(write_raw, make_cfg):
    """If one of a month's files fails, none of that month is published."""
    write_raw([trip_row(1, Trip_Start_Timestamp="03/02/2024 09:00:00 AM")], name=FIRST_HALF)
    write_raw(
        [trip_row(2, Trip_Start_Timestamp="03/20/2024 09:00:00 AM", Fare="oops")],
        name=SECOND_HALF,
    )
    cfg = make_cfg()
    with pytest.raises(IngestError):
        run_ingest(cfg)
    assert published_files(cfg) == []


def test_key_collision_raises():
    """Two different trip IDs with the same key in one month fail the check."""
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE staged AS
        SELECT * FROM (VALUES ('id-a', 42::UBIGINT), ('id-b', 42::UBIGINT))
            AS t(trip_id, trip_key)
    """)
    with pytest.raises(IngestError, match="2024-03: trip_key collision"):
        check_unique_keys(con, "staged", "2024-03")


def test_same_trip_same_key_is_not_a_collision():
    """Duplicates of one trip share a key legitimately."""
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE staged AS
        SELECT * FROM (VALUES ('id-a', 42::UBIGINT), ('id-a', 42::UBIGINT))
            AS t(trip_id, trip_key)
    """)
    check_unique_keys(con, "staged", "2024-03")


# --- Completeness warnings ---------------------------------------------------------------


def test_days_missing_from_file_names_warned(write_raw, make_cfg):
    """A month whose file names leave out days gets a warning, not an error."""
    write_raw([trip_row(1, Trip_Start_Timestamp="03/02/2024 09:00:00 AM")], name=FIRST_HALF)
    cfg = make_cfg()
    result = run_ingest(cfg)

    [month] = result.months
    assert month.missing_declared_days[0] == date(2024, 3, 16)
    assert len(month.missing_declared_days) == 16
    assert any("16 day(s) not covered by any raw file name" in w for w in result.warnings)


def test_declared_days_without_trips_warned(write_raw, make_cfg):
    """Days a file claims to cover but has no trips for are reported."""
    write_raw([trip_row(1)])  # declares all of March, but every trip is on March 10
    cfg = make_cfg()
    result = run_ingest(cfg)

    [month] = result.months
    assert month.days_with_trips == 1
    assert len(month.days_without_trips) == 30
    assert any(
        "30 day(s) covered by a raw file name but with no trips" in w for w in result.warnings
    )


# --- Runs and manifest ---------------------------------------------------------------------


def test_rerun_gives_identical_output(write_raw, make_cfg):
    """Running ingest twice replaces each month rather than appending to it."""
    write_raw([trip_row(1), trip_row(1), trip_row(2)])
    cfg = make_cfg()
    run_ingest(cfg)
    first = read_trips(cfg)
    run_ingest(cfg)

    assert read_trips(cfg) == first
    assert len(published_files(cfg)) == 1


def test_manifest_records_run(write_raw, make_cfg):
    """The manifest records the status, checksums, file reports and month reports."""
    raw_file = write_raw([trip_row(1), trip_row(2)])
    cfg = make_cfg()
    result = run_ingest(cfg)

    manifest = json.loads(result.manifest_path.read_text())
    assert manifest["status"] == "succeeded"
    [file_entry] = manifest["files"]
    assert file_entry["sha256"] == hashlib.sha256(raw_file.read_bytes()).hexdigest()
    assert file_entry["rows_read"] == 2
    assert file_entry["declared_start"] == "2024-03-01"
    [month_entry] = manifest["months"]
    assert (month_entry["year"], month_entry["month"]) == (2024, 3)
    assert month_entry["rows_published"] == 2
    assert month_entry["source_files"] == [raw_file.as_posix()]
    assert manifest["warnings"]


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
    assert manifest["files"][0]["failed_casts"]["fare"] == 1


def test_no_raw_files_raises(make_cfg):
    """Running ingest with no raw files raises IngestError."""
    with pytest.raises(IngestError, match="No raw files"):
        run_ingest(make_cfg())


def test_only_one_month_staged_at_a_time(write_raw, make_cfg, monkeypatch):
    """Each month's staging is deleted before the next month starts."""
    write_raw(
        [trip_row(1, Trip_Start_Timestamp="02/10/2024 09:00:00 AM")],
        name="tnp_2024-02-01_to_2024-02-29.csv.gz",
    )
    write_raw([trip_row(2)])
    cfg = make_cfg()
    staging_dir = cfg.paths.interim / "_staging"
    staged_at_publish = []
    real_publish = rideforecast.ingest.publish_month

    def recording_publish(*args, **kwargs):
        """Record which month folders exist in staging, then publish as normal."""
        staged_at_publish.append(sorted(p.name for p in staging_dir.iterdir()))
        return real_publish(*args, **kwargs)

    monkeypatch.setattr(rideforecast.ingest, "publish_month", recording_publish)
    run_ingest(cfg)

    assert staged_at_publish == [["2024-02"], ["2024-03"]]


def test_staging_is_cleaned_up(write_raw, make_cfg):
    """The staging directory is removed after the run."""
    write_raw([trip_row()])
    cfg = make_cfg()
    run_ingest(cfg)
    assert not (cfg.paths.interim / "_staging").exists()


def test_staging_is_cleaned_up_after_failure(write_raw, make_cfg):
    """A failed month's staging files are removed too."""
    write_raw([trip_row(1, Fare="oops")])
    cfg = make_cfg()
    with pytest.raises(IngestError):
        run_ingest(cfg)
    assert not (cfg.paths.interim / "_staging").exists()
