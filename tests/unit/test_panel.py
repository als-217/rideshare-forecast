import json
from datetime import datetime

import duckdb
import pytest

from rideforecast.config import Config
from rideforecast.panel import N_COMMUNITY_AREAS, PanelError, run_panel


def read_table(path) -> list[dict]:
    """Read a published Parquet file.

    Args:
        path: Location of the file.

    Returns:
        One dictionary per row, mapping column names to values, in file order.
    """
    rel = duckdb.connect().sql(f"SELECT * FROM '{path.as_posix()}'")
    return [dict(zip(rel.columns, row, strict=True)) for row in rel.fetchall()]


def panel_row(cfg: Config, area: int, start_hour: str) -> dict:
    """Find one area-hour in the published panel.

    Args:
        cfg: Config whose processed directory holds the panel.
        area: Community area number.
        start_hour: Start of the hour, as "YYYY-MM-DD HH:MM:SS".

    Returns:
        The matching panel row.
    """
    rows = read_table(cfg.paths.processed / "panel.parquet")
    target = datetime.fromisoformat(start_hour)
    [row] = [r for r in rows if r["community_area"] == area and r["start_hour"] == target]
    return row


# --- Counts and grid -----------------------------------------------------------------


def test_trips_counted_by_area_and_start_hour(write_interim, make_cfg):
    """Trips are counted in the hour they start, per pickup community area."""
    write_interim(
        [("2024-03-12 08:00:00", 8), ("2024-03-12 08:45:00", 8), ("2024-03-12 09:00:00", 8)]
    )
    cfg = make_cfg()
    run_panel(cfg)

    assert panel_row(cfg, 8, "2024-03-12 08:00:00")["pickups"] == 2
    assert panel_row(cfg, 8, "2024-03-12 09:00:00")["pickups"] == 1


def test_grid_has_every_area_and_hour(write_interim, make_cfg):
    """One day of data gives a row for each of the 77 areas in each of the 24 hours."""
    write_interim([("2024-03-12 08:00:00", 8)])
    cfg = make_cfg()
    result = run_panel(cfg)

    rows = read_table(result.panel_path)
    assert len(rows) == N_COMMUNITY_AREAS * 24
    assert len({(r["community_area"], r["start_hour"]) for r in rows}) == len(rows)


def test_area_without_pickups_is_zero(write_interim, make_cfg):
    """An area with no pickups in an hour with trips elsewhere has 0 pickups."""
    write_interim([("2024-03-12 08:00:00", 8)])
    cfg = make_cfg()
    run_panel(cfg)

    row = panel_row(cfg, 32, "2024-03-12 08:00:00")
    assert row["pickups"] == 0
    assert row["data_gap"] is False


def test_hour_without_trips_citywide_is_null_gap(write_interim, make_cfg):
    """An hour with no trips anywhere is missing data: NULL pickups, data_gap true."""
    write_interim([("2024-03-12 08:00:00", 8)])
    cfg = make_cfg()
    run_panel(cfg)

    row = panel_row(cfg, 8, "2024-03-12 03:00:00")
    assert row["pickups"] is None
    assert row["data_gap"] is True


def test_grid_spans_missing_days(write_interim, make_cfg):
    """Days with no trips between the first and last day are in the grid as gaps."""
    write_interim([("2024-03-01 12:00:00", 8), ("2024-03-03 12:00:00", 8)])
    cfg = make_cfg()
    result = run_panel(cfg)

    assert result.stats.hours == 72
    assert panel_row(cfg, 8, "2024-03-02 12:00:00")["data_gap"] is True
    assert result.stats.gap_hours_by_month == {"2024-03": 70}


def test_output_column_types(write_interim, make_cfg):
    """The panel's columns have compact, explicit types."""
    write_interim([("2024-03-12 08:00:00", 8)])
    cfg = make_cfg()
    result = run_panel(cfg)

    described = duckdb.connect().sql(f"DESCRIBE SELECT * FROM '{result.panel_path.as_posix()}'")
    types = {name: sql_type for name, sql_type, *_ in described.fetchall()}
    assert types == {
        "community_area": "SMALLINT",
        "start_hour": "TIMESTAMP",
        "pickups": "INTEGER",
        "data_gap": "BOOLEAN",
        "dst_ambiguous": "BOOLEAN",
    }


# --- Unknown area and reconciliation --------------------------------------------------


def test_trips_without_area_go_to_unknown_area_table(write_interim, make_cfg):
    """Trips without a pickup area are counted separately, not in the panel."""
    write_interim(
        [("2024-03-12 08:00:00", 8), ("2024-03-12 08:30:00", None), ("2024-03-12 09:00:00", 8)]
    )
    cfg = make_cfg()
    result = run_panel(cfg)

    unknown = {r["start_hour"]: r["pickups"] for r in read_table(result.unknown_area_path)}
    assert unknown[datetime(2024, 3, 12, 8)] == 1
    assert unknown[datetime(2024, 3, 12, 9)] == 0  # trips elsewhere, none without an area
    assert unknown[datetime(2024, 3, 12, 10)] is None  # no trips citywide: a gap
    assert result.stats.unknown_area_trips == 1
    panel_total = sum(r["pickups"] or 0 for r in read_table(result.panel_path))
    assert panel_total == 2


def test_area_out_of_range_rejected(write_interim, make_cfg):
    """A pickup community area outside 1-77 fails the audit; nothing is published."""
    write_interim([("2024-03-12 08:00:00", 78)])
    cfg = make_cfg()
    with pytest.raises(PanelError, match="outside 1-77"):
        run_panel(cfg)
    assert not (cfg.paths.processed / "panel.parquet").exists()


# --- Daylight saving time ---------------------------------------------------------------


def test_spring_forward_hour_has_no_row(write_interim, make_cfg):
    """The 2 AM hour skipped on the spring-forward date is not in the grid."""
    write_interim([("2024-03-10 12:00:00", 8)])
    cfg = make_cfg()
    result = run_panel(cfg)

    hours = {r["start_hour"] for r in read_table(result.unknown_area_path)}
    assert result.stats.hours == 23
    assert datetime(2024, 3, 10, 2) not in hours
    assert datetime(2024, 3, 10, 3) in hours


def test_trip_in_spring_forward_hour_rejected(write_interim, make_cfg):
    """A trip starting in an hour that does not exist in local time fails the audit."""
    write_interim([("2024-03-10 02:15:00", 8)])
    with pytest.raises(PanelError, match="does not exist in local time"):
        run_panel(make_cfg())


def test_fall_back_hour_flagged_ambiguous(write_interim, make_cfg):
    """The repeated 1 AM hour on the fall-back date is flagged; other hours are not."""
    write_interim([("2023-11-05 01:30:00", 8)], month_dir="year=2023/month=11")
    cfg = make_cfg()
    result = run_panel(cfg)

    assert result.stats.hours == 24
    assert result.stats.dst_ambiguous_hours == [datetime(2023, 11, 5, 1)]
    assert panel_row(cfg, 8, "2023-11-05 01:00:00")["dst_ambiguous"] is True
    assert panel_row(cfg, 8, "2023-11-05 02:00:00")["dst_ambiguous"] is False


# --- Runs and manifest ----------------------------------------------------------------


def test_no_interim_trips_raises(make_cfg):
    """Running the panel before ingest raises PanelError."""
    with pytest.raises(PanelError, match="Run ingest first"):
        run_panel(make_cfg())


def test_rerun_replaces_output(write_interim, make_cfg):
    """Rebuilding the panel replaces the previous files with the new result."""
    path = write_interim([("2024-03-12 08:00:00", 8)])
    cfg = make_cfg()
    run_panel(cfg)
    path.unlink()
    write_interim([("2024-03-12 08:00:00", 8), ("2024-03-12 08:05:00", 8)])
    run_panel(cfg)

    assert panel_row(cfg, 8, "2024-03-12 08:00:00")["pickups"] == 2
    assert sorted(p.name for p in cfg.paths.processed.glob("*.parquet")) == [
        "panel.parquet",
        "unknown_area_pickups.parquet",
    ]


def test_manifest_records_run(write_interim, make_cfg):
    """The manifest records the inputs and the panel summary."""
    input_path = write_interim([("2024-03-12 08:00:00", 8)])
    cfg = make_cfg()
    result = run_panel(cfg)

    manifest = json.loads(result.manifest_path.read_text())
    assert manifest["stage"] == "panel"
    assert manifest["status"] == "succeeded"
    assert manifest["inputs"][0]["path"] == input_path.as_posix()
    assert manifest["stats"]["trips"] == 1
    assert manifest["stats"]["gap_hours"] == 23


def test_failed_run_still_writes_manifest(write_interim, make_cfg):
    """A failed run writes a manifest with status "failed" and the error message."""
    write_interim([("2024-03-12 08:00:00", 78)])
    cfg = make_cfg()
    with pytest.raises(PanelError):
        run_panel(cfg)

    [path] = (cfg.paths.processed / "_manifests").glob("*.json")
    manifest = json.loads(path.read_text())
    assert manifest["status"] == "failed"
    assert "outside 1-77" in manifest["error"]
