"""Panel stage: interim trips -> pickups per community area per local clock hour.

The panel is a complete grid: every community area has a row for every local clock hour
from the first to the last day with trips. Pickups are counted by start hour for trips
with a pickup community area (ADR 0002). Trips without one are counted per hour in a
separate unknown-area table, so the two together reconcile exactly with interim.

Hours with no trips anywhere in the city are missing data, not zero demand: their pickups
are NULL and data_gap is true. The fall-back 1 AM hour holds two real hours of trips and is
flagged dst_ambiguous. The spring-forward 2 AM hour does not exist in local time and has no
row. The SQL is in `sql/panel.sql`; this module runs it, audits the result, and publishes
both tables only if every check passes.
"""

import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path

import duckdb

from rideforecast.common import quote_str, write_manifest
from rideforecast.config import Config

# Chicago's 77 community areas are numbered 1 to 77.
N_COMMUNITY_AREAS = 77

PANEL_FILE = "panel.parquet"
UNKNOWN_AREA_FILE = "unknown_area_pickups.parquet"


class PanelError(Exception):
    """The panel failed an audit check. Nothing is published."""


@dataclass
class PanelStats:
    """Summary of a built panel. Serialized into the run manifest.

    Attributes:
        trips: Trips read from interim.
        unknown_area_trips: Trips without a pickup community area.
        hours: Local clock hours in the grid.
        first_hour: First hour in the grid.
        last_hour: Last hour in the grid.
        gap_hours: Hours with no trips anywhere in the city.
        gap_hours_by_month: Gap hours per month ("YYYY-MM"), for months that have any.
        dst_ambiguous_hours: Fall-back hours that contain two real hours of trips.
    """

    trips: int = 0
    unknown_area_trips: int = 0
    hours: int = 0
    first_hour: datetime | None = None
    last_hour: datetime | None = None
    gap_hours: int = 0
    gap_hours_by_month: dict[str, int] = field(default_factory=dict)
    dst_ambiguous_hours: list[datetime] = field(default_factory=list)


@dataclass
class PanelResult:
    """Outcome of a successful run.

    Attributes:
        panel_path: Location of the published panel.
        unknown_area_path: Location of the published unknown-area pickups.
        manifest_path: Location of the JSON manifest written for the run.
        stats: Summary of the panel.
    """

    panel_path: Path
    unknown_area_path: Path
    manifest_path: Path
    stats: PanelStats


def run_panel(cfg: Config) -> PanelResult:
    """Build, audit and publish the pickup panel from the interim trips.

    Args:
        cfg: Validated project config.

    Returns:
        The published file locations, the manifest location and a summary.

    Raises:
        PanelError: If there are no interim trips or the panel fails an audit check.
            Nothing is published. The manifest is still written, with status "failed"
            and the error message.
    """
    trips_dir = cfg.paths.interim / "trips"
    input_files = sorted(trips_dir.rglob("*.parquet"))
    out_dir = cfg.paths.processed
    manifest_dir = out_dir / "_manifests"

    started = datetime.now(UTC)
    stats = PanelStats()
    status, error = "succeeded", None
    con = duckdb.connect()
    try:
        if not input_files:
            raise PanelError(f"No interim trips found in {trips_dir}. Run ingest first.")
        build_panel(con, trips_dir)
        stats = audit_panel(con)
        panel_path, unknown_area_path = publish_panel(con, out_dir)
    except PanelError as exc:
        status, error = "failed", str(exc)
        raise
    except duckdb.Error as exc:
        status, error = "failed", str(exc)
        raise PanelError(str(exc)) from exc
    finally:
        con.close()
        manifest_path = write_manifest(
            manifest_dir,
            "panel",
            cfg,
            started,
            status,
            error,
            {
                "inputs": [
                    {"path": p.as_posix(), "size_bytes": p.stat().st_size} for p in input_files
                ],
                "stats": asdict(stats),
            },
        )
    return PanelResult(panel_path, unknown_area_path, manifest_path, stats)


def build_panel(con: duckdb.DuckDBPyConnection, trips_dir: Path) -> None:
    """Run the panel SQL, creating its tables in the connection.

    Args:
        con: DuckDB connection. The panel's temporary tables are created in it.
        trips_dir: Root of the interim monthly trip files.
    """
    trips_glob = (trips_dir / "**" / "*.parquet").as_posix()
    con.execute(f"SET VARIABLE trips_glob = {quote_str(trips_glob)}")
    con.execute(f"SET VARIABLE n_areas = {N_COMMUNITY_AREAS}")
    con.execute(files("rideforecast").joinpath("sql", "panel.sql").read_text())


def audit_panel(con: duckdb.DuckDBPyConnection) -> PanelStats:
    """Check the built panel and summarize it.

    Args:
        con: DuckDB connection holding the tables created by build_panel.

    Returns:
        A summary of the panel.

    Raises:
        PanelError: If any trip has a community area outside 1 to N_COMMUNITY_AREAS,
            any trip starts in an hour that does not exist in local time, the grid is
            not one row per area and hour, or the pickups do not add up to the trips.
    """
    trips, unknown_area_trips, bad_areas = con.sql(f"""
        SELECT
            count(*)::BIGINT,
            count(*) FILTER (WHERE pickup_community_area IS NULL)::BIGINT,
            count(*) FILTER (
                WHERE pickup_community_area NOT BETWEEN 1 AND {N_COMMUNITY_AREAS}
            )::BIGINT
        FROM trips
    """).fetchall()[0]
    if bad_areas:
        raise PanelError(
            f"{bad_areas:,} trips have a pickup community area outside 1-{N_COMMUNITY_AREAS}"
        )

    [(off_grid,)] = con.sql("""
        SELECT count(*)::BIGINT FROM trips ANTI JOIN local_hours USING (start_hour)
    """).fetchall()
    if off_grid:
        raise PanelError(
            f"{off_grid:,} trips start in an hour that does not exist in local time "
            "(the spring-forward hour). Check the timestamps' time zone."
        )

    hours, first_hour, last_hour = con.sql("""
        SELECT count(*)::BIGINT, min(start_hour), max(start_hour) FROM local_hours
    """).fetchall()[0]
    rows, distinct_rows, panel_pickups = con.sql("""
        SELECT
            count(*)::BIGINT,
            count(DISTINCT (community_area, start_hour))::BIGINT,
            coalesce(sum(pickups), 0)::BIGINT
        FROM panel
    """).fetchall()[0]
    expected_rows = N_COMMUNITY_AREAS * hours
    if rows != expected_rows or distinct_rows != rows:
        raise PanelError(
            f"Panel grid has {rows:,} rows ({distinct_rows:,} distinct area-hours); "
            f"expected {expected_rows:,} ({N_COMMUNITY_AREAS} areas x {hours:,} hours)"
        )

    [(unknown_pickups,)] = con.sql("""
        SELECT coalesce(sum(pickups), 0)::BIGINT FROM unknown_area_pickups
    """).fetchall()
    if panel_pickups + unknown_pickups != trips:
        raise PanelError(
            f"Pickups do not reconcile: {panel_pickups:,} in the panel + {unknown_pickups:,} "
            f"without an area != {trips:,} trips in interim"
        )

    gaps = con.sql("""
        SELECT strftime(start_hour, '%Y-%m') AS month, count(*)::BIGINT AS gap_hours
        FROM unknown_area_pickups
        WHERE data_gap
        GROUP BY month
        ORDER BY month
    """).fetchall()
    ambiguous = con.sql("""
        SELECT start_hour FROM local_hours WHERE dst_ambiguous ORDER BY start_hour
    """).fetchall()

    return PanelStats(
        trips=trips,
        unknown_area_trips=unknown_area_trips,
        hours=hours,
        first_hour=first_hour,
        last_hour=last_hour,
        gap_hours=sum(n for _, n in gaps),
        gap_hours_by_month=dict(gaps),
        dst_ambiguous_hours=[start_hour for (start_hour,) in ambiguous],
    )


def publish_panel(con: duckdb.DuckDBPyConnection, out_dir: Path) -> tuple[Path, Path]:
    """Write the panel and unknown-area pickups, replacing any previous versions.

    Both files are written under temporary names first and renamed only once both
    writes succeed, so the pair is never left half-updated.

    Args:
        con: DuckDB connection holding the audited tables.
        out_dir: Directory to write the files into.

    Returns:
        Locations of the panel and the unknown-area pickups.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    outputs = [
        ("panel", out_dir / PANEL_FILE),
        ("unknown_area_pickups", out_dir / UNKNOWN_AREA_FILE),
    ]
    for table, path in outputs:
        tmp_path = path.with_name(f".{path.name}.tmp")
        con.execute(
            f"COPY {table} TO {quote_str(tmp_path.as_posix())} (FORMAT parquet, COMPRESSION zstd)"
        )
    for _, path in outputs:
        os.replace(path.with_name(f".{path.name}.tmp"), path)
    return outputs[0][1], outputs[1][1]
