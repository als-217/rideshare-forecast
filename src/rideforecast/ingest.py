"""Ingest stage: raw TNP trip CSVs -> typed, deduplicated Parquet, one file per month.

Each raw file goes through a write-audit-publish sequence:

1. **Write.** Every row is parsed with the explicit rules in `schema.py` and written to a
   staging Parquet file. A `parse_failures` bitmask records which columns failed to parse.
2. **Audit.** The staged rows are checked: parse-failure rates, rows without a start time,
   trip-key collisions, and months already published from another file in this run. A
   failed audit raises IngestError and nothing from the file is published.
3. **Publish.** Each calendar month is deduplicated on trip ID and written to
   `interim/trips/year=YYYY/month=M/data.parquet`, replacing that month's previous output.

Rows are never filtered on content (ADR 0002), with two exceptions: exact trip-ID
duplicates are removed, and rows without a parseable start time are excluded because they
cannot be assigned to a month. Both are counted in the run manifest.

Every run writes a JSON manifest to `interim/_manifests/`, whether it succeeds or fails.
"""

import hashlib
import json
import os
import shutil
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from rideforecast.config import Config
from rideforecast.schema import PARSED, RAW_NAMES, REQUIRED_RAW_NAMES, TRIP_ID, Column

# First 64 bits of the MD5 digest of the trip ID. MD5 is a fixed, published algorithm, so a
# trip gets the same key on every run and every DuckDB version. DuckDB's own hash() is not
# used because it is meant for internal hash tables, not for persisted values.
TRIP_KEY_SQL = "('0x' || left(md5({raw}), 16))::UBIGINT"

# Which copy of a duplicated trip ID is kept, so reruns always keep the same row.
DEDUP_ORDER = "trip_end_timestamp NULLS LAST, trip_total NULLS LAST, parse_failures"

# Row order within a month file. Sorting by time improves compression and lets readers
# skip row groups when filtering on time.
OUTPUT_ORDER = "trip_start_timestamp, pickup_community_area"


class IngestError(Exception):
    """A raw file failed validation. Nothing from that file is published."""


@dataclass
class MonthReport:
    """What was published for one calendar month.

    Attributes:
        year: Calendar year of the trips' start times.
        month: Calendar month (1-12) of the trips' start times.
        rows_published: Rows written to the month's Parquet file.
        duplicates_removed: Rows dropped because their trip ID appeared more than once.
    """

    year: int
    month: int
    rows_published: int
    duplicates_removed: int


@dataclass
class FileReport:
    """What happened to one raw file. Serialized into the run manifest.

    Attributes:
        path: Location of the raw file.
        dataset: Portal dataset ID, taken from the name of the file's folder.
        size_bytes: Size of the raw file on disk.
        sha256: Checksum of the raw file, to detect if it changes between runs.
        rows_read: Rows in the raw file.
        rows_without_start: Rows with no parseable start timestamp. These are not
            published, because they cannot be assigned to a month.
        failed_casts: For each parsed column, the number of rows whose raw value did
            not parse and became NULL.
        months: What was published for each month the file covers.
    """

    path: str
    dataset: str
    size_bytes: int
    sha256: str
    rows_read: int = 0
    rows_without_start: int = 0
    failed_casts: dict[str, int] = field(default_factory=dict)
    months: list[MonthReport] = field(default_factory=list)


@dataclass
class IngestResult:
    """Outcome of a successful run.

    Attributes:
        manifest_path: Location of the JSON manifest written for the run.
        files: A report for each raw file, in the order they were processed.
    """

    manifest_path: Path
    files: list[FileReport]


def run_ingest(cfg: Config) -> IngestResult:
    """Ingest every raw file matching the configured glob.

    Args:
        cfg: Validated project config.

    Returns:
        The manifest location and a report for each raw file.

    Raises:
        IngestError: If no raw files are found or any file fails its audit. Files
            processed before the failing one stay published. The manifest is still
            written, with status "failed" and the error message.
    """
    raw_files = sorted(p for p in cfg.paths.raw.glob(cfg.ingest.raw_glob) if p.is_file())
    if not raw_files:
        raise IngestError(f"No raw files match {cfg.ingest.raw_glob!r} in {cfg.paths.raw}")

    trips_dir = cfg.paths.interim / "trips"
    staging_dir = cfg.paths.interim / "_staging"
    manifest_dir = cfg.paths.interim / "_manifests"

    started = datetime.now(UTC)
    reports: list[FileReport] = []
    published_by: dict[tuple[int, int], str] = {}
    status, error = "succeeded", None
    con = duckdb.connect()
    try:
        staging_dir.mkdir(parents=True, exist_ok=True)
        for raw_file in raw_files:
            reports.append(_ingest_file(con, cfg, raw_file, staging_dir, trips_dir, published_by))
    except IngestError as exc:
        status, error = "failed", str(exc)
        raise
    finally:
        con.close()
        shutil.rmtree(staging_dir, ignore_errors=True)
        manifest_path = _write_manifest(manifest_dir, cfg, started, status, error, reports)
    return IngestResult(manifest_path=manifest_path, files=reports)


def _ingest_file(
    con: duckdb.DuckDBPyConnection,
    cfg: Config,
    raw_file: Path,
    staging_dir: Path,
    trips_dir: Path,
    published_by: dict[tuple[int, int], str],
) -> FileReport:
    """Write, audit and publish one raw file.

    Args:
        con: DuckDB connection used for every query.
        cfg: Validated project config.
        raw_file: The raw CSV to ingest.
        staging_dir: Directory for the file's temporary staging Parquet file.
        trips_dir: Root of the published monthly Parquet files.
        published_by: Months published so far in this run, mapped to the raw file they
            came from. Updated with the months this file publishes.

    Returns:
        A report of what was read and published.

    Raises:
        IngestError: If the file fails any check, or contains a month already published
            from another file in this run. Nothing from the file is published.
    """
    dataset = raw_file.parent.name
    report = FileReport(
        path=raw_file.as_posix(),
        dataset=dataset,
        size_bytes=raw_file.stat().st_size,
        sha256=_sha256(raw_file),
    )
    stage_path = staging_dir / f"{dataset}__{raw_file.name}.parquet"
    stage = f"read_parquet({_quote_str(stage_path.as_posix())})"

    try:
        header = read_header(con, raw_file)
        check_header(header, raw_file)
        con.execute(
            build_stage_sql(raw_file, header, dataset, cfg.ingest.timestamp_format, stage_path)
        )
        months = audit_stage(con, stage, report, cfg.ingest.max_failed_cast_rate)
        check_unique_keys(con, stage)
    except duckdb.Error as exc:
        raise IngestError(f"{raw_file}: {exc}") from exc

    for year, month, _ in months:
        owner = published_by.get((year, month))
        if owner is not None:
            raise IngestError(
                f"{raw_file}: {year}-{month:02d} was already published from {owner}. "
                "Each month's trips must come from a single raw file."
            )

    for year, month, duplicates in months:
        rows = publish_month(con, stage, trips_dir, year, month)
        published_by[(year, month)] = raw_file.as_posix()
        report.months.append(MonthReport(year, month, rows, duplicates))

    stage_path.unlink()
    return report


def read_header(con: duckdb.DuckDBPyConnection, raw_file: Path) -> list[str]:
    """Read the column names from a raw CSV's header row.

    Args:
        con: DuckDB connection.
        raw_file: The raw CSV, compressed or not.

    Returns:
        The column names, in file order.
    """
    source = _quote_str(raw_file.as_posix())
    return con.sql(f"SELECT * FROM read_csv({source}, all_varchar = true) LIMIT 0").columns


def check_header(header: list[str], raw_file: Path) -> None:
    """Fail on unknown columns or missing required columns.

    Args:
        header: Column names from the raw file's header row.
        raw_file: The raw file, named in the error message.

    Raises:
        IngestError: If the header has a column not in the schema, or lacks a column
            that every dataset should have.
    """
    unexpected = sorted(set(header) - RAW_NAMES)
    missing = sorted(REQUIRED_RAW_NAMES - set(header))
    problems = []
    if unexpected:
        problems.append(f"unexpected columns {unexpected}")
    if missing:
        problems.append(f"missing required columns {missing}")
    if problems:
        raise IngestError(f"{raw_file}: " + "; ".join(problems) + ". Update schema.py?")


def parse_expr(col: Column, raw_sql: str, timestamp_format: str) -> str:
    """Build the SQL expression that parses one raw column into its typed value.

    Every expression uses TRY_ functions, so a value that fails to parse becomes NULL
    instead of stopping the query. Failures are counted separately (see build_stage_sql).

    Args:
        col: The column's schema entry, whose kind selects the parsing rule.
        raw_sql: SQL expression for the raw string value, usually a quoted column name.
        timestamp_format: strptime format used for timestamp columns.

    Returns:
        A SQL expression producing the typed value, or NULL if parsing fails.

    Raises:
        ValueError: If the column's kind has no parsing rule (id and text columns).
    """
    match col.kind:
        case "timestamp":
            return f"try_strptime({raw_sql}, {_quote_str(timestamp_format)})"
        case "number":
            cleaned = f"replace(replace(trim({raw_sql}), '$', ''), ',', '')"
            return f"TRY_CAST({cleaned} AS {col.sql_type})"
        case "percent":
            cleaned = f"trim(replace({raw_sql}, '%', ''))"
            return f"TRY_CAST({cleaned} AS {col.sql_type})"
        case "boolean":
            return f"TRY_CAST(trim({raw_sql}) AS BOOLEAN)"
        case _:
            raise ValueError(f"Column {col.raw_name!r} of kind {col.kind!r} is not parsed")


def build_stage_sql(
    raw_file: Path, header: list[str], dataset: str, timestamp_format: str, out_path: Path
) -> str:
    """Build the COPY statement that parses a raw file into a staging Parquet file.

    Optional columns missing from the file become NULL. Bit i of `parse_failures` is set
    when PARSED[i] had a raw value that did not parse.

    Args:
        raw_file: The raw CSV to read.
        header: Column names from the raw file's header row.
        dataset: Portal dataset ID, stored on every row.
        timestamp_format: strptime format of the raw timestamps.
        out_path: Where to write the staging Parquet file.

    Returns:
        A complete COPY statement, ready to execute.
    """
    present = set(header)

    def raw(col: Column) -> str:
        """Return the SQL for a column's raw value.

        Args:
            col: The column's schema entry.

        Returns:
            The quoted column name, or a typed NULL if the file lacks the column.
        """
        return _quote_ident(col.raw_name) if col.raw_name in present else "NULL::VARCHAR"

    trip_id = _quote_ident(TRIP_ID.raw_name)
    start_col = next(c for c in PARSED if c.name == "trip_start_timestamp")
    start = parse_expr(start_col, raw(start_col), timestamp_format)

    select = [f"{trip_id} AS trip_id", f"{TRIP_KEY_SQL.format(raw=trip_id)} AS trip_key"]
    failure_bits = []
    for bit, col in enumerate(PARSED):
        value = parse_expr(col, raw(col), timestamp_format)
        select.append(f"{value} AS {col.name}")
        failure_bits.append(
            f"CASE WHEN {raw(col)} IS NOT NULL AND {value} IS NULL THEN {1 << bit} ELSE 0 END"
        )
    select += [
        f"({' + '.join(failure_bits)})::UINTEGER AS parse_failures",
        f"{_quote_str(dataset)} AS source_dataset",
        f"{_quote_str(raw_file.name)} AS source_file",
        f"year({start}) AS year",
        f"month({start}) AS month",
    ]
    columns = ",\n        ".join(select)
    source = _quote_str(raw_file.as_posix())
    return f"""
    COPY (
        SELECT
        {columns}
        FROM read_csv({source}, all_varchar = true)
    ) TO {_quote_str(out_path.as_posix())} (FORMAT parquet)
    """


def audit_stage(
    con: duckdb.DuckDBPyConnection, stage: str, report: FileReport, max_rate: float
) -> list[tuple[int, int, int]]:
    """Check a staged file's parse-failure rates and summarize it by month.

    Fills in the row counts and failed-cast counts on `report`.

    Args:
        con: DuckDB connection.
        stage: SQL relation reading the staging Parquet file.
        report: The file's report, updated in place.
        max_rate: Largest allowed share of rows failing any one check.

    Returns:
        (year, month, duplicate trip IDs) for every month in the file.

    Raises:
        IngestError: If any column's failure rate, or the share of rows without a start
            time, exceeds `max_rate`.
    """
    failure_sums = ", ".join(
        f"coalesce(sum((parse_failures >> {bit}) & 1), 0)::BIGINT AS {col.name}"
        for bit, col in enumerate(PARSED)
    )
    row = con.sql(f"""
        SELECT
            count(*)::BIGINT AS rows_read,
            count(*) FILTER (WHERE trip_start_timestamp IS NULL)::BIGINT AS rows_without_start,
            {failure_sums}
        FROM {stage}
    """).fetchall()[0]
    report.rows_read, report.rows_without_start = row[0], row[1]
    report.failed_casts = {col.name: n for col, n in zip(PARSED, row[2:], strict=True)}

    if report.rows_read:
        too_many = {
            name: n
            for name, n in {
                **report.failed_casts,
                "missing start timestamp": report.rows_without_start,
            }.items()
            if n / report.rows_read > max_rate
        }
        if too_many:
            raise IngestError(
                f"{report.path}: parse failures above max_failed_cast_rate={max_rate} "
                f"out of {report.rows_read:,} rows: {too_many}"
            )

    months = con.sql(f"""
        SELECT year, month, (count(trip_id) - count(DISTINCT trip_id))::BIGINT
        FROM {stage}
        WHERE year IS NOT NULL
        GROUP BY year, month
        ORDER BY year, month
    """).fetchall()
    return [(int(y), int(m), int(d)) for y, m, d in months]


def check_unique_keys(con: duckdb.DuckDBPyConnection, relation: str) -> None:
    """Verify that distinct trip IDs map to distinct trip keys within each month.

    Duplicates of one trip share a start time, so they always fall in the same month;
    keys only need to be unique within a month. This check is exact, not probabilistic:
    a month that passes it has no collisions.

    Args:
        con: DuckDB connection.
        relation: SQL relation with trip_id, trip_key, year and month columns.

    Raises:
        IngestError: If two different trip IDs in one month share a key.
    """
    collisions = con.sql(f"""
        SELECT year, month
        FROM {relation}
        WHERE year IS NOT NULL
        GROUP BY year, month
        HAVING count(DISTINCT trip_id) <> count(DISTINCT trip_key)
    """).fetchall()
    if collisions:
        months = ", ".join(f"{y}-{m:02d}" for y, m in collisions)
        raise IngestError(f"trip_key collision in {months}: two trip IDs share a 64-bit key")


def publish_month(
    con: duckdb.DuckDBPyConnection, stage: str, trips_dir: Path, year: int, month: int
) -> int:
    """Deduplicate one month of staged rows and write it to interim, replacing old output.

    The file is written under a temporary name and then renamed, so readers never see a
    half-written month.

    Args:
        con: DuckDB connection.
        stage: SQL relation reading the staging Parquet file.
        trips_dir: Root of the published monthly Parquet files.
        year: Year of the month to publish.
        month: Month (1-12) to publish.

    Returns:
        Number of rows written.
    """
    month_dir = trips_dir / f"year={year}" / f"month={month}"
    month_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = month_dir / ".data.parquet.tmp"
    columns = ", ".join(["trip_key", *(c.name for c in PARSED), "source_dataset", "source_file"])
    rows = con.execute(f"""
        COPY (
            SELECT {columns}
            FROM (
                SELECT
                    *,
                    row_number() OVER (PARTITION BY trip_id ORDER BY {DEDUP_ORDER}) AS copy_number
                FROM {stage}
                WHERE year = {year} AND month = {month}
            )
            WHERE trip_id IS NULL OR copy_number = 1
            ORDER BY {OUTPUT_ORDER}
        ) TO {_quote_str(tmp_path.as_posix())} (FORMAT parquet, COMPRESSION zstd)
    """).fetchall()[0][0]
    for old in month_dir.glob("*.parquet"):
        old.unlink()
    os.replace(tmp_path, month_dir / "data.parquet")
    return int(rows)


def _write_manifest(
    manifest_dir: Path,
    cfg: Config,
    started: datetime,
    status: str,
    error: str | None,
    reports: list[FileReport],
) -> Path:
    """Write the run manifest as JSON.

    Args:
        manifest_dir: Directory to write the manifest into.
        cfg: Config used for the run, recorded in full along with its checksum.
        started: When the run started, in UTC. Also used in the file name.
        status: "succeeded" or "failed".
        error: The error message if the run failed, otherwise None.
        reports: Reports for the files processed before the run ended.

    Returns:
        Location of the manifest file.
    """
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "stage": "ingest",
        "status": status,
        "error": error,
        "started_utc": started.isoformat(),
        "finished_utc": datetime.now(UTC).isoformat(),
        "git_commit": _git_commit(),
        "config_sha256": hashlib.sha256(cfg.model_dump_json().encode()).hexdigest(),
        "config": cfg.model_dump(mode="json"),
        "files": [asdict(r) for r in reports],
    }
    path = manifest_dir / f"ingest-{started:%Y%m%dT%H%M%S}Z.json"
    path.write_text(json.dumps(manifest, indent=2) + "\n")
    return path


def _sha256(path: Path, chunk_size: int = 1 << 20) -> str:
    """Compute a file's SHA-256 checksum.

    The file is read in chunks, so large files are never fully loaded into memory.

    Args:
        path: The file to checksum.
        chunk_size: Bytes read at a time.

    Returns:
        The checksum as a hex string.
    """
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _git_commit() -> str | None:
    """Look up the current git commit.

    Returns:
        The commit hash, or None if git is unavailable or this is not a repository.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier, such as a column name containing spaces.

    Args:
        name: The identifier to quote.

    Returns:
        The identifier in double quotes, with any inner double quotes escaped.
    """
    return '"' + name.replace('"', '""') + '"'


def _quote_str(value: str) -> str:
    """Quote a value as a SQL string literal.

    Args:
        value: The string to quote.

    Returns:
        The value in single quotes, with any inner single quotes escaped.
    """
    return "'" + value.replace("'", "''") + "'"
