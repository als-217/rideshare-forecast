"""Ingest stage: raw TNP trip CSVs -> typed, deduplicated Parquet, one file per month.

Raw files must be named `tnp_YYYY-MM-DD_to_YYYY-MM-DD.csv.gz`, with both dates (inclusive)
in the same calendar month. A month may be split across several files. Ingest groups files
by month from their names, then runs write-audit-publish one month at a time:

1. **Write.** Each of the month's files is parsed with the explicit rules in `schema.py`
   and written to a staging Parquet file. A `parse_failures` bitmask records which columns
   failed to parse.
2. **Audit.** Each file is checked for parse-failure rates, rows without a start time, and
   rows starting outside the dates in its name. The month as a whole is checked for
   trip-key collisions across its files.
3. **Publish.** The month's rows from all its files are deduplicated on trip ID and written
   to `interim/trips/year=YYYY/month=MM/data.parquet`, replacing the previous output.
4. **Clean up.** The month's staging files are deleted before the next month starts, so
   disk use peaks at one month of staging.

A failed audit stops the run before that month is published. Earlier months stay
published; each published month is complete and fully audited. Rows are never filtered on
content (ADR 0002), with two exceptions: duplicate trip IDs are removed, and rows without a
parseable start time are excluded. Both are counted in the run manifest, which every run
writes to `interim/_manifests/`, whether it succeeds or fails.
"""

import calendar
import hashlib
import json
import os
import re
import shutil
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb

from rideforecast.config import Config
from rideforecast.schema import PARSED, RAW_NAMES, REQUIRED_RAW_NAMES, TRIP_ID, Column

# Raw file names declare the inclusive date range of the trips they contain.
RAW_NAME_PATTERN = re.compile(
    r"^tnp_(?P<start>\d{4}-\d{2}-\d{2})_to_(?P<end>\d{4}-\d{2}-\d{2})\.csv(\.\w+)?$"
)

# First 64 bits of the MD5 digest of the trip ID. MD5 is a fixed, published algorithm, so a
# trip gets the same key on every run and every DuckDB version. DuckDB's own hash() is not
# used because it is meant for internal hash tables, not for persisted values.
TRIP_KEY_SQL = "('0x' || left(md5({raw}), 16))::UBIGINT"

# Which copy of a duplicated trip ID is kept, so reruns always keep the same row. The
# source file name is the final tie-breaker when the same trip appears in two files.
DEDUP_ORDER = "trip_end_timestamp NULLS LAST, trip_total NULLS LAST, parse_failures, source_file"

# Row order within a month file. Sorting by time improves compression and lets readers
# skip row groups when filtering on time.
OUTPUT_ORDER = "trip_start_timestamp, pickup_community_area"

# Warnings list at most this many missing days; the manifest lists all of them.
MAX_DAYS_IN_WARNING = 5


class IngestError(Exception):
    """A raw file or month failed validation. Nothing from that month is published."""


@dataclass(frozen=True)
class RawFile:
    """A raw file and the date range declared in its name.

    Attributes:
        path: Location of the raw file.
        dataset: Portal dataset ID, taken from the name of the file's folder.
        start: First day of trips in the file, inclusive.
        end: Last day of trips in the file, inclusive.
    """

    path: Path
    dataset: str
    start: date
    end: date

    @property
    def month(self) -> tuple[int, int]:
        """The (year, month) the file's trips belong to."""
        return (self.start.year, self.start.month)


@dataclass
class FileReport:
    """What happened to one raw file. Serialized into the run manifest.

    Attributes:
        path: Location of the raw file.
        dataset: Portal dataset ID, taken from the name of the file's folder.
        declared_start: First day of trips according to the file name.
        declared_end: Last day of trips according to the file name.
        size_bytes: Size of the raw file on disk.
        sha256: Checksum of the raw file, to detect if it changes between runs.
        rows_read: Rows in the raw file.
        rows_without_start: Rows with no parseable start timestamp. These are not
            published, because they cannot be assigned to a month.
        rows_outside_range: Rows starting outside the declared dates. Any such row
            rejects the file.
        failed_casts: For each parsed column, the number of rows whose raw value did
            not parse and became NULL.
    """

    path: str
    dataset: str
    declared_start: date
    declared_end: date
    size_bytes: int
    sha256: str
    rows_read: int = 0
    rows_without_start: int = 0
    rows_outside_range: int = 0
    failed_casts: dict[str, int] = field(default_factory=dict)


@dataclass
class MonthReport:
    """What was published for one calendar month. Serialized into the run manifest.

    Attributes:
        year: Calendar year of the trips' start times.
        month: Calendar month (1-12) of the trips' start times.
        source_files: The raw files the month was built from.
        rows_published: Rows written to the month's Parquet file.
        duplicates_removed: Rows dropped because their trip ID appeared more than once,
            within one file or across files.
        first_start: Earliest trip start time in the month.
        last_start: Latest trip start time in the month.
        days_with_trips: Number of days with at least one trip.
        missing_declared_days: Days of the month not covered by any file name.
        days_without_trips: Days covered by a file name but with no trips.
    """

    year: int
    month: int
    source_files: list[str]
    rows_published: int = 0
    duplicates_removed: int = 0
    first_start: datetime | None = None
    last_start: datetime | None = None
    days_with_trips: int = 0
    missing_declared_days: list[date] = field(default_factory=list)
    days_without_trips: list[date] = field(default_factory=list)


@dataclass
class IngestResult:
    """Outcome of a successful run.

    Attributes:
        manifest_path: Location of the JSON manifest written for the run.
        files: A report for each raw file, in the order they were processed.
        months: A report for each published month, in calendar order.
        warnings: Completeness warnings. They do not stop the run.
    """

    manifest_path: Path
    files: list[FileReport]
    months: list[MonthReport]
    warnings: list[str]


def run_ingest(cfg: Config) -> IngestResult:
    """Ingest every raw file matching the configured glob, one month at a time.

    Args:
        cfg: Validated project config.

    Returns:
        The manifest location, plus reports for each file and month and any warnings.

    Raises:
        IngestError: If no raw files are found, a file name is invalid, or a file or
            month fails its audit. Months published before the failure stay published.
            The manifest is still written, with status "failed" and the error message.
    """
    paths = sorted(p for p in cfg.paths.raw.glob(cfg.ingest.raw_glob) if p.is_file())
    if not paths:
        raise IngestError(f"No raw files match {cfg.ingest.raw_glob!r} in {cfg.paths.raw}")

    trips_dir = cfg.paths.interim / "trips"
    staging_dir = cfg.paths.interim / "_staging"
    manifest_dir = cfg.paths.interim / "_manifests"

    started = datetime.now(UTC)
    file_reports: list[FileReport] = []
    month_reports: list[MonthReport] = []
    warnings: list[str] = []
    status, error = "succeeded", None
    con = duckdb.connect()
    try:
        by_month = group_by_month([parse_raw_file(p) for p in paths])
        for (year, month), raw_files in sorted(by_month.items()):
            report = _ingest_month(
                con,
                cfg,
                year,
                month,
                raw_files,
                staging_dir / f"{year}-{month:02d}",
                trips_dir,
                file_reports,
            )
            month_reports.append(report)
            warnings.extend(month_warnings(report))
    except IngestError as exc:
        status, error = "failed", str(exc)
        raise
    finally:
        con.close()
        shutil.rmtree(staging_dir, ignore_errors=True)
        manifest_path = _write_manifest(
            manifest_dir, cfg, started, status, error, file_reports, month_reports, warnings
        )
    return IngestResult(manifest_path, file_reports, month_reports, warnings)


def parse_raw_file(path: Path) -> RawFile:
    """Read the declared date range from a raw file's name.

    Args:
        path: Location of the raw file, inside a folder named after its dataset ID.

    Returns:
        The file with its dataset and declared dates.

    Raises:
        IngestError: If the name does not match `tnp_YYYY-MM-DD_to_YYYY-MM-DD.csv.gz`,
            a date is invalid, the end is before the start, or the dates fall in
            different calendar months.
    """
    match = RAW_NAME_PATTERN.match(path.name)
    if match is None:
        raise IngestError(f"{path}: name must look like tnp_YYYY-MM-DD_to_YYYY-MM-DD.csv.gz")
    try:
        start = date.fromisoformat(match["start"])
        end = date.fromisoformat(match["end"])
    except ValueError:
        raise IngestError(f"{path}: name contains an invalid date") from None
    if end < start:
        raise IngestError(f"{path}: end date is before start date")
    if (start.year, start.month) != (end.year, end.month):
        raise IngestError(
            f"{path}: dates span more than one month. "
            "Each raw file must cover dates within a single calendar month."
        )
    return RawFile(path=path, dataset=path.parent.name, start=start, end=end)


def group_by_month(raw_files: list[RawFile]) -> dict[tuple[int, int], list[RawFile]]:
    """Group raw files by the calendar month their names declare.

    Args:
        raw_files: Parsed raw files.

    Returns:
        Each (year, month) mapped to its files, sorted by path.
    """
    groups: dict[tuple[int, int], list[RawFile]] = {}
    for raw_file in sorted(raw_files, key=lambda f: f.path):
        groups.setdefault(raw_file.month, []).append(raw_file)
    return groups


def _ingest_month(
    con: duckdb.DuckDBPyConnection,
    cfg: Config,
    year: int,
    month: int,
    raw_files: list[RawFile],
    staging_dir: Path,
    trips_dir: Path,
    file_reports: list[FileReport],
) -> MonthReport:
    """Write, audit and publish one month, then delete its staging files.

    Args:
        con: DuckDB connection used for every query.
        cfg: Validated project config.
        year: Year of the month.
        month: Month (1-12).
        raw_files: The month's raw files.
        staging_dir: Directory for the month's staging files. Deleted on return.
        trips_dir: Root of the published monthly Parquet files.
        file_reports: Reports of files processed so far in the run. A report for each of
            this month's files is appended, including a file that fails its audit.

    Returns:
        A report of what was published.

    Raises:
        IngestError: If any file or the month fails its audit. Nothing from the month
            is published.
    """
    label = f"{year}-{month:02d}"
    staging_dir.mkdir(parents=True, exist_ok=True)
    try:
        staged = []
        for raw_file in raw_files:
            report = FileReport(
                path=raw_file.path.as_posix(),
                dataset=raw_file.dataset,
                declared_start=raw_file.start,
                declared_end=raw_file.end,
                size_bytes=raw_file.path.stat().st_size,
                sha256=_sha256(raw_file.path),
            )
            file_reports.append(report)
            stage_path = staging_dir / f"{raw_file.dataset}__{raw_file.path.name}.parquet"
            try:
                header = read_header(con, raw_file.path)
                check_header(header, raw_file.path)
                con.execute(
                    build_stage_sql(
                        raw_file.path,
                        header,
                        raw_file.dataset,
                        cfg.ingest.timestamp_format,
                        stage_path,
                    )
                )
                audit_file(
                    con,
                    _parquet_relation([stage_path]),
                    raw_file,
                    report,
                    cfg.ingest.max_failed_cast_rate,
                )
            except duckdb.Error as exc:
                raise IngestError(f"{raw_file.path}: {exc}") from exc
            staged.append(stage_path)

        relation = _parquet_relation(staged)
        try:
            check_unique_keys(con, relation, label)
            month_report = summarize_month(con, relation, year, month, raw_files)
            month_report.rows_published = publish_month(con, relation, trips_dir, year, month)
        except duckdb.Error as exc:
            raise IngestError(f"{label}: {exc}") from exc
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)
    return month_report


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


def audit_file(
    con: duckdb.DuckDBPyConnection,
    stage: str,
    raw_file: RawFile,
    report: FileReport,
    max_rate: float,
) -> None:
    """Check one staged file's parse failures and declared date range.

    Fills in the row counts and failed-cast counts on `report`.

    Args:
        con: DuckDB connection.
        stage: SQL relation reading the file's staging Parquet file.
        raw_file: The raw file, with the date range declared in its name.
        report: The file's report, updated in place.
        max_rate: Largest allowed share of rows failing any one parse check.

    Raises:
        IngestError: If any column's failure rate, or the share of rows without a start
            time, exceeds `max_rate`, or if any row starts outside the declared dates.
    """
    failure_sums = ", ".join(
        f"coalesce(sum((parse_failures >> {bit}) & 1), 0)::BIGINT AS {col.name}"
        for bit, col in enumerate(PARSED)
    )
    start_date = "trip_start_timestamp::DATE"
    row = con.sql(f"""
        SELECT
            count(*)::BIGINT AS rows_read,
            count(*) FILTER (WHERE trip_start_timestamp IS NULL)::BIGINT AS rows_without_start,
            count(*) FILTER (
                WHERE {start_date} < DATE '{raw_file.start}' OR {start_date} > DATE '{raw_file.end}'
            )::BIGINT AS rows_outside_range,
            {failure_sums}
        FROM {stage}
    """).fetchall()[0]
    report.rows_read, report.rows_without_start, report.rows_outside_range = row[:3]
    report.failed_casts = {col.name: n for col, n in zip(PARSED, row[3:], strict=True)}

    if report.rows_outside_range:
        raise IngestError(
            f"{report.path}: {report.rows_outside_range:,} rows start outside the dates "
            f"in the file name ({raw_file.start} to {raw_file.end})"
        )
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


def check_unique_keys(con: duckdb.DuckDBPyConnection, relation: str, label: str) -> None:
    """Verify that distinct trip IDs map to distinct trip keys within one month.

    Duplicates of one trip share a start time, so they always fall in the same month;
    keys only need to be unique within a month. This check is exact, not probabilistic:
    a month that passes it has no collisions.

    Args:
        con: DuckDB connection.
        relation: SQL relation with the month's trip_id and trip_key columns, from all
            of its files.
        label: The month, named in the error message.

    Raises:
        IngestError: If two different trip IDs share a key.
    """
    distinct_ids, distinct_keys = con.sql(f"""
        SELECT count(DISTINCT trip_id), count(DISTINCT trip_key) FROM {relation}
    """).fetchall()[0]
    if distinct_ids != distinct_keys:
        raise IngestError(f"{label}: trip_key collision: two trip IDs share a 64-bit key")


def summarize_month(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    year: int,
    month: int,
    raw_files: list[RawFile],
) -> MonthReport:
    """Count duplicates and measure how completely a month is covered.

    Coverage is checked twice: from the dates declared in the file names (a missing
    file shows up before any data is read) and from the trips actually present.

    Args:
        con: DuckDB connection.
        relation: SQL relation with the month's staged rows from all of its files.
        year: Year of the month.
        month: Month (1-12).
        raw_files: The month's raw files.

    Returns:
        The month's report, with everything except rows_published filled in.
    """
    duplicates, first_start, last_start, trip_days = con.sql(f"""
        SELECT
            (count(trip_id) - count(DISTINCT trip_id))::BIGINT,
            min(trip_start_timestamp),
            max(trip_start_timestamp),
            list(DISTINCT trip_start_timestamp::DATE)
        FROM {relation}
        WHERE trip_start_timestamp IS NOT NULL
    """).fetchall()[0]

    days_in_month = calendar.monthrange(year, month)[1]
    all_days = {date(year, month, d) for d in range(1, days_in_month + 1)}
    declared = {
        f.start + timedelta(days=i) for f in raw_files for i in range((f.end - f.start).days + 1)
    }
    with_trips = set(trip_days or [])
    return MonthReport(
        year=year,
        month=month,
        source_files=[f.path.as_posix() for f in raw_files],
        duplicates_removed=int(duplicates),
        first_start=first_start,
        last_start=last_start,
        days_with_trips=len(with_trips),
        missing_declared_days=sorted(all_days - declared),
        days_without_trips=sorted(declared - with_trips),
    )


def month_warnings(report: MonthReport) -> list[str]:
    """Describe gaps in a month's coverage.

    Args:
        report: The month's report.

    Returns:
        One warning per kind of gap found, or an empty list if the month is complete.
    """
    label = f"{report.year}-{report.month:02d}"
    warnings = []
    for days, problem in [
        (report.missing_declared_days, "not covered by any raw file name"),
        (report.days_without_trips, "covered by a raw file name but with no trips"),
    ]:
        if days:
            shown = ", ".join(str(d) for d in days[:MAX_DAYS_IN_WARNING])
            more = (
                f" and {len(days) - MAX_DAYS_IN_WARNING} more"
                if len(days) > MAX_DAYS_IN_WARNING
                else ""
            )
            warnings.append(f"{label}: {len(days)} day(s) {problem}: {shown}{more}")
    return warnings


def publish_month(
    con: duckdb.DuckDBPyConnection, relation: str, trips_dir: Path, year: int, month: int
) -> int:
    """Deduplicate a month's staged rows and write them to interim, replacing old output.

    The file is written under a temporary name and then renamed, so readers never see a
    half-written month.

    Args:
        con: DuckDB connection.
        relation: SQL relation with the month's staged rows from all of its files.
        trips_dir: Root of the published monthly Parquet files.
        year: Year of the month to publish.
        month: Month (1-12) to publish.

    Returns:
        Number of rows written.
    """
    month_dir = trips_dir / f"year={year}" / f"month={month:02d}"
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
                FROM {relation}
                WHERE trip_start_timestamp IS NOT NULL
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
    file_reports: list[FileReport],
    month_reports: list[MonthReport],
    warnings: list[str],
) -> Path:
    """Write the run manifest as JSON.

    Args:
        manifest_dir: Directory to write the manifest into.
        cfg: Config used for the run, recorded in full along with its checksum.
        started: When the run started, in UTC. Also used in the file name.
        status: "succeeded" or "failed".
        error: The error message if the run failed, otherwise None.
        file_reports: Reports for the files processed before the run ended.
        month_reports: Reports for the months published before the run ended.
        warnings: Completeness warnings raised during the run.

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
        "warnings": warnings,
        "files": [asdict(r) for r in file_reports],
        "months": [asdict(r) for r in month_reports],
    }
    path = manifest_dir / f"ingest-{started:%Y%m%dT%H%M%S}Z.json"
    path.write_text(json.dumps(manifest, indent=2, default=str) + "\n")
    return path


def _parquet_relation(paths: list[Path]) -> str:
    """Build a SQL relation that reads one or more Parquet files as a single table.

    Args:
        paths: The Parquet files to read.

    Returns:
        A read_parquet(...) expression usable in a FROM clause.
    """
    files = ", ".join(_quote_str(p.as_posix()) for p in paths)
    return f"read_parquet([{files}])"


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
