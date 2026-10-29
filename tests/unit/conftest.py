"""Shared fixtures: synthetic raw TNP files and configs pointing at a temporary directory.

Tests never use real data. Each test builds the exact rows it needs, so the edge case it
checks is visible in the test itself.
"""

import csv
import gzip
import hashlib
from collections.abc import Callable
from pathlib import Path

import pytest

from rideforecast.config import Config
from rideforecast.schema import COLUMNS

# Header of the 2023-2024 and 2025- datasets.
HEADER_NEW = [c.raw_name for c in COLUMNS]

# Header of the 2018-2022 dataset, which lacks the optional columns.
HEADER_OLD = [c.raw_name for c in COLUMNS if not c.optional]

TIMESTAMP_FORMAT = "%m/%d/%Y %I:%M:%S %p"


def trip_row(n: int = 0, **overrides: str) -> dict[str, str]:
    """Build one valid raw trip row, formatted the way the portal exports it.

    Args:
        n: Makes the trip ID unique; rows with the same n share a trip ID.
        **overrides: Raw values to replace, keyed by raw column name with spaces
            replaced by underscores (e.g. Trip_Start_Timestamp="").

    Returns:
        A new dictionary mapping raw column names to raw string values.
    """
    row = {
        "Trip ID": hashlib.sha1(str(n).encode()).hexdigest(),
        "Trip Start Timestamp": "03/10/2024 01:00:00 PM",
        "Trip End Timestamp": "03/10/2024 01:15:00 PM",
        "Trip Seconds": "1,020",
        "Trip Miles": "3.4",
        "Percent Time Chicago": "100%",
        "Percent Distance Chicago": "100%",
        "Pickup Census Tract": "17031081500",
        "Dropoff Census Tract": "17031320100",
        "Pickup Community Area": "8",
        "Dropoff Community Area": "32",
        "Fare": "$12.50",
        "Tip": "$2.00",
        "Additional Charges": "$3.18",
        "Trip Total": "$17.68",
        "Shared Trip Authorized": "false",
        "Shared Trip Match": "false",
        "Trips Pooled": "1",
        "Pickup Centroid Latitude": "41.899",
        "Pickup Centroid Longitude": "-87.633",
        "Pickup Centroid Location": "POINT (-87.633 41.899)",
        "Dropoff Centroid Latitude": "41.880",
        "Dropoff Centroid Longitude": "-87.629",
        "Dropoff Centroid Location": "POINT (-87.629 41.880)",
    }
    for key, value in overrides.items():
        row[key.replace("_", " ")] = value
    return row


WriteRaw = Callable[..., Path]


@pytest.fixture
def raw_dir(tmp_path: Path) -> Path:
    """Provide the raw data directory inside the test's temporary directory.

    Args:
        tmp_path: pytest's per-test temporary directory.

    Returns:
        The path tmp_path/data/raw. It is not created until a file is written to it.
    """
    return tmp_path / "data" / "raw"


@pytest.fixture
def write_raw(raw_dir: Path) -> WriteRaw:
    """Provide a function that writes raw trip rows as a gzipped CSV.

    Args:
        raw_dir: The raw data directory fixture.

    Returns:
        A function that writes a file to raw_dir/<dataset>/<name>.
    """

    def _write(
        rows: list[dict[str, str]],
        name: str = "tnp_2024-03-01_to_2024-03-31.csv.gz",
        dataset: str = "n26f-ihde",
        header: list[str] | None = None,
    ) -> Path:
        """Write rows to a gzipped CSV in the raw data directory.

        Args:
            rows: Raw rows, as built by trip_row. Columns missing from a row are empty.
            name: File name, declaring the file's dates. Defaults to all of March 2024,
                which contains trip_row's default start time.
            dataset: Portal dataset ID, used as the folder name.
            header: Column names to write. Defaults to the newer datasets' header.

        Returns:
            Location of the written file.
        """
        columns = header or HEADER_NEW
        path = raw_dir / dataset / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for row in rows:
                writer.writerow([row.get(c, "") for c in columns])
        return path

    return _write


@pytest.fixture
def make_cfg(tmp_path: Path, raw_dir: Path) -> Callable[..., Config]:
    """Provide a function that builds a Config whose data paths live in tmp_path.

    Args:
        tmp_path: pytest's per-test temporary directory.
        raw_dir: The raw data directory fixture.

    Returns:
        A function that builds a validated Config.
    """

    def _make(max_failed_cast_rate: float = 0.0) -> Config:
        """Build a Config pointing at the temporary directory.

        Args:
            max_failed_cast_rate: Allowed share of rows failing to parse. Defaults to 0,
                so any parse failure rejects the file unless a test allows it.

        Returns:
            A validated Config.
        """
        return Config.model_validate(
            {
                "paths": {
                    "raw": raw_dir,
                    "interim": tmp_path / "data" / "interim",
                    "processed": tmp_path / "data" / "processed",
                },
                "forecast": {"horizons": [1], "bin": "1h", "grain": "community_area"},
                "ingest": {
                    "raw_glob": "*/*.csv.gz",
                    "timestamp_format": TIMESTAMP_FORMAT,
                    "max_failed_cast_rate": max_failed_cast_rate,
                },
            }
        )

    return _make
