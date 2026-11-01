"""Helpers shared by the pipeline stages: SQL quoting and run manifests."""

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rideforecast.config import Config


def quote_ident(name: str) -> str:
    """Quote a SQL identifier, such as a column name containing spaces.

    Args:
        name: The identifier to quote.

    Returns:
        The identifier in double quotes, with any inner double quotes escaped.
    """
    return '"' + name.replace('"', '""') + '"'


def quote_str(value: str) -> str:
    """Quote a value as a SQL string literal.

    Args:
        value: The string to quote.

    Returns:
        The value in single quotes, with any inner single quotes escaped.
    """
    return "'" + value.replace("'", "''") + "'"


def git_commit() -> str | None:
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


def write_manifest(
    manifest_dir: Path,
    stage: str,
    cfg: Config,
    started: datetime,
    status: str,
    error: str | None,
    details: dict[str, Any],
) -> Path:
    """Write a stage's run manifest as JSON.

    Every manifest records the same run information (stage, status, timing, git commit
    and config), followed by the stage's own details.

    Args:
        manifest_dir: Directory to write the manifest into.
        stage: Name of the pipeline stage, also used in the file name.
        cfg: Config used for the run, recorded in full along with its checksum.
        started: When the run started, in UTC. Also used in the file name.
        status: "succeeded" or "failed".
        error: The error message if the run failed, otherwise None.
        details: Stage-specific contents. Dates and other non-JSON values are written
            as strings.

    Returns:
        Location of the manifest file.
    """
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "stage": stage,
        "status": status,
        "error": error,
        "started_utc": started.isoformat(),
        "finished_utc": datetime.now(UTC).isoformat(),
        "git_commit": git_commit(),
        "config_sha256": hashlib.sha256(cfg.model_dump_json().encode()).hexdigest(),
        "config": cfg.model_dump(mode="json"),
        **details,
    }
    path = manifest_dir / f"{stage}-{started:%Y%m%dT%H%M%S}Z.json"
    path.write_text(json.dumps(manifest, indent=2, default=str) + "\n")
    return path
