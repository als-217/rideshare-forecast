from pathlib import Path
from typing import Annotated

import typer
from pydantic import ValidationError

from rideforecast.config import Config, load_config
from rideforecast.ingest import IngestError, run_ingest

DEFAULT_CONFIG = Path("configs/base.yaml")

ConfigOption = Annotated[
    Path,
    typer.Option("--config", "-c", help="Path to the YAML config file."),
]

app = typer.Typer(
    help="Short-term demand forecasting on Chicago rideshare trip data.",
    no_args_is_help=True,
)


def _load(config_path: Path) -> Config:
    """Load the config, exiting with a readable message if it cannot be loaded.

    Args:
        config_path: Location of the YAML config file.

    Returns:
        The validated Config object.

    Raises:
        typer.Exit: With code 1 if the file is missing or fails validation.
            The reason is printed to stderr instead of a raw traceback.
    """
    try:
        return load_config(config_path)
    except FileNotFoundError:
        typer.echo(f"Config file not found: {config_path}", err=True)
        raise typer.Exit(code=1) from None
    except ValidationError as exc:
        typer.echo(f"Invalid config {config_path}:\n{exc}", err=True)
        raise typer.Exit(code=1) from None


@app.command(help="Convert raw trip CSVs into typed, monthly Parquet files.")
def ingest(config: ConfigOption = DEFAULT_CONFIG) -> None:
    """Run the ingest stage (raw -> interim).

    Args:
        config: Path to the YAML config file.

    Raises:
        typer.Exit: With code 1 if any raw file fails its audit.
    """
    cfg = _load(config)
    try:
        result = run_ingest(cfg)
    except IngestError as exc:
        typer.echo(f"[ingest] failed: {exc}", err=True)
        raise typer.Exit(code=1) from None
    for report in result.months:
        typer.echo(
            f"[ingest] {report.year}-{report.month:02d}: {report.rows_published:,} rows "
            f"published from {len(report.source_files)} file(s)"
        )
    for warning in result.warnings:
        typer.echo(f"[ingest] warning: {warning}", err=True)
    typer.echo(f"[ingest] manifest: {result.manifest_path}")


@app.command(help="Aggregate trips into a zone x hour panel with DuckDB.")
def panel(config: ConfigOption = DEFAULT_CONFIG) -> None:
    """Run the panel stage (interim -> processed).

    Args:
        config: Path to the YAML config file.
    """
    cfg = _load(config)
    typer.echo(
        f"[panel] {cfg.paths.interim} -> {cfg.paths.processed} "
        f"(grain={cfg.forecast.grain}, bin={cfg.forecast.bin}) (not implemented)"
    )


@app.command(help="Build leakage-safe model features with Polars.")
def features(config: ConfigOption = DEFAULT_CONFIG) -> None:
    """Run the features stage (panel -> feature table).

    Args:
        config: Path to the YAML config file.
    """
    cfg = _load(config)
    typer.echo(f"[features] horizons={cfg.forecast.horizons} (not implemented)")


@app.command(help="Train forecasting models.")
def train(config: ConfigOption = DEFAULT_CONFIG) -> None:
    """Run the train stage (feature table -> fitted models).

    Args:
        config: Path to the YAML config file.
    """
    _load(config)
    typer.echo("[train] (not implemented)")


@app.command(help="Run rolling-origin backtests and report metrics.")
def backtest(config: ConfigOption = DEFAULT_CONFIG) -> None:
    """Run the backtest stage (models + features -> metrics).

    Args:
        config: Path to the YAML config file.
    """
    _load(config)
    typer.echo("[backtest] (not implemented)")
