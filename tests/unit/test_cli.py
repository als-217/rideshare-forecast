import pytest
import yaml
from typer.testing import CliRunner

from rideforecast.cli import app

from .conftest import trip_row

runner = CliRunner()

STAGES = ["ingest", "panel", "features", "train", "backtest"]
STUB_STAGES = ["features", "train", "backtest"]


def test_help_lists_every_stage():
    """The top-level help mentions all five pipeline stages."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for stage in STAGES:
        assert stage in result.output


@pytest.mark.parametrize("stage", STUB_STAGES)
def test_stub_stage_runs_with_base_config(stage):
    """Each not-yet-implemented stage loads the default config and exits cleanly."""
    result = runner.invoke(app, [stage])
    assert result.exit_code == 0
    assert f"[{stage}]" in result.output


@pytest.mark.parametrize("stage", STAGES)
def test_missing_config_exits_with_error(stage):
    """A nonexistent config path gives exit code 1 and a clear message."""
    result = runner.invoke(app, [stage, "--config", "configs/does_not_exist.yaml"])
    assert result.exit_code == 1
    assert "not found" in result.output


def write_config(cfg, tmp_path):
    """Write a Config to a YAML file the CLI can load.

    Args:
        cfg: The Config to write.
        tmp_path: pytest's per-test temporary directory.

    Returns:
        Location of the written YAML file.
    """
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg.model_dump(mode="json")))
    return path


def test_ingest_succeeds_on_valid_data(write_raw, make_cfg, tmp_path):
    """The ingest command processes valid raw data and reports what it published."""
    write_raw([trip_row(1), trip_row(2)])
    config_path = write_config(make_cfg(), tmp_path)

    result = runner.invoke(app, ["ingest", "--config", str(config_path)])
    assert result.exit_code == 0, result.output
    assert "2024-03: 2 rows published from 1 file(s)" in result.output
    assert "manifest:" in result.output


def test_ingest_failure_exits_with_error(write_raw, make_cfg, tmp_path):
    """A file that fails its audit gives exit code 1 and a clear message."""
    write_raw([trip_row(1, Fare="oops")])
    config_path = write_config(make_cfg(), tmp_path)

    result = runner.invoke(app, ["ingest", "--config", str(config_path)])
    assert result.exit_code == 1
    assert "[ingest] failed" in result.output


def test_panel_succeeds_on_interim_trips(write_interim, make_cfg, tmp_path):
    """The panel command builds the panel and reports where it was written."""
    write_interim([("2024-03-12 08:15:00", 8), ("2024-03-12 09:00:00", None)])
    config_path = write_config(make_cfg(), tmp_path)

    result = runner.invoke(app, ["panel", "--config", str(config_path)])
    assert result.exit_code == 0, result.output
    assert "2 trips, 24 hours" in result.output
    assert "1 trips without a pickup area" in result.output


def test_panel_without_interim_exits_with_error(make_cfg, tmp_path):
    """Running the panel before ingest gives exit code 1 and a clear message."""
    config_path = write_config(make_cfg(), tmp_path)

    result = runner.invoke(app, ["panel", "--config", str(config_path)])
    assert result.exit_code == 1
    assert "Run ingest first" in result.output
