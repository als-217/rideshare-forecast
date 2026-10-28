import pytest
from typer.testing import CliRunner

from rideforecast.cli import app

runner = CliRunner()

STAGES = ["ingest", "panel", "features", "train", "backtest"]


def test_help_lists_every_stage():
    """The top-level help mentions all five pipeline stages."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for stage in STAGES:
        assert stage in result.output


@pytest.mark.parametrize("stage", STAGES)
def test_stage_runs_with_base_config(stage):
    """Each stage loads the default config and exits cleanly."""
    result = runner.invoke(app, [stage])
    assert result.exit_code == 0
    assert f"[{stage}]" in result.output


@pytest.mark.parametrize("stage", STAGES)
def test_missing_config_exits_with_error(stage):
    """A nonexistent config path gives exit code 1 and a clear message."""
    result = runner.invoke(app, [stage, "--config", "configs/does_not_exist.yaml"])
    assert result.exit_code == 1
    assert "not found" in result.output
