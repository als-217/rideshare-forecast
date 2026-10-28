from pathlib import Path

import pytest
from pydantic import ValidationError

from rideforecast.config import Config, load_config


def make_valid_data() -> dict[str, dict]:
    """Build a known-good raw config dictionary.

    Returns:
        A new nested dictionary that passes validation. A fresh copy is
        returned on every call so tests can modify it without affecting
        each other.
    """
    return {
        "paths": {"raw": "a", "interim": "b", "processed": "c"},
        "forecast": {"horizons": [1, 2], "bin": "1h", "grain": "community_area"},
    }


def test_load_base_config():
    """The real configs/base.yaml loads, with sensible horizons and Path types."""
    cfg = load_config("configs/base.yaml")
    assert cfg.forecast.horizons
    assert all(1 <= h <= 6 for h in cfg.forecast.horizons)
    assert cfg.paths.raw == Path("data/raw")


def test_missing_file_raises():
    """A nonexistent config path raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_config("configs/does_not_exist.yaml")


def test_valid_data_parses():
    """Control case: the known-good data validates, so failure tests are meaningful."""
    cfg = Config.model_validate(make_valid_data())
    assert cfg.forecast.horizons == [1, 2]


def test_unsupported_bin_rejected():
    """A bin size other than 1h is rejected until other sizes are supported."""
    data = make_valid_data()
    data["forecast"]["bin"] = "15min"
    with pytest.raises(ValidationError, match="Input should be '1h'"):
        Config.model_validate(data)


def test_zero_horizon_rejected():
    """A horizon of 0 (predicting the present) is rejected."""
    data = make_valid_data()
    data["forecast"]["horizons"] = [0, 1]
    with pytest.raises(ValidationError, match="must be >= 1"):
        Config.model_validate(data)


def test_empty_horizons_rejected():
    """An empty horizon list is rejected."""
    data = make_valid_data()
    data["forecast"]["horizons"] = []
    with pytest.raises(ValidationError, match="must not be empty"):
        Config.model_validate(data)


def test_unsorted_horizon_rejected():
    """Horizons that are not in ascending order are rejected."""
    data = make_valid_data()
    data["forecast"]["horizons"] = [3, 1]
    with pytest.raises(ValidationError, match="sorted and unique"):
        Config.model_validate(data)


def test_duplicate_horizon_rejected():
    """Repeated horizons are rejected."""
    data = make_valid_data()
    data["forecast"]["horizons"] = [1, 1, 2]
    with pytest.raises(ValidationError, match="sorted and unique"):
        Config.model_validate(data)


def test_unknown_key_rejected():
    """A misspelled or unexpected key is rejected rather than silently ignored."""
    data = make_valid_data()
    data["forecast"]["horizon"] = [1]  # typo: extra key
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        Config.model_validate(data)
