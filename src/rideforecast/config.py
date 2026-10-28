from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, field_validator


class Paths(BaseModel):
    """Filesystem locations for each stage of the data pipeline."""

    model_config = ConfigDict(extra="forbid")

    raw: Path
    interim: Path
    processed: Path


class Forecast(BaseModel):
    """Settings that define the forecasting task.

    Attributes:
        horizons: Hours ahead to predict, e.g. [1, 2, 3] means t+1, t+2, t+3.
        bin: Time bucket size for the panel, e.g. "1h".
        grain: Spatial unit of the panel, e.g. "community_area".
    """

    model_config = ConfigDict(extra="forbid")

    horizons: list[int]
    bin: str
    grain: str

    @field_validator("horizons")
    @classmethod
    def horizons_positive_and_sorted(cls, v: list[int]) -> list[int]:
        """Validate the forecast horizons.

        Pydantic calls this automatically while parsing, after it has checked
        that the value is a list of integers.

        Args:
            v: The horizons parsed from the config, in hours ahead.

        Returns:
            The same list, unchanged, if it passes all checks. Pydantic uses
            this return value as the field's final value.

        Raises:
            ValueError: If the list is empty, contains a value below 1, or is
                not sorted ascending with no duplicates. Pydantic catches this
                and reports it as a ValidationError naming the field.
        """
        if not v:
            raise ValueError("horizons must not be empty")
        if any(h < 1 for h in v):
            raise ValueError("horizons must be >= 1 (hours ahead)")
        if v != sorted(set(v)):
            raise ValueError("horizons must be sorted and unique")
        return v


class Config(BaseModel):
    """Top-level project configuration, mirroring the structure of the YAML file."""

    model_config = ConfigDict(extra="forbid")

    paths: Paths
    forecast: Forecast


def load_config(path: str | Path = "configs/base.yaml") -> Config:
    """Load and validate the project configuration from a YAML file.

    Args:
        path: Location of the YAML file, as a string or Path. Defaults to
            "configs/base.yaml", relative to the current working directory.

    Returns:
        A fully validated Config object.

    Raises:
        FileNotFoundError: If no file exists at `path`.
        pydantic.ValidationError: If the file's contents are missing fields,
            have the wrong types, contain unknown keys, or fail a validator.
    """
    with open(path) as f:
        data = yaml.safe_load(f)
    return Config.model_validate(data)
