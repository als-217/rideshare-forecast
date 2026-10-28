from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class SocrataCredentials(BaseSettings):
    """Chicago Data Portal API credentials, read from the environment or .env."""

    model_config = SettingsConfigDict(env_prefix="SOCRATA_", env_file=".env", extra="ignore")

    key_id: str
    key_secret: SecretStr
