"""Settings read from the environment (and a local ``.env``): the run mode, logging, metrics and ClickHouse."""

from typing import Any, ClassVar

from pydantic import SecretStr
from pydantic_settings import BaseSettings as PydanticBaseSettings
from pydantic_settings import SettingsConfigDict


class BaseSettings(PydanticBaseSettings):
    """Holds what every command needs: where ClickHouse is, how to log, and where to serve metrics."""

    # "local" logs colored text; anything else logs one JSON object per line.
    ENV: str = "local"
    LOG_LEVEL: str = "INFO"
    # Unset turns the Prometheus endpoint off.
    METRICS_PORT: int | None = None

    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "default"
    CLICKHOUSE_SECURE: bool = False

    # Fields whose names say they hold a secret. An RPC URL can carry its provider's key in the path.
    SENSITIVE_KEYWORDS: ClassVar[tuple[str, ...]] = ("password", "secret", "token", "key", "private", "rpc_url", "_urls")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", env_ignore_empty=True, case_sensitive=False)

    def model_post_init(self, __context: Any) -> None:
        self.validate_config()

    def validate_config(self) -> None:
        """Checks fields against each other, for subclasses to override."""

    def is_sensitive(self, field_name: str) -> bool:
        """Tells whether a field name looks like it holds a secret."""
        lowered = field_name.lower()
        return any(keyword in lowered for keyword in self.SENSITIVE_KEYWORDS)

    def redactable(self) -> list[str]:
        """Lists the secret values that must never appear in log output, each item of a comma-separated one too."""
        secrets: list[str] = []
        for name, value in self.model_dump().items():
            if value and (isinstance(value, SecretStr) or self.is_sensitive(name)):
                text = value.get_secret_value() if isinstance(value, SecretStr) else str(value)
                items = [item.strip() for item in text.split(",")]
                secrets += [text, *(item for item in items if item and item != text)]
        return secrets
