"""ClickHouse over HTTP, addressed by the ``CLICKHOUSE_*`` settings."""

import re
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

    from chainindex.settings import BaseSettings

# What ClickHouse accepts as an unquoted identifier, so a name that matches can go into SQL as is.
DATABASE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# A pod that has just started may not reach ClickHouse for a few seconds (network policies, a server still starting).
CONNECT_ATTEMPTS = 15
CONNECT_PAUSE_SECONDS = 2.0


def safe_database(name: str) -> str:
    """Returns a database name after checking it can be put into SQL as is."""
    if not DATABASE_NAME.fullmatch(name):
        raise ValueError(f"Unsafe ClickHouse database name {name!r}")
    return name


def is_clickhouse_error(error: BaseException) -> bool:
    """Tells whether an error came from ClickHouse or from reaching it, which every stream shares."""
    from clickhouse_connect.driver.exceptions import ClickHouseError

    return isinstance(error, ClickHouseError)


def get_client(settings: "BaseSettings", *, database: str | None = None, **overrides: Any) -> "Client":
    """Connects a ``clickhouse_connect`` client, retrying a refused connection for about 30 s.

    ``database`` overrides ``CLICKHOUSE_DATABASE``; extra keyword arguments go straight to ``clickhouse_connect``.
    """
    import clickhouse_connect
    from clickhouse_connect.driver.exceptions import OperationalError

    def connect() -> "Client":
        return clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=database or settings.CLICKHOUSE_DATABASE,
            secure=settings.CLICKHOUSE_SECURE,
            **overrides,
        )

    for _attempt in range(CONNECT_ATTEMPTS - 1):
        try:
            return connect()
        except OperationalError:
            time.sleep(CONNECT_PAUSE_SECONDS)
    return connect()
