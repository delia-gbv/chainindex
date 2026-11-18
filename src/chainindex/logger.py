"""Logging as colored text locally and one JSON object per line everywhere else, with secrets masked.

A JSON line has ``time``, a lowercase ``level`` (debug, info, warn, error), ``logger`` and ``msg``, which log shippers
such as Grafana Alloy or Vector can turn into labels as they are.
"""

import json
import logging
import logging.config
import os
import socket
import sys
import threading
from collections.abc import Sequence
from datetime import UTC, datetime

_PID = os.getpid()
_HOSTNAME = socket.gethostname()
_LEVEL_NAMES = {logging.DEBUG: "debug", logging.INFO: "info", logging.WARNING: "warn", logging.ERROR: "error"}
# Chatty libraries are held at WARNING: some log request URLs, and an RPC URL carries its key.
_QUIET_LOGGERS = ("urllib3", "httpcore", "httpx", "aiohttp", "websockets", "asyncio", "clickhouse_connect")


def _level_name(levelno: int) -> str:
    if levelno >= logging.CRITICAL:
        return "error"
    return _LEVEL_NAMES.get(levelno, logging.getLevelName(levelno).lower())


class RedactingFormatter(logging.Formatter):
    """Masks known secrets in everything it formats, tracebacks included."""

    def __init__(self, redact: Sequence[str] = ()):
        super().__init__()
        # Longest first, so a secret that starts a longer one cannot leave the rest of it behind.
        self.secrets = sorted({secret for secret in redact if secret}, key=len, reverse=True)

    def redact(self, text: str) -> str:
        """Returns the text with every secret replaced by ``***``."""
        for secret in self.secrets:
            text = text.replace(secret, "***")
        return text


class JSONFormatter(RedactingFormatter):
    """Formats each record as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "time": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": _level_name(record.levelno),
            "logger": record.name,
            "msg": self.redact(record.getMessage()),
            "pid": _PID,
            "hostname": _HOSTNAME,
        }
        # Redacted before encoding, since JSON escapes would hide a secret holding quotes or backslashes.
        if record.exc_info:
            entry["exc_info"] = self.redact(self.formatException(record.exc_info))
        if record.stack_info:
            entry["stack_info"] = self.redact(self.formatStack(record.stack_info))
        return json.dumps(entry, separators=(",", ":"), default=str)


class ColoredFormatter(RedactingFormatter):
    """Formats records as readable text for a terminal."""

    COLORS = {"DEBUG": "\033[36m", "INFO": "\033[32m", "WARNING": "\033[33m", "ERROR": "\033[31m", "CRITICAL": "\033[35m"}
    RESET = "\033[0m"
    BOLD = "\033[1m"

    def format(self, record: logging.LogRecord) -> str:
        time_text = datetime.fromtimestamp(record.created).strftime("%H:%M:%S.%f")[:-3]
        color = self.COLORS.get(record.levelname)
        level = f"{color}{record.levelname:<7}{self.RESET}" if color else f"{record.levelname:<7}"
        line = f"[{time_text}] {level} {self.BOLD}[{record.name}]{self.RESET} {record.getMessage()}"
        if record.exc_info:
            line += f"\n{self.formatException(record.exc_info)}"
        if record.stack_info:
            line += f"\n{self.formatStack(record.stack_info)}"
        return self.redact(line)


def _log_uncaught(error: BaseException | None) -> None:
    """Logs a crash through the root handler, so its traceback is masked like any other record."""
    logging.getLogger().critical("Uncaught exception.", exc_info=error)


def get_logger(name: str) -> logging.Logger:
    """Returns a named logger."""
    return logging.getLogger(name)


def configure_logging(env: str = "local", *, level: str | None = None, redact: Sequence[str] | None = None) -> None:
    """Installs the root handler: colored text when ``env`` is local, JSON lines otherwise.

    ``LOG_LEVEL`` in the environment wins over ``level``. ``redact`` strings are masked in every line, since API keys
    end up inside provider URLs and so inside exception messages.
    """
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)
    chosen = os.environ.get("LOG_LEVEL", level or "INFO").upper()
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "json": {"()": JSONFormatter, "redact": redact or ()},
                "colored": {"()": ColoredFormatter, "redact": redact or ()},
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                    "formatter": "colored" if env == "local" else "json",
                }
            },
            "root": {"level": logging.getLevelNamesMapping().get(chosen, logging.INFO), "handlers": ["console"]},
        }
    )
    sys.excepthook = lambda kind, error, traceback: _log_uncaught(error)
    threading.excepthook = lambda uncaught: _log_uncaught(uncaught.exc_value)
    for name in _QUIET_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
