"""Logging configuration: stdout + rotating files, with credential redaction."""

from __future__ import annotations

import logging
import logging.handlers
import re
from pathlib import Path

_REDACTIONS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"(?i)(api_hash\s*[=:]\s*)\S+"), r"\1***"),
    (re.compile(r"(?i)(api_id\s*[=:]\s*)\S+"), r"\1***"),
    (re.compile(r"(?i)(password\s*[=:]\s*)\S+"), r"\1***"),
    (re.compile(r"(?i)(code\s*[=:]\s*)\d{3,}"), r"\1***"),
    (re.compile(r"\b[0-9a-f]{32}\b"), "***"),  # bare API hashes
    (re.compile(r"(?i)(authorization:\s*bearer\s+)\S+"), r"\1***"),
)


class RedactingFilter(logging.Filter):
    """Scrub credential-looking substrings from log records."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            message = record.getMessage()
        except Exception:  # pragma: no cover - defensive
            return True
        for pattern, replacement in _REDACTIONS:
            message = pattern.sub(replacement, message)
        record.msg = message
        record.args = None
        return True


def setup_logging(
    log_dir: Path,
    *,
    level: str = "INFO",
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 5,
    filename: str = "collector.log",
) -> logging.Logger:
    """Configure the root logger once.  Safe to call repeatedly."""

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(name)s: %(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z"
    )
    redactor = RedactingFilter()

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.addFilter(redactor)
    root.addHandler(console)

    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / filename,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.addFilter(redactor)
    root.addHandler(file_handler)

    # Telethon is chatty at INFO and can log entity details.
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return root
