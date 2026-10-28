"""Configuration loading and validation.

Parsing, validation, fatal errors and warnings are kept separate on purpose:

* ``load_config()`` raises :class:`ConfigError` only for **malformed values**
  (a bad integer, a bad date, a bad boolean) and — when
  ``require_credentials=True`` — for every problem that makes the collector
  unsafe to start.
* :meth:`Config.problems` returns structured ``Problem`` objects with a
  severity, so callers never have to pattern-match English prose.
* Commands that only read local state (``status``, ``db-maintenance``) load the
  configuration with ``require_credentials=False`` and are never blocked by
  missing Telegram credentials.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Literal

ForwardMode = Literal["forward", "copy"]

_SHA256_HEX = re.compile(r"^[0-9a-fA-F]{32}$")
_TRUE = {"1", "true", "yes", "on", "y"}
_FALSE = {"0", "false", "no", "off", "n", ""}

ERROR = "error"
WARNING = "warning"


class ConfigError(RuntimeError):
    """Raised when configuration is malformed or unsafe to start with."""


# --------------------------------------------------------------------------
# small env helpers
# --------------------------------------------------------------------------
def _env(environ: dict[str, str], key: str, default: str | None = None) -> str | None:
    value = environ.get(key)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def _as_bool(environ: dict[str, str], key: str, default: bool = False) -> bool:
    raw = _env(environ, key)
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    raise ConfigError(f"{key} must be a boolean (got {raw!r})")


def _as_int(
    environ: dict[str, str],
    key: str,
    default: int | None = None,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    raw = _env(environ, key)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{key} must be an integer (got {raw!r})") from exc
    if minimum is not None and value < minimum:
        raise ConfigError(f"{key} must be >= {minimum} (got {value})")
    if maximum is not None and value > maximum:
        raise ConfigError(f"{key} must be <= {maximum} (got {value})")
    return value


def _as_float(
    environ: dict[str, str],
    key: str,
    default: float,
    *,
    minimum: float | None = None,
) -> float:
    raw = _env(environ, key)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{key} must be a number (got {raw!r})") from exc
    if minimum is not None and value < minimum:
        raise ConfigError(f"{key} must be >= {minimum} (got {value})")
    return value


def _as_date(environ: dict[str, str], key: str) -> datetime | None:
    raw = _env(environ, key)
    if raw is None:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ConfigError(
            f"{key} must be an ISO-8601 date or datetime, e.g. 2024-01-31 (got {raw!r})"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


# --------------------------------------------------------------------------
# source groups
# --------------------------------------------------------------------------
@dataclass(slots=True)
class SourceGroup:
    """One source chat to collect from.

    ``identifier`` is whatever Telethon can resolve: a public ``@username``, a
    numeric chat id (``-1001234567890``), a ``t.me`` invite link the account has
    already joined, or a stored phone/entity string.  Access control is never
    bypassed -- the account must already be a member.
    """

    identifier: str
    alias: str | None = None
    enabled: bool = True

    @property
    def label(self) -> str:
        return self.alias or self.identifier


def parse_sources(raw: str | None) -> list[SourceGroup]:
    """Parse ``SOURCE_GROUPS``.

    Grammar: comma or newline separated entries, each

        identifier
        identifier|alias
        identifier|alias|disabled

    Blank entries and ``#`` comments are ignored so the variable can be written
    across several lines in ``.env``.
    """

    if not raw:
        return []
    groups: list[SourceGroup] = []
    seen: set[str] = set()
    for chunk in re.split(r"[,\n]", raw):
        entry = chunk.split("#", 1)[0].strip()
        if not entry:
            continue
        parts = [p.strip() for p in entry.split("|")]
        identifier = parts[0]
        if not identifier:
            continue
        alias = parts[1] if len(parts) > 1 and parts[1] else None
        enabled = True
        if len(parts) > 2 and parts[2]:
            enabled = parts[2].lower() not in {"disabled", "off", "no", "false", "0"}
        key = identifier.lower().lstrip("@")
        if key in seen:
            continue
        seen.add(key)
        groups.append(SourceGroup(identifier=identifier, alias=alias, enabled=enabled))
    return groups


@dataclass(slots=True, frozen=True)
class Problem:
    """One validation finding, with a machine-readable severity."""

    severity: str
    field: str
    message: str

    def __str__(self) -> str:  # pragma: no cover - display only
        return f"[{self.severity}] {self.field}: {self.message}"


# --------------------------------------------------------------------------
# main config object
# --------------------------------------------------------------------------
@dataclass(slots=True)
class Config:
    api_id: int | None
    api_hash: str | None
    destination_chat: str | None
    sources: list[SourceGroup] = field(default_factory=list)

    # storage
    data_dir: Path = Path("data")
    session_path: Path | None = None
    database_path: Path | None = None
    log_dir: Path | None = None
    restrict_permissions: bool = True

    # behaviour
    forward_mode: ForwardMode = "forward"
    annotate_source: bool = False
    include_edits: bool = False
    include_deleted: bool = False
    include_service_messages: bool = False
    include_outgoing: bool = False

    # history window (all accessible history by default)
    history_start_date: datetime | None = None
    history_end_date: datetime | None = None
    history_max_count: int | None = None
    history_batch_size: int = 100

    # queue / workers
    worker_concurrency: int = 1
    queue_poll_interval: float = 2.0
    lease_seconds: float = 300.0
    max_attempts: int = 5
    verify_before_retry: bool = True
    retry_verify_scan_limit: int = 100
    backoff_base: float = 2.0
    backoff_cap: float = 900.0
    min_forward_interval: float = 0.6

    # supervision / observability
    heartbeat_interval: float = 30.0
    import_recheck_interval: float = 300.0
    stale_heartbeat_seconds: float = 180.0
    reconcile_scan_limit: int = 500
    event_retention_days: int = 30

    # dashboard (optional)
    dashboard_enabled: bool = False
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8080
    dashboard_username: str | None = None
    dashboard_password: str | None = None

    log_level: str = "INFO"
    log_max_bytes: int = 5 * 1024 * 1024
    log_backup_count: int = 5

    def __post_init__(self) -> None:
        self.data_dir = Path(self.data_dir).expanduser()
        self.session_path = Path(
            self.session_path or self.data_dir / "collector.session"
        ).expanduser()
        self.database_path = Path(
            self.database_path or self.data_dir / "collector.sqlite3"
        ).expanduser()
        self.log_dir = Path(self.log_dir or self.data_dir / "logs").expanduser()

    # -- convenience -------------------------------------------------------
    @property
    def enabled_sources(self) -> list[SourceGroup]:
        return [s for s in self.sources if s.enabled]

    def ensure_directories(self) -> None:
        """Create directories, owner-only when ``restrict_permissions``."""

        mode = 0o700 if self.restrict_permissions else 0o755
        for path in (self.data_dir, self.log_dir):
            path.mkdir(parents=True, exist_ok=True, mode=mode)
        self.database_path.parent.mkdir(parents=True, exist_ok=True, mode=mode)
        if self.restrict_permissions:
            for path in (self.data_dir, self.log_dir, self.database_path.parent):
                try:
                    os.chmod(path, mode)
                except OSError:  # pragma: no cover - filesystem dependent
                    pass

    # -- validation --------------------------------------------------------
    def problems(self, *, require_credentials: bool = True) -> list[Problem]:
        """Structured validation findings (errors and warnings)."""

        found: list[Problem] = []

        def error(field_name: str, message: str) -> None:
            found.append(Problem(ERROR, field_name, message))

        def warning(field_name: str, message: str) -> None:
            found.append(Problem(WARNING, field_name, message))

        if require_credentials:
            if self.api_id is None:
                error("API_ID", "missing or not an integer")
            elif self.api_id <= 0:
                error("API_ID", "must be a positive integer")
            if not self.api_hash:
                error("API_HASH", "missing")
            elif not _SHA256_HEX.match(self.api_hash):
                error("API_HASH", "must be 32 hexadecimal characters")
            if not self.destination_chat:
                error("DESTINATION_CHAT", "missing (private channel you control)")

        if not self.sources:
            error("SOURCE_GROUPS", "empty: configure at least one source chat")
        elif not self.enabled_sources:
            error("SOURCE_GROUPS", "all entries are disabled")

        if self.destination_chat:
            dest_key = self.destination_chat.lower().lstrip("@")
            for source in self.enabled_sources:
                if source.identifier.lower().lstrip("@") == dest_key:
                    error(
                        "SOURCE_GROUPS",
                        f"destination {self.destination_chat!r} is also configured as a source; "
                        "this would loop messages into themselves",
                    )

        if self.forward_mode not in ("forward", "copy"):
            error("FORWARD_MODE", "must be 'forward' or 'copy'")
        if self.forward_mode == "copy":
            warning(
                "FORWARD_MODE",
                "copy mode loses native attribution and does not preserve album grouping",
            )
        if not 1 <= self.worker_concurrency <= 4:
            error("WORKER_CONCURRENCY", "must be between 1 and 4 (Telegram rate limits)")
        if self.max_attempts < 1:
            error("MAX_ATTEMPTS", "must be >= 1")
        if self.retry_verify_scan_limit < 1:
            error("RETRY_VERIFY_SCAN_LIMIT", "must be >= 1")
        if self.history_batch_size < 1:
            error("HISTORY_BATCH_SIZE", "must be >= 1")
        if self.lease_seconds <= 0:
            error("LEASE_SECONDS", "must be > 0")
        if self.queue_poll_interval <= 0:
            error("QUEUE_POLL_INTERVAL", "must be > 0")
        if self.heartbeat_interval <= 0:
            error("HEARTBEAT_INTERVAL", "must be > 0")
        if self.reconcile_scan_limit < 1:
            error("RECONCILE_SCAN_LIMIT", "must be >= 1")
        if self.history_max_count is not None and self.history_max_count < 1:
            error("HISTORY_MAX_COUNT", "must be >= 1 when set")
        if self.history_start_date and self.history_end_date:
            if self.history_start_date > self.history_end_date:
                error("HISTORY_START_DATE", "is after HISTORY_END_DATE")
        if self.event_retention_days < 0:
            error("EVENT_RETENTION_DAYS", "must be >= 0 (0 disables pruning)")

        if self.dashboard_enabled:
            if not self.dashboard_username or not self.dashboard_password:
                error(
                    "DASHBOARD_USERNAME",
                    "DASHBOARD_USERNAME and DASHBOARD_PASSWORD are required when the dashboard is enabled",
                )
            elif len(self.dashboard_password) < 12:
                warning("DASHBOARD_PASSWORD", "shorter than 12 characters")
            if self.dashboard_host not in ("127.0.0.1", "localhost", "::1"):
                warning(
                    "DASHBOARD_HOST",
                    "binds beyond loopback; put TLS and a reverse proxy in front of it",
                )

        return found

    def errors(self, *, require_credentials: bool = True) -> list[str]:
        return [
            f"{p.field}: {p.message}"
            for p in self.problems(require_credentials=require_credentials)
            if p.severity == ERROR
        ]

    def warnings(self) -> list[str]:
        return [
            f"{p.field}: {p.message}" for p in self.problems() if p.severity == WARNING
        ]

    def validate(self, *, require_credentials: bool = True) -> list[str]:
        """Backwards-compatible error list (empty means valid)."""

        return self.errors(require_credentials=require_credentials)


# --------------------------------------------------------------------------
# loader
# --------------------------------------------------------------------------
def load_dotenv(path: str | Path = ".env", *, override: bool = False) -> None:
    """Load ``.env`` if python-dotenv is available; otherwise ignore quietly."""

    env_path = Path(path)
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv as _load  # type: ignore
    except ImportError:  # pragma: no cover - optional dependency
        return
    _load(dotenv_path=env_path, override=override)


def load_config(
    environ: dict[str, str] | None = None,
    *,
    dotenv_path: str | Path | None = ".env",
    require_credentials: bool = True,
) -> Config:
    """Build a :class:`Config` from the environment.

    Malformed values always raise :class:`ConfigError`.  When
    ``require_credentials`` is true, *every* validation error is fatal too, so
    an unsafe configuration can never reach the collector.
    """

    if environ is None:
        if dotenv_path:
            load_dotenv(dotenv_path)
        environ = dict(os.environ)

    env = environ
    data_dir = Path(_env(env, "DATA_DIR", "./data") or "./data")

    config = Config(
        api_id=_as_int(env, "API_ID"),
        api_hash=_env(env, "API_HASH"),
        destination_chat=_env(env, "DESTINATION_CHAT"),
        sources=parse_sources(_env(env, "SOURCE_GROUPS")),
        data_dir=data_dir,
        session_path=Path(_env(env, "SESSION_PATH", str(data_dir / "collector.session")) or ""),
        database_path=Path(_env(env, "DATABASE_PATH", str(data_dir / "collector.sqlite3")) or ""),
        log_dir=Path(_env(env, "LOG_DIR", str(data_dir / "logs")) or ""),
        restrict_permissions=_as_bool(env, "RESTRICT_PERMISSIONS", True),
        forward_mode=(_env(env, "FORWARD_MODE", "forward") or "forward").lower(),  # type: ignore[arg-type]
        annotate_source=_as_bool(env, "ANNOTATE_SOURCE", False),
        include_edits=_as_bool(env, "INCLUDE_EDITS", False),
        include_deleted=_as_bool(env, "INCLUDE_DELETED", False),
        include_service_messages=_as_bool(env, "INCLUDE_SERVICE_MESSAGES", False),
        include_outgoing=_as_bool(env, "INCLUDE_OUTGOING", False),
        history_start_date=_as_date(env, "HISTORY_START_DATE"),
        history_end_date=_as_date(env, "HISTORY_END_DATE"),
        history_max_count=_as_int(env, "HISTORY_MAX_COUNT", None, minimum=1),
        history_batch_size=_as_int(env, "HISTORY_BATCH_SIZE", 100, minimum=1, maximum=500) or 100,
        worker_concurrency=_as_int(env, "WORKER_CONCURRENCY", 1, minimum=1, maximum=4) or 1,
        queue_poll_interval=_as_float(env, "QUEUE_POLL_INTERVAL", 2.0, minimum=0.1),
        lease_seconds=_as_float(env, "LEASE_SECONDS", 300.0, minimum=5.0),
        max_attempts=_as_int(env, "MAX_ATTEMPTS", 5, minimum=1) or 5,
        verify_before_retry=_as_bool(env, "VERIFY_BEFORE_RETRY", True),
        retry_verify_scan_limit=_as_int(env, "RETRY_VERIFY_SCAN_LIMIT", 100, minimum=1) or 100,
        backoff_base=_as_float(env, "BACKOFF_BASE", 2.0, minimum=0.1),
        backoff_cap=_as_float(env, "BACKOFF_CAP", 900.0, minimum=1.0),
        min_forward_interval=_as_float(env, "MIN_FORWARD_INTERVAL", 0.6, minimum=0.0),
        heartbeat_interval=_as_float(env, "HEARTBEAT_INTERVAL", 30.0, minimum=1.0),
        import_recheck_interval=_as_float(env, "IMPORT_RECHECK_INTERVAL", 300.0, minimum=10.0),
        stale_heartbeat_seconds=_as_float(env, "STALE_HEARTBEAT_SECONDS", 180.0, minimum=10.0),
        reconcile_scan_limit=_as_int(env, "RECONCILE_SCAN_LIMIT", 500, minimum=1) or 500,
        event_retention_days=_as_int(env, "EVENT_RETENTION_DAYS", 30, minimum=0) or 0,
        dashboard_enabled=_as_bool(env, "DASHBOARD_ENABLED", False),
        dashboard_host=_env(env, "DASHBOARD_HOST", "127.0.0.1") or "127.0.0.1",
        dashboard_port=_as_int(env, "DASHBOARD_PORT", 8080, minimum=1, maximum=65535) or 8080,
        dashboard_username=_env(env, "DASHBOARD_USERNAME"),
        dashboard_password=_env(env, "DASHBOARD_PASSWORD"),
        log_level=(_env(env, "LOG_LEVEL", "INFO") or "INFO").upper(),
        log_max_bytes=_as_int(env, "LOG_MAX_BYTES", 5 * 1024 * 1024, minimum=65536)
        or 5 * 1024 * 1024,
        log_backup_count=_as_int(env, "LOG_BACKUP_COUNT", 5, minimum=1) or 5,
    )

    if require_credentials:
        errors = config.errors(require_credentials=True)
        if errors:
            raise ConfigError("; ".join(errors))
    return config


def describe(config: Config) -> str:
    """Redacted, human readable summary.  Never includes secrets."""

    lines: Iterable[str] = (
        f"api_id            : {config.api_id if config.api_id else '(unset)'}",
        "api_hash          : " + ("(set)" if config.api_hash else "(unset)"),
        f"session_path      : {config.session_path}",
        f"database_path     : {config.database_path}",
        f"log_dir           : {config.log_dir}",
        f"destination_chat  : {config.destination_chat or '(unset)'}",
        f"forward_mode      : {config.forward_mode}",
        f"annotate_source   : {config.annotate_source}",
        f"worker_concurrency: {config.worker_concurrency}",
        f"max_attempts      : {config.max_attempts}",
        f"history_limit     : {config.history_max_count or 'all accessible history'}",
        f"dashboard_enabled : {config.dashboard_enabled}",
        f"sources ({len(config.enabled_sources)} enabled / {len(config.sources)} total):",
    )
    body = list(lines)
    for source in config.sources:
        state = "enabled " if source.enabled else "disabled"
        body.append(
            f"  - [{state}] {source.identifier}"
            + (f"  (alias: {source.alias})" if source.alias else "")
        )
    return "\n".join(body)
