"""SQLite persistence layer.

Design notes
------------
* One connection, WAL journal, ``busy_timeout`` set, and a single asyncio lock
  guarding every multi-statement transaction.  Writes are therefore serialized
  inside the process, which removes SQLITE_BUSY between workers while keeping
  throughput irrelevant (forwarding is rate limited by Telegram long before
  SQLite becomes the bottleneck).
* ``processed_messages`` is the deduplication table: ``UNIQUE(source_chat_id,
  source_message_id)`` is the key that makes re-discovery harmless.
* ``processing_queue`` is the durable work queue, with an explicit lifecycle:

      pending --claim--> leased --mark_sending--> sending --complete--> done
          ^                 |                        |
          |                 | lease expires          | lease expires
          |                 v                        v
          +-------------- pending                  ambiguous  (never auto-claimed)

  ``leased`` means "claimed, nothing sent yet" so an expired lease can safely be
  re-queued.  ``sending`` means "the Telegram request may already be in flight",
  so an expired lease becomes ``ambiguous`` and waits for reconciliation.  This
  is what stops a crash from blindly re-forwarding a message.
* A queue row is a *delivery unit*: one message, or one album (several messages
  sharing a ``grouped_id``).  ``processed_messages.unit_key`` links each member
  back to its unit so a single forward marks the whole album, and
  ``destination_message_ids`` records every destination id Telegram returned.
* A read-only connection mode exists for the dashboard: it opens the file with
  ``mode=ro``, never changes the journal mode and never runs migrations.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

try:  # aiosqlite is the preferred driver
    import aiosqlite

    DRIVER = "aiosqlite"
    _Row = aiosqlite.Row
except ImportError:  # pragma: no cover - exercised by the stdlib-driver tests
    aiosqlite = None  # type: ignore[assignment]
    DRIVER = "sqlite3 (stdlib fallback)"
    _Row = None

SCHEMA_VERSION = 2

# Queue states.  ``ambiguous`` and ``dead`` are never claimed by workers.
QUEUE_PENDING = "pending"
QUEUE_LEASED = "leased"
QUEUE_SENDING = "sending"
QUEUE_DONE = "done"
QUEUE_AMBIGUOUS = "ambiguous"
QUEUE_DEAD = "dead"

MESSAGE_STATUSES = (
    "discovered",
    "queued",
    "processing",
    "forwarded",
    "retry_pending",
    "permanently_failed",
    "ambiguous",
)

MIGRATIONS: list[tuple[int, tuple[str, ...]]] = [
    (
        1,
        (
            """
            CREATE TABLE IF NOT EXISTS source_groups (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                identifier        TEXT    NOT NULL UNIQUE,
                alias             TEXT,
                resolved_chat_id  INTEGER,
                title             TEXT,
                enabled           INTEGER NOT NULL DEFAULT 1,
                last_validated_at TEXT,
                created_at        TEXT    NOT NULL,
                updated_at        TEXT    NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS processed_messages (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                source_chat_id        INTEGER NOT NULL,
                source_message_id     INTEGER NOT NULL,
                unit_key              TEXT    NOT NULL,
                destination_chat_id   INTEGER,
                destination_message_id INTEGER,
                message_date          TEXT,
                grouped_id            INTEGER,
                status                TEXT    NOT NULL,
                attempts              INTEGER NOT NULL DEFAULT 0,
                error_class           TEXT,
                last_error            TEXT,
                discovered_at         TEXT    NOT NULL,
                updated_at            TEXT    NOT NULL,
                metadata              TEXT,
                UNIQUE (source_chat_id, source_message_id)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_pm_status ON processed_messages(status)",
            "CREATE INDEX IF NOT EXISTS idx_pm_unit_key ON processed_messages(unit_key)",
            "CREATE INDEX IF NOT EXISTS idx_pm_message_date ON processed_messages(message_date)",
            """
            CREATE TABLE IF NOT EXISTS processing_queue (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                message_key       TEXT    NOT NULL UNIQUE,
                source_chat_id    INTEGER NOT NULL,
                source_message_id INTEGER NOT NULL,
                priority          INTEGER NOT NULL DEFAULT 100,
                state             TEXT    NOT NULL DEFAULT 'pending',
                attempts          INTEGER NOT NULL DEFAULT 0,
                available_at      REAL    NOT NULL,
                lease_until       REAL,
                worker_id         TEXT,
                created_at        TEXT    NOT NULL,
                updated_at        TEXT    NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_queue_ready "
            "ON processing_queue(state, available_at, priority)",
            "CREATE INDEX IF NOT EXISTS idx_queue_lease ON processing_queue(state, lease_until)",
            """
            CREATE TABLE IF NOT EXISTS import_progress (
                source_chat_id            INTEGER PRIMARY KEY,
                status                    TEXT    NOT NULL DEFAULT 'pending',
                oldest_message_id         INTEGER,
                newest_message_id         INTEGER,
                last_processed_message_id INTEGER,
                messages_seen             INTEGER NOT NULL DEFAULT 0,
                messages_queued           INTEGER NOT NULL DEFAULT 0,
                started_at                TEXT,
                completed_at              TEXT,
                last_error                TEXT,
                updated_at                TEXT    NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS application_settings (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS application_events (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at        TEXT    NOT NULL,
                level             TEXT    NOT NULL,
                event             TEXT    NOT NULL,
                source_chat_id    INTEGER,
                source_message_id INTEGER,
                detail            TEXT
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_events_created ON application_events(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_events_level ON application_events(level, created_at)",
        ),
    ),
    (
        2,
        (
            # per-member destination ids (albums return several messages)
            "ALTER TABLE processed_messages ADD COLUMN destination_message_ids TEXT",
            "ALTER TABLE processed_messages ADD COLUMN reconciled_at TEXT",
            "ALTER TABLE processed_messages ADD COLUMN reconcile_result TEXT",
            # scan checkpoint is tracked separately from the queue checkpoint
            "ALTER TABLE import_progress ADD COLUMN last_scanned_message_id INTEGER",
            "ALTER TABLE import_progress ADD COLUMN messages_skipped INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE import_progress ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
            # annotation posts are tracked so a retry cannot duplicate them
            "ALTER TABLE processing_queue ADD COLUMN annotation_message_id INTEGER",
            "ALTER TABLE processing_queue ADD COLUMN annotation_sent_at TEXT",
            "ALTER TABLE source_groups ADD COLUMN last_live_event_at TEXT",
            "CREATE INDEX IF NOT EXISTS idx_queue_state ON processing_queue(state)",
        ),
    ),
]


class DatabaseError(RuntimeError):
    pass


class _SyncCursor:
    """Async facade over a stdlib ``sqlite3`` cursor (fallback driver only)."""

    __slots__ = ("_cursor",)

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    async def fetchone(self) -> Any:
        return await asyncio.to_thread(self._cursor.fetchone)

    async def fetchall(self) -> Any:
        return await asyncio.to_thread(self._cursor.fetchall)

    async def close(self) -> None:
        await asyncio.to_thread(self._cursor.close)


class _SyncConnection:
    """Async facade over a stdlib ``sqlite3`` connection.

    Only used when aiosqlite is not installed, so that the collector (and
    ``python -m collector selftest``) still works on a bare Python install with
    no third-party packages.  Calls run in a worker thread; the collector
    serialises writes through one lock, so ordering is unchanged.
    """

    __slots__ = ("_conn",)

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @classmethod
    async def open(cls, target: Any, *, uri: bool = False) -> "_SyncConnection":
        def _connect() -> Any:
            import sqlite3

            conn = sqlite3.connect(
                str(target), uri=uri, isolation_level=None, check_same_thread=False, timeout=10
            )
            conn.row_factory = sqlite3.Row
            return conn

        return cls(await asyncio.to_thread(_connect))

    async def execute(self, sql: str, params: Any = ()) -> _SyncCursor:
        cursor = await asyncio.to_thread(self._conn.execute, sql, tuple(params))
        return _SyncCursor(cursor)

    @property
    def in_transaction(self) -> bool:
        return bool(self._conn.in_transaction)

    async def close(self) -> None:
        await asyncio.to_thread(self._conn.close)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(moment: datetime | None = None) -> str:
    return (moment or utcnow()).astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def message_key(chat_id: int, message_id: int) -> str:
    return f"{chat_id}:{message_id}"


@dataclass(slots=True)
class QueueItem:
    queue_id: int
    key: str
    source_chat_id: int
    source_message_id: int
    message_ids: list[int]
    grouped_id: int | None
    attempts: int
    priority: int
    state: str = QUEUE_LEASED
    annotation_sent: bool = False


@dataclass(slots=True)
class ReleaseResult:
    requeued: int = 0
    ambiguous: int = 0

    @property
    def total(self) -> int:
        return self.requeued + self.ambiguous


class Database:
    """Async wrapper around the collector's SQLite database."""

    def __init__(self, path: str | Path, *, read_only: bool = False) -> None:
        self.path = Path(path)
        self.read_only = read_only
        self._conn: Any | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ life
    async def connect(self) -> None:
        if self._conn is not None:
            return
        parent = self.path.parent
        existed = parent.exists()
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not existed:
            # The state directory holds a credential-equivalent session and the
            # archive ledger: owner-only from the moment it is created.
            try:
                os.chmod(parent, 0o700)
            except OSError:  # pragma: no cover - filesystem dependent
                pass

        if self.read_only:
            if not self.path.exists():
                raise DatabaseError(
                    f"database {self.path} does not exist; start the collector once to create it"
                )
            uri = f"file:{self.path}?mode=ro"
            conn = await self._open(uri, uri=True)
            self._conn = conn
            try:
                await conn.execute("PRAGMA busy_timeout=10000")
                await self._verify_schema()
            except BaseException:
                # Never leak a connection (and its driver thread) on a failed open:
                # the dashboard could otherwise hold a file it cannot use, and the
                # process would not exit cleanly.
                await self._conn.close()
                self._conn = None
                raise
            return

        conn = await self._open(self.path)
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.execute("PRAGMA busy_timeout=10000")
        self._conn = conn
        await self.migrate()
        self.harden_files()

    async def _verify_schema(self) -> None:
        try:
            cursor = await self.conn.execute("SELECT version FROM schema_version LIMIT 1")
            row = await cursor.fetchone()
            await cursor.close()
        except Exception as exc:
            raise DatabaseError(
                f"{self.path} is not a collector database (no schema_version table): {exc}"
            ) from exc
        version = int(row["version"]) if row else 0
        if version != SCHEMA_VERSION:
            raise DatabaseError(
                f"database schema version {version} does not match this build ({SCHEMA_VERSION}); "
                "start the collector once so it can migrate"
            )

    def harden_files(self) -> None:
        """Owner-only permissions for the database and its WAL side files."""

        if self.read_only:
            return
        for candidate in (
            self.path,
            self.path.with_name(self.path.name + "-wal"),
            self.path.with_name(self.path.name + "-shm"),
        ):
            try:
                if candidate.exists():
                    os.chmod(candidate, 0o600)
            except OSError:  # pragma: no cover - filesystem dependent
                pass

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> "Database":
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    @staticmethod
    async def _open(target: Any, *, uri: bool = False) -> Any:
        """Open a connection with whichever driver is available."""

        if aiosqlite is not None:
            conn = await aiosqlite.connect(str(target), uri=uri, isolation_level=None)
            conn.row_factory = aiosqlite.Row
            return conn
        return await _SyncConnection.open(target, uri=uri)

    @property
    def conn(self) -> Any:
        if self._conn is None:
            raise DatabaseError("database is not connected; call connect() first")
        return self._conn

    async def _begin(self) -> None:
        """Start a write transaction, clearing any stale one first.

        A task cancelled inside a transaction cannot run its rollback (cancellation
        is a BaseException), which would leave the connection with an open
        transaction and break every later write.  This guard makes the layer
        self-healing instead of wedged.
        """

        if self.conn.in_transaction:
            await self.conn.execute("ROLLBACK")
        await self.conn.execute("BEGIN IMMEDIATE")

    async def _rollback(self) -> None:
        try:
            if self.conn.in_transaction:
                await self.conn.execute("ROLLBACK")
        except Exception:  # pragma: no cover - defensive
            pass

    async def migrate(self) -> int:
        if self.read_only:
            await self._verify_schema()
            return SCHEMA_VERSION
        conn = self.conn
        await conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
        cursor = await conn.execute("SELECT version FROM schema_version LIMIT 1")
        row = await cursor.fetchone()
        await cursor.close()
        current = int(row["version"]) if row else 0
        if current > SCHEMA_VERSION:
            raise DatabaseError(
                f"database schema version {current} is newer than this build ({SCHEMA_VERSION}); "
                "upgrade the collector instead of downgrading the database"
            )
        for version, statements in MIGRATIONS:
            if version <= current:
                continue
            await conn.execute("BEGIN IMMEDIATE")
            try:
                for statement in statements:
                    await conn.execute(statement)
                if current == 0 and version == MIGRATIONS[0][0]:
                    await conn.execute("INSERT INTO schema_version(version) VALUES (?)", (version,))
                else:
                    await conn.execute("UPDATE schema_version SET version = ?", (version,))
                await conn.execute("COMMIT")
            except Exception:
                await conn.execute("ROLLBACK")
                raise
            current = version
        return current

    # -------------------------------------------------------------- sources
    async def sync_source_groups(self, sources: Iterable[Any]) -> None:
        """Insert/update configured sources (keeps resolved chat ids)."""

        now = iso()
        async with self._lock:
            await self._begin()
            try:
                for source in sources:
                    await self.conn.execute(
                        """
                        INSERT INTO source_groups (identifier, alias, enabled, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(identifier) DO UPDATE SET
                            alias = excluded.alias,
                            enabled = excluded.enabled,
                            updated_at = excluded.updated_at
                        """,
                        (
                            source.identifier,
                            getattr(source, "alias", None),
                            1 if getattr(source, "enabled", True) else 0,
                            now,
                            now,
                        ),
                    )
                await self.conn.execute("COMMIT")
            except BaseException:
                await self._rollback()
                raise

    async def set_source_resolved(
        self, identifier: str, chat_id: int | None, title: str | None, *, error: str | None = None
    ) -> None:
        async with self._lock:
            await self.conn.execute(
                """
                UPDATE source_groups
                   SET resolved_chat_id = ?, title = ?, last_validated_at = ?, updated_at = ?
                 WHERE identifier = ?
                """,
                (chat_id, title, iso(), iso(), identifier),
            )
        if error:
            await self.log_event(
                "error", "source_resolution_failed", detail=f"{identifier}: {error}"
            )

    async def touch_source_live(self, chat_id: int) -> None:
        """Record that a live event was seen for this chat."""

        await self.conn.execute(
            "UPDATE source_groups SET last_live_event_at = ?, updated_at = ? "
            "WHERE resolved_chat_id = ?",
            (iso(), iso(), chat_id),
        )

    async def list_source_groups(self) -> list[dict[str, Any]]:
        cursor = await self.conn.execute("SELECT * FROM source_groups ORDER BY id")
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    # -------------------------------------------------------------- discover
    async def discover_unit(
        self,
        *,
        chat_id: int,
        messages: Sequence[dict[str, Any]],
        priority: int = 100,
        now: float | None = None,
    ) -> int:
        """Record a delivery unit (single message or album) and enqueue it.

        ``messages`` is a chronological list of
        ``{"message_id": int, "date": datetime|None, "grouped_id": int|None}``.
        ``chat_id`` must be a **marked peer id** (see ``collector.ids``).

        Returns 1 if this call created the queue entry, 0 if it already existed
        (deduplication).  Re-running discovery never duplicates work.
        """

        if not messages:
            return 0
        clock = time.time() if now is None else now
        stamp = iso()
        ids = [int(m["message_id"]) for m in messages]
        placeholders = ",".join("?" for _ in ids)

        async with self._lock:
            await self._begin()
            try:
                # Which ids are new?  A late-arriving album member must not join a
                # unit that has already been delivered: that member would never be
                # forwarded, and re-opening the unit would duplicate the rest.
                cursor = await self.conn.execute(
                    f"SELECT source_message_id, unit_key, grouped_id FROM processed_messages "
                    f"WHERE source_chat_id = ? AND source_message_id IN ({placeholders})",
                    (chat_id, *ids),
                )
                known_rows = await cursor.fetchall()
                await cursor.close()
                known = {row["source_message_id"] for row in known_rows}
                new_items = [m for m in messages if int(m["message_id"]) not in known]

                if not new_items:
                    # Everything already known: make sure the queue row exists
                    # (a crash between the two inserts is possible).
                    key = message_key(chat_id, ids[0])
                    cursor = await self.conn.execute(
                        "SELECT 1 FROM processing_queue WHERE message_key = ?", (key,)
                    )
                    exists = await cursor.fetchone()
                    await cursor.close()
                    if exists:
                        await self.conn.execute("COMMIT")
                        return 0
                    cursor = await self.conn.execute(
                        """
                        INSERT OR IGNORE INTO processing_queue (
                            message_key, source_chat_id, source_message_id, priority,
                            state, attempts, available_at, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                        """,
                        (key, chat_id, ids[0], priority, clock, stamp, stamp),
                    )
                    queued = cursor.rowcount
                    await cursor.close()
                    await self.conn.execute("COMMIT")
                    return 1 if queued else 0

                # An album that is still waiting to be delivered can absorb new
                # members: history and live discovery then agree on one unit instead
                # of delivering a split album.  A unit that is already leased,
                # sending, done or ambiguous is never merged into -- its delivery may
                # already be in flight or complete.
                grouped = messages[0].get("grouped_id")
                merge_key: str | None = None
                if grouped is not None and known_rows:
                    candidates = {
                        row["unit_key"]
                        for row in known_rows
                        if row["grouped_id"] is not None and row["grouped_id"] == grouped
                    }
                    for candidate in sorted(candidates):
                        cursor = await self.conn.execute(
                            "SELECT state, available_at FROM processing_queue "
                            "WHERE message_key = ?",
                            (candidate,),
                        )
                        row = await cursor.fetchone()
                        await cursor.close()
                        if (
                            row is not None
                            and row["state"] == QUEUE_PENDING
                            and row["available_at"] <= clock
                        ):
                            merge_key = candidate
                            break

                key = merge_key or message_key(chat_id, int(new_items[0]["message_id"]))
                metadata = (
                    json.dumps({"album_size": len(new_items)}) if len(new_items) > 1 else None
                )
                for item in new_items:
                    await self.conn.execute(
                        """
                        INSERT OR IGNORE INTO processed_messages (
                            source_chat_id, source_message_id, unit_key, message_date,
                            grouped_id, status, attempts, discovered_at, updated_at, metadata
                        ) VALUES (?, ?, ?, ?, ?, 'discovered', 0, ?, ?, ?)
                        """,
                        (
                            chat_id,
                            int(item["message_id"]),
                            key,
                            iso(item["date"]) if item.get("date") else None,
                            item.get("grouped_id"),
                            stamp,
                            stamp,
                            metadata,
                        ),
                    )

                if merge_key is not None:
                    # The unit already has a queue row: upgrade its priority so live
                    # discovery still jumps the history queue, and let the workers
                    # deliver every member in one album forward.
                    await self.conn.execute(
                        "UPDATE processing_queue SET priority = MIN(priority, ?), updated_at = ? "
                        "WHERE message_key = ?",
                        (priority, stamp, merge_key),
                    )
                    await self.conn.execute("COMMIT")
                    return 1

                cursor = await self.conn.execute(
                    """
                    INSERT OR IGNORE INTO processing_queue (
                        message_key, source_chat_id, source_message_id, priority,
                        state, attempts, available_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                    """,
                    (key, chat_id, int(new_items[0]["message_id"]), priority, clock, stamp, stamp),
                )
                queued = cursor.rowcount
                await cursor.close()
                await self.conn.execute("COMMIT")
                return 1 if queued else 0
            except BaseException:
                await self._rollback()
                raise

    async def is_known(self, chat_id: int, message_id: int) -> bool:
        cursor = await self.conn.execute(
            "SELECT 1 FROM processed_messages WHERE source_chat_id = ? AND source_message_id = ?",
            (chat_id, message_id),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row is not None

    async def get_status(self, chat_id: int, message_id: int) -> str | None:
        cursor = await self.conn.execute(
            "SELECT status FROM processed_messages WHERE source_chat_id = ? AND source_message_id = ?",
            (chat_id, message_id),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row["status"] if row else None

    # ----------------------------------------------------------------- queue
    async def claim_next(self, worker_id: str, lease_seconds: float) -> QueueItem | None:
        """Atomically lease the next ready queue row.

        Only ``pending`` rows are claimable, so ``ambiguous`` work (which may
        already have been delivered) is never picked up automatically.
        """

        now = time.time()
        stamp = iso()
        async with self._lock:
            await self._begin()
            try:
                cursor = await self.conn.execute(
                    """
                    SELECT id, message_key, source_chat_id, source_message_id, priority,
                           attempts, annotation_message_id
                      FROM processing_queue
                     WHERE state = 'pending' AND available_at <= ?
                     ORDER BY priority ASC, id ASC
                     LIMIT 1
                    """,
                    (now,),
                )
                row = await cursor.fetchone()
                await cursor.close()
                if row is None:
                    await self.conn.execute("COMMIT")
                    return None

                cursor = await self.conn.execute(
                    """
                    UPDATE processing_queue
                       SET state = 'leased', worker_id = ?, lease_until = ?, updated_at = ?
                     WHERE id = ? AND state = 'pending'
                    """,
                    (worker_id, now + lease_seconds, stamp, row["id"]),
                )
                if cursor.rowcount != 1:  # lost a race; try again next poll
                    await cursor.close()
                    await self.conn.execute("ROLLBACK")
                    return None
                await cursor.close()

                cursor = await self.conn.execute(
                    "SELECT source_message_id, grouped_id FROM processed_messages WHERE unit_key = ? "
                    "ORDER BY source_message_id",
                    (row["message_key"],),
                )
                members = await cursor.fetchall()
                await cursor.close()
                message_ids = [m["source_message_id"] for m in members] or [
                    row["source_message_id"]
                ]
                grouped_id = next((m["grouped_id"] for m in members if m["grouped_id"]), None)

                await self.conn.execute("COMMIT")
                return QueueItem(
                    queue_id=row["id"],
                    key=row["message_key"],
                    source_chat_id=row["source_chat_id"],
                    source_message_id=row["source_message_id"],
                    message_ids=message_ids,
                    grouped_id=grouped_id,
                    attempts=row["attempts"],
                    priority=row["priority"],
                    state=QUEUE_LEASED,
                    annotation_sent=row["annotation_message_id"] is not None,
                )
            except BaseException:
                await self._rollback()
                raise

    async def mark_sending(self, item: QueueItem) -> bool:
        """Flag that a Telegram request is about to be issued.

        After this point the delivery is no longer safe to retry blindly: if the
        lease expires the unit becomes ``ambiguous`` instead of ``pending``.
        """

        stamp = iso()
        async with self._lock:
            await self._begin()
            try:
                cursor = await self.conn.execute(
                    "UPDATE processing_queue SET state = 'sending', updated_at = ? "
                    "WHERE id = ? AND state IN ('leased', 'sending')",
                    (stamp, item.queue_id),
                )
                ok = cursor.rowcount == 1
                await cursor.close()
                if ok:
                    await self.conn.execute(
                        "UPDATE processed_messages SET status = 'processing', updated_at = ? "
                        "WHERE unit_key = ?",
                        (stamp, item.key),
                    )
                await self.conn.execute("COMMIT")
                if ok:
                    item.state = QUEUE_SENDING
                return ok
            except BaseException:
                await self._rollback()
                raise

    async def mark_annotation_sent(self, item: QueueItem, destination_message_id: int | None) -> None:
        stamp = iso()
        async with self._lock:
            await self.conn.execute(
                "UPDATE processing_queue SET annotation_message_id = ?, annotation_sent_at = ?, "
                "updated_at = ? WHERE id = ?",
                (destination_message_id, stamp, stamp, item.queue_id),
            )
        item.annotation_sent = True

    async def complete(
        self,
        item: QueueItem,
        *,
        destination_chat_id: int | None,
        destination_message_id: int | None,
        member_destination_ids: dict[int, int] | None = None,
    ) -> None:
        """Record success.  ``member_destination_ids`` maps album members to ids."""

        stamp = iso()
        async with self._lock:
            await self._begin()
            try:
                await self.conn.execute(
                    """
                    UPDATE processed_messages
                       SET status = 'forwarded',
                           destination_chat_id = ?,
                           destination_message_id = ?,
                           attempts = attempts + 1,
                           error_class = NULL,
                           last_error = NULL,
                           updated_at = ?
                     WHERE unit_key = ?
                    """,
                    (destination_chat_id, destination_message_id, stamp, item.key),
                )
                if member_destination_ids:
                    for source_id, dest_id in member_destination_ids.items():
                        await self.conn.execute(
                            "UPDATE processed_messages SET destination_message_id = ? "
                            "WHERE unit_key = ? AND source_message_id = ?",
                            (dest_id, item.key, source_id),
                        )
                    await self.conn.execute(
                        "UPDATE processed_messages SET destination_message_ids = ? WHERE unit_key = ?",
                        (json.dumps(member_destination_ids), item.key),
                    )
                await self.conn.execute(
                    """
                    UPDATE processing_queue
                       SET state = 'done', lease_until = NULL, worker_id = NULL,
                           attempts = attempts + 1, updated_at = ?
                     WHERE id = ?
                    """,
                    (stamp, item.queue_id),
                )
                await self.conn.execute("COMMIT")
            except BaseException:
                await self._rollback()
                raise

    async def fail(
        self,
        item: QueueItem,
        *,
        error_class: str,
        error: str,
        retry_at: float | None,
    ) -> None:
        """Record a failure.  ``retry_at is None`` means permanently failed."""

        stamp = iso()
        state = QUEUE_PENDING if retry_at is not None else QUEUE_DEAD
        message_status = "retry_pending" if retry_at is not None else "permanently_failed"
        async with self._lock:
            await self._begin()
            try:
                await self.conn.execute(
                    """
                    UPDATE processed_messages
                       SET status = ?, attempts = attempts + 1, error_class = ?,
                           last_error = ?, updated_at = ?
                     WHERE unit_key = ?
                    """,
                    (message_status, error_class, error[:1000], stamp, item.key),
                )
                await self.conn.execute(
                    """
                    UPDATE processing_queue
                       SET state = ?, attempts = attempts + 1, available_at = COALESCE(?, available_at),
                           lease_until = NULL, worker_id = NULL, updated_at = ?
                     WHERE id = ?
                    """,
                    (state, retry_at, stamp, item.queue_id),
                )
                await self.conn.execute("COMMIT")
            except BaseException:
                await self._rollback()
                raise

    async def mark_ambiguous(self, item: QueueItem, *, error: str) -> None:
        """Park a possibly-delivered unit: workers will never claim it again."""

        stamp = iso()
        async with self._lock:
            await self._begin()
            try:
                await self.conn.execute(
                    """
                    UPDATE processed_messages
                       SET status = 'ambiguous', error_class = 'ambiguous', last_error = ?, updated_at = ?
                     WHERE unit_key = ?
                    """,
                    (error[:1000], stamp, item.key),
                )
                await self.conn.execute(
                    "UPDATE processing_queue SET state = 'ambiguous', lease_until = NULL, "
                    "worker_id = NULL, updated_at = ? WHERE id = ?",
                    (stamp, item.queue_id),
                )
                await self.conn.execute("COMMIT")
            except BaseException:
                await self._rollback()
                raise

    async def release_stale_leases(
        self, *, older_than_seconds: float = 0.0, only_expired: bool = True
    ) -> ReleaseResult:
        """Recover abandoned work without risking duplicate delivery.

        * ``leased`` (claimed, nothing sent) -> back to ``pending`` / ``queued``.
        * ``sending`` (a Telegram request may have gone out) -> ``ambiguous``,
          which no worker will claim; ``reconcile`` settles it.

        ``only_expired=False`` sweeps every lease (graceful shutdown).
        """

        now = time.time()
        stamp = iso()
        result = ReleaseResult()
        async with self._lock:
            await self._begin()
            try:
                if only_expired:
                    cursor = await self.conn.execute(
                        "SELECT id, message_key, state FROM processing_queue "
                        "WHERE state IN ('leased', 'sending') "
                        "AND (lease_until IS NULL OR lease_until <= ?)",
                        (now - older_than_seconds,),
                    )
                else:
                    cursor = await self.conn.execute(
                        "SELECT id, message_key, state FROM processing_queue "
                        "WHERE state IN ('leased', 'sending')"
                    )
                rows = await cursor.fetchall()
                await cursor.close()
                for row in rows:
                    if row["state"] == QUEUE_SENDING:
                        await self.conn.execute(
                            "UPDATE processing_queue SET state = 'ambiguous', lease_until = NULL, "
                            "worker_id = NULL, updated_at = ? WHERE id = ?",
                            (stamp, row["id"]),
                        )
                        await self.conn.execute(
                            "UPDATE processed_messages SET status = 'ambiguous', "
                            "error_class = 'ambiguous', "
                            "last_error = 'delivery interrupted while in flight', updated_at = ? "
                            "WHERE unit_key = ? AND status = 'processing'",
                            (stamp, row["message_key"]),
                        )
                        result.ambiguous += 1
                    else:
                        await self.conn.execute(
                            "UPDATE processing_queue SET state = 'pending', lease_until = NULL, "
                            "worker_id = NULL, available_at = ?, updated_at = ? WHERE id = ?",
                            (now, stamp, row["id"]),
                        )
                        await self.conn.execute(
                            "UPDATE processed_messages SET status = 'queued', updated_at = ? "
                            "WHERE unit_key = ? AND status IN ('processing', 'discovered')",
                            (stamp, row["message_key"]),
                        )
                        result.requeued += 1
                await self.conn.execute("COMMIT")
                return result
            except BaseException:
                await self._rollback()
                raise

    async def release_worker(self, worker_id: str) -> ReleaseResult:
        """Graceful shutdown: give this worker's leases back, same rules."""

        now = time.time()
        stamp = iso()
        result = ReleaseResult()
        async with self._lock:
            await self._begin()
            try:
                cursor = await self.conn.execute(
                    "SELECT id, message_key, state FROM processing_queue "
                    "WHERE state IN ('leased', 'sending') AND worker_id = ?",
                    (worker_id,),
                )
                rows = await cursor.fetchall()
                await cursor.close()
                for row in rows:
                    if row["state"] == QUEUE_SENDING:
                        await self.conn.execute(
                            "UPDATE processing_queue SET state = 'ambiguous', lease_until = NULL, "
                            "worker_id = NULL, updated_at = ? WHERE id = ?",
                            (stamp, row["id"]),
                        )
                        await self.conn.execute(
                            "UPDATE processed_messages SET status = 'ambiguous', "
                            "error_class = 'ambiguous', "
                            "last_error = 'shutdown while delivery was in flight', updated_at = ? "
                            "WHERE unit_key = ? AND status = 'processing'",
                            (stamp, row["message_key"]),
                        )
                        result.ambiguous += 1
                    else:
                        await self.conn.execute(
                            "UPDATE processing_queue SET state = 'pending', lease_until = NULL, "
                            "worker_id = NULL, available_at = ?, updated_at = ? WHERE id = ?",
                            (now, stamp, row["id"]),
                        )
                        await self.conn.execute(
                            "UPDATE processed_messages SET status = 'queued', updated_at = ? "
                            "WHERE unit_key = ? AND status IN ('processing', 'discovered')",
                            (stamp, row["message_key"]),
                        )
                        result.requeued += 1
                await self.conn.execute("COMMIT")
                return result
            except BaseException:
                await self._rollback()
                raise

    async def queue_size(self) -> int:
        cursor = await self.conn.execute(
            "SELECT COUNT(*) AS n FROM processing_queue WHERE state IN ('pending', 'leased', 'sending')"
        )
        row = await cursor.fetchone()
        await cursor.close()
        return int(row["n"]) if row else 0

    async def next_available_at(self) -> float | None:
        cursor = await self.conn.execute(
            "SELECT MIN(available_at) AS t FROM processing_queue WHERE state = 'pending'"
        )
        row = await cursor.fetchone()
        await cursor.close()
        return float(row["t"]) if row and row["t"] is not None else None

    async def oldest_pending_created_at(self) -> str | None:
        cursor = await self.conn.execute(
            "SELECT MIN(created_at) AS t FROM processing_queue WHERE state = 'pending'"
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row["t"] if row else None

    # ------------------------------------------------------- import progress
    async def get_import_progress(self, chat_id: int) -> dict[str, Any] | None:
        cursor = await self.conn.execute(
            "SELECT * FROM import_progress WHERE source_chat_id = ?", (chat_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None

    async def list_import_progress(self) -> list[dict[str, Any]]:
        cursor = await self.conn.execute("SELECT * FROM import_progress ORDER BY source_chat_id")
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def start_import(self, chat_id: int, *, increment_attempts: bool = False) -> dict[str, Any]:
        stamp = iso()
        async with self._lock:
            await self.conn.execute(
                """
                INSERT INTO import_progress (source_chat_id, status, started_at, updated_at, attempts)
                VALUES (?, 'running', ?, ?, ?)
                ON CONFLICT(source_chat_id) DO UPDATE SET
                    status = 'running',
                    started_at = COALESCE(import_progress.started_at, excluded.started_at),
                    completed_at = NULL,
                    last_error = NULL,
                    attempts = import_progress.attempts + ?,
                    updated_at = excluded.updated_at
                """,
                (chat_id, stamp, stamp, 1 if increment_attempts else 0, 1 if increment_attempts else 0),
            )
        return await self.get_import_progress(chat_id) or {}

    async def update_import_progress(
        self,
        chat_id: int,
        *,
        oldest_message_id: int | None = None,
        newest_message_id: int | None = None,
        last_processed_message_id: int | None = None,
        last_scanned_message_id: int | None = None,
        seen_delta: int = 0,
        queued_delta: int = 0,
        skipped_delta: int = 0,
        status: str | None = None,
        error: str | None = None,
        clear_error: bool = False,
        completed: bool = False,
    ) -> None:
        stamp = iso()
        async with self._lock:
            await self.conn.execute(
                """
                UPDATE import_progress SET
                    oldest_message_id = COALESCE(?, oldest_message_id),
                    newest_message_id = COALESCE(?, newest_message_id),
                    last_processed_message_id = COALESCE(?, last_processed_message_id),
                    last_scanned_message_id = COALESCE(?, last_scanned_message_id),
                    messages_seen = messages_seen + ?,
                    messages_queued = messages_queued + ?,
                    messages_skipped = messages_skipped + ?,
                    status = COALESCE(?, status),
                    last_error = CASE WHEN ? THEN NULL ELSE COALESCE(?, last_error) END,
                    completed_at = CASE WHEN ? THEN ? ELSE completed_at END,
                    updated_at = ?
                WHERE source_chat_id = ?
                """,
                (
                    oldest_message_id,
                    newest_message_id,
                    last_processed_message_id,
                    last_scanned_message_id,
                    seen_delta,
                    queued_delta,
                    skipped_delta,
                    status,
                    1 if clear_error else 0,
                    error,
                    1 if completed else 0,
                    stamp,
                    stamp,
                    chat_id,
                ),
            )

    async def incomplete_imports(self) -> list[dict[str, Any]]:
        """Groups whose import is not finished and could still be retried."""

        cursor = await self.conn.execute(
            "SELECT * FROM import_progress WHERE status IN "
            "('pending', 'running', 'retry_pending', 'stopped', 'failed')"
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    # ---------------------------------------------------------------- events
    async def log_event(
        self,
        level: str,
        event: str,
        *,
        source_chat_id: int | None = None,
        source_message_id: int | None = None,
        detail: str | None = None,
    ) -> None:
        await self.conn.execute(
            "INSERT INTO application_events (created_at, level, event, source_chat_id, "
            "source_message_id, detail) VALUES (?, ?, ?, ?, ?, ?)",
            (iso(), level.upper(), event, source_chat_id, source_message_id, detail),
        )

    async def recent_events(
        self, limit: int = 10, *, level: str | None = None
    ) -> list[dict[str, Any]]:
        if level:
            cursor = await self.conn.execute(
                "SELECT * FROM application_events WHERE level = ? ORDER BY id DESC LIMIT ?",
                (level.upper(), limit),
            )
        else:
            cursor = await self.conn.execute(
                "SELECT * FROM application_events ORDER BY id DESC LIMIT ?", (limit,)
            )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def prune_events(self, older_than_days: int) -> int:
        """Delete old audit events only (never archive/queue records)."""

        if older_than_days <= 0:
            return 0
        cutoff = iso(utcnow() - timedelta(days=older_than_days))
        cursor = await self.conn.execute(
            "DELETE FROM application_events WHERE created_at < ?", (cutoff,)
        )
        deleted = cursor.rowcount or 0
        await cursor.close()
        return deleted

    # --------------------------------------------------------------- reports
    async def status_counts(self) -> dict[str, int]:
        cursor = await self.conn.execute(
            "SELECT status, COUNT(*) AS n FROM processed_messages GROUP BY status"
        )
        rows = await cursor.fetchall()
        await cursor.close()
        counts = {row["status"]: int(row["n"]) for row in rows}
        for status in MESSAGE_STATUSES:
            counts.setdefault(status, 0)
        cursor = await self.conn.execute(
            "SELECT state, COUNT(*) AS n FROM processing_queue GROUP BY state"
        )
        rows = await cursor.fetchall()
        await cursor.close()
        for row in rows:
            counts[f"queue_{row['state']}"] = int(row["n"])
        for state in (QUEUE_PENDING, QUEUE_LEASED, QUEUE_SENDING, QUEUE_DONE, QUEUE_AMBIGUOUS, QUEUE_DEAD):
            counts.setdefault(f"queue_{state}", 0)
        return counts

    async def stats_by_source(self) -> list[dict[str, Any]]:
        """Per-source operational statistics (see ``collector status``)."""

        cursor = await self.conn.execute(
            """
            SELECT pm.source_chat_id AS chat_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN pm.status = 'forwarded' THEN 1 ELSE 0 END) AS forwarded,
                   SUM(CASE WHEN pm.status = 'permanently_failed' THEN 1 ELSE 0 END) AS failed,
                   SUM(CASE WHEN pm.status = 'ambiguous' THEN 1 ELSE 0 END) AS ambiguous,
                   SUM(CASE WHEN pm.status = 'retry_pending' THEN 1 ELSE 0 END) AS retry_pending,
                   SUM(CASE WHEN pm.status IN ('discovered', 'queued', 'processing')
                            THEN 1 ELSE 0 END) AS pending,
                   MAX(pm.source_message_id) AS last_seen_message_id,
                   MAX(CASE WHEN pm.status = 'forwarded' THEN pm.updated_at END) AS last_forwarded_at,
                   MAX(pm.attempts) AS max_attempts_used
              FROM processed_messages pm
             GROUP BY pm.source_chat_id
            """
        )
        rows = await cursor.fetchall()
        await cursor.close()
        stats = {int(row["chat_id"]): dict(row) for row in rows}

        cursor = await self.conn.execute("SELECT * FROM source_groups")
        sources = await cursor.fetchall()
        await cursor.close()

        result: list[dict[str, Any]] = []
        for source in sources:
            chat_id = source["resolved_chat_id"]
            entry = stats.pop(int(chat_id), None) if chat_id is not None else None
            entry = entry or {
                "chat_id": chat_id,
                "total": 0,
                "forwarded": 0,
                "failed": 0,
                "ambiguous": 0,
                "retry_pending": 0,
                "pending": 0,
                "last_seen_message_id": None,
                "last_forwarded_at": None,
                "max_attempts_used": 0,
            }
            entry["identifier"] = source["identifier"]
            entry["title"] = source["title"]
            entry["enabled"] = bool(source["enabled"])
            entry["last_live_event_at"] = source["last_live_event_at"]
            result.append(entry)
        # messages from chats no longer configured still deserve to be visible
        for chat_id, entry in stats.items():
            entry["identifier"] = f"(unconfigured {chat_id})"
            entry["title"] = None
            entry["enabled"] = False
            entry["last_live_event_at"] = None
            result.append(entry)
        return result

    async def last_forwarded(self) -> dict[str, Any] | None:
        cursor = await self.conn.execute(
            "SELECT * FROM processed_messages WHERE status = 'forwarded' "
            "ORDER BY updated_at DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None

    async def recent_failures(self, limit: int = 5) -> list[dict[str, Any]]:
        cursor = await self.conn.execute(
            "SELECT * FROM processed_messages WHERE status IN ('permanently_failed', 'ambiguous', "
            "'retry_pending') ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------- recovery
    async def retry_failed(self, *, include_ambiguous: bool = False) -> int:
        """Move permanently-failed (and optionally ambiguous) work back to the queue."""

        statuses = ["permanently_failed"]
        if include_ambiguous:
            statuses.append("ambiguous")
        placeholders = ",".join("?" for _ in statuses)
        now = time.time()
        stamp = iso()
        async with self._lock:
            await self._begin()
            try:
                cursor = await self.conn.execute(
                    f"SELECT unit_key, source_chat_id, source_message_id FROM processed_messages "
                    f"WHERE status IN ({placeholders})",
                    tuple(statuses),
                )
                rows = await cursor.fetchall()
                await cursor.close()
                for row in rows:
                    await self.conn.execute(
                        "INSERT INTO processing_queue (message_key, source_chat_id, source_message_id, "
                        "priority, state, attempts, available_at, created_at, updated_at) "
                        "VALUES (?, ?, ?, 50, 'pending', 0, ?, ?, ?) "
                        "ON CONFLICT(message_key) DO UPDATE SET state = 'pending', attempts = 0, "
                        "available_at = excluded.available_at, lease_until = NULL, worker_id = NULL, "
                        "updated_at = excluded.updated_at",
                        (
                            row["unit_key"],
                            row["source_chat_id"],
                            row["source_message_id"],
                            now,
                            stamp,
                            stamp,
                        ),
                    )
                    await self.conn.execute(
                        "UPDATE processed_messages SET status = 'queued', error_class = NULL, "
                        "last_error = NULL, updated_at = ? WHERE unit_key = ?",
                        (stamp, row["unit_key"]),
                    )
                await self.conn.execute("COMMIT")
                return len(rows)
            except BaseException:
                await self._rollback()
                raise

    async def ambiguous_units(self, limit: int = 100) -> list[dict[str, Any]]:
        cursor = await self.conn.execute(
            "SELECT DISTINCT unit_key, source_chat_id, source_message_id FROM processed_messages "
            "WHERE status = 'ambiguous' ORDER BY id LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def requeue_unit(self, unit_key: str) -> None:
        """Put a single unit back in the queue (operator-driven retry)."""

        now = time.time()
        stamp = iso()
        async with self._lock:
            await self._begin()
            try:
                await self.conn.execute(
                    "UPDATE processing_queue SET state = 'pending', attempts = 0, available_at = ?, "
                    "lease_until = NULL, worker_id = NULL, updated_at = ? WHERE message_key = ?",
                    (now, stamp, unit_key),
                )
                await self.conn.execute(
                    "UPDATE processed_messages SET status = 'queued', error_class = NULL, "
                    "last_error = NULL, updated_at = ? WHERE unit_key = ?",
                    (stamp, unit_key),
                )
                await self.conn.execute("COMMIT")
            except BaseException:
                await self._rollback()
                raise

    async def resolve_ambiguous(
        self, unit_key: str, *, destination_chat_id: int | None, destination_message_id: int | None
    ) -> None:
        stamp = iso()
        async with self._lock:
            await self._begin()
            try:
                await self.conn.execute(
                    "UPDATE processed_messages SET status = 'forwarded', destination_chat_id = ?, "
                    "destination_message_id = ?, error_class = NULL, last_error = NULL, "
                    "reconcile_result = 'confirmed', reconciled_at = ?, updated_at = ? "
                    "WHERE unit_key = ?",
                    (destination_chat_id, destination_message_id, stamp, stamp, unit_key),
                )
                await self.conn.execute(
                    "UPDATE processing_queue SET state = 'done', lease_until = NULL, worker_id = NULL, "
                    "updated_at = ? WHERE message_key = ?",
                    (stamp, unit_key),
                )
                await self.conn.execute("COMMIT")
            except BaseException:
                await self._rollback()
                raise

    async def record_reconcile_result(self, unit_key: str, result: str) -> None:
        """Persist an inconclusive reconciliation attempt (``unresolved``/``error``)."""

        stamp = iso()
        await self.conn.execute(
            "UPDATE processed_messages SET reconcile_result = ?, reconciled_at = ?, updated_at = ? "
            "WHERE unit_key = ? AND status = 'ambiguous'",
            (result, stamp, stamp, unit_key),
        )

    # -------------------------------------------------------------- settings
    async def set_setting(self, key: str, value: str) -> None:
        await self.conn.execute(
            "INSERT INTO application_settings (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            (key, value, iso()),
        )

    async def get_setting(self, key: str) -> str | None:
        cursor = await self.conn.execute(
            "SELECT value FROM application_settings WHERE key = ?", (key,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row["value"] if row else None

    async def heartbeat(self) -> None:
        await self.set_setting("heartbeat_at", iso())

    async def last_heartbeat(self) -> datetime | None:
        return parse_iso(await self.get_setting("heartbeat_at"))

    # ----------------------------------------------------------- maintenance
    async def integrity_check(self) -> str:
        cursor = await self.conn.execute("PRAGMA integrity_check")
        row = await cursor.fetchone()
        await cursor.close()
        return str(row[0]) if row else "unknown"

    async def checkpoint_wal(self) -> None:
        if self.read_only:
            return
        await self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    async def database_size(self) -> int:
        try:
            return self.path.stat().st_size
        except OSError:  # pragma: no cover - filesystem dependent
            return 0
