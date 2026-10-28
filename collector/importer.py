"""Historical import: oldest message first, resumable, self-healing.

Guarantees this module tries to keep:

* deterministic chronological order (``reverse=True``)
* bounded memory (batches of ``HISTORY_BATCH_SIZE``)
* a **scan checkpoint** separate from the queue checkpoint, so skipped service
  messages cannot cause re-scans after a restart
* ``HISTORY_MAX_COUNT`` is a **total** budget across restarts, not a per-run one
* transient failures are retried with backoff by the supervisor loop instead of
  silently ending the background import; permanent/inaccessible failures stop
  retrying and are reported
* the whole group import is cancellable at a batch boundary
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Sequence

from .config import Config
from .db import Database
from .errors import ErrorClass, backoff_delay, classify, retry_budget

log = logging.getLogger(__name__)

HISTORY_PRIORITY = 100
LIVE_PRIORITY = 10

#: Import states persisted in ``import_progress.status``.
IMPORT_PENDING = "pending"
IMPORT_RUNNING = "running"
IMPORT_COMPLETED = "completed"
IMPORT_RETRY_PENDING = "retry_pending"
IMPORT_STOPPED = "stopped"
IMPORT_FAILED = "permanently_failed"

RETRYABLE_IMPORT_STATES = (IMPORT_PENDING, IMPORT_RETRY_PENDING, IMPORT_STOPPED, "failed")


def split_units(messages: Sequence[Any]) -> tuple[list[list[Any]], list[Any]]:
    """Group messages into delivery units, keeping trailing albums together.

    Returns ``(units, remainder)``.  ``remainder`` holds a trailing album that
    may still be growing, so the caller can fetch more before committing it.
    """

    units: list[list[Any]] = []
    current: list[Any] = []
    current_grouped: Any = None

    for message in messages:
        grouped = getattr(message, "grouped_id", None)
        if grouped is None:
            if current:
                units.append(current)
                current = []
                current_grouped = None
            units.append([message])
            continue
        if current and grouped != current_grouped:
            units.append(current)
            current = []
        current.append(message)
        current_grouped = grouped

    remainder: list[Any] = []
    if current:
        # An album (single or multi message) at the very end may continue in the
        # next batch, so hand it back instead of committing it now.
        remainder = current
    return units, remainder


def is_service_message(message: Any) -> bool:
    return getattr(message, "action", None) is not None


def message_date(message: Any) -> datetime | None:
    return getattr(message, "date", None)


def to_unit_payload(messages: Iterable[Any]) -> list[dict[str, Any]]:
    return [
        {
            "message_id": int(message.id),
            "date": message_date(message),
            "grouped_id": getattr(message, "grouped_id", None),
        }
        for message in messages
    ]


@dataclass(slots=True)
class ImportStats:
    queued: int = 0
    scanned: int = 0
    skipped_service: int = 0
    skipped_outgoing: int = 0
    completed: bool = False
    status: str = IMPORT_RUNNING
    attempts: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return self.status == IMPORT_FAILED


class HistoryImporter:
    def __init__(
        self,
        client: Any,
        db: Database,
        config: Config,
        notify: asyncio.Event | None = None,
    ) -> None:
        self.client = client
        self.db = db
        self.config = config
        self.notify = notify
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------ run
    async def run(self, sources: dict[int, Any], *, force: bool = False) -> dict[int, ImportStats]:
        """Import every source with retries.  One failing group never stops the others."""

        results: dict[int, ImportStats] = {}
        for chat_id, entity in sources.items():
            if self._stop.is_set():
                log.info("history import stopped on request")
                break
            results[chat_id] = await self.import_group_with_retries(chat_id, entity, force=force)
        return results

    async def resume_incomplete(self, sources: dict[int, Any]) -> dict[int, ImportStats]:
        """Re-run imports that are not finished (used by the service watchdog).

        Groups already marked ``completed`` or ``permanently_failed`` are left
        alone, so an inaccessible group is not retried forever.
        """

        results: dict[int, ImportStats] = {}
        for chat_id, entity in sources.items():
            if self._stop.is_set():
                break
            progress = await self.db.get_import_progress(chat_id)
            if not progress:
                # Never started: a normal `run` will handle it.
                continue
            if progress["status"] not in RETRYABLE_IMPORT_STATES:
                continue
            attempts = int(progress.get("attempts") or 0)
            if attempts >= self.config.max_attempts * 3:
                log.warning(
                    "chat %s import abandoned after %s attempts; fix the cause then run "
                    "`python -m collector import-history --force`",
                    chat_id,
                    attempts,
                )
                continue
            log.info("resuming incomplete history import for chat %s", chat_id)
            results[chat_id] = await self.import_group_with_retries(chat_id, entity)
        return results

    # -------------------------------------------------------------- retries
    async def import_group_with_retries(
        self, chat_id: int, entity: Any, *, force: bool = False
    ) -> ImportStats:
        attempts = 0
        while not self._stop.is_set():
            try:
                stats = await self.import_group(chat_id, entity, force=force)
                stats.attempts = attempts
                return stats
            except Exception as exc:
                failure = classify(exc)
                attempts += 1
                budget = retry_budget(failure, self.config.max_attempts)
                detail = f"{failure.error_class.value}: {type(exc).__name__}: {exc}"
                log.error(
                    "history import for chat %s failed (attempt %s/%s): %s",
                    chat_id,
                    attempts,
                    budget,
                    detail,
                )
                await self.db.log_event(
                    "error" if not failure.retryable else "warning",
                    "history_import_failed" if not failure.retryable else "history_import_retry",
                    source_chat_id=chat_id,
                    detail=detail[:1000],
                )

                if failure.error_class in (ErrorClass.PERMANENT, ErrorClass.INACCESSIBLE):
                    await self.db.update_import_progress(
                        chat_id, status=IMPORT_FAILED, error=detail
                    )
                    return ImportStats(
                        status=IMPORT_FAILED, attempts=attempts, errors=[detail]
                    )

                if attempts >= budget:
                    await self.db.update_import_progress(
                        chat_id, status=IMPORT_FAILED, error=f"retries exhausted: {detail}"
                    )
                    return ImportStats(
                        status=IMPORT_FAILED,
                        attempts=attempts,
                        errors=[f"retries exhausted: {detail}"],
                    )

                delay = failure.retry_after or backoff_delay(
                    attempts, base=self.config.backoff_base, cap=self.config.backoff_cap
                )
                await self.db.update_import_progress(
                    chat_id, status=IMPORT_RETRY_PENDING, error=detail
                )
                log.info("retrying history import for chat %s in %.0fs", chat_id, delay)
                await self._sleep(delay)
        return ImportStats(status=IMPORT_STOPPED, attempts=attempts)

    async def _sleep(self, seconds: float) -> None:
        """Sleep, but wake immediately on shutdown."""

        try:
            await asyncio.wait_for(self._stop.wait(), timeout=max(0.0, seconds))
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------------
    async def import_group(self, chat_id: int, entity: Any, *, force: bool = False) -> ImportStats:
        progress = await self.db.get_import_progress(chat_id)
        stats = ImportStats()

        if progress and progress["status"] == IMPORT_COMPLETED and not force:
            log.info(
                "history already imported for chat %s; skipping (use --force to re-scan)", chat_id
            )
            stats.completed = True
            stats.status = IMPORT_COMPLETED
            return stats

        already_scanned = int(progress["messages_seen"]) if progress else 0
        remaining: int | None = None
        if self.config.history_max_count is not None:
            remaining = self.config.history_max_count - already_scanned
            if remaining <= 0:
                log.info(
                    "chat %s already reached HISTORY_MAX_COUNT (%s messages scanned)",
                    chat_id,
                    already_scanned,
                )
                await self.db.update_import_progress(
                    chat_id, status=IMPORT_COMPLETED, completed=True
                )
                stats.completed = True
                stats.status = IMPORT_COMPLETED
                return stats

        await self.db.start_import(chat_id, increment_attempts=True)
        progress = await self.db.get_import_progress(chat_id) or {}
        # Resume from the *scan* checkpoint: service/outgoing messages that were
        # skipped still count as scanned, so they are not re-read after a restart.
        resume_after = progress.get("last_scanned_message_id") or progress.get(
            "last_processed_message_id"
        )
        log.info(
            "importing history for chat %s (%s), resuming after message %s, limit %s",
            chat_id,
            entity_title(entity),
            resume_after or "-",
            remaining if remaining is not None else "unlimited",
        )

        kwargs: dict[str, Any] = {
            "reverse": True,   # oldest -> newest: deterministic chronological order
            "limit": remaining,
            "wait_time": 2,    # let Telethon absorb small FloodWaits during long scans
        }
        if resume_after:
            # In reverse mode min_id is the exclusive lower bound.
            kwargs["min_id"] = int(resume_after)
        elif self.config.history_start_date:
            # offset_date is inclusive: messages dated on/after this date.
            kwargs["offset_date"] = self.config.history_start_date

        end_date = self.config.history_end_date

        batch: list[Any] = []
        carry: list[Any] = []
        oldest_id: int | None = None
        newest_id: int | None = None
        last_scanned_id: int | None = None
        last_queued_id: int | None = None
        reached_end = False
        pending_seen = 0
        pending_skipped = 0
        scanned_since_commit = 0

        async def commit(messages: list[Any], *, final: bool = False) -> None:
            nonlocal oldest_id, newest_id, last_queued_id, pending_seen, pending_skipped
            nonlocal scanned_since_commit
            units, remainder = split_units(messages) if messages else ([], [])
            if final and remainder:
                units.append(remainder)
                remainder = []
            queued = 0
            committed_ids: list[int] = []
            for unit in units:
                payload = to_unit_payload(unit)
                if not payload:
                    continue
                created = await self.db.discover_unit(
                    chat_id=chat_id, messages=payload, priority=HISTORY_PRIORITY
                )
                queued += created
                committed_ids.extend(int(message.id) for message in unit)
                if created and self.notify is not None:
                    self.notify.set()
            if messages:
                if oldest_id is None:
                    oldest_id = messages[0].id
                newest_id = messages[-1].id
            if committed_ids:
                last_queued_id = max(committed_ids)

            # The durable checkpoint must never move past a message that only
            # exists in memory.  A trailing album sits in ``remainder`` until the
            # next batch proves it complete; checkpointing beyond it would skip
            # those members forever after a crash (they are re-read on resume
            # because ``min_id`` is the exclusive lower bound).
            if remainder:
                carry_floor = int(remainder[0].id) - 1
                safe_scanned = (
                    carry_floor if last_scanned_id is None else min(last_scanned_id, carry_floor)
                )
            else:
                safe_scanned = last_scanned_id

            await self.db.update_import_progress(
                chat_id,
                oldest_message_id=oldest_id,
                newest_message_id=newest_id,
                last_processed_message_id=last_queued_id,
                last_scanned_message_id=safe_scanned,
                seen_delta=pending_seen,
                queued_delta=queued,
                skipped_delta=pending_skipped,
                status=IMPORT_RUNNING,
                clear_error=True,
            )
            stats.queued += queued
            pending_seen = 0
            pending_skipped = 0
            scanned_since_commit = 0
            carry[:] = remainder

        try:
            iterator = self.client.iter_messages(entity, **kwargs)
            async for message in iterator:
                if self._stop.is_set():
                    log.info(
                        "stopping history import for chat %s at message %s", chat_id, message.id
                    )
                    break

                last_scanned_id = message.id
                pending_seen += 1
                stats.scanned += 1
                scanned_since_commit += 1

                date = message_date(message)
                if end_date is not None and date is not None and date > end_date:
                    # End date is inclusive; anything strictly later ends the run.
                    reached_end = True
                    pending_seen -= 1
                    stats.scanned -= 1
                    last_scanned_id = None
                    break
                if is_service_message(message) and not self.config.include_service_messages:
                    stats.skipped_service += 1
                    pending_skipped += 1
                elif getattr(message, "out", False) and not self.config.include_outgoing:
                    stats.skipped_outgoing += 1
                    pending_skipped += 1
                else:
                    batch.append(message)

                if len(batch) >= self.config.history_batch_size or (
                    scanned_since_commit >= self.config.history_batch_size
                ):
                    await commit(batch)
                    batch = list(carry)
                    carry.clear()

            await commit(batch + carry, final=True)

            stopped = self._stop.is_set()
            stats.completed = not stopped and not reached_end
            stats.status = IMPORT_STOPPED if stopped else IMPORT_COMPLETED
            await self.db.update_import_progress(
                chat_id,
                status=stats.status,
                completed=stats.completed,
                last_scanned_message_id=last_scanned_id,
            )
            log.info(
                "history import for chat %s %s: scanned=%s queued=%s service_skipped=%s "
                "outgoing_skipped=%s",
                chat_id,
                stats.status,
                stats.scanned,
                stats.queued,
                stats.skipped_service,
                stats.skipped_outgoing,
            )
            await self.db.log_event(
                "info",
                "history_import_completed",
                source_chat_id=chat_id,
                detail=(
                    f"status={stats.status} scanned={stats.scanned} queued={stats.queued} "
                    f"service_skipped={stats.skipped_service}"
                ),
            )
        except Exception:
            # Let the supervisor decide whether to retry; persist what we scanned
            # so a retry resumes instead of restarting.
            await commit(batch, final=True)
            raise
        return stats


def entity_title(entity: Any) -> str:
    return (
        getattr(entity, "title", None)
        or getattr(entity, "username", None)
        or str(getattr(entity, "id", "?"))
    )
