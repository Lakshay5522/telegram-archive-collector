"""Service orchestration: one place where the pieces are wired together.

Startup order matters:

1. restrictive umask + directories (before any secret is written)
2. database + migrations (so nothing is lost on a crash)
3. recovery of abandoned leases (``leased`` re-queued, ``sending`` ambiguous)
4. Telegram client + entity validation (canonical peer ids)
5. **live monitor registered first** so new messages are captured while the
   historical import is still running
6. forwarder workers started
7. historical import runs in the background, with a watchdog that resumes
   incomplete imports and a heartbeat so health checks mean something

Any failure during startup tears down whatever was already opened and re-raises
the original exception: no leaked client, no leaked tasks, no leaked connection.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from typing import Any

from .config import Config
from .db import Database
from .forwarder import Forwarder, RateLimiter
from .ids import IdentityError, canonical_id, describe_id
from .importer import HistoryImporter
from .logging_setup import setup_logging
from .monitor import LiveMonitor
from .telegram_client import (
    TelegramUnavailable,
    build_client,
    entity_title,
    harden_session,
    resolve_destination,
    resolve_sources,
)

log = logging.getLogger(__name__)


class CollectorService:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.db = Database(config.database_path)
        self.notify = asyncio.Event()
        self.client: Any = None
        self.destination: Any = None
        self.destination_chat_id: int | None = None
        self.sources: dict[int, Any] = {}
        self.rate_limiter = RateLimiter(config.min_forward_interval)
        self.monitor: LiveMonitor | None = None
        self.forwarder: Forwarder | None = None
        self.importer: HistoryImporter | None = None
        self._tasks: list[asyncio.Task[Any]] = []
        self._stopping = asyncio.Event()
        self._import_lock = asyncio.Lock()
        self._started = False
        self.connection_state = "disconnected"

    # ------------------------------------------------------------------ setup
    async def start(self, *, import_history: bool = True, force_import: bool = False) -> None:
        if self.config.restrict_permissions:
            os.umask(0o077)

        self.config.ensure_directories()
        setup_logging(
            self.config.log_dir,
            level=self.config.log_level,
            max_bytes=self.config.log_max_bytes,
            backup_count=self.config.log_backup_count,
        )

        try:
            await self._start(import_history=import_history, force_import=force_import)
        except BaseException:
            log.error("startup failed; releasing resources", exc_info=True)
            await self._teardown()
            raise
        self._started = True

    async def _start(self, *, import_history: bool, force_import: bool) -> None:
        await self.db.connect()
        await self.db.sync_source_groups(self.config.sources)

        # A fresh process holds no leases, so every lease found here is abandoned:
        # in-flight units become ambiguous immediately instead of waiting for the
        # lease to expire, and unsent ones are re-queued at once.
        released = await self.db.release_stale_leases(only_expired=False)
        if released.total:
            log.warning(
                "recovered %s interrupted queue item(s): %s re-queued, %s marked ambiguous "
                "(run `python -m collector reconcile` to settle those)",
                released.total,
                released.requeued,
                released.ambiguous,
            )
        await self.db.heartbeat()

        self.client = build_client(self.config)
        await self.client.connect()
        self.connection_state = "connected"
        if not await self.client.is_user_authorized():
            raise TelegramUnavailable(
                "The Telegram session is not authorised. Run `python -m collector login` first."
            )
        harden_session(self.config)
        me = await self.client.get_me()
        log.info(
            "connected to Telegram as %s", getattr(me, "username", None) or getattr(me, "id", "?")
        )

        # Destination first: without it nothing can be delivered.
        if not self.config.destination_chat:
            raise TelegramUnavailable("DESTINATION_CHAT is not configured")
        self.destination = await resolve_destination(self.client, self.config.destination_chat)
        try:
            self.destination_chat_id = canonical_id(self.destination)
        except IdentityError as exc:  # pragma: no cover - defensive
            raise TelegramUnavailable(f"destination id could not be resolved: {exc}") from exc
        log.info(
            "destination channel: %s (%s)",
            entity_title(self.destination),
            describe_id(self.destination_chat_id),
        )

        self.sources = await resolve_sources(self.client, self.config)
        for source in self.config.sources:
            chat_id = next(
                (cid for cid, entity in self.sources.items() if self._matches(source.identifier, entity)),
                None,
            )
            title = entity_title(self.sources[chat_id]) if chat_id is not None else None
            await self.db.set_source_resolved(source.identifier, chat_id, title)

        self.monitor = LiveMonitor(
            self.client,
            self.db,
            self.config,
            self.sources,
            self.notify,
            rate_limiter=self.rate_limiter,
            destination=self.destination,
        )
        self.monitor.register()

        self.forwarder = Forwarder(
            self.client,
            self.db,
            self.config,
            self.sources,
            self.destination,
            self.notify,
            rate_limiter=self.rate_limiter,
        )
        self._tasks.append(asyncio.create_task(self.forwarder.run(), name="forwarder"))

        self.importer = HistoryImporter(self.client, self.db, self.config, self.notify)
        if import_history:
            self._tasks.append(
                asyncio.create_task(self._run_import(force_import), name="history-import")
            )
        self._tasks.append(asyncio.create_task(self._heartbeat_loop(), name="heartbeat"))
        self._tasks.append(asyncio.create_task(self._watchdog_loop(), name="watchdog"))

        await self.db.log_event("info", "service_started")
        log.info("collector is running")

    @staticmethod
    def _matches(identifier: str, entity: Any) -> bool:
        """Match a configured identifier against a resolved entity.

        Works on canonical ids, usernames and titles, never on raw ``entity.id``.
        """

        value = identifier.strip().lstrip("@").lower()
        username = (getattr(entity, "username", None) or "").lower()
        if username and username == value:
            return True
        title = (getattr(entity, "title", None) or "").lower()
        if title and title == value:
            return True
        try:
            return canonical_id(entity) == int(value)
        except (ValueError, IdentityError):
            return False

    async def _run_import(self, force: bool) -> None:
        assert self.importer is not None
        try:
            async with self._import_lock:
                await self.importer.run(self.sources, force=force)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("history import task failed")
        finally:
            log.info("history import task finished")

    # ---------------------------------------------------------- supervision
    async def _heartbeat_loop(self) -> None:
        """Prove the process is alive; dashboards and `status` read this."""

        try:
            while not self._stopping.is_set():
                await self.db.heartbeat()
                await self._sleep(self.config.heartbeat_interval)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive
            log.exception("heartbeat loop failed")

    async def _watchdog_loop(self) -> None:
        """Resume incomplete imports and prune old audit events."""

        try:
            while not self._stopping.is_set():
                await self._sleep(self.config.import_recheck_interval)
                if self._stopping.is_set():
                    return
                try:
                    if not self._import_lock.locked():
                        async with self._import_lock:
                            results = await self.importer.resume_incomplete(self.sources)  # type: ignore[union-attr]
                        if results:
                            log.info("watchdog resumed %s incomplete import(s)", len(results))
                    if self.config.event_retention_days:
                        deleted = await self.db.prune_events(self.config.event_retention_days)
                        if deleted:
                            log.info("pruned %s old audit event(s)", deleted)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("watchdog iteration failed")
        except asyncio.CancelledError:
            raise

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    # -------------------------------------------------------------- shutdown
    async def stop(self) -> None:
        if self._stopping.is_set():
            return
        self._stopping.set()
        log.info("shutting down")
        await self._teardown()
        log.info("shutdown complete")

    async def _teardown(self) -> None:
        if self.importer is not None:
            self.importer.request_stop()
        if self.monitor is not None:
            self.monitor.unregister()
        if self.forwarder is not None:
            self.forwarder.stop()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        if self.db is not None:
            try:
                if self._started:
                    # Workers release their own leases; sweep anything left (a
                    # worker cancelled mid-claim cannot always clean up itself).
                    released = await self.db.release_stale_leases(only_expired=False)
                    if released.total:
                        log.info(
                            "shutdown sweep: %s re-queued, %s ambiguous",
                            released.requeued,
                            released.ambiguous,
                        )
                    await self.db.log_event("info", "service_stopped")
                    await self.db.checkpoint_wal()
            except Exception:  # pragma: no cover - shutdown best effort
                log.debug("database cleanup failed", exc_info=True)
            finally:
                try:
                    await self.db.close()
                except Exception:  # pragma: no cover
                    log.debug("database close failed", exc_info=True)

        if self.client is not None:
            try:
                await self.client.disconnect()
            except Exception:  # pragma: no cover
                log.debug("disconnect failed", exc_info=True)
            finally:
                self.client = None
        self.connection_state = "disconnected"
        self._started = False

    async def run_until_stopped(self) -> None:
        """Block until SIGINT/SIGTERM, then shut down gracefully."""

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stopping.set)
            except NotImplementedError:  # pragma: no cover - non-Unix
                pass
        try:
            await self._stopping.wait()
        finally:
            await self.stop()

    async def status_report(self) -> str:
        from .status import build_report

        return await build_report(self.db, self.config, connection_state=self.connection_state)
