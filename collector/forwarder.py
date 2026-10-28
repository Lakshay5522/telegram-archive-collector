"""Forwarding workers.

Single coordinated path for *all* deliveries:

    claim (lease) -> mark_sending -> Telegram call -> commit / retry / ambiguous

Why the extra ``mark_sending`` step matters: a lease that expires while nothing
has been sent is safe to re-queue, but a lease that expires *during* the Telegram
call is not — the request may already have been applied.  Only the second case
becomes ``ambiguous``, which no worker will ever claim again.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .config import Config
from .db import Database, QueueItem
from .errors import ErrorClass, backoff_delay, classify, retry_budget

log = logging.getLogger(__name__)

ANNOTATION_TEMPLATE = "[source] {title} · chat {chat_id} · message {message_id}"


class RateLimiter:
    """Serialises Telegram write calls and enforces a minimum spacing.

    Also holds a global cooldown after a FloodWait so every worker backs off,
    not just the one that was told to wait.

    Acquisition is **cancellation safe**: if the task is cancelled while waiting
    for its turn, the lock is released before the cancellation propagates, so a
    cancelled worker can never wedge the other workers.
    """

    def __init__(self, min_interval: float = 0.6) -> None:
        self.min_interval = min_interval
        self._lock = asyncio.Lock()
        self._last = 0.0
        self.cooldown_until = 0.0

    async def __aenter__(self) -> "RateLimiter":
        await self._lock.acquire()
        try:
            now = time.monotonic()
            wait = max(0.0, self._last + self.min_interval - now, self.cooldown_until - now)
            if wait > 0:
                await asyncio.sleep(wait)
        except BaseException:
            # Cancelled (or anything else) between acquire and body: never leak
            # the lock, or every other worker would block forever.
            self._lock.release()
            raise
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self._last = time.monotonic()
        if self._lock.locked():
            self._lock.release()

    def penalise(self, seconds: float) -> None:
        until = time.monotonic() + max(1.0, seconds)
        self.cooldown_until = max(self.cooldown_until, until)

    @property
    def cooling_down(self) -> float:
        return max(0.0, self.cooldown_until - time.monotonic())


def _ids_from_result(result: Any, expected: list[int]) -> tuple[int | None, dict[int, int]]:
    """Map Telegram's forward result to (first destination id, member map).

    ``forward_messages`` returns one message per input when a list was given,
    which is what lets an album record a destination id for every member.
    """

    if result is None:
        return None, {}
    if not isinstance(result, (list, tuple)):
        return getattr(result, "id", None), {}
    ids = [getattr(message, "id", None) for message in result]
    ids = [message_id for message_id in ids if message_id is not None]
    if not ids:
        return None, {}
    members = dict(zip(expected, ids)) if len(ids) == len(expected) else {}
    return ids[0], members


def _peer_id(entity: Any) -> int | None:
    from .ids import IdentityError, canonical_id

    try:
        return canonical_id(entity)
    except IdentityError:
        return None


class Forwarder:
    def __init__(
        self,
        client: Any,
        db: Database,
        config: Config,
        sources: dict[int, Any],
        destination: Any,
        notify: asyncio.Event,
        *,
        rate_limiter: RateLimiter | None = None,
    ) -> None:
        self.client = client
        self.db = db
        self.config = config
        self.sources = sources
        self.destination = destination
        self.notify = notify
        self.rate_limiter = rate_limiter or RateLimiter(config.min_forward_interval)
        self._tasks: list[asyncio.Task[None]] = []
        self._stop = asyncio.Event()
        self.destination_chat_id = _peer_id(destination)
        self.forwarded_count = 0

    # ------------------------------------------------------------------ run
    async def run(self) -> None:
        """Start workers and block until :meth:`stop` is called."""

        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._worker(index), name=f"forwarder-{index}")
            for index in range(self.config.worker_concurrency)
        ]
        log.info(
            "forwarder started: %s worker(s), mode=%s, min interval=%ss",
            self.config.worker_concurrency,
            self.config.forward_mode,
            self.config.min_forward_interval,
        )
        try:
            await self._stop.wait()
        finally:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()
            log.info("forwarder stopped")

    def stop(self) -> None:
        self._stop.set()

    async def drain_once(self) -> int:
        """Process ready work in the current task (used by tests and CLI)."""

        processed = 0
        while True:
            item = await self.db.claim_next("cli", self.config.lease_seconds)
            if item is None:
                return processed
            await self._process(item)
            processed += 1

    # -------------------------------------------------------------- workers
    async def _worker(self, index: int) -> None:
        worker_id = f"worker-{index}"
        try:
            while not self._stop.is_set():
                cooling = self.rate_limiter.cooling_down
                if cooling > 0:
                    await self._sleep(min(cooling, 30.0))
                    continue
                item = await self.db.claim_next(worker_id, self.config.lease_seconds)
                if item is None:
                    await self._wait_for_work()
                    continue
                await self._process(item)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive
            log.exception("worker %s crashed", worker_id)
            raise
        finally:
            try:
                released = await self.db.release_worker(worker_id)
            except asyncio.CancelledError:  # pragma: no cover - shutdown race
                released = None
            except Exception:  # pragma: no cover - defensive
                log.debug("failed to release leases for %s", worker_id, exc_info=True)
                released = None
            if released is not None and released.total:
                log.info(
                    "worker %s released %s item(s) on shutdown (%s re-queued, %s ambiguous)",
                    worker_id,
                    released.total,
                    released.requeued,
                    released.ambiguous,
                )

    async def _wait_for_work(self) -> None:
        try:
            await asyncio.wait_for(self.notify.wait(), timeout=self.config.queue_poll_interval)
        except asyncio.TimeoutError:
            pass
        finally:
            self.notify.clear()

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------ processing
    async def _process(self, item: QueueItem) -> None:
        # Announce that a Telegram request is about to happen.  From here on an
        # interrupted delivery is ambiguous, not safely retryable.
        if not await self.db.mark_sending(item):
            log.warning("unit %s is no longer in a claimable state; skipping", item.key)
            return

        try:
            destination_message_id, member_ids = await self._deliver(item)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._handle_failure(item, exc)
            return

        await self.db.complete(
            item,
            destination_chat_id=self.destination_chat_id,
            destination_message_id=destination_message_id,
            member_destination_ids=member_ids,
        )
        self.forwarded_count += 1
        log.info(
            "forwarded %s message(s) from chat %s -> destination message %s",
            len(item.message_ids),
            item.source_chat_id,
            destination_message_id,
        )

    async def _deliver(self, item: QueueItem) -> tuple[int | None, dict[int, int]]:
        source = await self._source_entity(item.source_chat_id)

        if self.config.annotate_source and not item.annotation_sent:
            # Tracked in the queue row, so a retry after a failed forward does not
            # post the annotation twice.
            annotation = ANNOTATION_TEMPLATE.format(
                title=getattr(source, "title", None) or item.source_chat_id,
                chat_id=item.source_chat_id,
                message_id=item.source_message_id,
            )
            async with self.rate_limiter:
                sent = await self.client.send_message(self.destination, annotation)
            await self.db.mark_annotation_sent(item, getattr(sent, "id", None))

        if self.config.forward_mode == "copy":
            return await self._copy(item, source)

        async with self.rate_limiter:
            result = await self.client.forward_messages(
                entity=self.destination,
                messages=list(item.message_ids),
                from_peer=source,
            )
        first_id, members = _ids_from_result(result, list(item.message_ids))
        if len(item.message_ids) > 1 and not members and first_id is not None:
            # Telegram does not always return one message per input.  Recording a
            # guessed mapping would be worse than recording none, so we keep the
            # first id and say so loudly.
            log.warning(
                "album %s: Telegram returned no per-member result, so only the first "
                "destination id was recorded (%s member(s) unmapped)",
                item.key,
                len(item.message_ids) - 1,
            )
        return first_id, members

    async def _copy(self, item: QueueItem, source: Any) -> tuple[int | None, dict[int, int]]:
        """Copy instead of forward (explicit opt-in only).

        Native attribution is lost in this mode, which is why it is never the
        default.  Album members are sent as individual messages.
        """

        messages = await self.client.get_messages(source, ids=list(item.message_ids))
        if messages is None:
            raise ValueError("source messages are no longer available")
        if not isinstance(messages, (list, tuple)):
            messages = [messages]

        first_id: int | None = None
        member_ids: dict[int, int] = {}
        for message in messages:
            if message is None:
                continue
            async with self.rate_limiter:
                if getattr(message, "media", None) is not None:
                    sent = await self.client.send_file(
                        self.destination,
                        file=message.media,
                        caption=getattr(message, "message", "") or "",
                    )
                else:
                    sent = await self.client.send_message(
                        self.destination, getattr(message, "message", "") or ""
                    )
            sent_id = getattr(sent, "id", None)
            if sent_id is not None:
                member_ids[int(getattr(message, "id", 0))] = sent_id
                first_id = first_id or sent_id
        return first_id, member_ids

    async def _source_entity(self, chat_id: int) -> Any:
        entity = self.sources.get(chat_id)
        if entity is not None:
            return entity
        entity = await self.client.get_entity(chat_id)
        self.sources[chat_id] = entity
        return entity

    async def _delivery_evidence(self, item: QueueItem) -> int | None:
        """Return the destination id if this unit is already there, else ``None``.

        Bounded scan (``RETRY_VERIFY_SCAN_LIMIT``) so a retry costs one small
        history read rather than a duplicate message.  Any failure here is
        non-fatal: we fall back to the normal retry path.
        """

        from .reconcile import scan_destination

        try:
            async with self.rate_limiter:
                found = await scan_destination(
                    self.client, self.destination, limit=self.config.retry_verify_scan_limit
                )
        except Exception:
            log.debug("delivery verification failed; falling back to retry", exc_info=True)
            return None
        return found.get((item.source_chat_id, item.source_message_id))

    # -------------------------------------------------------------- failures
    async def _handle_failure(self, item: QueueItem, exc: BaseException) -> None:
        failure = classify(exc)
        attempts = item.attempts + 1
        budget = retry_budget(failure, self.config.max_attempts)
        log.warning(
            "forwarding %s failed (%s, attempt %s/%s): %s",
            item.key,
            failure.error_class.value,
            attempts,
            budget,
            failure.message,
        )
        if failure.error_class is ErrorClass.UNKNOWN:
            # Give the operator the type and traceback (never the payload).
            log.debug("unclassified forwarding failure", exc_info=exc)

        await self.db.log_event(
            "warning" if failure.retryable else "error",
            "forward_failed",
            source_chat_id=item.source_chat_id,
            source_message_id=item.source_message_id,
            detail=f"{failure.error_class.value}: {failure.message}"[:1000],
        )

        now = time.time()

        if failure.error_class in (ErrorClass.PERMANENT, ErrorClass.INACCESSIBLE):
            # Telegram rejected it, so nothing was delivered: safe to dead-letter.
            await self.db.fail(
                item,
                error_class=failure.error_class.value,
                error=failure.message,
                retry_at=None,
            )
            return

        if attempts < budget:
            # Before retrying a request that may already have landed, look for
            # evidence of it at the destination.  This is the duplicate window the
            # "sending" state cannot resolve on its own: the forward succeeded but
            # the response never made it back.
            if (
                self.config.verify_before_retry
                and failure.error_class in (ErrorClass.TRANSIENT, ErrorClass.UNKNOWN)
            ):
                delivered_id = await self._delivery_evidence(item)
                if delivered_id is not None:
                    await self.db.complete(
                        item,
                        destination_chat_id=self.destination_chat_id,
                        destination_message_id=delivered_id,
                    )
                    await self.db.log_event(
                        "info",
                        "retry_verified_delivered",
                        source_chat_id=item.source_chat_id,
                        source_message_id=item.source_message_id,
                        detail=f"destination message {delivered_id} found; retry suppressed",
                    )
                    log.info(
                        "unit %s had already been delivered as %s; suppressed the retry",
                        item.key,
                        delivered_id,
                    )
                    return

            delay = (
                failure.retry_after
                if failure.error_class is ErrorClass.RATE_LIMIT and failure.retry_after
                else backoff_delay(
                    attempts, base=self.config.backoff_base, cap=self.config.backoff_cap
                )
            )
            if failure.error_class is ErrorClass.RATE_LIMIT:
                self.rate_limiter.penalise(delay)
                log.info("rate limited: pausing all workers for %.0fs", delay)
            await self.db.fail(
                item,
                error_class=failure.error_class.value,
                error=failure.message,
                retry_at=now + delay,
            )
            return

        # Retries exhausted on a possibly-delivered request: Telegram may have
        # accepted the forward before the failure surfaced.  Flag it ambiguous so
        # `reconcile` can check the destination instead of blindly duplicating.
        await self.db.mark_ambiguous(
            item,
            error=f"retries exhausted ({failure.error_class.value}): {failure.message}",
        )
        await self.db.log_event(
            "error",
            "delivery_ambiguous",
            source_chat_id=item.source_chat_id,
            source_message_id=item.source_message_id,
            detail=failure.message[:1000],
        )
