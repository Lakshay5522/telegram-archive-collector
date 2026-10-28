from __future__ import annotations

import asyncio
import time

import pytest
from types import SimpleNamespace

from conftest import MARKED_CHANNEL, FakeClient, FakeEntity, make_message

from collector.config import Config
from collector.db import Database
from collector.forwarder import Forwarder, RateLimiter

CHAT = MARKED_CHANNEL
DEST = -100999


class FloodWaitError(Exception):
    def __init__(self, seconds: int) -> None:
        super().__init__("flood")
        self.seconds = seconds


class ChatForwardsRestrictedError(Exception):
    pass


class RpcCallFailError(Exception):
    pass


class WeirdUnexpectedError(Exception):
    pass


def build(config: Config, db: Database, client: FakeClient) -> Forwarder:
    return Forwarder(
        client,
        db,
        config,
        {CHAT: FakeEntity(1234567890, "Source one", "source_one")},
        FakeEntity(999, "Destination"),
        asyncio.Event(),
        rate_limiter=RateLimiter(0.0),
    )


def unit(message_id: int, grouped_id=None):
    return [{"message_id": message_id, "date": None, "grouped_id": grouped_id}]


async def test_successful_forward_marks_forwarded(db: Database, config: Config):
    await db.discover_unit(chat_id=CHAT, messages=unit(1))
    client = FakeClient()
    forwarder = build(config, db, client)

    assert await forwarder.drain_once() == 1
    assert len(client.forwarded) == 1
    assert client.forwarded[0]["ids"] == [1]
    assert await db.get_status(CHAT, 1) == "forwarded"


async def test_native_forward_is_used_by_default(db: Database, config: Config):
    await db.discover_unit(chat_id=CHAT, messages=unit(2))
    client = FakeClient()
    await build(config, db, client).drain_once()
    assert client.forwarded and not client.sent  # no copy fallback, no annotation


async def test_album_is_forwarded_in_one_call_and_member_ids_are_stored(
    db: Database, config: Config
):
    await db.discover_unit(chat_id=CHAT, messages=unit(10, 42) + unit(11, 42) + unit(12, 42))
    client = FakeClient()
    await build(config, db, client).drain_once()

    assert len(client.forwarded) == 1
    assert client.forwarded[0]["ids"] == [10, 11, 12]
    for message_id in (10, 11, 12):
        assert await db.get_status(CHAT, message_id) == "forwarded"

    # every member has its own destination id (audit item 6/13)
    cursor = await db.conn.execute(
        "SELECT source_message_id, destination_message_id FROM processed_messages "
        "WHERE source_chat_id = ? ORDER BY source_message_id",
        (CHAT,),
    )
    rows = await cursor.fetchall()
    await cursor.close()
    dest_ids = [row["destination_message_id"] for row in rows]
    assert dest_ids == [9000, 9001, 9002]


async def test_annotation_mode_posts_source_header(db: Database, config: Config):
    config.annotate_source = True
    await db.discover_unit(chat_id=CHAT, messages=unit(3))
    client = FakeClient()
    await build(config, db, client).drain_once()

    assert client.sent and f"chat {CHAT}" in client.sent[0][1]
    assert client.forwarded  # native forward still used


async def test_annotation_is_not_repeated_after_a_failed_forward(db: Database, config: Config):
    """Audit item 12: a retry must not post the annotation twice."""

    config.annotate_source = True
    await db.discover_unit(chat_id=CHAT, messages=unit(4))
    client = FakeClient()
    client.raise_on_forward = RpcCallFailError("rpc call fail")
    forwarder = build(config, db, client)
    await forwarder.drain_once()

    assert len(client.sent) == 1
    assert await db.get_status(CHAT, 4) == "retry_pending"

    # Second attempt succeeds; the annotation must not be sent again.
    client.raise_on_forward = None
    await asyncio.sleep(0.15)   # backoff_delay floors at 0.1s
    await forwarder.drain_once()
    assert len(client.sent) == 1
    assert await db.get_status(CHAT, 4) == "forwarded"


async def test_protected_content_is_not_retried(db: Database, config: Config):
    await db.discover_unit(chat_id=CHAT, messages=unit(5))
    client = FakeClient()
    client.raise_on_forward = ChatForwardsRestrictedError("forwards are restricted")
    await build(config, db, client).drain_once()

    assert await db.get_status(CHAT, 5) == "permanently_failed"
    assert await db.claim_next("w", 60) is None


async def test_transient_error_schedules_retry(db: Database, config: Config):
    await db.discover_unit(chat_id=CHAT, messages=unit(6))
    client = FakeClient()
    client.raise_on_forward = RpcCallFailError("rpc call fail")
    await build(config, db, client).drain_once()

    assert await db.get_status(CHAT, 6) == "retry_pending"
    next_at = await db.next_available_at()
    assert next_at is not None and next_at >= time.time() - 1


async def test_flood_wait_pauses_all_workers(db: Database, config: Config):
    await db.discover_unit(chat_id=CHAT, messages=unit(7))
    client = FakeClient()
    client.raise_on_forward = FloodWaitError(30)
    forwarder = build(config, db, client)
    await forwarder.drain_once()

    assert forwarder.rate_limiter.cooling_down > 0
    assert await db.get_status(CHAT, 7) == "retry_pending"


async def test_exhausted_retries_become_ambiguous_and_unclaimable(db: Database, config: Config):
    config.max_attempts = 1
    await db.discover_unit(chat_id=CHAT, messages=unit(8))
    client = FakeClient()
    client.raise_on_forward = RpcCallFailError("still failing")
    await build(config, db, client).drain_once()

    assert await db.get_status(CHAT, 8) == "ambiguous"
    assert await db.claim_next("w", 60) is None
    events = [e["event"] for e in await db.recent_events(limit=10)]
    assert "delivery_ambiguous" in events


async def test_unknown_errors_get_a_short_retry_budget(db: Database, config: Config):
    """Audit item 18: unknown exceptions must not be retried like network errors."""

    config.max_attempts = 5
    await db.discover_unit(chat_id=CHAT, messages=unit(9))
    client = FakeClient()
    client.raise_on_forward = WeirdUnexpectedError("what is this")
    forwarder = build(config, db, client)

    await forwarder.drain_once()   # attempt 1 -> retry
    await asyncio.sleep(0.15)      # backoff_delay floors at 0.1s
    await forwarder.drain_once()   # attempt 2 -> ambiguous (budget is 2)

    assert await db.get_status(CHAT, 9) == "ambiguous"
    events = [e["detail"] for e in await db.recent_events(limit=10)]
    assert any("unknown" in (detail or "") for detail in events)


async def test_copy_mode_uses_send_calls(db: Database, config: Config):
    config.forward_mode = "copy"
    messages = [make_message(11, text="hello")]
    await db.discover_unit(chat_id=CHAT, messages=unit(11))
    client = FakeClient(messages)
    await build(config, db, client).drain_once()

    assert not client.forwarded
    assert client.sent and client.sent[0][1] == "hello"
    assert await db.get_status(CHAT, 11) == "forwarded"


async def test_copy_mode_handles_media(db: Database, config: Config):
    config.forward_mode = "copy"
    media = object()
    messages = [make_message(12, text="caption", media=media)]
    await db.discover_unit(chat_id=CHAT, messages=unit(12))
    client = FakeClient(messages)
    await build(config, db, client).drain_once()

    assert client.copied and client.copied[0][1] is media
    assert client.copied[0][2] == "caption"
    assert await db.get_status(CHAT, 12) == "forwarded"


# ------------------------------------------------------------ rate limiter
@pytest.mark.slow
async def test_rate_limiter_spaces_calls():
    limiter = RateLimiter(0.05)
    started = time.monotonic()
    for _ in range(3):
        async with limiter:
            pass
    assert time.monotonic() - started >= 0.1


@pytest.mark.slow
async def test_rate_limiter_survives_cancellation():
    """Audit item 7: a cancelled waiter must not wedge the lock forever."""

    limiter = RateLimiter(0.5)
    entered = asyncio.Event()

    async def holder() -> None:
        async with limiter:
            entered.set()
            await asyncio.sleep(0.2)

    async def waiter() -> None:
        async with limiter:
            pass

    holder_task = asyncio.create_task(holder())
    await entered.wait()
    waiter_task = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)      # waiter is now blocked (or sleeping its interval)
    waiter_task.cancel()
    try:
        await waiter_task
    except asyncio.CancelledError:
        pass
    await holder_task

    # If the cancellation had leaked the lock this would hang forever.
    await asyncio.wait_for(_acquire(limiter), timeout=2.0)


async def _acquire(limiter: RateLimiter) -> None:
    async with limiter:
        return


@pytest.mark.slow
async def test_rate_limiter_cancellation_during_sleep_releases_lock():
    limiter = RateLimiter(5.0)

    async def first() -> None:
        async with limiter:
            pass

    await first()  # sets _last, so the next acquisition must sleep
    task = asyncio.create_task(_acquire(limiter))
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert not limiter._lock.locked()


# ------------------------------------------------------------------ workers
async def test_worker_loop_processes_queue_and_stops(db: Database, config: Config):
    for message_id in (20, 21):
        await db.discover_unit(chat_id=CHAT, messages=unit(message_id))
    client = FakeClient()
    forwarder = build(config, db, client)

    task = asyncio.create_task(forwarder.run())
    for _ in range(50):
        if await db.queue_size() == 0:
            break
        await asyncio.sleep(0.02)
    forwarder.stop()
    await asyncio.wait_for(task, timeout=5)

    assert len(client.forwarded) == 2
    assert await db.queue_size() == 0


async def test_destination_id_is_canonical(db: Database, config: Config):
    """Audit item 11: destination ids must use the same canonical helper."""

    await db.discover_unit(chat_id=CHAT, messages=unit(30))
    client = FakeClient()
    forwarder = build(config, db, client)
    await forwarder.drain_once()

    cursor = await db.conn.execute(
        "SELECT destination_chat_id FROM processed_messages WHERE source_chat_id = ?", (CHAT,)
    )
    row = await cursor.fetchone()
    await cursor.close()
    assert row["destination_chat_id"] == forwarder.destination_chat_id


class ShortResultClient(FakeClient):
    """Telegram returns fewer messages than it was given (album edge case)."""

    async def forward_messages(self, entity, messages, from_peer=None):
        ids = list(messages)
        self.forwarded.append({"entity": entity, "ids": ids, "from_peer": from_peer})
        return SimpleNamespace(id=8888)


async def test_album_without_per_member_result_records_only_the_first_id(
    db: Database, config: Config, caplog
):
    """Never invent a member mapping: keep the first id and say so."""

    import logging

    await db.discover_unit(chat_id=CHAT, messages=unit(40, 44) + unit(41, 44))
    client = ShortResultClient()
    with caplog.at_level(logging.WARNING, logger="collector.forwarder"):
        await build(config, db, client).drain_once()

    assert await db.get_status(CHAT, 40) == "forwarded"
    assert await db.get_status(CHAT, 41) == "forwarded"

    cursor = await db.conn.execute(
        "SELECT destination_message_id, destination_message_ids FROM processed_messages "
        "WHERE source_chat_id = ? ORDER BY source_message_id",
        (CHAT,),
    )
    rows = await cursor.fetchall()
    await cursor.close()
    assert [row["destination_message_id"] for row in rows] == [8888, 8888]
    assert all(row["destination_message_ids"] is None for row in rows)
    assert any("no per-member result" in record.message for record in caplog.records)


# --------------------------------------- verify before retry (1.3.0)
class VerifyClient(FakeClient):
    """Records what reached the destination, and can fail after accepting it."""

    def __init__(self, *, accept_then_fail: bool) -> None:
        super().__init__([])
        self.accept_then_fail = accept_then_fail
        self.calls = 0
        self.destination_id = 999
        self.destination_history: list[SimpleNamespace] = []

    async def forward_messages(self, entity, messages, from_peer=None):
        ids = list(messages) if isinstance(messages, (list, tuple)) else [messages]
        self.calls += 1
        if self.accept_then_fail:
            # Telegram accepted it; the response never came back.
            self.destination_history.append(
                SimpleNamespace(
                    id=7000 + self.calls,
                    fwd_from=SimpleNamespace(
                        channel_post=ids[0],
                        saved_from_peer=SimpleNamespace(
                            channel_id=int(str(CHAT).replace("-100", "", 1)),
                            chat_id=None,
                            user_id=None,
                        ),
                        from_id=None,
                        saved_from_msg_id=None,
                    ),
                )
            )
            raise RpcCallFailError("connection dropped after the request")
        raise RpcCallFailError("connection dropped before the request")


async def test_retry_is_suppressed_when_the_forward_already_landed(db: Database, config: Config):
    """The duplicate window the 'sending' state cannot close on its own."""

    await db.discover_unit(chat_id=CHAT, messages=unit(60))
    client = VerifyClient(accept_then_fail=True)
    forwarder = build(config, db, client)

    await forwarder.drain_once()

    assert client.calls == 1                      # never re-sent
    assert await db.get_status(CHAT, 60) == "forwarded"
    assert await db.claim_next("w", 60) is None
    events = [e["event"] for e in await db.recent_events(limit=10)]
    assert "retry_verified_delivered" in events


async def test_retry_still_happens_when_nothing_landed(db: Database, config: Config):
    await db.discover_unit(chat_id=CHAT, messages=unit(61))
    client = VerifyClient(accept_then_fail=False)
    forwarder = build(config, db, client)

    await forwarder.drain_once()
    assert await db.get_status(CHAT, 61) == "retry_pending"
    assert client.calls == 1

    await asyncio.sleep(0.15)  # backoff floor
    await forwarder.drain_once()
    assert client.calls == 2


async def test_verification_can_be_disabled(db: Database, config: Config):
    config.verify_before_retry = False
    await db.discover_unit(chat_id=CHAT, messages=unit(62))
    client = VerifyClient(accept_then_fail=True)
    forwarder = build(config, db, client)

    await forwarder.drain_once()
    # Without verification the retry is scheduled even though it already landed —
    # this is the documented cost of disabling the check.
    assert await db.get_status(CHAT, 62) == "retry_pending"
    assert client.calls == 1
