from __future__ import annotations

import asyncio
import time

import pytest

from collector.db import (
    QUEUE_AMBIGUOUS,
    QUEUE_PENDING,
    QUEUE_SENDING,
    Database,
    DatabaseError,
    message_key,
)

CHAT = -1001234567890


def payload(*ids, grouped_id=None):
    return [{"message_id": i, "date": None, "grouped_id": grouped_id} for i in ids]


# --------------------------------------------------------------------- dedup
async def test_deduplication_is_idempotent(db: Database):
    first = await db.discover_unit(chat_id=CHAT, messages=payload(10))
    second = await db.discover_unit(chat_id=CHAT, messages=payload(10))
    assert first == 1
    assert second == 0
    assert await db.queue_size() == 1
    assert await db.is_known(CHAT, 10)


async def test_album_is_one_unit_with_all_members(db: Database):
    created = await db.discover_unit(chat_id=CHAT, messages=payload(20, 21, 22, grouped_id=555))
    assert created == 1
    assert await db.queue_size() == 1

    item = await db.claim_next("w0", lease_seconds=60)
    assert item is not None
    assert item.message_ids == [20, 21, 22]
    assert item.grouped_id == 555


async def test_late_album_member_forms_its_own_unit(db: Database):
    """A member discovered after its unit was delivered must not be lost."""
    await db.discover_unit(chat_id=CHAT, messages=payload(200))
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.complete(item, destination_chat_id=-100999, destination_message_id=1)

    created = await db.discover_unit(
        chat_id=CHAT, messages=payload(200, 201, 202, grouped_id=777), priority=10
    )
    assert created == 1
    assert await db.get_status(CHAT, 200) == "forwarded"
    assert await db.get_status(CHAT, 201) == "discovered"

    follow_up = await db.claim_next("w0", 60)
    assert follow_up is not None
    assert follow_up.message_ids == [201, 202]
    assert follow_up.grouped_id == 777


# --------------------------------------------------------------------- queue
async def test_claim_is_atomic_and_respects_lease(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(30))
    first = await db.claim_next("w0", lease_seconds=60)
    second = await db.claim_next("w1", lease_seconds=60)
    assert first is not None
    assert second is None


async def test_claim_does_not_imply_sending(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(31))
    item = await db.claim_next("w0", 60)
    assert item is not None
    assert item.state == "leased"
    assert await db.get_status(CHAT, 31) == "discovered"


async def test_mark_sending_moves_unit_into_flight(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(32))
    item = await db.claim_next("w0", 60)
    assert item is not None
    assert await db.mark_sending(item) is True
    assert item.state == QUEUE_SENDING
    assert await db.get_status(CHAT, 32) == "processing"


async def test_complete_records_destination_and_member_ids(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(40, 41, grouped_id=99))
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_sending(item)
    await db.complete(
        item,
        destination_chat_id=-100999,
        destination_message_id=777,
        member_destination_ids={40: 777, 41: 778},
    )

    assert await db.get_status(CHAT, 40) == "forwarded"
    assert await db.get_status(CHAT, 41) == "forwarded"
    counts = await db.status_counts()
    assert counts["forwarded"] == 2
    assert counts["queue_done"] == 1
    last = await db.last_forwarded()
    assert last is not None

    cursor = await db.conn.execute(
        "SELECT destination_message_id, destination_message_ids FROM processed_messages "
        "WHERE source_chat_id = ? AND source_message_id = 41",
        (CHAT,),
    )
    row = await cursor.fetchone()
    await cursor.close()
    assert row["destination_message_id"] == 778
    assert '"41": 778' in row["destination_message_ids"]


async def test_retryable_failure_schedules_retry_not_claimable_yet(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(50))
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_sending(item)
    await db.fail(item, error_class="transient", error="boom", retry_at=time.time() + 60)

    assert await db.get_status(CHAT, 50) == "retry_pending"
    assert await db.claim_next("w0", 60) is None


async def test_retryable_failure_becomes_claimable_after_delay(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(51))
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.fail(item, error_class="transient", error="boom", retry_at=time.time() - 1)
    again = await db.claim_next("w0", 60)
    assert again is not None
    assert again.attempts == 1


async def test_permanent_failure_dead_letters(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(60))
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_sending(item)
    await db.fail(item, error_class="permanent", error="protected content", retry_at=None)

    assert await db.get_status(CHAT, 60) == "permanently_failed"
    assert await db.claim_next("w0", 60) is None
    failures = await db.recent_failures()
    assert failures[0]["error_class"] == "permanent"


# ------------------------------------------------------- crash recovery rules
async def test_expired_lease_before_sending_is_requeued(db: Database):
    """Claimed but never sent: safe to retry, no duplicate risk."""

    await db.discover_unit(chat_id=CHAT, messages=payload(70))
    item = await db.claim_next("w0", lease_seconds=0.01)
    assert item is not None
    time.sleep(0.05)

    released = await db.release_stale_leases()
    assert released.requeued == 1
    assert released.ambiguous == 0
    assert await db.get_status(CHAT, 70) == "queued"
    reclaimed = await db.claim_next("w1", lease_seconds=60)
    assert reclaimed is not None and reclaimed.key == message_key(CHAT, 70)


async def test_expired_lease_while_sending_becomes_ambiguous_and_is_never_reclaimed(
    db: Database,
):
    """This is the audit's crash window: Telegram may already have the message."""

    await db.discover_unit(chat_id=CHAT, messages=payload(71))
    item = await db.claim_next("w0", lease_seconds=0.01)
    assert item is not None
    await db.mark_sending(item)
    time.sleep(0.05)

    released = await db.release_stale_leases()
    assert released.ambiguous == 1
    assert released.requeued == 0
    assert await db.get_status(CHAT, 71) == "ambiguous"

    cursor = await db.conn.execute(
        "SELECT state FROM processing_queue WHERE message_key = ?", (message_key(CHAT, 71),)
    )
    row = await cursor.fetchone()
    await cursor.close()
    assert row["state"] == QUEUE_AMBIGUOUS

    # The critical assertion: no worker can pick it up and duplicate the forward.
    assert await db.claim_next("w1", 60) is None
    assert await db.queue_size() == 0


async def test_graceful_release_requeues_unsent_work(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(80))
    await db.claim_next("w0", lease_seconds=600)
    released = await db.release_worker("w0")
    assert released.requeued == 1
    assert await db.get_status(CHAT, 80) == "queued"
    assert await db.claim_next("w1", lease_seconds=60) is not None


async def test_graceful_release_marks_in_flight_work_ambiguous(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(81))
    item = await db.claim_next("w0", lease_seconds=600)
    assert item is not None
    await db.mark_sending(item)

    released = await db.release_worker("w0")
    assert released.ambiguous == 1
    assert await db.get_status(CHAT, 81) == "ambiguous"
    assert await db.claim_next("w1", 60) is None


async def test_shutdown_sweep_handles_long_leases(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(120))
    await db.claim_next("w0", lease_seconds=3600)  # still valid
    released = await db.release_stale_leases(only_expired=False)
    assert released.total == 1
    assert await db.get_status(CHAT, 120) == "queued"
    assert await db.queue_size() == 1


async def test_retry_failed_requeues_dead_letters(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(90))
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.fail(item, error_class="permanent", error="nope", retry_at=None)

    count = await db.retry_failed()
    assert count == 1
    assert await db.get_status(CHAT, 90) == "queued"
    assert await db.claim_next("w0", 60) is not None


async def test_retry_failed_skips_ambiguous_by_default(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(91))
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_ambiguous(item, error="uncertain")

    assert await db.retry_failed() == 0
    assert await db.retry_failed(include_ambiguous=True) == 1
    assert await db.get_status(CHAT, 91) == "queued"


# ------------------------------------------------------------- annotations
async def test_annotation_tracking_survives_retry(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(100))
    item = await db.claim_next("w0", 60)
    assert item is not None
    assert item.annotation_sent is False
    await db.mark_annotation_sent(item, 4242)
    await db.fail(item, error_class="transient", error="boom", retry_at=time.time() - 1)

    again = await db.claim_next("w0", 60)
    assert again is not None
    assert again.annotation_sent is True


# -------------------------------------------------------- import checkpoints
async def test_import_progress_accumulates_and_checkpoints(db: Database):
    await db.start_import(CHAT)
    await db.update_import_progress(
        CHAT,
        oldest_message_id=1,
        newest_message_id=100,
        last_processed_message_id=100,
        last_scanned_message_id=100,
        seen_delta=100,
        queued_delta=90,
        skipped_delta=10,
    )
    await db.update_import_progress(
        CHAT, seen_delta=5, queued_delta=5, last_scanned_message_id=105
    )
    progress = await db.get_import_progress(CHAT)
    assert progress is not None
    assert progress["messages_seen"] == 105
    assert progress["messages_queued"] == 95
    assert progress["messages_skipped"] == 10
    assert progress["last_scanned_message_id"] == 105
    assert progress["status"] == "running"

    await db.update_import_progress(CHAT, status="completed", completed=True)
    progress = await db.get_import_progress(CHAT)
    assert progress["status"] == "completed"
    assert progress["completed_at"] is not None


async def test_start_import_counts_attempts(db: Database):
    await db.start_import(CHAT, increment_attempts=True)
    await db.start_import(CHAT, increment_attempts=True)
    progress = await db.get_import_progress(CHAT)
    assert progress["attempts"] == 2


async def test_incomplete_imports_lists_only_unfinished(db: Database):
    await db.start_import(CHAT)
    await db.update_import_progress(CHAT, status="completed", completed=True)
    assert await db.incomplete_imports() == []

    await db.start_import(-100999)
    await db.update_import_progress(-100999, status="retry_pending", error="boom")
    assert [row["source_chat_id"] for row in await db.incomplete_imports()] == [-100999]


# ---------------------------------------------------------------- ambiguous
async def test_resolve_ambiguous_marks_forwarded(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(110))
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_sending(item)
    await db.mark_ambiguous(item, error="interrupted")
    assert await db.get_status(CHAT, 110) == "ambiguous"

    units = await db.ambiguous_units()
    assert units and units[0]["unit_key"] == message_key(CHAT, 110)

    await db.resolve_ambiguous(item.key, destination_chat_id=-100999, destination_message_id=1234)
    assert await db.get_status(CHAT, 110) == "forwarded"
    assert await db.queue_size() == 0


async def test_unresolved_reconcile_is_recorded(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(111))
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_ambiguous(item, error="uncertain")
    await db.record_reconcile_result(item.key, "unresolved")

    cursor = await db.conn.execute(
        "SELECT reconcile_result, reconciled_at FROM processed_messages WHERE unit_key = ?",
        (item.key,),
    )
    row = await cursor.fetchone()
    await cursor.close()
    assert row["reconcile_result"] == "unresolved"
    assert row["reconciled_at"] is not None


# ------------------------------------------------------------------ reports
async def test_stats_by_source_reports_per_source_numbers(db: Database):
    await db.sync_source_groups([type("S", (), {"identifier": "@one", "alias": None, "enabled": True})()])
    await db.set_source_resolved("@one", CHAT, "Group one")
    await db.discover_unit(chat_id=CHAT, messages=payload(130))
    await db.discover_unit(chat_id=CHAT, messages=payload(131))
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.complete(item, destination_chat_id=-100999, destination_message_id=1)
    await db.touch_source_live(CHAT)

    stats = await db.stats_by_source()
    row = next(entry for entry in stats if entry["chat_id"] == CHAT)
    assert row["forwarded"] == 1
    assert row["pending"] == 1
    assert row["last_seen_message_id"] == 131
    assert row["last_live_event_at"] is not None


async def test_events_are_logged_and_queryable(db: Database):
    await db.log_event("warning", "forward_failed", source_chat_id=CHAT, source_message_id=1, detail="x")
    await db.log_event("info", "service_started")
    events = await db.recent_events(limit=5)
    assert events[0]["event"] == "service_started"
    warnings = await db.recent_events(level="warning")
    assert len(warnings) == 1


async def test_prune_events_keeps_recent_rows(db: Database):
    await db.log_event("info", "old_event")
    await db.conn.execute(
        "UPDATE application_events SET created_at = '2020-01-01T00:00:00+00:00'"
    )
    await db.log_event("info", "new_event")
    deleted = await db.prune_events(30)
    assert deleted == 1
    events = await db.recent_events(limit=10)
    assert [e["event"] for e in events] == ["new_event"]


async def test_heartbeat_roundtrip(db: Database):
    assert await db.last_heartbeat() is None
    await db.heartbeat()
    heartbeat = await db.last_heartbeat()
    assert heartbeat is not None


async def test_integrity_check_and_size(db: Database):
    assert await db.integrity_check() == "ok"
    await db.checkpoint_wal()
    assert await db.database_size() > 0


# ------------------------------------------------------- schema and read-only
async def test_schema_version_is_recorded(db: Database):
    cursor = await db.conn.execute("SELECT version FROM schema_version")
    row = await cursor.fetchone()
    await cursor.close()
    assert row["version"] == 2


async def test_migrations_are_idempotent(tmp_path):
    path = tmp_path / "again.sqlite3"
    async with Database(path) as first:
        await first.migrate()
    async with Database(path) as second:
        version = await second.migrate()
    assert version == 2


async def test_read_only_connection_can_read_but_not_write(tmp_path):
    path = tmp_path / "ro.sqlite3"
    async with Database(path) as writer:
        await writer.discover_unit(chat_id=CHAT, messages=payload(140))
        await writer.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    async with Database(path, read_only=True) as reader:
        assert await reader.queue_size() == 1
        with pytest.raises(Exception):
            await reader.conn.execute("INSERT INTO application_events (created_at, level, event) "
                                      "VALUES ('x', 'INFO', 'y')")


async def test_read_only_connection_requires_existing_database(tmp_path):
    with pytest.raises(DatabaseError):
        async with Database(tmp_path / "missing.sqlite3", read_only=True):
            pass


async def test_read_only_connection_rejects_other_sqlite_files(tmp_path):
    import aiosqlite

    path = tmp_path / "foreign.sqlite3"
    conn = await aiosqlite.connect(path)
    await conn.execute("CREATE TABLE t (x INTEGER)")
    await conn.commit()
    await conn.close()

    with pytest.raises(DatabaseError):
        async with Database(path, read_only=True):
            pass


async def test_database_files_are_owner_only(tmp_path):
    path = tmp_path / "perms.sqlite3"
    async with Database(path) as database:
        await database.discover_unit(chat_id=CHAT, messages=payload(150))
        mode = (path.stat().st_mode) & 0o777
    assert mode == 0o600


async def test_state_directory_is_created_owner_only(tmp_path):
    """Even a read-write open from a CLI command must not create a world-readable dir."""

    target = tmp_path / "fresh-state" / "collector.sqlite3"
    async with Database(target):
        pass
    assert (target.parent.stat().st_mode & 0o777) == 0o700


# ------------------------------------------------ album merging (1.2.0 fixes)
async def test_pending_album_absorbs_late_members(db: Database):
    """A not-yet-delivered album unit must absorb new members, not split."""

    await db.discover_unit(chat_id=CHAT, messages=payload(300, grouped_id=42))
    created = await db.discover_unit(
        chat_id=CHAT, messages=payload(300, 301, 302, grouped_id=42), priority=10
    )
    assert created == 1
    assert await db.queue_size() == 1

    item = await db.claim_next("w0", 60)
    assert item is not None
    assert item.message_ids == [300, 301, 302]
    assert item.priority == 10  # live priority won


async def test_in_flight_album_is_not_merged(db: Database):
    """Once delivery may be in flight, new members must form their own unit."""

    await db.discover_unit(chat_id=CHAT, messages=payload(310, grouped_id=43))
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_sending(item)

    await db.discover_unit(
        chat_id=CHAT, messages=payload(310, 311, grouped_id=43), priority=10
    )
    # 311 is deliverable on its own; 310 is untouched and still in flight.
    follow_up = await db.claim_next("w1", 60)
    assert follow_up is not None
    assert follow_up.message_ids == [311]
    assert await db.get_status(CHAT, 310) == "processing"


async def test_merge_does_not_happen_for_single_messages(db: Database):
    await db.discover_unit(chat_id=CHAT, messages=payload(320))
    await db.discover_unit(chat_id=CHAT, messages=payload(320, 321))
    assert await db.queue_size() == 2


async def test_album_in_retry_backoff_is_not_merged(db: Database):
    """A delayed (retry-pending) unit must not absorb members: it is not ready."""

    await db.discover_unit(chat_id=CHAT, messages=payload(410, grouped_id=89))
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.fail(item, error_class="transient", error="boom", retry_at=time.time() + 3600)

    await db.discover_unit(chat_id=CHAT, messages=payload(410, 411, grouped_id=89), priority=10)
    fresh = await db.claim_next("w1", 60)
    assert fresh is not None
    assert fresh.message_ids == [411]


async def test_failed_read_only_open_does_not_leak_the_connection(tmp_path):
    """Regression: a leaked driver thread kept the process alive for minutes."""

    import threading

    import aiosqlite

    path = tmp_path / "foreign2.sqlite3"
    conn = await aiosqlite.connect(path)
    await conn.execute("CREATE TABLE t (x INTEGER)")
    await conn.commit()
    await conn.close()

    before = {thread.name for thread in threading.enumerate()}
    database = Database(path, read_only=True)
    with pytest.raises(DatabaseError):
        await database.connect()

    assert database._conn is None
    await asyncio.sleep(0.1)
    leaked = {thread.name for thread in threading.enumerate()} - before
    assert not leaked, f"leaked thread(s): {leaked}"
