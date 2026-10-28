from __future__ import annotations

from datetime import datetime, timezone

from conftest import MARKED_CHANNEL, FakeClient, FakeEntity, make_message

from collector.config import Config
from collector.db import Database
from collector.importer import (
    IMPORT_COMPLETED,
    IMPORT_FAILED,
    HistoryImporter,
    split_units,
)

CHAT = MARKED_CHANNEL


class RpcCallFailError(Exception):
    pass


def test_split_units_groups_albums():
    messages = [
        make_message(1),
        make_message(2, grouped_id=99),
        make_message(3, grouped_id=99),
        make_message(4),
    ]
    units, remainder = split_units(messages)
    # The album is closed by the following non-album message, so it is complete.
    assert [len(u) for u in units] == [1, 2, 1]
    assert [m.id for m in units[1]] == [2, 3]
    assert remainder == []


def test_split_units_keeps_trailing_album_pending():
    units, remainder = split_units([make_message(1), make_message(2, grouped_id=7)])
    assert [len(u) for u in units] == [1]
    assert [m.id for m in remainder] == [2]


async def test_import_queues_history_chronologically(db: Database, config: Config):
    messages = [make_message(i) for i in range(1, 6)]
    importer = HistoryImporter(FakeClient(messages), db, config)

    stats = await importer.run({CHAT: FakeEntity(CHAT, "One")})
    assert stats[CHAT].queued == 5
    assert await db.queue_size() == 5

    item = await db.claim_next("w0", 60)
    assert item is not None and item.source_message_id == 1  # oldest first

    progress = await db.get_import_progress(CHAT)
    assert progress["status"] == IMPORT_COMPLETED
    assert progress["last_scanned_message_id"] == 5


async def test_import_skips_service_messages_and_outgoing(db: Database, config: Config):
    messages = [
        make_message(1),
        make_message(2, action=object()),
        make_message(3, out=True),
        make_message(4),
    ]
    importer = HistoryImporter(FakeClient(messages), db, config)
    stats = await importer.run({CHAT: FakeEntity(CHAT)})
    assert stats[CHAT].queued == 2
    assert stats[CHAT].skipped_service == 1
    assert stats[CHAT].skipped_outgoing == 1


async def test_skipped_messages_still_advance_the_scan_checkpoint(db: Database, config: Config):
    """Audit item 15: skipped messages must not be re-scanned after a restart."""

    config.history_batch_size = 2
    messages = [make_message(1, action=object()), make_message(2, action=object()), make_message(3)]
    importer = HistoryImporter(FakeClient(messages), db, config)
    await importer.run({CHAT: FakeEntity(CHAT)})

    progress = await db.get_import_progress(CHAT)
    assert progress["messages_skipped"] == 2
    assert progress["last_scanned_message_id"] == 3
    assert progress["last_processed_message_id"] == 3


async def test_import_resumes_from_scan_checkpoint(db: Database, config: Config):
    messages = [make_message(i) for i in range(1, 6)]
    importer = HistoryImporter(FakeClient(messages), db, config)
    await db.start_import(CHAT)
    await db.update_import_progress(CHAT, last_scanned_message_id=3)

    stats = await importer.run({CHAT: FakeEntity(CHAT)})
    # min_id=3 means only messages 4 and 5 are fetched again.
    assert stats[CHAT].queued == 2
    assert not await db.is_known(CHAT, 1)
    assert await db.is_known(CHAT, 5)


async def test_import_is_not_repeated_when_completed(db: Database, config: Config):
    importer = HistoryImporter(FakeClient([make_message(1)]), db, config)
    await importer.run({CHAT: FakeEntity(CHAT)})
    again = await importer.run({CHAT: FakeEntity(CHAT)})
    assert again[CHAT].queued == 0
    assert await db.queue_size() == 1


async def test_history_max_count_is_a_total_budget_across_restarts(db: Database, config: Config):
    """Audit item 5: the limit must not reset on every run."""

    config.history_max_count = 4
    messages = [make_message(i) for i in range(1, 11)]

    importer = HistoryImporter(FakeClient(messages), db, config)
    await importer.run({CHAT: FakeEntity(CHAT)})
    progress = await db.get_import_progress(CHAT)
    assert progress["messages_seen"] == 4

    # Second run: 0 remaining, so nothing more is scanned.
    importer2 = HistoryImporter(FakeClient(messages), db, config)
    stats = await importer2.run({CHAT: FakeEntity(CHAT)})
    assert stats[CHAT].queued == 0
    progress = await db.get_import_progress(CHAT)
    assert progress["messages_seen"] == 4
    assert await db.queue_size() == 4


async def test_history_start_date_is_inclusive(db: Database, config: Config):
    config.history_start_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    messages = [
        make_message(1, date=datetime(2024, 1, 1, tzinfo=timezone.utc)),
        make_message(2, date=datetime(2024, 1, 2, tzinfo=timezone.utc)),
        make_message(3, date=datetime(2024, 1, 3, tzinfo=timezone.utc)),
    ]
    importer = HistoryImporter(FakeClient(messages), db, config)
    await importer.run({CHAT: FakeEntity(CHAT)})
    assert await db.is_known(CHAT, 2)
    assert await db.is_known(CHAT, 3)
    assert not await db.is_known(CHAT, 1)


async def test_history_end_date_is_inclusive(db: Database, config: Config):
    config.history_end_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
    messages = [
        make_message(1, date=datetime(2024, 1, 1, tzinfo=timezone.utc)),
        make_message(2, date=datetime(2024, 1, 2, tzinfo=timezone.utc)),
        make_message(3, date=datetime(2024, 1, 3, tzinfo=timezone.utc)),
    ]
    importer = HistoryImporter(FakeClient(messages), db, config)
    stats = await importer.run({CHAT: FakeEntity(CHAT)})
    assert stats[CHAT].queued == 2
    assert await db.is_known(CHAT, 2)
    assert not await db.is_known(CHAT, 3)


async def test_message_arriving_during_import_is_not_lost(db: Database, config: Config):
    """A live message discovered mid-import keeps priority and stays unique."""

    messages = [make_message(i) for i in range(1, 4)]
    importer = HistoryImporter(FakeClient(messages), db, config)

    # Simulate the live handler firing while history is being read.
    await db.discover_unit(
        chat_id=CHAT,
        messages=[{"message_id": 3, "date": None, "grouped_id": None}],
        priority=10,
    )
    await importer.run({CHAT: FakeEntity(CHAT)})

    assert await db.queue_size() == 3
    first = await db.claim_next("w0", 60)
    assert first is not None
    assert first.source_message_id == 3  # live priority wins


async def test_import_handles_empty_group(db: Database, config: Config):
    importer = HistoryImporter(FakeClient([]), db, config)
    stats = await importer.run({CHAT: FakeEntity(CHAT)})
    assert stats[CHAT].queued == 0
    progress = await db.get_import_progress(CHAT)
    assert progress["status"] == IMPORT_COMPLETED


async def test_transient_import_failure_is_retried_and_then_succeeds(
    db: Database, config: Config
):
    """Audit item 4: a transient error must not end the background import."""

    class FlakyClient(FakeClient):
        def __init__(self, messages, failures=1):
            super().__init__(messages)
            self.failures = failures

        async def iter_messages(self, entity, limit=None, **kwargs):
            if self.failures:
                self.failures -= 1
                raise RpcCallFailError("network blip")
            async for message in super().iter_messages(entity, limit=limit, **kwargs):
                yield message

    client = FlakyClient([make_message(1), make_message(2)])
    importer = HistoryImporter(client, db, config)
    stats = await importer.run({CHAT: FakeEntity(CHAT)})

    assert stats[CHAT].queued == 2
    assert stats[CHAT].status == IMPORT_COMPLETED
    assert await db.queue_size() == 2
    events = [e["event"] for e in await db.recent_events(limit=10)]
    assert "history_import_retry" in events


async def test_permanent_import_failure_is_not_retried_forever(db: Database, config: Config):
    class ExplodingClient(FakeClient):
        def __init__(self):
            super().__init__([])
            self.calls = 0

        async def iter_messages(self, entity, limit=None, **kwargs):
            self.calls += 1
            raise RuntimeError("permanently broken")
            yield  # pragma: no cover

    client = ExplodingClient()
    importer = HistoryImporter(client, db, config)
    stats = await importer.run({CHAT: FakeEntity(CHAT)})

    assert stats[CHAT].failed
    assert client.calls == 1
    progress = await db.get_import_progress(CHAT)
    assert progress["status"] == IMPORT_FAILED


async def test_resume_incomplete_skips_finished_and_abandoned(db: Database, config: Config):
    await db.start_import(CHAT)
    await db.update_import_progress(CHAT, status=IMPORT_COMPLETED, completed=True)
    other = -1009999999999
    await db.start_import(other)
    await db.update_import_progress(other, status="retry_pending", error="boom")

    importer = HistoryImporter(FakeClient([make_message(1)]), db, config)
    results = await importer.resume_incomplete(
        {CHAT: FakeEntity(CHAT), other: FakeEntity(other)}
    )
    assert list(results) == [other]
    assert await db.is_known(other, 1)


async def test_import_failure_does_not_stop_other_groups(db: Database, config: Config):
    class ExplodingClient(FakeClient):
        async def iter_messages(self, entity, limit=None, **kwargs):
            if getattr(entity, "id", None) == 1:
                raise RuntimeError("network is down")
            async for message in super().iter_messages(entity, limit=limit, **kwargs):
                yield message

    importer = HistoryImporter(ExplodingClient([make_message(1)]), db, config)
    stats = await importer.run({1: FakeEntity(1, "Broken"), 2: FakeEntity(2, "Healthy")})
    assert stats[1].failed
    assert stats[2].queued == 1


class SimulatedCrash(BaseException):
    """BaseException so no ``except Exception`` cleanup path can rescue the run."""


class CrashAfterBatchClient(FakeClient):
    """Yields messages, then dies hard — the process-death case."""

    async def iter_messages(self, entity, limit=None, **kwargs):
        min_id = kwargs.get("min_id") or 0
        for message in self.messages:
            if message.id > min_id:
                yield message
        raise SimulatedCrash("hard crash")


async def test_carried_album_member_survives_a_crash(db: Database, config: Config):
    """Regression (audit 1.2.0, issue 1): the checkpoint must not pass a carried member."""

    config.history_batch_size = 2
    messages = [make_message(1), make_message(2, grouped_id=77)]

    crashing = HistoryImporter(CrashAfterBatchClient(messages), db, config)
    try:
        await crashing.run({CHAT: FakeEntity(CHAT)})
    except SimulatedCrash:
        pass

    progress = await db.get_import_progress(CHAT)
    assert progress["last_scanned_message_id"] == 1  # not 2
    assert not await db.is_known(CHAT, 2)

    # Restart: the carried member is re-read because min_id is exclusive.
    resumed = HistoryImporter(FakeClient(messages), db, config)
    await resumed.run({CHAT: FakeEntity(CHAT)})
    assert await db.is_known(CHAT, 2)


async def test_complete_batch_commits_the_whole_album(db: Database, config: Config):
    """A clean run still delivers the album as one unit."""

    config.history_batch_size = 2
    messages = [make_message(1), make_message(2, grouped_id=77), make_message(3, grouped_id=77)]
    importer = HistoryImporter(FakeClient(messages), db, config)
    await importer.run({CHAT: FakeEntity(CHAT)})

    progress = await db.get_import_progress(CHAT)
    assert progress["last_scanned_message_id"] == 3
    items = []
    while (item := await db.claim_next("w", 60)) is not None:
        items.append(item)
    assert [i.message_ids for i in items] == [[1], [2, 3]]
