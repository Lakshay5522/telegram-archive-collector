"""Live monitor: canonical ids, live stats, edit/delete annotations."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import collector.ids as ids_module
from conftest import FakeClient, FakeEntity, make_message

from collector.config import Config
from collector.db import Database
from collector.forwarder import RateLimiter
from collector.monitor import LiveMonitor

MARKED = -1001234567890


@pytest.fixture
def marked(monkeypatch):
    def fake_peer_id(entity):
        if isinstance(entity, FakeEntity) and getattr(entity, "marked", None):
            return entity.marked
        return getattr(entity, "id")

    monkeypatch.setattr(ids_module, "_telethon_peer_id", fake_peer_id)


def build(db: Database, config: Config, client: FakeClient, destination=None):
    notify = asyncio.Event()
    sources = {MARKED: FakeEntity(1234567890, "Source", "source_one", marked=MARKED)}
    monitor = LiveMonitor(
        client,
        db,
        config,
        sources,
        notify,
        rate_limiter=RateLimiter(0.0),
        destination=destination,
    )
    return monitor, notify


async def test_new_message_is_queued_with_live_priority(marked, db: Database, config: Config):
    client = FakeClient()
    monitor, notify = build(db, config, client)

    event = SimpleNamespace(chat_id=MARKED, message=make_message(1, text="hi"))
    await monitor._on_new_message(event)

    assert notify.is_set()
    assert await db.queue_size() == 1
    item = await db.claim_next("w", 60)
    assert item is not None and item.priority == 10


async def test_event_for_unknown_chat_is_ignored(marked, db: Database, config: Config):
    client = FakeClient()
    monitor, _ = build(db, config, client)

    event = SimpleNamespace(chat_id=-1009999999999, message=make_message(2))
    await monitor._on_new_message(event)
    assert await db.queue_size() == 0


async def test_raw_entity_id_would_not_match_but_canonical_does(marked, db: Database, config: Config):
    """Regression for audit item 1."""

    client = FakeClient()
    monitor, _ = build(db, config, client)

    # A raw id in the event (the old bug's shape) does not match the source set.
    await monitor._on_new_message(
        SimpleNamespace(chat_id=1234567890, message=make_message(3))
    )
    assert await db.queue_size() == 0

    # The canonical marked id does.
    await monitor._on_new_message(
        SimpleNamespace(chat_id=MARKED, message=make_message(4))
    )
    assert await db.queue_size() == 1


async def test_album_is_queued_as_one_unit(marked, db: Database, config: Config):
    client = FakeClient()
    monitor, _ = build(db, config, client)

    event = SimpleNamespace(
        chat_id=MARKED,
        messages=[make_message(10, grouped_id=5), make_message(11, grouped_id=5)],
    )
    await monitor._on_album(event)
    assert await db.queue_size() == 1
    item = await db.claim_next("w", 60)
    assert item is not None and item.message_ids == [10, 11]


async def test_grouped_message_from_new_message_handler_is_skipped(marked, db: Database, config: Config):
    client = FakeClient()
    monitor, _ = build(db, config, client)

    await monitor._on_new_message(
        SimpleNamespace(chat_id=MARKED, message=make_message(12, grouped_id=9))
    )
    assert await db.queue_size() == 0  # the Album handler owns it


async def test_live_event_timestamp_is_recorded(marked, db: Database, config: Config):
    await db.sync_source_groups([SimpleNamespace(identifier="@source_one", alias=None, enabled=True)])
    await db.set_source_resolved("@source_one", MARKED, "Source")

    client = FakeClient()
    monitor, _ = build(db, config, client)
    await monitor._on_new_message(SimpleNamespace(chat_id=MARKED, message=make_message(13)))

    stats = await db.stats_by_source()
    row = next(entry for entry in stats if entry["chat_id"] == MARKED)
    assert row["last_live_event_at"] is not None


async def test_edits_are_logged_but_not_reforwarded(marked, db: Database, config: Config):
    client = FakeClient()
    monitor, _ = build(db, config, client)

    await monitor._on_edited(
        SimpleNamespace(chat_id=MARKED, message=make_message(14, text="edited text"))
    )
    events = [e["event"] for e in await db.recent_events(limit=5)]
    assert "message_edited" in events
    assert await db.queue_size() == 0
    assert client.sent == []  # INCLUDE_EDITS is off


async def test_edits_are_annotated_when_enabled(marked, db: Database, config: Config):
    config.include_edits = True
    client = FakeClient()
    destination = FakeEntity(999, "Destination")
    monitor, _ = build(db, config, client, destination=destination)

    await monitor._on_edited(
        SimpleNamespace(chat_id=MARKED, message=make_message(15, text="edited text"))
    )
    assert client.sent and "edited" in client.sent[0][1]
    assert await db.queue_size() == 0  # still no re-forward


async def test_deletions_are_annotated_when_enabled(marked, db: Database, config: Config):
    config.include_deleted = True
    client = FakeClient()
    destination = FakeEntity(999, "Destination")
    monitor, _ = build(db, config, client, destination=destination)

    await monitor._on_deleted(SimpleNamespace(chat_id=MARKED, deleted_ids=[16, 17]))
    assert client.sent and "deleted" in client.sent[0][1]


async def test_handlers_register_and_unregister(marked, db: Database, config: Config):
    client = FakeClient()
    monitor, _ = build(db, config, client)
    monitor.register()
    assert len(client.handlers) == 2  # NewMessage + Album
    monitor.unregister()
    assert client.handlers == []


async def test_handler_errors_do_not_escape(marked, db: Database, config: Config):
    class ExplodingDb:
        async def discover_unit(self, **kwargs):
            raise RuntimeError("db is gone")

    monitor = LiveMonitor(
        FakeClient(), ExplodingDb(), config, {MARKED: FakeEntity(1, marked=MARKED)}, asyncio.Event()
    )
    # Must not raise: a handler exception would otherwise kill the event loop task.
    await monitor._on_new_message(
        SimpleNamespace(chat_id=MARKED, message=make_message(18))
    )
