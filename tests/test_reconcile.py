from __future__ import annotations

from types import SimpleNamespace

from conftest import MARKED_CHANNEL, FakeClient, FakeEntity

from collector.config import Config
from collector.db import Database
from collector.reconcile import _origin_pair, reconcile, scan_destination

DEST = -100999
SOURCE_CHANNEL_RAW = 1234567890  # -> marked id -1001234567890


def forwarded_message(message_id: int, *, source_channel: int, source_msg: int) -> SimpleNamespace:
    return SimpleNamespace(
        id=message_id,
        fwd_from=SimpleNamespace(
            channel_post=source_msg,
            saved_from_peer=SimpleNamespace(channel_id=source_channel, chat_id=None, user_id=None),
            from_id=None,
            saved_from_msg_id=None,
        ),
    )


def forwarded_from_basic_group(message_id: int, *, source_chat: int, source_msg: int) -> SimpleNamespace:
    return SimpleNamespace(
        id=message_id,
        fwd_from=SimpleNamespace(
            channel_post=None,
            saved_from_peer=SimpleNamespace(channel_id=None, chat_id=source_chat, user_id=None),
            from_id=None,
            saved_from_msg_id=source_msg,
        ),
    )


class ReconcileClient(FakeClient):
    def __init__(self, destination_messages):
        super().__init__([])
        self.destination_messages = destination_messages

    async def iter_messages(self, entity, limit=None, **kwargs):
        for message in self.destination_messages:
            yield message


def test_origin_pair_uses_marked_channel_ids():
    message = forwarded_message(1, source_channel=SOURCE_CHANNEL_RAW, source_msg=42)
    assert _origin_pair(message) == (MARKED_CHANNEL, 42)


def test_origin_pair_handles_basic_groups():
    message = forwarded_from_basic_group(1, source_chat=555, source_msg=7)
    assert _origin_pair(message) == (-555, 7)


def test_origin_pair_ignores_non_forwarded():
    assert _origin_pair(SimpleNamespace(id=1, fwd_from=None)) is None


async def test_scan_destination_maps_origins():
    client = ReconcileClient([forwarded_message(500, source_channel=SOURCE_CHANNEL_RAW, source_msg=42)])
    mapping = await scan_destination(client, FakeEntity(DEST), limit=10)
    assert mapping[(MARKED_CHANNEL, 42)] == 500


async def test_reconcile_resolves_matching_ambiguous_unit(db: Database, config: Config):
    await db.discover_unit(
        chat_id=MARKED_CHANNEL, messages=[{"message_id": 42, "date": None, "grouped_id": None}]
    )
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_sending(item)
    await db.mark_ambiguous(item, error="process died mid-forward")

    client = ReconcileClient(
        [forwarded_message(500, source_channel=SOURCE_CHANNEL_RAW, source_msg=42)]
    )
    summary = await reconcile(client, db, config, FakeEntity(DEST))

    assert summary["matched"] == 1
    assert await db.get_status(MARKED_CHANNEL, 42) == "forwarded"


async def test_reconcile_leaves_unknown_units_alone_and_records_it(db: Database, config: Config):
    await db.discover_unit(
        chat_id=MARKED_CHANNEL, messages=[{"message_id": 43, "date": None, "grouped_id": None}]
    )
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_ambiguous(item, error="unknown")

    summary = await reconcile(ReconcileClient([]), db, config, FakeEntity(DEST))
    assert summary["unresolved"] == 1
    assert await db.get_status(MARKED_CHANNEL, 43) == "ambiguous"

    cursor = await db.conn.execute(
        "SELECT reconcile_result FROM processed_messages WHERE source_chat_id = ?",
        (MARKED_CHANNEL,),
    )
    row = await cursor.fetchone()
    await cursor.close()
    assert row["reconcile_result"] == "unresolved"


async def test_reconcile_can_requeue_unresolved(db: Database, config: Config):
    await db.discover_unit(
        chat_id=MARKED_CHANNEL, messages=[{"message_id": 44, "date": None, "grouped_id": None}]
    )
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_ambiguous(item, error="unknown")

    await reconcile(ReconcileClient([]), db, config, FakeEntity(DEST), retry_unresolved=True)
    assert await db.get_status(MARKED_CHANNEL, 44) == "queued"
    assert await db.claim_next("w1", 60) is not None


async def test_reconcile_dry_run_changes_nothing(db: Database, config: Config):
    await db.discover_unit(
        chat_id=MARKED_CHANNEL, messages=[{"message_id": 45, "date": None, "grouped_id": None}]
    )
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_ambiguous(item, error="unknown")

    client = ReconcileClient(
        [forwarded_message(600, source_channel=SOURCE_CHANNEL_RAW, source_msg=45)]
    )
    summary = await reconcile(client, db, config, FakeEntity(DEST), dry_run=True)
    assert summary["matched"] == 1
    assert await db.get_status(MARKED_CHANNEL, 45) == "ambiguous"


async def test_reconcile_releases_stale_leases_first(db: Database, config: Config):
    import time

    await db.discover_unit(
        chat_id=MARKED_CHANNEL, messages=[{"message_id": 46, "date": None, "grouped_id": None}]
    )
    item = await db.claim_next("w0", lease_seconds=0.01)
    assert item is not None
    await db.mark_sending(item)
    time.sleep(0.05)

    summary = await reconcile(ReconcileClient([]), db, config, FakeEntity(DEST))
    assert summary["released_ambiguous"] == 1
    assert summary["ambiguous"] == 1


# ------------------------------------------- canonical destination id (1.2.0)
class ChannelEntity:
    """Telethon channel: raw ``id`` differs from the marked peer id."""

    def __init__(self, raw_id: int, marked_id: int) -> None:
        self.id = raw_id
        self.marked = marked_id


async def test_reconcile_stores_the_canonical_destination_id(monkeypatch, db: Database, config: Config):
    import collector.ids as ids_module

    monkeypatch.setattr(
        ids_module, "_telethon_peer_id", lambda entity: getattr(entity, "marked", getattr(entity, "id"))
    )
    destination = ChannelEntity(999, -1000000000999)

    await db.discover_unit(
        chat_id=MARKED_CHANNEL, messages=[{"message_id": 50, "date": None, "grouped_id": None}]
    )
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_sending(item)
    await db.mark_ambiguous(item, error="interrupted")

    client = ReconcileClient(
        [forwarded_message(800, source_channel=SOURCE_CHANNEL_RAW, source_msg=50)]
    )
    summary = await reconcile(client, db, config, destination)
    assert summary["matched"] == 1

    cursor = await db.conn.execute(
        "SELECT destination_chat_id FROM processed_messages WHERE source_chat_id = ?",
        (MARKED_CHANNEL,),
    )
    row = await cursor.fetchone()
    await cursor.close()
    assert row["destination_chat_id"] == -1000000000999  # marked, not 999


# --------------------------------------------------- scan window (1.2.0)
class WindowClient(ReconcileClient):
    def __init__(self, messages):
        super().__init__(messages)
        self.limits: list[int | None] = []

    async def iter_messages(self, entity, limit=None, **kwargs):
        self.limits.append(limit)
        for message in self.destination_messages:
            yield message


async def test_reconcile_reports_its_scan_window(db: Database, config: Config):
    client = WindowClient([])
    summary = await reconcile(client, db, config, FakeEntity(DEST), scan_limit=250)
    assert summary["window"] == 250


async def test_reconcile_limit_zero_scans_the_entire_history(db: Database, config: Config):
    await db.discover_unit(
        chat_id=MARKED_CHANNEL, messages=[{"message_id": 53, "date": None, "grouped_id": None}]
    )
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_ambiguous(item, error="unknown")

    client = WindowClient([])
    summary = await reconcile(client, db, config, FakeEntity(DEST), scan_limit=0)
    assert summary["window"] == "all"
    assert client.limits == [None]  # iter_messages(limit=None) => no cap


async def test_forced_retry_is_counted_and_warned(db: Database, config: Config):
    await db.discover_unit(
        chat_id=MARKED_CHANNEL, messages=[{"message_id": 51, "date": None, "grouped_id": None}]
    )
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_ambiguous(item, error="unknown")

    summary = await reconcile(
        ReconcileClient([]), db, config, FakeEntity(DEST), retry_unresolved=True
    )
    assert summary["retried_unresolved"] == 1
    assert await db.get_status(MARKED_CHANNEL, 51) == "queued"


async def test_dry_run_does_not_count_forced_retries(db: Database, config: Config):
    await db.discover_unit(
        chat_id=MARKED_CHANNEL, messages=[{"message_id": 52, "date": None, "grouped_id": None}]
    )
    item = await db.claim_next("w0", 60)
    assert item is not None
    await db.mark_ambiguous(item, error="unknown")

    await reconcile(
        ReconcileClient([]), db, config, FakeEntity(DEST), retry_unresolved=True, dry_run=True
    )
    assert await db.get_status(MARKED_CHANNEL, 52) == "ambiguous"
