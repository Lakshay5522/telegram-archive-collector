"""Service wiring: startup ordering, cleanup on failure, graceful shutdown."""

from __future__ import annotations

import asyncio

import pytest

import collector.service as service_module
from conftest import MARKED_CHANNEL, FakeClient, FakeEntity

from collector.config import Config
from collector.db import Database
from collector.service import CollectorService
from collector.telegram_client import SourceUnavailable

DEST = -100999


def wire(monkeypatch, config: Config, client: FakeClient, *, destination_error: Exception | None = None):
    monkeypatch.setattr(service_module, "build_client", lambda cfg: client)

    async def resolve_destination(c, identifier):
        if destination_error is not None:
            raise destination_error
        return FakeEntity(999, "Destination")

    async def resolve_sources(c, cfg):
        return {MARKED_CHANNEL: FakeEntity(1234567890, "Source", "source_one")}

    monkeypatch.setattr(service_module, "resolve_destination", resolve_destination)
    monkeypatch.setattr(service_module, "resolve_sources", resolve_sources)


async def test_startup_failure_releases_everything(monkeypatch, config: Config):
    """Audit item 20: a partial startup must not leak client/tasks/connection."""

    client = FakeClient()
    wire(monkeypatch, config, client, destination_error=SourceUnavailable("destination gone"))

    service = CollectorService(config)
    with pytest.raises(SourceUnavailable):
        await service.start(import_history=False)

    assert client.disconnected is True
    assert service.db._conn is None
    assert service._tasks == []
    assert service.connection_state == "disconnected"


async def test_full_startup_forwarding_and_shutdown(monkeypatch, config: Config):
    client = FakeClient()
    wire(monkeypatch, config, client)

    service = CollectorService(config)
    await service.start(import_history=False)

    assert service.connection_state == "connected"
    assert service.destination_chat_id == 999
    assert MARKED_CHANNEL in service.sources
    assert client.handlers  # live monitor registered before any import
    assert await service.db.last_heartbeat() is not None

    # A live message flows through the real queue and forwarder path.
    await service.db.discover_unit(
        chat_id=MARKED_CHANNEL,
        messages=[{"message_id": 1, "date": None, "grouped_id": None}],
        priority=10,
    )
    service.notify.set()

    for _ in range(100):
        if await service.db.get_status(MARKED_CHANNEL, 1) == "forwarded":
            break
        await asyncio.sleep(0.02)
    assert await service.db.get_status(MARKED_CHANNEL, 1) == "forwarded"
    assert client.forwarded

    await service.stop()
    assert client.disconnected is True
    assert service._tasks == []
    assert service.db._conn is None


async def test_startup_recovers_stale_leases(monkeypatch, config: Config, tmp_path):
    """A crashed previous run leaves work that startup must recover."""

    path = config.database_path
    async with Database(path) as db:
        await db.discover_unit(
            chat_id=MARKED_CHANNEL, messages=[{"message_id": 2, "date": None, "grouped_id": None}]
        )
        item = await db.claim_next("dead-worker", lease_seconds=0.01)
        assert item is not None
        await db.mark_sending(item)
    await asyncio.sleep(0.05)

    client = FakeClient()
    wire(monkeypatch, config, client)
    service = CollectorService(config)
    await service.start(import_history=False)

    # In-flight work from the dead process is parked, never re-forwarded blindly.
    assert await service.db.get_status(MARKED_CHANNEL, 2) == "ambiguous"
    await service.stop()


async def test_shutdown_requeues_unsent_work(monkeypatch, config: Config):
    client = FakeClient()
    wire(monkeypatch, config, client)
    service = CollectorService(config)
    await service.start(import_history=False)

    # Simulate a worker that claimed a row and then died with the process.
    await service.db.discover_unit(
        chat_id=MARKED_CHANNEL, messages=[{"message_id": 3, "date": None, "grouped_id": None}]
    )
    item = await service.db.claim_next("worker-0", lease_seconds=3600)
    assert item is not None

    await service.stop()

    async with Database(config.database_path, read_only=True) as db:
        assert await db.get_status(MARKED_CHANNEL, 3) in ("queued", "discovered")
        assert await db.queue_size() == 1


async def test_service_logs_start_and_stop_events(monkeypatch, config: Config):
    client = FakeClient()
    wire(monkeypatch, config, client)
    service = CollectorService(config)
    await service.start(import_history=False)
    await service.stop()

    async with Database(config.database_path, read_only=True) as db:
        events = [e["event"] for e in await db.recent_events(limit=10)]
    assert "service_started" in events
    assert "service_stopped" in events


async def test_startup_sweeps_unexpired_leases_too(monkeypatch, config: Config):
    """A fresh process holds no leases, so even a long lease must be recovered."""

    async with Database(config.database_path) as db:
        await db.discover_unit(
            chat_id=MARKED_CHANNEL, messages=[{"message_id": 9, "date": None, "grouped_id": None}]
        )
        item = await db.claim_next("dead-worker", lease_seconds=3600)  # far from expiry
        assert item is not None
        await db.mark_sending(item)

    client = FakeClient()
    wire(monkeypatch, config, client)
    service = CollectorService(config)
    await service.start(import_history=False)

    assert await service.db.get_status(MARKED_CHANNEL, 9) == "ambiguous"
    assert await service.db.claim_next("w", 60) is None
    await service.stop()
