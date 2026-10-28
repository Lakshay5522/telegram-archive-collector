"""Status report and health snapshot."""

from __future__ import annotations

from conftest import MARKED_CHANNEL

from collector.config import Config
from collector.db import Database
from collector.status import build_report, health_snapshot


async def test_health_is_unknown_without_heartbeat(db: Database, config: Config):
    health = await health_snapshot(db, config)
    assert health["state"] == "unknown"
    assert any("no heartbeat" in problem for problem in health["problems"])


async def test_health_is_ok_when_heartbeat_is_fresh(db: Database, config: Config):
    await db.heartbeat()
    health = await health_snapshot(db, config)
    assert health["state"] == "ok"
    assert health["problems"] == []


async def test_health_degrades_on_ambiguous_deliveries(db: Database, config: Config):
    await db.heartbeat()
    await db.discover_unit(
        chat_id=MARKED_CHANNEL, messages=[{"message_id": 1, "date": None, "grouped_id": None}]
    )
    item = await db.claim_next("w", 60)
    assert item is not None
    await db.mark_ambiguous(item, error="uncertain")

    health = await health_snapshot(db, config)
    assert health["state"] == "degraded"
    assert any("ambiguous" in problem for problem in health["problems"])


async def test_health_degrades_on_incomplete_imports(db: Database, config: Config):
    await db.heartbeat()
    await db.start_import(MARKED_CHANNEL)
    await db.update_import_progress(MARKED_CHANNEL, status="retry_pending", error="boom")

    health = await health_snapshot(db, config)
    assert health["state"] == "degraded"
    assert health["incomplete_imports"] == 1


async def test_report_contains_operational_sections(db: Database, config: Config):
    await db.sync_source_groups([type("S", (), {"identifier": "@x", "alias": None, "enabled": True})()])
    await db.set_source_resolved("@x", MARKED_CHANNEL, "Group one")
    await db.heartbeat()
    await db.discover_unit(
        chat_id=MARKED_CHANNEL, messages=[{"message_id": 5, "date": None, "grouped_id": None}]
    )

    report = await build_report(db, config, connection_state="connected")
    for expected in (
        "telegram connection",
        "health",
        "queue",
        "source groups",
        "Group one",
        "historical import",
        "recent failures",
        "recent events",
        "last successful forwarding",
    ):
        assert expected in report
    # never leak credentials
    assert "api_hash" not in report.lower()
    assert config.api_hash not in report
