"""Crash-consistency harness: it must prove safety, and it must catch breakage."""

from __future__ import annotations

import pytest

import collector.db as db_module
import collector.reconcile as reconcile_module
from collector.chaos import run_chaos
from collector.db import Database, ReleaseResult


pytestmark = pytest.mark.slow


async def test_chaos_run_loses_and_duplicates_nothing():
    report = await run_chaos(messages=15, rounds=6, seed=3, crash_probability=0.4)
    assert report.crashes > 0, "the run did not actually simulate any crashes"
    assert report.ok, report.summary() + " | " + "; ".join(report.notes[:5])
    assert report.forwarded == 15
    assert report.duplicates == 0
    assert report.lost == 0


async def test_chaos_is_deterministic_for_a_seed():
    first = await run_chaos(messages=10, rounds=4, seed=11, crash_probability=0.4)
    second = await run_chaos(messages=10, rounds=4, seed=11, crash_probability=0.4)
    assert first.summary() == second.summary()


async def test_chaos_detects_loss_when_recovery_is_disabled(monkeypatch):
    """If abandoned leases are never recovered, the harness must report loss."""

    async def no_recovery(self, **kwargs):
        return ReleaseResult()

    monkeypatch.setattr(db_module.Database, "release_stale_leases", no_recovery)
    report = await run_chaos(messages=12, rounds=4, seed=5, crash_probability=0.5)
    assert not report.ok
    assert report.lost > 0


async def test_chaos_detects_duplicates_when_reconcile_retries_blindly(monkeypatch):
    """Blind re-queueing of ambiguous units must show up as duplicates."""

    async def blind_reconcile(client, db, config, destination, **kwargs):
        for unit in await db.ambiguous_units():
            await db.requeue_unit(unit["unit_key"])
        return {"matched": 0, "unresolved": 0}

    monkeypatch.setattr(reconcile_module, "reconcile", blind_reconcile)
    report = await run_chaos(messages=12, rounds=4, seed=5, crash_probability=0.5)
    assert not report.ok
    assert report.duplicates > 0


async def test_stdlib_driver_fallback_round_trip(monkeypatch, tmp_path):
    """The collector must work with only the standard library available."""

    monkeypatch.setattr(db_module, "aiosqlite", None)
    database = Database(tmp_path / "fallback.sqlite3")
    await database.connect()
    try:
        assert db_module.DRIVER  # reported in selftest output
        created = await database.discover_unit(
            chat_id=-1001234567890,
            messages=[{"message_id": 1, "date": None, "grouped_id": None}],
        )
        assert created == 1
        item = await database.claim_next("w", 60)
        assert item is not None
        await database.mark_sending(item)
        await database.complete(
            item, destination_chat_id=-100999, destination_message_id=42
        )
        assert await database.get_status(-1001234567890, 1) == "forwarded"
        assert await database.integrity_check() == "ok"
    finally:
        await database.close()


@pytest.mark.parametrize("seed", [0, 1, 2])
async def test_chaos_across_seeds(seed: int):
    report = await run_chaos(messages=8, rounds=3, seed=seed, crash_probability=0.5)
    assert report.ok, report.summary()
