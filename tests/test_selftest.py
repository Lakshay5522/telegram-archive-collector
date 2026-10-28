"""The built-in self-test must actually pass, and must actually catch breakage."""

from __future__ import annotations

import pytest

import collector.db as db_module
from collector.selftest import CHECKS, run_checks


pytestmark = pytest.mark.slow


async def test_every_self_check_passes():
    results = await run_checks()
    failures = [result for result in results if not result.ok]
    assert not failures, [f"{r.name}: {r.detail}" for r in failures]
    assert len(results) == len(CHECKS)


async def test_self_test_detects_a_broken_queue(monkeypatch):
    """If leasing breaks, the self-test must fail rather than reassure the user."""

    async def broken_claim(self, worker_id, lease_seconds):
        return None

    monkeypatch.setattr(db_module.Database, "claim_next", broken_claim)
    results = await run_checks()
    failed = {result.name for result in results if not result.ok}
    assert "atomic leasing" in failed
    assert "crash recovery (leased vs sending)" in failed
