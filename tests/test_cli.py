"""CLI behaviour: which commands need credentials, and what they return."""

from __future__ import annotations

import json

import pytest

from collector.__main__ import main
from collector.db import Database

CHAT = -1001234567890

VALID = {
    "API_ID": "12345",
    "API_HASH": "b" * 32,
    "DESTINATION_CHAT": "@archive",
    "SOURCE_GROUPS": "@alpha",
}


def env(tmp_path, **overrides):
    values = dict(VALID, DATA_DIR=str(tmp_path))
    values.update(overrides)
    return values


def apply(monkeypatch, values):
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    for key in ("API_ID", "API_HASH", "DESTINATION_CHAT", "SOURCE_GROUPS"):
        if key not in values:
            monkeypatch.delenv(key, raising=False)


async def seed_db(tmp_path):
    async with Database(tmp_path / "collector.sqlite3") as db:
        await db.heartbeat()
        await db.discover_unit(
            chat_id=CHAT, messages=[{"message_id": 1, "date": None, "grouped_id": None}]
        )


def test_validate_config_passes(monkeypatch, tmp_path, capsys):
    apply(monkeypatch, env(tmp_path))
    assert main(["--env-file", "/nonexistent", "validate-config"]) == 0
    assert "configuration is valid" in capsys.readouterr().out


def test_validate_config_reports_errors(monkeypatch, tmp_path, capsys):
    apply(monkeypatch, {"DATA_DIR": str(tmp_path)})
    code = main(["--env-file", "/nonexistent", "validate-config"])
    output = capsys.readouterr().out
    assert code == 1
    assert "[error] API_ID" in output
    assert "will refuse to start" in output


def test_status_works_without_telegram_credentials(monkeypatch, tmp_path, capsys):
    apply(monkeypatch, {"DATA_DIR": str(tmp_path)})
    import asyncio

    asyncio.run(seed_db(tmp_path))

    assert main(["--env-file", "/nonexistent", "status"]) == 0
    output = capsys.readouterr().out
    assert "Telegram Archive Collector — status" in output
    assert "health" in output


def test_status_json_is_machine_readable(monkeypatch, tmp_path, capsys):
    apply(monkeypatch, {"DATA_DIR": str(tmp_path)})
    import asyncio

    asyncio.run(seed_db(tmp_path))

    assert main(["--env-file", "/nonexistent", "status", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["health"]["state"] == "ok"
    assert payload["counts"]["queue_pending"] == 1


def test_status_without_database_fails_cleanly(monkeypatch, tmp_path, capsys):
    apply(monkeypatch, {"DATA_DIR": str(tmp_path / "fresh")})
    assert main(["--env-file", "/nonexistent", "status"]) == 1
    assert "cannot read status" in capsys.readouterr().err


def test_db_maintenance_reports_integrity(monkeypatch, tmp_path, capsys):
    apply(monkeypatch, {"DATA_DIR": str(tmp_path)})
    import asyncio

    asyncio.run(seed_db(tmp_path))

    assert main(["--env-file", "/nonexistent", "db-maintenance", "--prune-events", "30"]) == 0
    output = capsys.readouterr().out
    assert "integrity_check : ok" in output
    assert "events pruned" in output


def test_retry_failed_is_safe_when_nothing_failed(monkeypatch, tmp_path, capsys):
    apply(monkeypatch, {"DATA_DIR": str(tmp_path)})
    assert main(["--env-file", "/nonexistent", "retry-failed"]) == 0
    assert "re-queued 0 unit(s)" in capsys.readouterr().out


@pytest.mark.slow
def test_selftest_runs_without_credentials(monkeypatch, tmp_path, capsys):
    apply(monkeypatch, {"DATA_DIR": str(tmp_path)})
    assert main(["--env-file", "/nonexistent", "selftest"]) == 0
    output = capsys.readouterr().out
    assert "checks passed" in output
    assert "FAIL" not in output


def test_help_lists_every_documented_command(capsys):
    with pytest.raises(SystemExit):
        main(["--help"])
    output = capsys.readouterr().out
    for command in (
        "login",
        "list-groups",
        "validate-config",
        "import-history",
        "run",
        "status",
        "retry-failed",
        "reconcile",
        "db-maintenance",
        "selftest",
        "dashboard",
    ):
        assert command in output
