"""Built-in self-test: verifies the core invariants without pytest.

The reviewer's environment could not install test dependencies, so the unit suite
was unverifiable there.  ``python -m collector selftest`` closes that gap: it
needs only the runtime dependencies (aiosqlite, and nothing else) and exercises
the guarantees that matter, on a throwaway database in a temp directory:

* schema migration
* deduplication
* atomic leasing
* ``leased`` (never sent) vs ``sending`` (possibly delivered) crash recovery
* delivery bookkeeping, including per-album-member destination ids
* dead-letter retry
* ambiguous-delivery reconciliation against destination history
* the import checkpoint never passing an uncommitted album member
* read-only database access
* canonical chat-id construction
* configuration validation refusing an unsafe setup
* SQLite integrity

It is not a replacement for the real test suite or for integration testing
against a real Telegram account; it is a portable proof that the persistence and
recovery logic works on *this* machine.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Awaitable, Callable

DEST = -1009999999999


def _chat(offset: int) -> int:
    """A distinct canonical chat id per check, so checks never share a queue."""

    return -1001234567000 - offset


@dataclass(slots=True)
class CheckResult:
    name: str
    ok: bool
    detail: str = ""


class SimulatedCrash(BaseException):
    """BaseException on purpose: no ``except Exception`` cleanup path may run."""


def _message(message_id: int, grouped_id: int | None = None) -> Any:
    return SimpleNamespace(
        id=message_id,
        grouped_id=grouped_id,
        date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        message="",
        action=None,
        out=False,
        media=None,
    )


def _unit(*ids: int, grouped_id: int | None = None) -> list[dict[str, Any]]:
    return [{"message_id": i, "date": None, "grouped_id": grouped_id} for i in ids]


class _CrashingClient:
    def __init__(self, messages: list[Any]) -> None:
        self.messages = messages

    async def iter_messages(self, entity: Any, limit: int | None = None, **kwargs: Any):
        min_id = kwargs.get("min_id") or 0
        for message in self.messages:
            if message.id > min_id:
                yield message
        raise SimulatedCrash("hard crash: no cleanup path runs")


class _PlainClient:
    def __init__(self, messages: list[Any]) -> None:
        self.messages = messages

    async def iter_messages(self, entity: Any, limit: int | None = None, **kwargs: Any):
        min_id = kwargs.get("min_id") or 0
        for message in self.messages:
            if message.id > min_id:
                yield message


class _Forwarded:
    """A destination message that is a native forward of ``(chat, msg_id)``."""

    def __init__(self, message_id: int, channel_id: int, source_msg: int) -> None:
        self.id = message_id
        self.fwd_from = SimpleNamespace(
            channel_post=source_msg,
            saved_from_peer=SimpleNamespace(channel_id=channel_id, chat_id=None, user_id=None),
            from_id=None,
            saved_from_msg_id=None,
        )


class _DestinationClient:
    def __init__(self, messages: list[Any]) -> None:
        self.messages = messages

    async def iter_messages(self, entity: Any, limit: int | None = None, **kwargs: Any):
        for message in self.messages:
            yield message


# --------------------------------------------------------------------------
# checks
# --------------------------------------------------------------------------
async def _check_schema(db, config) -> str:
    cursor = await db.conn.execute("SELECT version FROM schema_version")
    row = await cursor.fetchone()
    await cursor.close()
    if row is None:
        raise AssertionError("no schema_version row")
    return f"schema version {row['version']}"


async def _check_dedup(db, config) -> str:
    chat = _chat(1)
    first = await db.discover_unit(chat_id=chat, messages=_unit(1))
    second = await db.discover_unit(chat_id=chat, messages=_unit(1))
    if (first, second) != (1, 0):
        raise AssertionError(f"expected (1, 0), got ({first}, {second})")
    return "second discovery is a no-op"


async def _check_lease(db, config) -> str:
    await db.discover_unit(chat_id=_chat(2), messages=_unit(2))
    first = await db.claim_next("selftest-a", 60)
    second = await db.claim_next("selftest-b", 60)
    if first is None or second is not None:
        raise AssertionError("a queue row was claimable twice")
    return "only one worker can claim a unit"


async def _check_recovery(db, config) -> str:
    chat = _chat(3)
    # never sent -> safe to retry
    await db.discover_unit(chat_id=chat, messages=_unit(3))
    item = await db.claim_next("selftest", 0.01)
    if item is None:
        raise AssertionError("could not claim the unit")
    await asyncio.sleep(0.05)
    released = await db.release_stale_leases()
    if released.requeued != 1:
        raise AssertionError(f"unsent lease was not re-queued (requeued={released.requeued})")

    # Drain the re-queued unit so the next claim cannot pick it up instead.
    requeued = await db.claim_next("selftest", 60)
    if requeued is None or requeued.source_message_id != 3:
        raise AssertionError("re-queued unit was not claimable")
    await db.mark_sending(requeued)
    await db.complete(requeued, destination_chat_id=DEST, destination_message_id=100)

    # possibly delivered -> ambiguous and never reclaimed
    await db.discover_unit(chat_id=chat, messages=_unit(4))
    item = await db.claim_next("selftest", 0.01)
    if item is None:
        raise AssertionError("could not claim the second unit")
    await db.mark_sending(item)
    await asyncio.sleep(0.05)
    released = await db.release_stale_leases()
    if released.ambiguous != 1:
        raise AssertionError("in-flight lease was not marked ambiguous")
    if await db.get_status(chat, 4) != "ambiguous":
        raise AssertionError("in-flight unit is not ambiguous")
    if await db.claim_next("selftest", 60) is not None:
        raise AssertionError("an ambiguous unit was claimable: duplicate forward risk")
    return "unsent work re-queued, in-flight work parked as ambiguous"


async def _check_delivery_bookkeeping(db, config) -> str:
    chat = _chat(4)
    await db.discover_unit(chat_id=chat, messages=_unit(5, 6, grouped_id=99))
    item = await db.claim_next("selftest", 60)
    if item is None:
        raise AssertionError("could not claim the album unit")
    await db.mark_sending(item)
    await db.complete(
        item,
        destination_chat_id=DEST,
        destination_message_id=500,
        member_destination_ids={5: 500, 6: 501},
    )
    if await db.get_status(chat, 5) != "forwarded" or await db.get_status(chat, 6) != "forwarded":
        raise AssertionError("album members were not all marked forwarded")
    cursor = await db.conn.execute(
        "SELECT destination_message_id FROM processed_messages "
        "WHERE source_chat_id = ? AND source_message_id = 6",
        (chat,),
    )
    row = await cursor.fetchone()
    await cursor.close()
    if row["destination_message_id"] != 501:
        raise AssertionError("per-member destination id was not recorded")
    return "album members recorded with their own destination ids"


async def _check_retry_failed(db, config) -> str:
    chat = _chat(5)
    await db.discover_unit(chat_id=chat, messages=_unit(7))
    item = await db.claim_next("selftest", 60)
    if item is None:
        raise AssertionError("could not claim the unit")
    await db.fail(item, error_class="permanent", error="selftest", retry_at=None)
    if await db.get_status(chat, 7) != "permanently_failed":
        raise AssertionError("permanent failure was not dead-lettered")
    if await db.retry_failed() != 1:
        raise AssertionError("retry_failed did not re-queue the dead letter")
    if await db.claim_next("selftest", 60) is None:
        raise AssertionError("re-queued unit is not claimable")
    return "dead letters are re-queueable on demand"


async def _check_reconcile(db, config) -> str:
    from collector.reconcile import reconcile

    chat = _chat(6)
    await db.discover_unit(chat_id=chat, messages=_unit(8))
    item = await db.claim_next("selftest", 60)
    if item is None:
        raise AssertionError("could not claim the unit")
    await db.mark_sending(item)
    await db.mark_ambiguous(item, error="selftest interruption")

    channel_id = int(str(chat).replace("-100", "", 1))
    client = _DestinationClient([_Forwarded(700, channel_id, 8)])
    summary = await reconcile(client, db, config, SimpleNamespace(id=999))
    if summary["matched"] != 1:
        raise AssertionError(f"reconcile did not match the forward: {summary}")
    if await db.get_status(chat, 8) != "forwarded":
        raise AssertionError("matched ambiguous unit was not resolved")
    return "ambiguous delivery matched against destination history"


async def _check_import_checkpoint(db, config) -> str:
    """A carried album member must survive a hard crash (regression, 1.2.0)."""

    from collector.importer import HistoryImporter

    chat = -1007777777777
    config.history_batch_size = 2
    crashing = HistoryImporter(_CrashingClient([_message(1), _message(2, 77)]), db, config)
    try:
        await crashing.run({chat: SimpleNamespace(id=1, title="selftest")})
    except SimulatedCrash:
        pass

    progress = await db.get_import_progress(chat)
    checkpoint = progress["last_scanned_message_id"] or 0
    if checkpoint >= 2:
        raise AssertionError(
            f"checkpoint advanced to {checkpoint} past an uncommitted album member"
        )

    resumed = HistoryImporter(_PlainClient([_message(1), _message(2, 77)]), db, config)
    await resumed.run({chat: SimpleNamespace(id=1, title="selftest")})
    if not await db.is_known(chat, 2):
        raise AssertionError("the carried album member was lost across the restart")
    return "carried album member recovered after a crash"


async def _check_read_only(db, config) -> str:
    from collector.db import Database

    async with Database(config.database_path, read_only=True) as reader:
        await reader.queue_size()
        try:
            await reader.conn.execute(
                "INSERT INTO application_events (created_at, level, event) "
                "VALUES ('x', 'INFO', 'selftest')"
            )
        except Exception:
            return "read-only connection refuses writes"
        raise AssertionError("read-only connection accepted a write")


async def _check_ids(db, config) -> str:
    from collector.ids import canonical_id, peer_id_from_parts

    if peer_id_from_parts(channel_id=1234567890) != -1001234567890:
        raise AssertionError("channel ids are not marked consistently")
    if peer_id_from_parts(chat_id=555) != -555:
        raise AssertionError("basic group ids are not marked consistently")
    if canonical_id(SimpleNamespace(id=42)) != 42:
        raise AssertionError("canonical_id fallback is broken")
    return "channel/basic-group/user ids are canonical"


async def _check_config(db, config) -> str:
    from collector.config import ConfigError, load_config

    env = {
        "API_ID": "1",
        "API_HASH": "a" * 32,
        "DESTINATION_CHAT": "@dest",
        "SOURCE_GROUPS": "@dest",
        "DATA_DIR": config.data_dir.as_posix(),
    }
    try:
        load_config(env, dotenv_path=None)
    except ConfigError as exc:
        if "also configured as a source" in str(exc):
            return "unsafe configuration is rejected at load time"
        raise AssertionError(f"unexpected configuration error: {exc}") from exc
    raise AssertionError("an unsafe configuration was accepted")


async def _check_chaos(db, config) -> str:
    """Crash/restart cycles must not lose or duplicate a single message."""

    from .chaos import run_chaos

    report = await run_chaos(messages=12, rounds=5, seed=7, crash_probability=0.35)
    if not report.ok:
        raise AssertionError(report.summary() + " | " + "; ".join(report.notes[:4]))
    return report.summary()


async def _check_integrity(db, config) -> str:
    result = await db.integrity_check()
    if result != "ok":
        raise AssertionError(f"integrity_check returned {result!r}")
    return "PRAGMA integrity_check ok"


CHECKS: list[tuple[str, Callable[[Any, Any], Awaitable[str]]]] = [
    ("schema migration", _check_schema),
    ("deduplication", _check_dedup),
    ("atomic leasing", _check_lease),
    ("crash recovery (leased vs sending)", _check_recovery),
    ("delivery bookkeeping (albums)", _check_delivery_bookkeeping),
    ("dead-letter retry", _check_retry_failed),
    ("ambiguous reconciliation", _check_reconcile),
    ("import checkpoint safety", _check_import_checkpoint),
    ("read-only database access", _check_read_only),
    ("canonical chat ids", _check_ids),
    ("configuration validation", _check_config),
    ("sqlite integrity", _check_integrity),
    ("crash consistency (chaos run)", _check_chaos),
]


async def run_checks(*, verbose: bool = False) -> list[CheckResult]:
    import logging

    from .config import Config
    from .db import Database

    # The checks deliberately provoke failures, so silence the collector's own
    # logging: the report is the output, not the trace of the chaos run.
    collector_logger = logging.getLogger("collector")
    previous_level = collector_logger.level
    if not verbose:
        collector_logger.setLevel(logging.CRITICAL)

    results: list[CheckResult] = []
    with tempfile.TemporaryDirectory(prefix="collector-selftest-") as tmp:
        for index, (name, check) in enumerate(CHECKS):
            # A fresh database per check: the queue is global, so a shared one would
            # let an earlier check's pending work be claimed by a later check.
            config = Config(
                api_id=1,
                api_hash="a" * 32,
                destination_chat="@selftest",
                sources=[],
                data_dir=Path(tmp) / f"check-{index:02d}",
                min_forward_interval=0.0,
            )
            config.ensure_directories()
            try:
                async with Database(config.database_path) as db:
                    detail = await check(db, config)
                results.append(CheckResult(name, True, detail))
            except Exception as exc:  # a failing check must not stop the rest
                results.append(CheckResult(name, False, f"{type(exc).__name__}: {exc}"))
                if verbose:
                    import traceback

                    traceback.print_exc()
    collector_logger.setLevel(previous_level)
    return results


def main(*, verbose: bool = False) -> int:
    results = asyncio.run(run_checks(verbose=verbose))
    passed = sum(1 for result in results if result.ok)
    total = len(results)

    from .db import DRIVER

    print("Telegram Archive Collector — self-test")
    print(f"python {sys.version.split()[0]} · driver {DRIVER} · pid {os.getpid()} · temp database")
    print("-" * 62)
    for result in results:
        status = "PASS" if result.ok else "FAIL"
        print(f"[{status}] {result.name}")
        if result.detail:
            print(f"       {result.detail}")
    print("-" * 62)
    print(f"{passed}/{total} checks passed")
    if passed != total:
        print(
            "Something in the persistence/recovery layer is broken on this host. "
            "Run `pytest -q` for detail.",
            file=sys.stderr,
        )
        return 1
    print(
        "This proves the database, queue, recovery and reconciliation logic works here. "
        "It does not replace integration testing against a real Telegram account "
        "(see docs/INTEGRATION_TESTS.md)."
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
