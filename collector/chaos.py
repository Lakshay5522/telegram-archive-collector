"""Crash-consistency harness.

Simulates the failure modes that actually threaten an archive — a process dying
*before* Telegram accepts a forward, and dying *after* it accepted one but before
SQLite recorded it — then restarts, reconciles, and counts what the destination
really received.

The invariant it checks is the one that matters and the one no static review can
settle: **every source message ends up delivered exactly once**. Duplicates and
losses are counted and reported, not assumed away.

Used by ``python -m collector selftest`` and by the test suite. No network, no
Telegram account, no third-party packages required.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

CHAT = -1001234567890
DEST = -1009999999999


class ProcessDeath(BaseException):
    """A hard crash: ``BaseException`` so no ``except Exception`` cleanup runs."""


class ConnectionDropped(Exception):
    """The request may or may not have been applied — a transient error."""


def _forwarded(destination_message_id: int, chat_id: int, source_message_id: int) -> Any:
    channel_id = int(str(chat_id).replace("-100", "", 1))
    return SimpleNamespace(
        id=destination_message_id,
        fwd_from=SimpleNamespace(
            channel_post=source_message_id,
            saved_from_peer=SimpleNamespace(channel_id=channel_id, chat_id=None, user_id=None),
            from_id=None,
            saved_from_msg_id=None,
        ),
    )


class ChaosClient:
    """Fake Telegram that can die at the worst possible moments.

    ``plan`` is a queue of per-call outcomes:

    ``ok``           — normal successful forward
    ``crash_before`` — process dies before the request reaches Telegram
    ``crash_after``  — Telegram accepted it, then the process died
    ``error_after``  — Telegram accepted it, then the connection dropped
    """

    def __init__(self, plan: list[str], *, chat: int = CHAT) -> None:
        self.plan = plan
        self.chat = chat
        self.delivered: dict[tuple[int, int], list[int]] = {}
        self.forward_calls = 0
        self.crashes = 0
        self._next_id = 5000

    async def forward_messages(self, entity: Any, messages: Any, from_peer: Any = None) -> Any:
        ids = list(messages) if isinstance(messages, (list, tuple)) else [messages]
        self.forward_calls += 1
        outcome = self.plan.pop(0) if self.plan else "ok"

        if outcome == "crash_before":
            self.crashes += 1
            raise ProcessDeath("died before Telegram accepted the forward")

        dest_ids = []
        for _ in ids:
            self._next_id += 1
            dest_ids.append(self._next_id)
        self.delivered.setdefault((self.chat, ids[0]), []).extend(dest_ids)

        if outcome == "crash_after":
            self.crashes += 1
            raise ProcessDeath("died after Telegram accepted the forward")
        if outcome == "error_after":
            raise ConnectionDropped("connection dropped after the request")
        result = [SimpleNamespace(id=dest_id) for dest_id in dest_ids]
        return result if len(result) > 1 else result[0]

    async def send_message(self, entity: Any, text: str) -> Any:
        self._next_id += 1
        return SimpleNamespace(id=self._next_id)

    async def iter_messages(self, entity: Any, limit: int | None = None, **kwargs: Any):
        """Destination history, newest first, as native forwards."""

        entries: list[Any] = []
        for (chat_id, source_message_id), dest_ids in self.delivered.items():
            for dest_id in dest_ids:
                entries.append(_forwarded(dest_id, chat_id, source_message_id))
        entries.sort(key=lambda message: message.id, reverse=True)
        if limit is not None:
            entries = entries[:limit]
        for message in entries:
            yield message

    def delivery_count(self, source_message_id: int) -> int:
        return len(self.delivered.get((self.chat, source_message_id), []))


@dataclass(slots=True)
class ChaosReport:
    messages: int
    rounds: int
    forwarded: int = 0
    duplicates: int = 0
    lost: int = 0
    ambiguous_left: int = 0
    forward_calls: int = 0
    crashes: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.lost == 0 and self.duplicates == 0

    def summary(self) -> str:
        return (
            f"{self.messages} messages, {self.rounds} crash/restart cycle(s), "
            f"{self.crashes} simulated crash(es): forwarded={self.forwarded} "
            f"duplicates={self.duplicates} lost={self.lost} "
            f"ambiguous_left={self.ambiguous_left} telegram_calls={self.forward_calls}"
        )


def _plan(rng: random.Random, count: int, crash_probability: float) -> list[str]:
    plan: list[str] = []
    for _ in range(count):
        roll = rng.random()
        if roll < crash_probability / 3:
            plan.append("crash_before")
        elif roll < 2 * crash_probability / 3:
            plan.append("crash_after")
        elif roll < crash_probability:
            plan.append("error_after")
        else:
            plan.append("ok")
    return plan


async def run_chaos(
    *,
    messages: int = 20,
    rounds: int = 6,
    seed: int = 0,
    crash_probability: float = 0.3,
    data_dir: Any = None,
) -> ChaosReport:
    """Drive real queue/forwarder/reconcile code through crashes and restarts."""

    import tempfile
    from pathlib import Path

    from .config import Config
    from .db import Database
    from .forwarder import Forwarder, RateLimiter
    from .reconcile import reconcile

    rng = random.Random(seed)
    source = SimpleNamespace(id=1234567890, title="Chaos source")
    destination = SimpleNamespace(id=999, title="Chaos destination")
    report = ChaosReport(messages=messages, rounds=rounds)

    temporary: tempfile.TemporaryDirectory[str] | None = None
    if data_dir is None:
        temporary = tempfile.TemporaryDirectory(prefix="collector-chaos-")
        data_dir = temporary.name

    try:
        config = Config(
            api_id=1,
            api_hash="a" * 32,
            destination_chat="@chaos",
            sources=[],
            data_dir=Path(data_dir),
            min_forward_interval=0.0,
            verify_before_retry=True,
            retry_verify_scan_limit=200,
        )
        config.ensure_directories()

        async with Database(config.database_path) as db:
            for message_id in range(1, messages + 1):
                await db.discover_unit(
                    chat_id=CHAT,
                    messages=[
                        {
                            "message_id": message_id,
                            "date": datetime(2024, 1, 1, tzinfo=timezone.utc),
                            "grouped_id": None,
                        }
                    ],
                )

            client = ChaosClient(_plan(rng, messages * 4, crash_probability))
            forwarder = Forwarder(
                client,
                db,
                config,
                {CHAT: source},
                destination,
                asyncio.Event(),
                rate_limiter=RateLimiter(0.0),
            )

            for _ in range(rounds):
                try:
                    await forwarder.drain_once()
                except ProcessDeath as exc:
                    report.notes.append(f"crash: {exc}")

                # Restart: no worker of this process holds a lease, so every lease
                # is abandoned.  In-flight units become ambiguous, unsent ones are
                # re-queued.
                await db.release_stale_leases(only_expired=False)
                await reconcile(
                    client, db, config, destination, scan_limit=0, retry_unresolved=True
                )

                if await db.queue_size() == 0 and not await db.ambiguous_units():
                    break

            # A final clean run with no crashes left in the plan.
            client.plan.clear()
            await forwarder.drain_once()
            await db.release_stale_leases(only_expired=False)
            await reconcile(client, db, config, destination, scan_limit=0, retry_unresolved=True)

            report.forward_calls = client.forward_calls
            report.crashes = client.crashes
            report.ambiguous_left = len(await db.ambiguous_units())

            for message_id in range(1, messages + 1):
                status = await db.get_status(CHAT, message_id)
                deliveries = client.delivery_count(message_id)
                if status == "forwarded":
                    report.forwarded += 1
                else:
                    report.lost += 1
                    report.notes.append(f"message {message_id} ended as {status!r}")
                if deliveries > 1:
                    report.duplicates += deliveries - 1
                    report.notes.append(f"message {message_id} delivered {deliveries} times")
                elif deliveries == 0 and status == "forwarded":
                    report.notes.append(
                        f"message {message_id} is marked forwarded but never reached the destination"
                    )
                    report.lost += 1
    finally:
        if temporary is not None:
            temporary.cleanup()

    return report
