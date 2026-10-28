from __future__ import annotations

import asyncio
import os
import signal
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio

from collector.config import Config, SourceGroup
from collector.db import Database

#: A canonical (marked) channel id, as `event.chat_id` reports it.
MARKED_CHANNEL = -1001234567890

#: Per-test wall-clock ceiling.  A hanging test must fail with its own name
#: rather than stall the whole run (a reviewer's suite once "timed out").
TEST_TIMEOUT = float(os.environ.get("COLLECTOR_TEST_TIMEOUT", "120"))


class TestTimeout(TimeoutError):
    """Raised by the watchdog when a single test exceeds ``TEST_TIMEOUT``."""


@pytest.fixture(autouse=True)
def _watchdog(request):
    """Bound every test with SIGALRM so a hang becomes a named failure."""

    def _fire(signum, frame):  # noqa: ANN001, ARG001
        raise TestTimeout(
            f"{request.node.name} exceeded {TEST_TIMEOUT:.0f}s "
            f"(raise COLLECTOR_TEST_TIMEOUT to allow more)"
        )

    try:
        previous = signal.signal(signal.SIGALRM, _fire)
        signal.setitimer(signal.ITIMER_REAL, TEST_TIMEOUT)
    except (AttributeError, ValueError):  # pragma: no cover - non-Unix / non-main thread
        yield
        return

    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@pytest.fixture
def config(tmp_path) -> Config:
    cfg = Config(
        api_id=12345,
        api_hash="a" * 32,
        destination_chat="@destination",
        sources=[SourceGroup(identifier="@source_one", alias="one")],
        data_dir=tmp_path,
        min_forward_interval=0.0,
        backoff_base=0.01,
        backoff_cap=0.05,
        queue_poll_interval=0.05,
        heartbeat_interval=1.0,
        import_recheck_interval=60.0,
    )
    cfg.ensure_directories()
    return cfg


@pytest_asyncio.fixture
async def db(tmp_path) -> Database:
    database = Database(tmp_path / "test.sqlite3")
    await database.connect()
    try:
        yield database
    finally:
        await database.close()


def make_message(
    message_id: int,
    *,
    grouped_id: int | None = None,
    date: datetime | None = None,
    text: str = "",
    action: Any = None,
    out: bool = False,
    media: Any = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=message_id,
        grouped_id=grouped_id,
        date=date or datetime(2024, 1, 1, tzinfo=timezone.utc),
        message=text,
        action=action,
        out=out,
        media=media,
    )


class FakeEntity(SimpleNamespace):
    """Entity double.

    ``raw_id`` mimics Telethon: for a channel, ``entity.id`` is the *raw* id
    while the canonical (marked) peer id is ``-100<raw_id>``.
    """

    def __init__(
        self,
        entity_id: int,
        title: str = "Source",
        username: str | None = None,
        *,
        marked: int | None = None,
    ) -> None:
        super().__init__(id=entity_id, title=title, username=username, marked=marked)


class FakeClient:
    """Minimal Telethon stand-in: records calls, can be told to raise."""

    def __init__(self, messages: list[Any] | None = None) -> None:
        self.messages = messages or []
        self.forwarded: list[dict[str, Any]] = []
        self.sent: list[tuple[Any, str]] = []
        self.copied: list[Any] = []
        self.handlers: list[tuple[Any, Any]] = []
        self.raise_on_forward: BaseException | None = None
        self.raise_on_send: BaseException | None = None
        self.connected = False
        self.disconnected = False
        self.destination_id: int | None = None
        self.destination_history: list[Any] = []
        self._next_id = 9000

    # -- lifecycle ------------------------------------------------------
    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.disconnected = True

    async def is_user_authorized(self) -> bool:
        return True

    async def get_me(self) -> Any:
        return SimpleNamespace(id=1, username="tester", first_name="Test", last_name="User")

    # -- handlers -------------------------------------------------------
    def add_event_handler(self, handler: Any, event_filter: Any = None) -> None:
        self.handlers.append((handler, event_filter))

    def remove_event_handler(self, handler: Any, event_filter: Any = None) -> None:
        self.handlers = [(h, f) for (h, f) in self.handlers if h is not handler]

    # -- writes ---------------------------------------------------------
    async def forward_messages(self, entity: Any, messages: Any, from_peer: Any = None) -> Any:
        if self.raise_on_forward is not None:
            raise self.raise_on_forward
        ids = list(messages) if isinstance(messages, (list, tuple)) else [messages]
        self.forwarded.append({"entity": entity, "ids": ids, "from_peer": from_peer})
        result = [SimpleNamespace(id=self._next_id + offset) for offset in range(len(ids))]
        self._next_id += len(ids)
        return result if len(result) > 1 else result[0]

    async def send_message(self, entity: Any, text: str) -> Any:
        if self.raise_on_send is not None:
            raise self.raise_on_send
        self.sent.append((entity, text))
        self._next_id += 1
        return SimpleNamespace(id=self._next_id)

    async def send_file(self, entity: Any, file: Any = None, caption: str = "") -> Any:
        self.copied.append((entity, file, caption))
        self._next_id += 1
        return SimpleNamespace(id=self._next_id)

    # -- reads ----------------------------------------------------------
    async def get_messages(self, entity: Any, ids: Any = None) -> Any:
        wanted = ids if isinstance(ids, (list, tuple)) else [ids]
        found = [m for m in self.messages if m.id in wanted]
        return found if isinstance(ids, (list, tuple)) else (found[0] if found else None)

    async def iter_messages(self, entity: Any, limit: int | None = None, **kwargs: Any):
        if self.destination_id is not None and getattr(entity, "id", None) == self.destination_id:
            count = 0
            for message in self.destination_history:
                if limit is not None and count >= limit:
                    return
                count += 1
                yield message
            return

        min_id = kwargs.get("min_id")
        offset_date = kwargs.get("offset_date")
        count = 0
        for message in self.messages:
            if min_id is not None and message.id <= min_id:
                continue
            if offset_date is not None and message.date is not None and message.date < offset_date:
                continue
            if limit is not None and count >= limit:
                return
            count += 1
            yield message

    async def get_entity(self, identifier: Any) -> Any:
        text = str(identifier)
        numeric = int(text) if text.lstrip("-").isdigit() else None
        return FakeEntity(numeric or 1, "Source", "source_one")

    async def iter_dialogs(self, limit: int | None = None):
        entity = FakeEntity(1234567890, "One", "source_one", marked=MARKED_CHANNEL)
        yield SimpleNamespace(
            id=MARKED_CHANNEL, entity=entity, is_group=True, is_channel=True, unread_count=0
        )

    def __call__(self, request: Any) -> Any:  # CheckChatInviteRequest etc.
        raise AssertionError("FakeClient.__call__ should be stubbed per test")


@pytest.fixture
def fake_client() -> FakeClient:
    return FakeClient()


def run(coro):
    """Run a coroutine in a fresh loop (for non-async test bodies)."""

    return asyncio.run(coro)


def pytest_sessionfinish(session, exitstatus):  # pragma: no cover - diagnostics only
    """Dump surviving threads when asked, to catch a process that will not exit."""

    if not os.environ.get("COLLECTOR_DEBUG_THREADS"):
        return
    import threading
    import sys

    alive = [t for t in threading.enumerate() if t is not threading.main_thread()]
    print(f"\n[threads] {len(alive)} non-main thread(s) at session finish:", file=sys.stderr)
    for thread in alive:
        print(f"  - {thread.name} daemon={thread.daemon} alive={thread.is_alive()}", file=sys.stderr)
