"""Entity resolution: usernames, numeric ids, invite links, canonical dialogs."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import collector.ids as ids_module
from conftest import MARKED_CHANNEL, FakeClient, FakeEntity

from collector.config import Config, SourceGroup
from collector.telegram_client import (
    InviteNotJoined,
    SourceUnavailable,
    list_dialogs,
    resolve_entity,
    resolve_sources,
)


def invite_already(chat) -> object:
    return type("ChatInviteAlready", (), {"chat": chat})()


def invite_not_joined() -> object:
    return type("ChatInvite", (), {"title": "Some group"})()


class InviteClient(FakeClient):
    def __init__(self, response):
        super().__init__([])
        self.response = response
        self.requests = []

    async def __call__(self, request):
        self.requests.append(request)
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response


class InviteHashExpiredError(Exception):
    pass


@pytest.fixture
def marked(monkeypatch):
    def fake_peer_id(entity):
        return getattr(entity, "marked", None) or getattr(entity, "id")

    monkeypatch.setattr(ids_module, "_telethon_peer_id", fake_peer_id)


async def test_invite_already_joined_returns_the_chat():
    chat = FakeEntity(1234567890, "Joined group", marked=MARKED_CHANNEL)
    entity = await resolve_entity(InviteClient(invite_already(chat)), "https://t.me/+AbCdEfGh")
    assert entity is chat


async def test_invite_not_joined_is_reported_clearly():
    """Audit item 14: a valid invite is not proof of membership."""

    with pytest.raises(InviteNotJoined) as excinfo:
        await resolve_entity(InviteClient(invite_not_joined()), "t.me/+AbCdEfGh")
    assert "has NOT joined" in str(excinfo.value)


async def test_expired_invite_is_reported_clearly():
    client = InviteClient(InviteHashExpiredError("expired"))
    with pytest.raises(SourceUnavailable) as excinfo:
        await resolve_entity(client, "t.me/+expired")
    assert "expired" in str(excinfo.value)
    assert not isinstance(excinfo.value, InviteNotJoined)


async def test_username_resolution_failure_is_wrapped():
    class FailingClient(FakeClient):
        async def get_entity(self, identifier):
            raise ValueError("no user has that username")

    with pytest.raises(SourceUnavailable) as excinfo:
        await resolve_entity(FailingClient(), "@nope")
    assert "cannot resolve" in str(excinfo.value)


async def test_numeric_id_resolution_uses_get_entity():
    entity = await resolve_entity(FakeClient(), "-1001234567890")
    assert getattr(entity, "id", None) is not None


async def test_tme_prefixes_are_stripped():
    class RecordingClient(FakeClient):
        def __init__(self):
            super().__init__([])
            self.seen = []

        async def get_entity(self, identifier):
            self.seen.append(identifier)
            return FakeEntity(1, "x", "alpha")

    client = RecordingClient()
    await resolve_entity(client, "https://t.me/alpha")
    assert client.seen == ["alpha"]


async def test_list_dialogs_reports_canonical_ids(marked):
    rows = await list_dialogs(FakeClient())
    assert rows
    assert rows[0]["id"] == MARKED_CHANNEL           # what you paste into SOURCE_GROUPS
    assert rows[0]["raw_entity_id"] == 1234567890    # what Telethon's entity.id says


async def test_resolve_sources_keys_are_canonical(marked):
    class Client(FakeClient):
        async def get_entity(self, identifier):
            return FakeEntity(1234567890, "Group one", "source_one", marked=MARKED_CHANNEL)

    config = Config(
        api_id=1,
        api_hash="a" * 32,
        destination_chat="@dest",
        sources=[SourceGroup(identifier="@source_one")],
    )
    resolved = await resolve_sources(Client(), config)
    assert list(resolved) == [MARKED_CHANNEL]
    assert MARKED_CHANNEL != 1234567890


async def test_resolve_sources_failure_names_the_source(marked):
    class Client(FakeClient):
        async def get_entity(self, identifier):
            raise ValueError("nope")

    config = Config(
        api_id=1,
        api_hash="a" * 32,
        destination_chat="@dest",
        sources=[SourceGroup(identifier="@missing", alias="Missing group")],
    )
    with pytest.raises(SourceUnavailable) as excinfo:
        await resolve_sources(Client(), config)
    assert "Missing group" in str(excinfo.value)
