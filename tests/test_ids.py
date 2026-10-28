"""Audit item 1/11: one canonical chat id everywhere."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import collector.ids as ids_module
from collector.ids import IdentityError, canonical_id, describe_id, event_chat_id, peer_id_from_parts


class Channel:
    """Mimics a Telethon channel: raw id differs from the marked peer id."""

    def __init__(self, raw_id: int) -> None:
        self.id = raw_id


class Chat:
    def __init__(self, raw_id: int) -> None:
        self.id = raw_id


class User:
    def __init__(self, raw_id: int) -> None:
        self.id = raw_id


@pytest.fixture
def marked(monkeypatch):
    """Simulate telethon.utils.get_peer_id for the three entity kinds."""

    def fake_peer_id(entity):
        if isinstance(entity, Channel):
            return int(f"-100{entity.id}")
        if isinstance(entity, Chat):
            return -entity.id
        return entity.id

    monkeypatch.setattr(ids_module, "_telethon_peer_id", fake_peer_id)
    return fake_peer_id


def test_channel_uses_marked_peer_id(marked):
    entity = Channel(1234567890)
    assert entity.id == 1234567890           # raw
    assert canonical_id(entity) == -1001234567890  # canonical


def test_basic_group_and_user_ids(marked):
    assert canonical_id(Chat(555)) == -555
    assert canonical_id(User(777)) == 777


def test_canonical_id_falls_back_when_telethon_cannot_help():
    """A test double (or any object with .id) still yields something usable."""

    assert canonical_id(SimpleNamespace(id=42)) == 42


def test_canonical_id_rejects_unusable_objects():
    with pytest.raises(IdentityError):
        canonical_id(None)
    with pytest.raises(IdentityError):
        canonical_id(object())


def test_event_chat_id_prefers_event_attribute(marked):
    event = SimpleNamespace(chat_id=-1001234567890)
    assert event_chat_id(event) == -1001234567890


def test_event_chat_id_falls_back_to_peer(marked):
    event = SimpleNamespace(chat_id=None, peer_id=Channel(1234567890))
    assert event_chat_id(event) == -1001234567890


def test_event_chat_id_returns_none_when_unknown(marked):
    assert event_chat_id(SimpleNamespace(chat_id=None, peer_id=None)) is None


def test_peer_id_from_parts_matches_canonical_form(marked):
    assert peer_id_from_parts(channel_id=1234567890) == canonical_id(Channel(1234567890))
    assert peer_id_from_parts(chat_id=555) == canonical_id(Chat(555))
    assert peer_id_from_parts(user_id=777) == canonical_id(User(777))
    assert peer_id_from_parts() is None


def test_describe_id_labels_kinds():
    assert "channel" in describe_id(-1001234567890)
    assert "basic group" in describe_id(-555)
    assert "user" in describe_id(777)


def test_live_event_matches_resolved_source(marked):
    """The regression that mattered: event id must equal the source dict key."""

    resolved_entity = Channel(1234567890)
    source_key = canonical_id(resolved_entity)
    event = SimpleNamespace(chat_id=-1001234567890)
    assert event_chat_id(event) in {source_key}
    # The old bug: comparing against entity.id would have missed this event.
    assert event_chat_id(event) != resolved_entity.id
