"""Canonical Telegram chat identifiers.

One rule, used everywhere (sources, destination, events, SQLite, queue,
checkpoints, reconciliation):

    a chat is identified by its **marked peer id**, exactly as
    ``telethon.utils.get_peer_id()`` returns it

which means channels/supergroups are ``-100<channel_id>``, basic groups are
``-<chat_id>`` and users are ``<user_id>``.  Telethon's raw ``entity.id`` is
*not* that value for channels, so mixing the two silently breaks deduplication
and live-event matching.  Nothing outside this module is allowed to derive an
id from ``entity.id`` directly.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


class IdentityError(ValueError):
    """Raised when an object cannot be mapped to a chat id."""


def _telethon_peer_id(entity: Any) -> int:
    """Indirection point so tests can simulate marked peer ids."""

    from telethon import utils

    return int(utils.get_peer_id(entity))


def canonical_id(entity: Any) -> int:
    """Return the marked peer id for an entity (or entity-like object)."""

    if entity is None:
        raise IdentityError("cannot derive a chat id from None")
    try:
        return _telethon_peer_id(entity)
    except Exception:  # Telethon absent, or a test double
        raw = getattr(entity, "id", None)
        if raw is None:
            raise IdentityError(f"object of type {type(entity).__name__} has no usable id") from None
        return int(raw)


def event_chat_id(event: Any) -> int | None:
    """Marked chat id of an event, or ``None`` if the event has none.

    Telethon already reports ``event.chat_id`` as a marked peer id, so this is
    only a defensive wrapper: it prefers the event attribute and falls back to
    the event's peer object.
    """

    chat_id = getattr(event, "chat_id", None)
    if chat_id is not None:
        return int(chat_id)
    peer = getattr(event, "peer_id", None)
    if peer is not None:
        try:
            return canonical_id(peer)
        except IdentityError:
            return None
    return None


def peer_id_from_parts(
    *, channel_id: int | None = None, chat_id: int | None = None, user_id: int | None = None
) -> int | None:
    """Build a marked peer id from a raw ``PeerChannel``/``PeerChat``/``PeerUser``.

    Used when reading ``fwd_from`` metadata during reconciliation, so the values
    stored there are comparable with :func:`canonical_id` output.
    """

    if channel_id:
        return int(f"-100{channel_id}")
    if chat_id:
        return -int(chat_id)
    if user_id:
        return int(user_id)
    return None


def describe_id(chat_id: int) -> str:
    """Human-readable hint about which kind of chat an id refers to."""

    text = str(chat_id)
    if text.startswith("-100"):
        return f"{chat_id} (channel/supergroup)"
    if text.startswith("-"):
        return f"{chat_id} (basic group)"
    return f"{chat_id} (user)"
