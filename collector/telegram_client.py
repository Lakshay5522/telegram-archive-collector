"""Telethon client construction, authentication and entity resolution.

The collector uses an authorised *user* account (MTProto) because a bot
cannot be added to the source groups.  Credentials come from the environment
only; the session file is written with owner-only permissions.

Chat identity rule: every id that leaves this module comes from
:mod:`collector.ids` (marked peer ids), never from ``entity.id``.
"""

from __future__ import annotations

import getpass
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

from .config import Config, SourceGroup
from .ids import canonical_id, describe_id

log = logging.getLogger(__name__)


class TelegramUnavailable(RuntimeError):
    """Telethon is not installed or the session cannot be used."""


class SourceUnavailable(RuntimeError):
    """A configured source chat cannot be resolved or read."""


class InviteNotJoined(SourceUnavailable):
    """The invite link is valid but this account has not joined the group."""


def _import_telethon() -> Any:
    try:
        import telethon  # noqa: F401
    except ImportError as exc:  # pragma: no cover - depends on environment
        raise TelegramUnavailable(
            "Telethon is not installed. Install dependencies with "
            "`pip install -e .` (or `pip install -r requirements.txt`)."
        ) from exc
    from telethon import TelegramClient

    return TelegramClient


def harden_file(path: Path, mode: int = 0o600) -> None:
    """Best-effort restrictive permissions on secrets (session files, database)."""

    try:
        if path.exists():
            os.chmod(path, mode)
    except OSError as exc:  # pragma: no cover - filesystem dependent
        log.warning("could not restrict permissions on %s: %s", path.name, exc)


def harden_session(config: Config) -> None:
    """Restrict the session file and its journal side files."""

    if not config.restrict_permissions:
        return
    for candidate in (
        config.session_path,
        Path(str(config.session_path) + "-journal"),
        Path(str(config.session_path) + "-wal"),
        Path(str(config.session_path) + "-shm"),
    ):
        harden_file(candidate)


def build_client(config: Config) -> Any:
    """Create (but do not connect) the Telethon client."""

    TelegramClient = _import_telethon()
    if not config.api_id or not config.api_hash:
        raise TelegramUnavailable("API_ID and API_HASH must be configured")

    config.session_path.parent.mkdir(parents=True, exist_ok=True)
    client = TelegramClient(
        str(config.session_path),
        config.api_id,
        config.api_hash,
        # We handle FloodWait ourselves so that waits are persisted and
        # visible in status output instead of being silently swallowed.
        flood_sleep_threshold=0,
        connection_retries=5,
        retry_delay=2,
        request_retries=3,
        timeout=30,
        device_model="Telegram Archive Collector",
        system_version="Linux",
        app_version="1.0",
    )
    harden_session(config)
    return client


@asynccontextmanager
async def connected_client(config: Config) -> AsyncIterator[Any]:
    """Yield a connected, authorised client and always disconnect."""

    client = build_client(config)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise TelegramUnavailable(
                "The Telegram session is not authorised. Run "
                "`python -m collector login` once on this host."
            )
        harden_session(config)
        yield client
    finally:
        try:
            await client.disconnect()
        except Exception:  # pragma: no cover - shutdown best effort
            log.debug("client disconnect failed", exc_info=True)


async def interactive_login(config: Config) -> None:
    """First-time login: phone number, code and (if enabled) 2FA password.

    Nothing typed here is logged; the session is persisted so later runs are
    non-interactive.
    """

    client = build_client(config)
    await client.connect()
    try:
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"Already authorised as {describe_user(me)}. Session: {config.session_path}")
            return
        await client.start(
            phone=lambda: input("Telegram phone number (international format): ").strip(),
            password=lambda: getpass.getpass("Two-factor password (leave blank if unused): "),
            code_callback=lambda: input("Login code from Telegram: ").strip(),
        )
        me = await client.get_me()
        print(f"Logged in as {describe_user(me)}.")
        harden_session(config)
        print(f"Session stored at {config.session_path} (mode 0600).")
    finally:
        await client.disconnect()


def describe_user(user: Any) -> str:
    if user is None:
        return "(unknown)"
    username = getattr(user, "username", None)
    name = " ".join(
        part for part in (getattr(user, "first_name", None), getattr(user, "last_name", None)) if part
    )
    ident = f"id={getattr(user, 'id', '?')}"
    if username:
        ident += f", @{username}"
    return f"{name or '(no name)'} ({ident})"


# --------------------------------------------------------------------------
# entity resolution
# --------------------------------------------------------------------------
def _normalise_identifier(identifier: str) -> str:
    value = identifier.strip()
    for prefix in (
        "https://t.me/",
        "http://t.me/",
        "t.me/",
        "https://telegram.me/",
        "telegram.me/",
        "https://telegram.dog/",
    ):
        if value.lower().startswith(prefix):
            value = value[len(prefix) :]
            break
    return value.strip("/")


async def resolve_entity(client: Any, identifier: str) -> Any:
    """Resolve a configured source/destination to a Telethon entity.

    Supports public usernames, numeric ids (``-100…``), stored entities and
    invite links for chats the account has **already joined**.  Access control
    is never bypassed.
    """

    from telethon.tl.functions.messages import CheckChatInviteRequest
    from telethon.tl.types import PeerChannel, PeerChat, PeerUser

    value = _normalise_identifier(identifier)
    if not value:
        raise SourceUnavailable("empty identifier")

    # Invite links (t.me/+hash, t.me/joinchat/hash)
    if value.startswith("+") or value.startswith("joinchat/"):
        invite_hash = value[1:] if value.startswith("+") else value.split("/", 1)[1]
        try:
            result = await client(CheckChatInviteRequest(invite_hash))
        except Exception as exc:
            name = type(exc).__name__
            if name in ("InviteHashExpiredError", "InviteHashInvalidError"):
                raise SourceUnavailable(
                    f"invite link for {identifier!r} is expired or invalid ({name}). "
                    "Ask the group for a fresh link and join manually."
                ) from exc
            raise SourceUnavailable(
                f"invite link for {identifier!r} could not be checked ({name})."
            ) from exc

        # ChatInviteAlready carries the joined chat; a bare ChatInvite means
        # "joinable but NOT joined" -- we must not join on the user's behalf.
        chat = getattr(result, "chat", None)
        if chat is not None and type(result).__name__ == "ChatInviteAlready":
            return chat
        raise InviteNotJoined(
            f"{identifier!r} is a valid invite this account has NOT joined. Join it manually "
            "with this account, then re-run; the collector does not bypass access controls."
        )

    # Numeric ids
    numeric: int | None = None
    try:
        numeric = int(value)
    except ValueError:
        numeric = None

    if numeric is not None:
        try:
            return await client.get_entity(numeric)
        except Exception:
            for peer in (PeerChannel, PeerChat, PeerUser):
                try:
                    return await client.get_entity(peer(numeric))
                except Exception:
                    continue
            # Fall back to scanning dialogs: get_entity needs a cached access
            # hash, which a fresh session may not have yet.
            async for dialog in client.iter_dialogs():
                if canonical_id(dialog.entity) == numeric or getattr(dialog, "id", None) == numeric:
                    return dialog.entity
            raise SourceUnavailable(
                f"chat id {describe_id(numeric)} is unknown to this session. Run "
                "`python -m collector list-groups` to see the ids the account can reach."
            )

    # Usernames / titles
    try:
        return await client.get_entity(value)
    except Exception as exc:
        raise SourceUnavailable(
            f"cannot resolve {identifier!r}: {type(exc).__name__}. Check the username, "
            "or that the account is a member of a private group."
        ) from exc


async def resolve_destination(client: Any, identifier: str) -> Any:
    return await resolve_entity(client, identifier)


def entity_title(entity: Any) -> str:
    title = getattr(entity, "title", None)
    if title:
        return title
    username = getattr(entity, "username", None)
    if username:
        return f"@{username}"
    return f"id={getattr(entity, 'id', '?')}"


async def list_dialogs(client: Any, *, limit: int | None = None) -> list[dict[str, Any]]:
    """Dialogs available to the account, for identifying source chat ids."""

    rows: list[dict[str, Any]] = []
    async for dialog in client.iter_dialogs(limit=limit):
        entity = dialog.entity
        rows.append(
            {
                # canonical id: what you must paste into SOURCE_GROUPS
                "id": canonical_id(entity),
                "raw_entity_id": getattr(entity, "id", None),
                "title": entity_title(entity),
                "type": type(entity).__name__,
                "username": getattr(entity, "username", None),
                "is_group": bool(getattr(dialog, "is_group", False)),
                "is_channel": bool(getattr(dialog, "is_channel", False)),
                "participants": getattr(entity, "participants_count", None),
                "unread": getattr(dialog, "unread_count", 0),
            }
        )
    return rows


async def resolve_sources(client: Any, config: Config) -> dict[int, Any]:
    """Resolve every enabled source; returns ``{canonical_chat_id: entity}``.

    The key is the marked peer id, which is what ``event.chat_id`` reports for
    live events and what gets written to SQLite.
    """

    resolved: dict[int, Any] = {}
    for source in config.enabled_sources:
        try:
            entity = await resolve_entity(client, source.identifier)
        except SourceUnavailable as exc:
            raise SourceUnavailable(f"source {source.label!r}: {exc}") from exc
        chat_id = canonical_id(entity)
        resolved[chat_id] = entity
        log.info(
            "source ready: %s -> %s (%s)", source.label, describe_id(chat_id), entity_title(entity)
        )
    if not resolved:
        raise SourceUnavailable("no usable source groups configured")
    return resolved


def source_label(sources: list[SourceGroup], identifier: str) -> str:
    for source in sources:
        if source.identifier == identifier:
            return source.label
    return identifier
