"""Live monitoring: new messages, albums, edits and deletions.

Handlers only *discover* work (they write a deduplicated queue row and wake the
workers).  All forwarding happens in the single forwarder path so that live and
historical messages can never be forwarded twice concurrently.

Chat ids here are canonical marked peer ids (``collector.ids``), which is what
``event.chat_id`` already reports — this is what makes the source dictionary
lookup actually match.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Sequence

from .config import Config
from .db import Database
from .ids import event_chat_id
from .importer import LIVE_PRIORITY, to_unit_payload
from .telegram_client import entity_title

log = logging.getLogger(__name__)

TRUNCATE = 500


class LiveMonitor:
    def __init__(
        self,
        client: Any,
        db: Database,
        config: Config,
        sources: dict[int, Any],
        notify: asyncio.Event,
        *,
        rate_limiter: Any = None,
        destination: Any = None,
    ) -> None:
        self.client = client
        self.db = db
        self.config = config
        self.sources = sources
        self.notify = notify
        self.rate_limiter = rate_limiter
        self.destination = destination
        self._handlers: list[tuple[Any, Any]] = []
        self.events_seen = 0
        self.last_event_at: str | None = None

    # ------------------------------------------------------------------ setup
    def register(self) -> None:
        from telethon import events

        chats = list(self.sources.values())
        if not chats:
            raise RuntimeError("no source chats to monitor")

        self._handlers = [
            (events.NewMessage(chats=chats), self._on_new_message),
            (events.Album(chats=chats), self._on_album),
        ]
        if self.config.include_edits:
            self._handlers.append((events.MessageEdited(chats=chats), self._on_edited))
        if self.config.include_deleted:
            self._handlers.append((events.MessageDeleted(chats=chats), self._on_deleted))

        for event_filter, handler in self._handlers:
            self.client.add_event_handler(handler, event_filter)
        log.info(
            "live monitoring started for %s chat(s) (edits=%s, deletions=%s)",
            len(chats),
            self.config.include_edits,
            self.config.include_deleted,
        )

    def unregister(self) -> None:
        for event_filter, handler in self._handlers:
            try:
                self.client.remove_event_handler(handler, event_filter)
            except Exception:  # pragma: no cover - shutdown best effort
                log.debug("failed to remove handler", exc_info=True)
        self._handlers.clear()

    # --------------------------------------------------------------- handlers
    async def _on_new_message(self, event: Any) -> None:
        try:
            message = event.message
            if getattr(message, "grouped_id", None):
                # Albums are handled by the Album event so the group is
                # forwarded as one unit.
                return
            await self._discover(event_chat_id(event), [message])
        except Exception:  # never let a handler kill the event loop
            log.exception("error while handling a new message")

    async def _on_album(self, event: Any) -> None:
        try:
            messages: Sequence[Any] = list(event.messages)
            if not messages:
                return
            await self._discover(event_chat_id(event), messages)
        except Exception:
            log.exception("error while handling an album")

    async def _discover(self, chat_id: int | None, messages: Sequence[Any]) -> None:
        if chat_id is None:
            chat_id = event_chat_id(messages[0])
        if chat_id is None:
            log.warning("live message without a chat id; skipping")
            return
        if chat_id not in self.sources:
            log.warning(
                "live event for chat %s which is not in the monitored set %s; ignoring "
                "(this should not happen: source ids are canonical)",
                chat_id,
                sorted(self.sources),
            )
            return
        if any(getattr(m, "action", None) is not None for m in messages) and not (
            self.config.include_service_messages
        ):
            return

        payload = to_unit_payload(messages)
        created = await self.db.discover_unit(
            chat_id=chat_id, messages=payload, priority=LIVE_PRIORITY
        )
        self.events_seen += 1
        await self.db.touch_source_live(chat_id)
        if created:
            log.info(
                "queued live message(s) %s from %s",
                [p["message_id"] for p in payload],
                chat_id,
            )
            self.notify.set()

    async def _on_edited(self, event: Any) -> None:
        """Edits are recorded and, if enabled, annotated at the destination.

        We deliberately do not re-forward: that would duplicate the archive.
        """

        try:
            message = event.message
            text = (getattr(message, "message", "") or "").strip()
            chat_id = event_chat_id(event)
            await self.db.log_event(
                "info",
                "message_edited",
                source_chat_id=chat_id,
                source_message_id=getattr(message, "id", None),
                detail=text[:TRUNCATE],
            )
            if not self.config.include_edits or self.destination is None or self.rate_limiter is None:
                return
            note = (
                f"[edited] source {chat_id} · message {getattr(message, 'id', '?')}\n"
                f"{text[:TRUNCATE] if text else '(no text; media edited)'}"
            )
            async with self.rate_limiter:
                await self.client.send_message(self.destination, note)
        except Exception:
            log.exception("error while handling an edit")

    async def _on_deleted(self, event: Any) -> None:
        try:
            chat_id = event_chat_id(event)
            ids = list(getattr(event, "deleted_ids", []) or [])
            await self.db.log_event(
                "info",
                "messages_deleted",
                source_chat_id=chat_id,
                detail=",".join(str(i) for i in ids[:50]),
            )
            if not self.config.include_deleted or self.destination is None or self.rate_limiter is None:
                return
            if not ids:
                return
            note = f"[deleted] source {chat_id} · message(s) {', '.join(str(i) for i in ids[:20])}"
            async with self.rate_limiter:
                await self.client.send_message(self.destination, note)
        except Exception:
            log.exception("error while handling a deletion")


def describe_sources(sources: dict[int, Any]) -> str:
    return ", ".join(f"{chat_id} ({entity_title(entity)})" for chat_id, entity in sources.items())
