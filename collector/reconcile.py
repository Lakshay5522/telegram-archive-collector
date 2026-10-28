"""Reconciliation for ambiguous deliveries.

When a forward is interrupted we cannot tell whether Telegram accepted it.
``reconcile`` reads recent destination history and matches native forwards by
their ``fwd_from`` origin, using the same canonical peer ids everywhere, which
lets us settle the ambiguity without duplicating content.

Album units are matched on their first message id, but every member's
destination id is recorded when Telegram returns one, so the mapping stays
complete for multi-message deliveries.
"""

from __future__ import annotations

import logging
from typing import Any

from .config import Config
from .db import Database
from .ids import IdentityError, canonical_id, peer_id_from_parts

log = logging.getLogger(__name__)


def _origin_pair(message: Any) -> tuple[int, int] | None:
    """Return ``(source_chat_id, source_message_id)`` for a forwarded message."""

    fwd = getattr(message, "fwd_from", None)
    if fwd is None:
        return None

    msg_id = getattr(fwd, "channel_post", None) or getattr(fwd, "saved_from_msg_id", None)
    if not msg_id:
        return None

    saved_peer = getattr(fwd, "saved_from_peer", None)
    peer = saved_peer or getattr(fwd, "from_id", None)
    if peer is None:
        return None

    chat_id = peer_id_from_parts(
        channel_id=getattr(peer, "channel_id", None),
        chat_id=getattr(peer, "chat_id", None),
        user_id=getattr(peer, "user_id", None),
    )
    if chat_id is None:
        return None
    return chat_id, int(msg_id)


async def scan_destination(
    client: Any, destination: Any, *, limit: int | None = 500
) -> dict[tuple[int, int], int]:
    """Map ``(source_chat_id, source_message_id)`` -> destination message id.

    ``limit=None`` scans the whole destination history (slow but exhaustive).
    """

    found: dict[tuple[int, int], int] = {}
    async for message in client.iter_messages(destination, limit=limit):
        pair = _origin_pair(message)
        if pair is not None:
            found.setdefault(pair, message.id)
    return found


async def reconcile(
    client: Any,
    db: Database,
    config: Config,
    destination: Any,
    *,
    scan_limit: int | None = None,
    dry_run: bool = False,
    retry_unresolved: bool = False,
) -> dict[str, int]:
    """Settle ambiguous units against the destination channel.

    Releases expired leases first, so a crashed process's work is picked up
    rather than stranded.  Results are recorded per unit: ``confirmed`` when a
    match is found, ``unresolved`` when it is not (which is *not* proof that the
    forward failed — only that the scan window did not contain it).
    """

    released = await db.release_stale_leases()
    ambiguous = await db.ambiguous_units()

    # Resolve the scan window before anything can report on it.
    # ``scan_limit=0`` means "scan everything": the only way to settle a very old
    # interrupted delivery is to look far enough back.
    limit = config.reconcile_scan_limit if scan_limit is None else scan_limit
    scan_window: int | None = None if limit == 0 else limit

    summary = {
        "released_requeued": released.requeued,
        "released_ambiguous": released.ambiguous,
        "ambiguous": len(ambiguous),
        "matched": 0,
        "unresolved": 0,
        "retried_unresolved": 0,
        "window": "all" if scan_window is None else scan_window,
    }

    if not ambiguous:
        log.info(
            "nothing to reconcile (%s re-queued, %s newly ambiguous)",
            released.requeued,
            released.ambiguous,
        )
        return summary

    forwarded = await scan_destination(client, destination, limit=scan_window)
    # Same canonical helper as the forwarder: a channel's raw ``entity.id`` is
    # not the marked peer id stored everywhere else.
    try:
        destination_chat_id: int | None = canonical_id(destination)
    except IdentityError:  # pragma: no cover - defensive
        destination_chat_id = None

    for unit in ambiguous:
        pair = (unit["source_chat_id"], unit["source_message_id"])
        destination_message_id = forwarded.get(pair)
        if destination_message_id is None:
            summary["unresolved"] += 1
            if retry_unresolved:
                summary["retried_unresolved"] += 1
                log.warning(
                    "unit %s not found in the scanned window; re-queueing it. This can "
                    "duplicate a forward that succeeded outside the window.",
                    unit["unit_key"],
                )
                if not dry_run:
                    await db.requeue_unit(unit["unit_key"])
            else:
                log.info(
                    "unit %s not found in the last %s destination message(s); left ambiguous "
                    "(re-run with --retry-unresolved to forward it again, or raise --limit)",
                    unit["unit_key"],
                    limit,
                )
                if not dry_run:
                    await db.record_reconcile_result(unit["unit_key"], "unresolved")
            continue
        summary["matched"] += 1
        log.info(
            "unit %s was delivered as destination message %s", unit["unit_key"], destination_message_id
        )
        if not dry_run:
            await db.resolve_ambiguous(
                unit["unit_key"],
                destination_chat_id=destination_chat_id,
                destination_message_id=destination_message_id,
            )
    if not dry_run:
        await db.log_event(
            "info",
            "reconcile_completed",
            detail=(
                f"matched={summary['matched']} unresolved={summary['unresolved']} "
                f"window={limit}"
            ),
        )
    return summary
