"""Status reporting (never prints credentials or session contents).

Answers the questions that actually come up during an incident: is Telegram
connected, is the heartbeat fresh, is the queue moving, which source is behind,
which deliveries are ambiguous, and what failed recently.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .config import Config
from .db import Database, parse_iso, utcnow


def _fmt(value: Any, default: str = "-") -> str:
    return default if value in (None, "") else str(value)


def _age(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    seconds = max(0, int(seconds))
    if seconds < 90:
        return f"{seconds}s"
    if seconds < 5400:
        return f"{seconds // 60}m"
    if seconds < 172800:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


async def health_snapshot(db: Database, config: Config) -> dict[str, Any]:
    """Machine-readable health used by the dashboard and ``status``."""

    heartbeat = await db.last_heartbeat()
    now = utcnow()
    age = (now - heartbeat).total_seconds() if heartbeat else None
    counts = await db.status_counts()
    queue_size = await db.queue_size()
    oldest_pending = parse_iso(await db.oldest_pending_created_at())
    oldest_age = (now - oldest_pending).total_seconds() if oldest_pending else None
    incomplete = await db.incomplete_imports()

    problems: list[str] = []
    if heartbeat is None:
        problems.append("no heartbeat recorded (collector has not started against this database)")
    elif age is not None and age > config.stale_heartbeat_seconds:
        problems.append(f"heartbeat is stale ({_age(age)} old)")
    if counts.get("ambiguous", 0):
        problems.append(f"{counts['ambiguous']} ambiguous delivery(ies) awaiting reconcile")
    if oldest_age is not None and oldest_age > max(300, config.queue_poll_interval * 60):
        problems.append(f"oldest queued message is {_age(oldest_age)} old")
    if incomplete:
        problems.append(f"{len(incomplete)} incomplete historical import(s)")

    if heartbeat is None:
        state = "unknown"
    elif problems:
        state = "degraded"
    else:
        state = "ok"

    return {
        "state": state,
        "problems": problems,
        "heartbeat_at": heartbeat.isoformat(timespec="seconds") if heartbeat else None,
        "heartbeat_age_seconds": age,
        "queue_size": queue_size,
        "oldest_pending_age_seconds": oldest_age,
        "counts": counts,
        "incomplete_imports": len(incomplete),
        "checked_at": now.isoformat(timespec="seconds"),
    }


async def build_report(db: Database, config: Config, *, connection_state: str = "unknown") -> str:
    counts = await db.status_counts()
    progress = await db.list_import_progress()
    per_source = await db.stats_by_source()
    last_forward = await db.last_forwarded()
    failures = await db.recent_failures(limit=5)
    events = await db.recent_events(limit=5)
    health = await health_snapshot(db, config)

    lines: list[str] = []
    lines.append("Telegram Archive Collector — status")
    lines.append("=" * 40)
    lines.append(f"telegram connection : {connection_state}")
    lines.append(f"health              : {health['state']}")
    for problem in health["problems"]:
        lines.append(f"  ! {problem}")
    lines.append(f"heartbeat           : {_fmt(health['heartbeat_at'])} ({_age(health['heartbeat_age_seconds'])} ago)")
    lines.append(
        f"forward mode        : {config.forward_mode}"
        f"{' (+source annotation)' if config.annotate_source else ''}"
    )
    lines.append(f"database            : {config.database_path}")
    lines.append(f"database size       : {await db.database_size() / 1024:.1f} KiB")
    lines.append("")
    lines.append("queue")
    lines.append(f"  pending           : {counts.get('queue_pending', 0)}")
    lines.append(f"  leased            : {counts.get('queue_leased', 0)}")
    lines.append(f"  sending           : {counts.get('queue_sending', 0)}")
    lines.append(f"  ambiguous         : {counts.get('queue_ambiguous', 0)}")
    lines.append(f"  dead              : {counts.get('queue_dead', 0)}")
    lines.append(f"  oldest pending age: {_age(health['oldest_pending_age_seconds'])}")
    lines.append("")
    lines.append("messages")
    lines.append(f"  forwarded         : {counts.get('forwarded', 0)}")
    lines.append(f"  in progress       : {counts.get('processing', 0)}")
    lines.append(f"  retry pending     : {counts.get('retry_pending', 0)}")
    lines.append(f"  permanently failed: {counts.get('permanently_failed', 0)}")
    lines.append(f"  ambiguous         : {counts.get('ambiguous', 0)}")
    lines.append("")
    lines.append("source groups")
    if not per_source:
        lines.append("  (none configured in the database yet)")
    for source in per_source:
        lines.append(
            f"  - {source['identifier']}"
            f"  id={_fmt(source.get('chat_id'))}"
            f"  title={_fmt(source.get('title'))}"
            f"  [{'enabled' if source.get('enabled') else 'disabled'}]"
        )
        lines.append(
            f"      forwarded={source.get('forwarded', 0)} pending={source.get('pending', 0)}"
            f" failed={source.get('failed', 0)} ambiguous={source.get('ambiguous', 0)}"
            f" retry={source.get('retry_pending', 0)}"
        )
        lines.append(
            f"      last message={_fmt(source.get('last_seen_message_id'))}"
            f"  last forward={_fmt(source.get('last_forwarded_at'))}"
            f"  last live event={_fmt(source.get('last_live_event_at'))}"
        )
    lines.append("")
    lines.append("historical import")
    if not progress:
        lines.append("  (no import started yet)")
    for row in progress:
        lines.append(
            f"  - chat {row['source_chat_id']}: {row['status']}"
            f"  scanned={row['messages_seen']}  queued={row['messages_queued']}"
            f"  skipped={row.get('messages_skipped', 0)}"
            f"  last_scanned={_fmt(row.get('last_scanned_message_id'))}"
            f"  attempts={row.get('attempts', 0)}"
            f"  updated={_fmt(row['updated_at'])}"
            + (f"\n      error: {row['last_error']}" if row["last_error"] else "")
        )
    lines.append("")
    if last_forward:
        lines.append(
            "last successful forwarding: "
            f"chat {last_forward['source_chat_id']} message {last_forward['source_message_id']} "
            f"-> destination {_fmt(last_forward['destination_message_id'])} "
            f"at {last_forward['updated_at']}"
        )
    else:
        lines.append("last successful forwarding: (none yet)")
    lines.append("")
    lines.append("recent failures")
    if not failures:
        lines.append("  (none)")
    for row in failures:
        lines.append(
            f"  - {row['source_chat_id']}:{row['source_message_id']}  {row['status']}"
            f"  [{_fmt(row['error_class'])}]  {_fmt(row['last_error'])[:120]}"
        )
    lines.append("")
    lines.append("recent events")
    if not events:
        lines.append("  (none)")
    for event in events:
        lines.append(
            f"  - {event['created_at']} {event['level']:7} {event['event']} "
            f"{_fmt(event['detail'], '')[:100]}"
        )
    return "\n".join(lines)


def health_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
