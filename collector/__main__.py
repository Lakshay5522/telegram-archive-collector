"""Command line interface.

    python -m collector login
    python -m collector list-groups [--json] [--limit N]
    python -m collector validate-config [--check-telegram]
    python -m collector import-history [--force] [--group IDENTIFIER]
    python -m collector run [--no-history] [--force]
    python -m collector status [--json]
    python -m collector retry-failed [--include-ambiguous]
    python -m collector reconcile [--limit N] [--dry-run] [--retry-unresolved]
    python -m collector db-maintenance [--prune-events DAYS] [--vacuum]
    python -m collector selftest [--verbose]
    python -m collector dashboard
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Sequence

from .config import Config, ConfigError, describe, load_config
from .db import Database, DatabaseError
from .logging_setup import setup_logging


def _config(args: argparse.Namespace, *, require_credentials: bool = True) -> Config:
    """Load configuration.

    ``require_credentials=False`` is used by commands that only read local state
    (status, db-maintenance, dashboard): a missing API hash must not stop you
    from inspecting a database.
    """

    return load_config(dotenv_path=args.env_file, require_credentials=require_credentials)


def _logger(config: Config) -> None:
    setup_logging(
        config.log_dir,
        level=config.log_level,
        max_bytes=config.log_max_bytes,
        backup_count=config.log_backup_count,
    )


# --------------------------------------------------------------------------
# commands
# --------------------------------------------------------------------------
async def cmd_login(args: argparse.Namespace) -> int:
    from .telegram_client import interactive_login

    config = _config(args)
    _logger(config)
    await interactive_login(config)
    return 0


async def cmd_list_groups(args: argparse.Namespace) -> int:
    from .telegram_client import connected_client, list_dialogs

    config = _config(args)
    _logger(config)
    async with connected_client(config) as client:
        rows = await list_dialogs(client, limit=args.limit)

    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
        return 0

    if not rows:
        print("No dialogs are visible to this account.")
        return 0
    print(f"{'chat id':>18}  {'type':<12} {'participants':>12}  title / username")
    for row in rows:
        participants = row["participants"] if row["participants"] is not None else "-"
        title = row["title"] + (f"  (@{row['username']})" if row["username"] else "")
        print(f"{row['id']:>18}  {row['type']:<12} {participants!s:>12}  {title}")
    print(
        "\nThe 'chat id' column is the canonical marked peer id — paste that (or the "
        "username) into SOURCE_GROUPS. Private groups need the account to be a member; "
        "the collector never bypasses access controls."
    )
    return 0


async def cmd_validate_config(args: argparse.Namespace) -> int:
    try:
        config = _config(args, require_credentials=False)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    problems = config.problems(require_credentials=True)
    print(describe(config))
    print()
    if problems:
        print("problems found:")
        for problem in problems:
            print(f"  [{problem.severity}] {problem.field}: {problem.message}")
        if any(p.severity == "error" for p in problems):
            print("\nThe collector will refuse to start until the errors are fixed.")
            return 1
    else:
        print("configuration is valid.")

    if args.check_telegram:
        from .telegram_client import connected_client, resolve_destination, resolve_sources

        try:
            async with connected_client(config) as client:
                destination = await resolve_destination(client, config.destination_chat or "")
                print(f"destination resolved: id={getattr(destination, 'id', '?')}")
                sources = await resolve_sources(client, config)
                for chat_id, entity in sources.items():
                    print(f"source resolved: id={chat_id} title={getattr(entity, 'title', None)}")
        except Exception as exc:
            print(f"telegram check failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1
    return 0


async def cmd_import_history(args: argparse.Namespace) -> int:
    from .telegram_client import connected_client, resolve_destination, resolve_sources

    config = _config(args)
    _logger(config)
    async with connected_client(config) as client:
        await resolve_destination(client, config.destination_chat or "")
        sources = await resolve_sources(client, config)
        if args.group:
            wanted = args.group.strip().lstrip("@").lower()
            sources = {
                chat_id: entity
                for chat_id, entity in sources.items()
                if str(chat_id) == wanted
                or (getattr(entity, "username", "") or "").lower() == wanted
            }
            if not sources:
                print(f"no configured source matches {args.group!r}", file=sys.stderr)
                return 2

        async with Database(config.database_path) as db:
            await db.sync_source_groups(config.sources)
            from .importer import HistoryImporter

            importer = HistoryImporter(client, db, config)
            results = await importer.run(sources, force=args.force)
            for chat_id, stats in results.items():
                print(
                    f"chat {chat_id}: status={stats.status} scanned={stats.scanned} "
                    f"queued={stats.queued} service_skipped={stats.skipped_service} "
                    f"attempts={stats.attempts}"
                    + (f" errors={stats.errors}" if stats.errors else "")
                )
            return 0 if all(not stats.failed for stats in results.values()) else 1


async def cmd_run(args: argparse.Namespace) -> int:
    from .service import CollectorService

    config = _config(args)
    service = CollectorService(config)
    await service.start(import_history=not args.no_history, force_import=args.force)
    await service.run_until_stopped()
    return 0


async def cmd_status(args: argparse.Namespace) -> int:
    from .status import build_report, health_snapshot

    config = _config(args, require_credentials=False)
    try:
        db_context = Database(config.database_path, read_only=True)
        await db_context.connect()
    except DatabaseError as exc:
        print(f"cannot read status: {exc}", file=sys.stderr)
        print("Start the collector once (`python -m collector run`) to create the database.", file=sys.stderr)
        return 1
    try:
        report = await build_report(
            db_context, config, connection_state="not connected (status reads the database)"
        )
        if args.json:
            payload = {
                "health": await health_snapshot(db_context, config),
                "counts": await db_context.status_counts(),
                "sources": await db_context.stats_by_source(),
                "imports": await db_context.list_import_progress(),
            }
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(report)
    finally:
        await db_context.close()
    return 0


async def cmd_retry_failed(args: argparse.Namespace) -> int:
    config = _config(args, require_credentials=False)
    _logger(config)
    async with Database(config.database_path) as db:
        count = await db.retry_failed(include_ambiguous=args.include_ambiguous)
        await db.log_event("info", "retry_failed_requested", detail=f"requeued={count}")
    print(f"re-queued {count} unit(s). The running collector will pick them up.")
    if count and not args.include_ambiguous:
        print("Ambiguous deliveries were not touched; use --include-ambiguous if you accept duplicates.")
    return 0


async def cmd_reconcile(args: argparse.Namespace) -> int:
    from .reconcile import reconcile
    from .telegram_client import connected_client, resolve_destination

    config = _config(args)
    _logger(config)
    async with connected_client(config) as client:
        destination = await resolve_destination(client, config.destination_chat or "")
        async with Database(config.database_path) as db:
            summary = await reconcile(
                client,
                db,
                config,
                destination,
                scan_limit=args.limit,
                dry_run=args.dry_run,
                retry_unresolved=args.retry_unresolved,
            )
    print(json.dumps(summary, indent=2))
    if args.retry_unresolved and summary.get("retried_unresolved"):
        print(
            f"WARNING: re-queued {summary['retried_unresolved']} unit(s) that were not found in "
            "the scanned window. If the original forward succeeded outside that window, this "
            "will duplicate it in the destination.",
            file=sys.stderr,
        )
    if summary.get("unresolved"):
        print(
            f"{summary['unresolved']} unit(s) remain ambiguous. They are never retried "
            "automatically; widen the window with --limit or pass --retry-unresolved.",
            file=sys.stderr,
        )
    return 0


async def cmd_db_maintenance(args: argparse.Namespace) -> int:
    config = _config(args, require_credentials=False)
    _logger(config)
    async with Database(config.database_path) as db:
        integrity = await db.integrity_check()
        size_before = await db.database_size()
        pruned = 0
        if args.prune_events is not None:
            pruned = await db.prune_events(args.prune_events)
        await db.checkpoint_wal()
        if args.vacuum:
            await db.conn.execute("VACUUM")
        size_after = await db.database_size()
        counts = await db.status_counts()
    print(f"integrity_check : {integrity}")
    print(f"size            : {size_before / 1024:.1f} KiB -> {size_after / 1024:.1f} KiB")
    if args.prune_events is not None:
        print(f"events pruned   : {pruned} (older than {args.prune_events} days)")
    print(f"forwarded       : {counts.get('forwarded', 0)}")
    print(f"ambiguous       : {counts.get('ambiguous', 0)}")
    return 0 if integrity == "ok" else 1


def cmd_selftest(args: argparse.Namespace) -> int:
    """Verify the persistence/recovery invariants without pytest or a network."""

    from .selftest import main as selftest_main

    return selftest_main(verbose=args.verbose)


def cmd_dashboard(args: argparse.Namespace) -> int:
    from .dashboard import run_dashboard

    config = _config(args, require_credentials=False)
    _logger(config)
    run_dashboard(config)
    return 0


# --------------------------------------------------------------------------
# parser
# --------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m collector",
        description="Collect Telegram messages from authorised groups into a private channel.",
    )
    parser.add_argument("--env-file", default=".env", help="path to the .env file (default: .env)")
    sub = parser.add_subparsers(dest="command", required=True)

    login = sub.add_parser("login", help="interactive first-time Telegram login")
    login.set_defaults(func=cmd_login)

    groups = sub.add_parser("list-groups", help="list dialogs visible to this account")
    groups.add_argument("--limit", type=int, default=None)
    groups.add_argument("--json", action="store_true")
    groups.set_defaults(func=cmd_list_groups)

    validate = sub.add_parser("validate-config", help="validate configuration")
    validate.add_argument(
        "--check-telegram",
        action="store_true",
        help="also connect and resolve destination/sources",
    )
    validate.set_defaults(func=cmd_validate_config)

    history = sub.add_parser("import-history", help="import accessible history (resumable)")
    history.add_argument("--force", action="store_true", help="re-scan groups marked completed")
    history.add_argument("--group", default=None, help="only this source identifier")
    history.set_defaults(func=cmd_import_history)

    run = sub.add_parser("run", help="run live monitoring + forwarding (long-lived)")
    run.add_argument("--no-history", action="store_true", help="skip the historical import")
    run.add_argument("--force", action="store_true", help="re-scan history even if completed")
    run.set_defaults(func=cmd_run)

    status = sub.add_parser("status", help="print a status report from the database")
    status.add_argument("--json", action="store_true", help="machine-readable output")
    status.set_defaults(func=cmd_status)

    retry = sub.add_parser("retry-failed", help="re-queue permanently failed messages")
    retry.add_argument(
        "--include-ambiguous",
        action="store_true",
        help="also re-queue ambiguous deliveries (may duplicate content)",
    )
    retry.set_defaults(func=cmd_retry_failed)

    recon = sub.add_parser("reconcile", help="settle ambiguous deliveries against the destination")
    recon.add_argument(
        "--limit",
        type=int,
        default=None,
        help="destination messages to scan (default: RECONCILE_SCAN_LIMIT, 0 = entire history)",
    )
    recon.add_argument("--dry-run", action="store_true")
    recon.add_argument(
        "--retry-unresolved",
        action="store_true",
        help="re-queue units that were not found at the destination",
    )
    recon.set_defaults(func=cmd_reconcile)

    maintenance = sub.add_parser("db-maintenance", help="integrity check, WAL checkpoint, event pruning")
    maintenance.add_argument(
        "--prune-events", type=int, default=None, help="delete audit events older than N days"
    )
    maintenance.add_argument("--vacuum", action="store_true", help="VACUUM the database afterwards")
    maintenance.set_defaults(func=cmd_db_maintenance)

    selftest = sub.add_parser(
        "selftest",
        help="verify database/queue/recovery invariants (no network, no credentials)",
    )
    selftest.add_argument("--verbose", action="store_true", help="print tracebacks for failures")
    selftest.set_defaults(func=cmd_selftest)

    dash = sub.add_parser("dashboard", help="run the optional authenticated dashboard")
    dash.set_defaults(func=cmd_dashboard)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = args.func(args)
        if asyncio.iscoroutine(result):
            return int(asyncio.run(result) or 0)
        return int(result or 0)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
