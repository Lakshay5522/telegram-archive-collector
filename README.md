# Telegram Archive Collector — Advanced Edition

A production-oriented message collector for Telegram. It uses **your own authorised
Telegram account** (MTProto via [Telethon](https://docs.telethon.dev)) — not a bot — to
import the accessible history of ~10 public and private groups and to keep forwarding
new messages, live, into a **private channel you control**.

The Bot API is deliberately not used: a bot cannot be added to most of these groups, and
bots cannot read group history.

---

## Contents

1. [What it does](#1-what-it-does)
2. [Architecture](#2-architecture)
3. [Requirements](#3-requirements)
4. [Quick start](#4-quick-start)
5. [Configuration reference](#5-configuration-reference)
6. [Command line reference](#6-command-line-reference)
7. [Database schema](#7-database-schema)
8. [Delivery guarantees and duplicate prevention](#8-delivery-guarantees-and-duplicate-prevention)
9. [Failure handling](#9-failure-handling)
10. [Deployment on Ubuntu](#10-deployment-on-ubuntu)
11. [Session security and revocation](#11-session-security-and-revocation)
12. [Optional dashboard](#12-optional-dashboard)
13. [Docker](#13-docker)
14. [Tests](#14-tests)
15. [Dependency policy](#15-dependency-policy)
16. [Known limitations](#16-known-limitations)
17. [Troubleshooting](#17-troubleshooting)
18. [Changelog](#18-changelog)

---

## 1. What it does

- **Authenticates** as your user account with `API_ID` / `API_HASH` from the environment,
  performs the one-time interactive login, and persists a session file with `0600`
  permissions (created under a restrictive umask).
- **Uses one canonical chat identity everywhere.** Every id — sources, destination, live
  events, SQLite rows, queue keys, checkpoints, reconciliation — is a Telethon *marked
  peer id* (`-100…` for channels, `-id` for basic groups), never a raw `entity.id`. This is
  what makes live events actually match the configured sources and keeps deduplication
  consistent between the historical and live paths.
- **Validates** every configured source at startup and gives a precise error when a chat is
  unresolvable or inaccessible. Invite links distinguish "already joined" from "valid but
  not joined". It never attempts to bypass group access controls.
- **Imports history** oldest-first, in deterministic chronological order, in bounded
  batches. A **scan checkpoint** (separate from the queue checkpoint) means skipped service
  messages are not re-read after a restart, and `HISTORY_MAX_COUNT` is a **total** budget
  across restarts rather than a per-run one.
- **Self-heals the import.** Transient failures are retried with backoff by a supervisor
  loop, and a watchdog resumes any import that is still incomplete — so a network blip no
  longer leaves history silently unfinished while live monitoring runs.
- **Monitors live messages** from the same groups. The live handler is registered
  *before* the historical import starts, so nothing arriving during the import is lost.
- **Forwards** eligible messages using native Telegram forwarding (preserving original
  attribution) by default. Copying is available only as an explicit opt-in.
- **Deduplicates** on `(source_chat_id, source_message_id)` and drives everything through a
  single persistent queue with a three-step lifecycle (`pending → leased → sending`), so
  historical and live discovery of the same message cannot be forwarded twice, and a crash
  cannot blindly re-forward a message Telegram may already have received.
- **Keeps albums whole.** An album that is still waiting to be delivered absorbs members
  discovered later (history and live paths converge on one unit); a unit already in flight is
  never merged into. Per-member destination ids are recorded when Telegram returns them, and
  never guessed when it does not.
- **Survives failures**: FloodWait is honoured with a global cooldown, transient errors
  back off exponentially with jitter, permanent errors dead-letter instead of looping,
  unrecognised errors get a deliberately short retry budget, and interrupted deliveries are
  marked ambiguous and settled by `reconcile`.
- **Reports real health**: heartbeat freshness, queue movement, per-source counters, import
  progress, ambiguous deliveries and recent errors — with credentials and session data
  never shown.
- **Narrows the duplicate window on retry.** Before re-sending a request that may already
  have landed, the forwarder looks for it at the destination (one bounded history read). If
  it is there, the unit is recorded as delivered instead of being sent twice.
- **Verifies itself.** `python -m collector selftest` runs 13 checks — schema, dedup,
  leasing, both crash-recovery paths, delivery bookkeeping, reconciliation, import-checkpoint
  safety, read-only access, canonical ids, config validation, integrity, and a **chaos run**
  that kills the process mid-forward and proves nothing is lost or duplicated. No pytest, no
  network, no Telegram account, and no third-party packages: if `aiosqlite` is missing the
  collector falls back to the standard library's `sqlite3`.

Non-goals: reading groups the account cannot access, bypassing protected-content
restrictions, evading Telegram anti-abuse systems, or providing "exactly-once" delivery
(see §8 for why that cannot be promised).

---

## 2. Architecture

```
                    ┌──────────────────────────────┐
   source groups    │  Telethon client (MTProto)   │
   (~10 chats) ───► │  one process, one session    │
                    └───────────┬──────────────────┘
                                │
             ┌──────────────────┴───────────────────┐
             │                                      │
   ┌─────────▼─────────┐                  ┌─────────▼──────────┐
   │ HistoryImporter   │                  │ LiveMonitor        │
   │ oldest→newest,    │                  │ NewMessage / Album │
   │ batched, resumable│                  │ Edited / Deleted   │
   │ retries + watchdog│                  │ canonical chat ids │
   └─────────┬─────────┘                  └─────────┬──────────┘
             │  discover_unit(marked_chat_id, messages, priority)│
             └──────────────────┬─────────────────────────────┘
                                ▼
                 ┌──────────────────────────────────────┐
                 │  SQLite (WAL)                        │
                 │  processed_messages   (dedup ledger) │
                 │  processing_queue     (lease+state)  │
                 │  import_progress      (scan ckpt)    │
                 │  application_settings (heartbeat)    │
                 └──────────────┬───────────────────────┘
                                │ claim → leased → sending
                    ┌───────────▼────────────┐
                    │ Forwarder workers (1–4)│
                    │ rate limiter + FloodWait│
                    └───────────┬────────────┘
                                ▼
                  private destination channel
```

**Key design decisions**

| Decision | Reason |
|---|---|
| One process, one Telethon client | An MTProto session must not be used by two processes; a second login can invalidate the first. |
| Marked peer ids everywhere (`collector/ids.py`) | `event.chat_id` is a marked id; `entity.id` is not. Mixing them silently drops live messages and breaks dedup. |
| SQLite-backed queue instead of an in-memory `asyncio.Queue` | Work survives restarts, and leases make abandoned work detectable. |
| `pending → leased → sending` lifecycle | An expired *leased* row was never sent (safe to retry); an expired *sending* row may have been delivered (must be reconciled, never blindly retried). |
| A queue row is a *delivery unit* (one message or one whole album) | Albums are forwarded in a single `forward_messages` call so Telegram keeps the grouping, and every member's destination id is recorded. |
| Discovery only writes; a single forwarder path delivers | Live and historical events cannot race each other into two forwards. |
| Live handler registered before history import | Messages sent during a multi-hour import are queued immediately. |
| `flood_sleep_threshold=0` | FloodWait is handled explicitly (persisted, visible in `status`) rather than sleeping invisibly inside Telethon. |
| All DB transactions go through a `_begin()` guard | A task cancelled mid-transaction cannot leave an open transaction that wedges every later write. |

**Module map**

```
collector/
  __main__.py        CLI entry point (argparse subcommands)
  config.py          env parsing, .env loading, structured validation
  db.py              aiosqlite layer, migrations, queue leases, read-only mode
  errors.py          failure classification, retry budgets, backoff
  ids.py             canonical chat-id helpers (the single source of truth)
  importer.py        historical import, albums, checkpoints, retries
  monitor.py         live handlers (new / album / edited / deleted)
  forwarder.py       rate limiter + delivery workers
  reconcile.py       ambiguous-delivery resolution against the destination
  service.py         wiring, startup order, watchdog, heartbeat, shutdown
  status.py          status report + health snapshot
  dashboard.py       optional authenticated read-only FastAPI app
  telegram_client.py client construction, login, entity resolution
  logging_setup.py   stdout + rotating files, credential redaction
```

---

## 3. Requirements

- Linux (developed against Ubuntu 22.04/24.04), Python **3.11+**
- A Telegram account that is already a member of every source group
- `API_ID` / `API_HASH` from <https://my.telegram.org> → *API development tools*
- A private channel you own for the destination (the account must be able to post there)
- Persistent disk for the session file, SQLite database and logs
- **Not** suitable for free tiers that sleep the process: MTProto needs a long-lived
  connection.

---

## 4. Quick start

```bash
git clone <your-repo> telegram-archive-collector
cd telegram-archive-collector

python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"          # add ",dashboard" if you want the web UI

cp .env.example .env
chmod 600 .env
$EDITOR .env                     # API_ID, API_HASH, DESTINATION_CHAT, SOURCE_GROUPS

python -m collector validate-config
python -m collector login        # one-time: phone, code, 2FA password
python -m collector list-groups  # find the canonical chat ids to configure
python -m collector validate-config --check-telegram

python -m collector import-history   # optional: pre-load history before going live
python -m collector run              # long-running: live + forwarding
```

`import-history` and `run` are safe to use in either order; `run` starts the import in the
background unless you pass `--no-history`.

---

## 5. Configuration reference

All settings come from environment variables (a local `.env` is loaded automatically).
See `.env.example` for a ready-to-edit template.

| Variable | Default | Meaning |
|---|---|---|
| `API_ID` | — | Telegram app id (integer) |
| `API_HASH` | — | Telegram app hash (32 hex chars) |
| `DESTINATION_CHAT` | — | Private channel to forward into (`@name` or `-100…`) |
| `SOURCE_GROUPS` | — | Comma/newline separated sources: `identifier`, `identifier\|alias`, `identifier\|alias\|disabled` |
| `DATA_DIR` | `./data` | Base directory for session, database and logs |
| `SESSION_PATH` | `$DATA_DIR/collector.session` | Telethon session file |
| `DATABASE_PATH` | `$DATA_DIR/collector.sqlite3` | SQLite database |
| `LOG_DIR` | `$DATA_DIR/logs` | Rotating log directory |
| `RESTRICT_PERMISSIONS` | `true` | Create data/log dirs `0700`, secrets `0600`, set umask `0077` |
| `FORWARD_MODE` | `forward` | `forward` = native forward; `copy` = re-send (loses attribution) |
| `ANNOTATE_SOURCE` | `false` | Post `[source] title · chat id · message id` before each forward |
| `INCLUDE_EDITS` | `false` | Annotate edits at the destination (never re-forwards) |
| `INCLUDE_DELETED` | `false` | Annotate deletions at the destination |
| `INCLUDE_SERVICE_MESSAGES` | `false` | Import/forward joins, pins, title changes |
| `INCLUDE_OUTGOING` | `false` | Include messages your account sent in source groups |
| `HISTORY_START_DATE` | — | ISO date; inclusive start of the history window |
| `HISTORY_END_DATE` | — | ISO date; inclusive end of the history window |
| `HISTORY_MAX_COUNT` | — | **Total** messages to scan per group, across restarts |
| `HISTORY_BATCH_SIZE` | `100` | Messages per database commit |
| `WORKER_CONCURRENCY` | `1` | Delivery workers (1–4; higher is not faster, it is riskier) |
| `QUEUE_POLL_INTERVAL` | `2` | Seconds an idle worker waits before re-polling |
| `LEASE_SECONDS` | `300` | Queue lease length; expired leases are recovered |
| `MAX_ATTEMPTS` | `5` | Transient retry budget per message |
| `VERIFY_BEFORE_RETRY` | `true` | Check the destination before retrying a possibly-delivered unit |
| `RETRY_VERIFY_SCAN_LIMIT` | `100` | Destination messages scanned for that check |
| `BACKOFF_BASE` / `BACKOFF_CAP` | `2` / `900` | Exponential backoff base and ceiling (seconds) |
| `MIN_FORWARD_INTERVAL` | `0.6` | Minimum spacing between Telegram write calls |
| `HEARTBEAT_INTERVAL` | `30` | Seconds between heartbeat writes (drives health checks) |
| `IMPORT_RECHECK_INTERVAL` | `300` | Seconds between watchdog passes over incomplete imports |
| `STALE_HEARTBEAT_SECONDS` | `180` | Age at which health reports "degraded" |
| `RECONCILE_SCAN_LIMIT` | `500` | Default destination-history window for `reconcile` |
| `EVENT_RETENTION_DAYS` | `30` | Prune audit events older than this (0 = keep everything) |
| `DASHBOARD_ENABLED` | `false` | Serve the optional dashboard |
| `DASHBOARD_HOST` / `DASHBOARD_PORT` | `127.0.0.1` / `8080` | Dashboard bind address |
| `DASHBOARD_USERNAME` / `DASHBOARD_PASSWORD` | — | Required when the dashboard is enabled |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_MAX_BYTES` / `LOG_BACKUP_COUNT` | `5242880` / `5` | Rotation policy |

Validation is **structured**, not text-matched: `Config.problems()` returns
`Problem(severity, field, message)` objects, and `load_config()` raises for *every* error
when credentials are required — so a detected misconfiguration (including "destination is
also a source") can never be reported yet silently started. Commands that only read local
state (`status`, `db-maintenance`, `dashboard`) load with `require_credentials=False` and
work without Telegram credentials.

---

## 6. Command line reference

```
python -m collector login                        # interactive first-time login
python -m collector list-groups [--json] [--limit N]
python -m collector validate-config [--check-telegram]
python -m collector import-history [--force] [--group IDENTIFIER]
python -m collector run [--no-history] [--force]
python -m collector status [--json]
python -m collector retry-failed [--include-ambiguous]
python -m collector reconcile [--limit N] [--dry-run] [--retry-unresolved]
python -m collector db-maintenance [--prune-events DAYS] [--vacuum]
python -m collector dashboard
```

| Command | What it actually does |
|---|---|
| `login` | Connects, prompts for phone/code/2FA, saves the session (0600). Idempotent: prints the current account if already authorised. |
| `list-groups` | Iterates dialogs and prints the **canonical** chat id, raw entity id, type, participant count and title — the practical way to find private-group ids. |
| `validate-config` | Structured validation with `[error]`/`[warning]` lines; with `--check-telegram` it also connects and resolves destination + every source. |
| `import-history` | Runs the resumable import with retries for all sources (or one with `--group`), writing scan checkpoints after each batch. `--force` re-scans groups already marked complete (dedup means no duplicate forwards). |
| `run` | Long-lived service: recovery, live monitoring, forwarder workers, background history import, heartbeat and watchdog. Handles SIGTERM/SIGINT for graceful shutdown. |
| `status` | Reads the database read-only (no Telegram connection): health, queue states, per-source counters, import progress, last success, recent failures and events. `--json` for monitoring. |
| `retry-failed` | Re-queues `permanently_failed` units (and `ambiguous` ones with `--include-ambiguous`) at priority 50. |
| `reconcile` | Releases expired leases, then matches `ambiguous` units against destination history using forward metadata. `--limit N` widens the window, `--limit 0` scans the entire history, `--retry-unresolved` re-queues anything not found (and warns that it can duplicate a forward made outside the window). |
| `db-maintenance` | `PRAGMA integrity_check`, WAL checkpoint, optional event pruning and `VACUUM`, plus a size report. |
| `selftest` | Runs 13 invariant checks against a temporary database, ending with a crash/restart chaos run. Needs no third-party packages at all (falls back to the stdlib `sqlite3` driver) — use it when pytest is not installable. |
| `dashboard` | Serves the optional authenticated read-only web UI. |

`--env-file PATH` (global) points at a different env file.

---

## 7. Database schema

Schema **version 2** (`schema_version` table, migration list in `collector/db.py`;
migrations are forward-only and run automatically on startup).

| Table | Purpose |
|---|---|
| `source_groups` | Configured sources plus resolved canonical chat id, title, last validation time and last live event time. |
| `processed_messages` | **Deduplication + ledger.** `UNIQUE(source_chat_id, source_message_id)`; stores `unit_key`, status, attempts, error class/message, destination chat id, destination message id **and** `destination_message_ids` (per-album-member map), message date, grouped id, reconcile result/timestamp, metadata. |
| `processing_queue` | Durable work queue. One row per delivery unit: `message_key` (unique), priority (10 = live, 50 = manual retry, 100 = history), state, `available_at`, `lease_until`, `worker_id`, attempts, and `annotation_message_id`/`annotation_sent_at`. |
| `import_progress` | Per-group import state: status, oldest/newest/last processed ids, **last scanned id**, seen/queued/skipped counters, attempts, timestamps, last error. |
| `application_settings` | Key/value settings that must survive restarts (including `heartbeat_at`). |
| `application_events` | Append-only audit trail: imports, retries, failures, ambiguity, reconciliations, start/stop. Prunable by age. |

**Queue states** (`processing_queue.state`):

| State | Meaning | Recovery on expiry |
|---|---|---|
| `pending` | Ready to be claimed. | — |
| `leased` | Claimed by a worker; **nothing sent yet**. | back to `pending` (safe) |
| `sending` | A Telegram request may be in flight. | `ambiguous` (never auto-claimed) |
| `done` | Delivered and recorded. | — |
| `ambiguous` | Outcome unknown; needs `reconcile`. | — |
| `dead` | Permanently failed. | `retry-failed` (explicit) |

Message statuses: `discovered` → `queued` → `processing` → `forwarded`, with
`retry_pending`, `permanently_failed` and `ambiguous` as the non-happy paths.
Import statuses: `pending`, `running`, `completed`, `retry_pending`, `stopped`,
`permanently_failed`.

Timestamps are stored in UTC (ISO-8601); queue scheduling uses epoch seconds. Indexes
cover queue readiness, leases, states, status lookups, `unit_key` joins and message dates.
Message *content* is not stored — the destination channel is the archive; only metadata
needed for deduplication, retry and audit is kept.

The database uses WAL mode with `synchronous=NORMAL` and `busy_timeout=10000`; all
multi-statement writes are serialised through a single asyncio lock and every transaction
begins through a guard that clears a stale transaction left by a cancelled task.

---

## 8. Delivery guarantees and duplicate prevention

**Telegram's forward and the local database cannot be committed atomically.** The forward
either happens before the row is marked forwarded, or after; there is no distributed
transaction. This implementation therefore promises **minimised, reconciled duplication**,
not exactly-once:

1. `discover_unit()` inserts `(chat_id, message_id)` with `INSERT OR IGNORE` and enqueues
   the unit with `INSERT OR IGNORE`. Discovery is idempotent — re-running the history
   import cannot duplicate anything. Album members that arrive later join the unit while it
   is still `pending` (so one album stays one delivery); once a unit is `leased`, `sending`,
   `done` or `ambiguous`, new members form their own unit instead.
2. A worker **leases** a queue row (one `BEGIN IMMEDIATE` transaction) before it may talk to
   Telegram, so two workers cannot deliver the same unit.
3. Immediately before the Telegram call the worker flips the row to **`sending`**. This is
   the whole trick: from that instant the delivery is no longer safe to retry blindly.
4. On success the unit is marked `forwarded`, the destination message id is stored, and for
   albums every member's destination id is stored too.
5. **Crash recovery is state-aware.** A lease that expires while `leased` goes back to
   `pending` (nothing was sent). A lease that expires while `sending` becomes **`ambiguous`**
   — a state no worker will ever claim — and `reconcile` settles it by scanning recent
   destination history for a matching `fwd_from` origin:
   - match found → the unit is marked `forwarded` with the real destination id;
   - no match → recorded as `unresolved` and left ambiguous unless you pass
     `--retry-unresolved`.
6. Before retrying a unit whose previous attempt failed *after* the request was issued, the
   forwarder checks the destination for evidence of it (`VERIFY_BEFORE_RETRY`). If it is
   there, the unit is marked delivered and the retry is suppressed — so a dropped response
   does not become a duplicate.
7. `retry-failed` re-queues dead letters explicitly; `--include-ambiguous` is the
   operator's acknowledgement that a duplicate is acceptable.

The residual duplicate window is therefore: a forward succeeded, the process died before
committing, *and* the destination scan window in `reconcile --limit` did not reach the
message. Widening `--limit` narrows it further. A graceful shutdown (`SIGTERM`) applies the
same rules per worker, so shutdown can never convert an in-flight delivery into a retry.

---

## 9. Failure handling

Failures are classified in `collector/errors.py` by Telethon exception name, so the policy
is testable without a network:

| Class | Examples | Policy |
|---|---|---|
| `rate_limit` | `FloodWaitError`, `SlowModeWaitError`, any RPC error carrying `seconds` | Wait exactly as told; a **global** cooldown pauses every worker. Budget `3 × MAX_ATTEMPTS`, then dead-letter. |
| `transient` | `RpcCallFailError`, `ServerError`, timeouts, connection resets | Exponential backoff with ±25 % jitter, up to `MAX_ATTEMPTS`. When the budget is exhausted the unit becomes `ambiguous` (it may have been delivered). |
| `unknown` | anything unrecognised | **Deliberately short budget (2 attempts)** with the exception type logged, then `ambiguous` — an unexpected response is not assumed safe to retry. |
| `permanent` | `ChatForwardsRestrictedError` (protected content), `ChatWriteForbiddenError`, `MessageIdInvalidError`, `AuthKeyDuplicatedError` | Dead-lettered immediately with the reason recorded. Never retried. Protected content is never circumvented. |
| `inaccessible` | `ChannelPrivateError`, `ChatAdminRequiredError`, `UserNotParticipantError` | Dead-lettered immediately. |
| `ambiguous` | lease expiry while `sending`, exhausted retries | Settled by `reconcile`. |

One problematic message can never stop the collector: every delivery is wrapped
individually, handler exceptions are swallowed and logged, and each group's import is
isolated. Imports are supervised: transient failures retry with backoff, permanent ones
stop retrying, and the watchdog re-runs anything still incomplete every
`IMPORT_RECHECK_INTERVAL`. Shutdown (`SIGTERM`) stops the importer at a batch boundary,
unregisters handlers, cancels workers (each releases its leases in a `finally`), sweeps
remaining leases, checkpoints the WAL and closes the database and session.

---

## 10. Deployment on Ubuntu

Target: a small always-on VM (1 vCPU / 1 GB is enough). Persistent disk is mandatory.

### 10.1 System user and directories

```bash
sudo adduser --system --group --home /opt/telegram-collector telegram-collector
sudo mkdir -p /opt/telegram-collector /etc/telegram-collector /var/lib/telegram-collector
sudo chown -R telegram-collector:telegram-collector /opt/telegram-collector /var/lib/telegram-collector
sudo chmod 750 /etc/telegram-collector
sudo chmod 700 /var/lib/telegram-collector        # state dir: owner-only
```

### 10.2 Install the application

```bash
sudo -u telegram-collector git clone <your-repo> /opt/telegram-collector
cd /opt/telegram-collector
sudo -u telegram-collector python3 -m venv .venv
sudo -u telegram-collector .venv/bin/pip install --upgrade pip
sudo -u telegram-collector .venv/bin/pip install -e ".[dashboard]"   # or without [dashboard]
```

### 10.3 Configure secrets

```bash
sudo install -m 600 -o root -g root /dev/null /etc/telegram-collector/collector.env
sudo nano /etc/telegram-collector/collector.env
```

```ini
API_ID=123456
API_HASH=0123456789abcdef0123456789abcdef
DESTINATION_CHAT=-1001234567890
SOURCE_GROUPS=-1001111111111|group one,-1002222222222|group two
DATA_DIR=/var/lib/telegram-collector
SESSION_PATH=/var/lib/telegram-collector/collector.session
DATABASE_PATH=/var/lib/telegram-collector/collector.sqlite3
LOG_DIR=/var/lib/telegram-collector/logs
RESTRICT_PERMISSIONS=true
```

Then validate and log in **as the service user**, so the session file is owned by it:

```bash
sudo -u telegram-collector env $(grep -v '^#' /etc/telegram-collector/collector.env | xargs) \
  /opt/telegram-collector/.venv/bin/python -m collector validate-config
sudo -u telegram-collector env $(grep -v '^#' /etc/telegram-collector/collector.env | xargs) \
  /opt/telegram-collector/.venv/bin/python -m collector login
```

### 10.4 systemd service

```bash
sudo cp deploy/telegram-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now telegram-collector
sudo systemctl status telegram-collector
```

The unit restarts automatically on failure (`Restart=always`, `RestartSec=10`), runs as the
unprivileged `telegram-collector` user with `UMask=0077`, writes only inside
`/var/lib/telegram-collector`, sends SIGTERM and allows 90 s to shut down cleanly, and
journals stdout/stderr.

### 10.5 Verify the session survives restarts

```bash
sudo systemctl restart telegram-collector
sudo journalctl -u telegram-collector -n 40 --no-pager
# expect: "connected to Telegram as …" with no login prompt
ls -l /var/lib/telegram-collector/     # collector.session + collector.sqlite3 persist
```

Because the session file and the database live outside the code tree and outside the
process lifetime, a restart (or a redeploy, or a reboot) loses neither. The queue is
recovered from SQLite: `leased` work is re-queued and `sending` work is parked as
ambiguous for `reconcile`.

### 10.6 File permissions to verify

```bash
stat -c '%a %n' /var/lib/telegram-collector /var/lib/telegram-collector/* /etc/telegram-collector/collector.env
# expect: 700 on the directory, 600 on the session, database, WAL files and env file
```

The application enforces this itself (`0700` dirs, `0600` files, umask `0077`), but verify
after the first run — that is the only way to catch a pre-existing file with wider
permissions. Backups and snapshots of `/var/lib/telegram-collector` deserve the same
treatment: they contain a credential-equivalent session file.

### 10.7 Backup, restore and maintenance

```bash
# consistent backup (stop first, or use sqlite3 .backup for a hot copy)
sudo systemctl stop telegram-collector
sudo tar czf /var/backups/collector-$(date +%F).tgz -C /var/lib telegram-collector
sudo systemctl start telegram-collector

# hot backup without stopping the service
sudo -u telegram-collector sqlite3 /var/lib/telegram-collector/collector.sqlite3 \
  ".backup '/var/backups/collector-$(date +%F).sqlite3'"

# weekly maintenance: integrity check, WAL checkpoint, prune old events
cd /opt/telegram-collector
sudo -u telegram-collector .venv/bin/python -m collector db-maintenance --prune-events 30
```

**Restore**: stop the service, move the old `collector.sqlite3*` aside, extract the backup,
keep ownership (`chown telegram-collector:telegram-collector`), start the service and check
`collector status`. The session file can be restored the same way — or simply re-created
with `login`.

Monitoring that matters: disk space for `$DATA_DIR` (WAL grows during heavy imports),
`collector status` health (`degraded` when the heartbeat is stale, the queue is old, or
deliveries are ambiguous), and `journalctl -u telegram-collector`.

### 10.8 Day-to-day operations

```bash
sudo systemctl status telegram-collector
sudo systemctl restart telegram-collector
sudo journalctl -u telegram-collector -f
sudo journalctl -u telegram-collector --since "1 hour ago"

# status without disturbing the service (reads the database read-only)
cd /opt/telegram-collector
sudo -u telegram-collector .venv/bin/python -m collector status

# retry / reconcile
sudo -u telegram-collector .venv/bin/python -m collector retry-failed
sudo -u telegram-collector .venv/bin/python -m collector reconcile --limit 1000
```

### 10.9 Updating the application

```bash
cd /opt/telegram-collector
sudo -u telegram-collector git pull
sudo -u telegram-collector .venv/bin/pip install -e ".[dashboard]"
sudo systemctl restart telegram-collector
sudo -u telegram-collector .venv/bin/python -m collector status
```

Database migrations run automatically on startup and are forward-only; back up
`collector.sqlite3` and `collector.session` before a major upgrade.

> Free cloud tiers that suspend idle processes are not suitable — the MTProto connection
> drops and forwarding stalls. Use a small paid VM, a home server, or a Raspberry Pi.

---

## 11. Session security and revocation

- `API_ID` / `API_HASH` are read from the environment (or a root-owned
  `EnvironmentFile`), never from code, and are redacted from logs by a logging filter that
  scrubs 32-hex strings, `api_hash=…`, `code=…` and `password=…` patterns.
- The session file is created under umask `0077` and re-`chmod`ed to `0600` on every start
  (`harden_session()`); the database and its `-wal`/`-shm` files are `0600` too. `.gitignore`
  excludes `*.session`, `*.session-journal`, `*.sqlite3`, logs, `.env` and virtualenvs.
- Never paste the session file, `API_HASH` or a login code into a chat, issue tracker or
  support request.

**If the session may be compromised**

1. Stop the collector: `sudo systemctl stop telegram-collector`.
2. Revoke from Telegram itself — *Settings → Devices → Active Sessions* → terminate the
   session (this invalidates the auth key server-side; deleting the local file alone does
   not).
3. Rotate the app credentials in <https://my.telegram.org> if the hash leaked, then update
   `/etc/telegram-collector/collector.env`.
4. Delete the local artefacts:
   `sudo rm -f /var/lib/telegram-collector/collector.session*`.
5. Log in again: `python -m collector login`, restart the service, and check
   `journalctl` for a fresh "connected to Telegram as …" line.

**Replacing a session without revoking it** (e.g. moving to a new host): stop the old
service first, then run `login` on the new host. Two live processes sharing one account
fight over the same auth key and Telegram may terminate one of them
(`AuthKeyDuplicatedError` is classified as permanent for exactly this reason).

---

## 12. Optional dashboard

The core collector works without it. The dashboard is **read-only**: it opens the database
with `mode=ro`, never changes the journal mode and never runs migrations — so a `:ro`
volume mount is enough and it cannot disturb the collector.

```bash
pip install -e ".[dashboard]"
# in .env
DASHBOARD_ENABLED=true
DASHBOARD_HOST=127.0.0.1
DASHBOARD_PORT=8080
DASHBOARD_USERNAME=admin
DASHBOARD_PASSWORD=<long random string>
python -m collector dashboard
```

- HTTP Basic authentication is mandatory; the app refuses to start without credentials and
  compares them with `secrets.compare_digest`.
- **All** interpolated values are HTML-escaped (`html.escape`), including source titles,
  identifiers, error text and the status report — a malicious group title cannot inject
  markup.
- Bind to `127.0.0.1` and front it with a TLS reverse proxy (Caddy/nginx) if remote access
  is needed.
- No Telegram credentials, API hash or session data are ever sent to the browser.

Endpoints: `/` (HTML), `/api/status`, `/api/events`, `/health`, `/ready`.

`/health` is **liveness only** — it says the HTTP process is up. `/ready` reports actual
collector health: heartbeat freshness, queue movement, ambiguous deliveries and incomplete
imports, returning **503** when the collector is not healthy. Use `/ready` for monitoring
and container health checks.

### 12.1 Hosting the dashboard on a serverless platform (Vercel)

`app.py` at the repository root exports a module-level `app`, so a platform that expects an
ASGI entrypoint (`app:app`, already declared under `[tool.vercel]` in `pyproject.toml`) can
serve the dashboard without the CLI. `requirements.txt` carries the dashboard dependencies
for hosts that install from it.

```bash
vercel deploy
# environment variables to set in the project:
#   DASHBOARD_USERNAME, DASHBOARD_PASSWORD   (mandatory — no anonymous access)
#   DATABASE_PATH                            (a mounted/synced copy of collector.sqlite3)
```

**What a serverless deployment cannot do.** It serves the dashboard and nothing else. The
collector is a long-running daemon: it holds a Telethon session, keeps a live message
handler open and forwards into your channel. Serverless functions have no persistent
process, no writable disk (only `/tmp`) and no long-lived network connection, so the
collector must keep running on a service (systemd or Docker, sections 10 and 13).

Without `DATABASE_PATH` pointing at a real ledger the dashboard starts on an empty database
under `/tmp/data`: `/health` returns 200, `/ready` returns 503 (`no heartbeat recorded`)
and the pages render an empty archive. If `DASHBOARD_USERNAME`/`DASHBOARD_PASSWORD` are
missing, the app answers 503 with setup guidance rather than serving unauthenticated data.

---

## 13. Docker

```bash
cp .env.example .env && chmod 600 .env   # fill in values
docker compose build

# one-time interactive login (writes into the named volume)
docker compose run --rm collector python -m collector login
docker compose run --rm collector python -m collector validate-config --check-telegram

docker compose up -d collector
docker compose logs -f collector

# optional dashboard on 127.0.0.1:8080 (read-only database mount)
docker compose --profile dashboard up -d
```

State lives in the `collector-data` volume (`/data`): session, SQLite database and logs.
`stop_grace_period: 90s` matches the systemd `TimeoutStopSec`, so leases are released
cleanly. The dashboard container mounts the volume read-only and works because the
dashboard only ever opens the database in read-only mode.

### 13.1 Android (Termux)

The collector also runs on a phone under Termux, supervised by runit instead of systemd.
`bash scripts/termux-setup.sh` installs and wires it up; **`docs/TERMUX.md`** covers the
Android-specific traps (no `uvloop`, keeping state off `/sdcard`, wake locks and battery
exemption, autostart at boot) and how to move the data directory to a VPS later.

---

## 14. Tests

Unit tests (no network, no Telegram account needed):

```bash
pip install -e ".[dev]"
pytest -q                    # 189 tests, ~22s
pytest -q -m "not slow"      # 174 tests, ~15s (skips chaos + full self-test)
python tests/run_suite.py    # one file at a time, 180s cap each, PASS/FAIL per file
```

`python -m collector selftest` proves the core invariants on any host with **no pytest, no
network and no third-party packages** — use it when the environment cannot install anything.

Every test is bounded: an autouse watchdog turns a hang into a named failure instead of a
stalled run (`COLLECTOR_TEST_TIMEOUT=300` raises the 120s default). `tests/run_suite.py` is
the fallback for slow or constrained machines — it isolates each file so one stalled test
cannot hide the other fourteen.

**189 tests**, covering the pieces that actually break in production:

| File | Covers |
|---|---|
| `test_config.py` | env parsing, source grammar, structured problems, fatal-vs-warning severity, credential-free commands, permission hardening |
| `test_errors.py` | FloodWait / protected content / private channel / transient / programming / unknown classification, retry budgets, backoff bounds |
| `test_ids.py` | marked peer id vs raw entity id for channels, basic groups and users; event id matching; `fwd_from` peer reconstruction |
| `test_db.py` | dedup idempotency, album units, atomic leasing, `leased`→`pending` vs `sending`→`ambiguous` recovery, graceful release, annotations, retry/dead-letter, checkpoints, stats, pruning, read-only mode, file permissions, migrations |
| `test_importer.py` | chronological order, album splitting, scan-checkpoint resume, **HISTORY_MAX_COUNT across restarts**, inclusive date boundaries, transient retry then success, permanent failure not retried, watchdog resume, per-group isolation |
| `test_forwarder.py` | native forward default, single-call album with per-member destination ids, annotation not duplicated on retry, protected content dead-lettered, transient retry, FloodWait global pause, unknown-error short budget, exhausted retries → ambiguous, copy mode (text + media), **rate-limiter cancellation safety** |
| `test_monitor.py` | canonical-id matching (raw id ignored), album units, live-event timestamps, edit/delete annotations, handler exception containment |
| `test_reconcile.py` | forward-origin matching for channels and basic groups, unresolved recorded, dry-run, `--retry-unresolved`, stale-lease recovery |
| `test_service.py` | startup ordering, **cleanup on startup failure**, crash recovery on boot, shutdown re-queue, start/stop audit events |
| `test_dashboard.py` | auth required, **HTML escaping**, `/health` vs `/ready`, 503 when stale, read-only database (never writes) |
| `test_cli.py` | credential-free `status`/`db-maintenance`, `--json` output, clean failure when the database is missing, help completeness |
| `test_telegram_client.py` | username/numeric resolution, invite already-joined vs not-joined vs expired, canonical dialog ids |
| `test_selftest.py` | the built-in self-check passes, and it fails loudly when leasing is broken |
| `test_chaos.py` | crash/restart chaos runs across seeds; the harness detects loss when recovery is disabled and duplicates when reconciliation retries blindly; stdlib-driver round trip |

Marked `slow` (skipped by `-m "not slow"`): the chaos runs, the full self-test, the CLI
self-test, and the rate-limiter timing tests.

**Real-Telegram integration tests** are deliberately separate — see
[`docs/INTEGRATION_TESTS.md`](docs/INTEGRATION_TESTS.md). They need a test source group and
a test destination channel you control, plus a real session, so they are never part of the
default `pytest` run.

What unit tests cannot prove: real Telegram behaviour, real FloodWait timings, multi-day
soak stability, and cloud/VM reboot behaviour. §14 of the integration doc lists the manual
scenarios that cover those.

---

## 15. Dependency policy

`pyproject.toml` constrains every runtime dependency to a compatible range
(`telethon>=1.34,<2.0`, `aiosqlite>=0.19,<1.0`, `python-dotenv>=1.0,<2.0`,
`fastapi>=0.110,<1.0` for the dashboard). Rationale: Telethon's 1.x API surface is what the
code is written against — a 2.x release must be reviewed before adoption.

Updating:

```bash
pip list --outdated
pip install --upgrade "telethon<2.0"        # stay inside the pinned range
pytest -q                                   # must pass before deploying
```

For a reproducible deployment, freeze after a successful test run:

```bash
pip freeze > requirements.lock.txt
```

and install with `pip install -r requirements.lock.txt` in production. Re-test after any
upgrade; Telegram RPC error names occasionally change and are the first thing to check if
classification looks wrong.

---

## 16. Known limitations

1. **No exactly-once delivery.** Telegram forwarding and SQLite cannot be committed
   atomically. The design minimises and then reconciles duplicates (§8), and is explicit
   that a residual window exists.
2. **Protected content cannot be forwarded.** Channels with forwarding disabled raise
   `ChatForwardsRestrictedError`; those units are dead-lettered with the reason recorded.
   Copying to work around this is not implemented on purpose.
3. **Copy mode loses attribution and does not preserve album grouping** — each album member
   is sent individually. Use `FORWARD_MODE=forward` (the default) to keep grouping.
4. **Edits and deletions are annotations, not rewrites.** With `INCLUDE_EDITS` /
   `INCLUDE_DELETED` the collector posts a short note at the destination; it never edits or
   deletes previously forwarded messages (that would be destructive).
5. **Destination history scan is bounded.** `reconcile --limit` (default 500) limits how
   far back matching looks; a very busy destination may need a larger window. "Not found"
   means "not in the window", which is why unresolved units stay ambiguous instead of being
   re-sent automatically.
6. **Resume granularity is a message id.** In `reverse` mode the import resumes with
   `min_id=<last scanned id>`; a message deleted between runs is simply absent.
7. **One session per account.** Running two instances (e.g. old and new host) against the
   same account can get one terminated by Telegram.
8. **Service messages may still fail to forward** even when enabled, because Telegram
   refuses to forward some of them; they are dead-lettered rather than retried forever.
9. **No message content archive.** The database stores metadata only — the destination
   channel is the archive. Losing the channel loses the content.
10. **Rate limits are respected, not circumvented.** With `WORKER_CONCURRENCY>1` you will
    simply hit FloodWait sooner; the global cooldown handles it, but 1 worker is the sane
    default for a user account.
11. **An album split across the history/live boundary can still be delivered in two parts.**
    Members discovered while the unit is still `pending` are merged into it, so the common
    case yields one album. If a member arrives after delivery has started, it forms a new
    unit with the remaining members (deliberately — the alternative is either losing it or
    duplicating the members already sent). Grouping is preserved within each part.
12. **Per-album-member destination ids depend on Telegram's response.** When
    `forward_messages` returns one message per input, every member is mapped. When it
    returns fewer, only the first id is recorded and a warning is logged — a guessed mapping
    would be worse than a missing one.
13. **Health is only as good as the heartbeat.** If the whole VM is frozen, `/ready` will
    report degraded only once something can read the database again.

---

## 17. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `The Telegram session is not authorised` | No session, or it was revoked | `python -m collector login` |
| `AuthKeyDuplicatedError` | The session was used from two places | Revoke the session in Telegram, log in again, run one instance only |
| `cannot resolve '…': ChannelPrivateError` | Account is not a member (or left) | Join the group with this account; `list-groups` to confirm |
| `… has NOT joined` | Invite link is valid but you never joined | Join manually with this account, then re-run |
| `chat id … is unknown to this session` | Private-group id needs a cached access hash | Run `list-groups` once, or use the invite link |
| Live messages arrive but are ignored | Source id in `.env` is a raw id, not a marked id | Use the `chat id` column from `list-groups` (`-100…`) |
| `destination … is also configured as a source` | Misconfiguration | Fix `SOURCE_GROUPS` / `DESTINATION_CHAT` |
| Everything stalls for minutes | FloodWait global cooldown | Expected; `status` shows `retry_pending`, logs show the wait |
| `ambiguous` count keeps growing | Crashes during forwarding | Run `collector reconcile --limit 1000`; it settles what it can find |
| `reconcile` reports units as unresolved | The forward is older than the scan window | Re-run with a larger `--limit`, or `--limit 0` to scan the whole history |
| Suspected missing album members after a crash | Import checkpoint vs carried album | Fixed in 1.2.0; verify with `python -m collector selftest` |
| `permanently_failed` with "forwards restricted" | Protected content | Nothing to do; forwarding is not bypassed |
| Import stuck at `retry_pending` | Network/Telegram problem | Watchdog retries automatically; check logs, then `import-history --group …` |
| `cannot read status: database … does not exist` | Collector never ran | `python -m collector run` once (or point `DATABASE_PATH` correctly) |
| Dashboard returns 401 | Wrong credentials | Check `DASHBOARD_USERNAME` / `DASHBOARD_PASSWORD` |
| `/ready` returns 503 | Collector unhealthy (stale heartbeat, ambiguous, stalled queue) | Read the `problems` list in the response or run `collector status` |
| `database is locked` | Two processes writing the same file | One writer at a time (service or CLI, not both) |

Logs: `journalctl -u telegram-collector -f` and `${LOG_DIR}/collector.log` (rotated).

---

## 18. Changelog

### 1.3.1 — suite hygiene, and a real leak behind the "pytest timed out"

The review reported the full pytest suite timing out in its environment. It was not a slow
suite — it was a bug: when a **read-only** database open failed schema verification, the
connection was never closed, so aiosqlite's worker thread stayed alive and the interpreter
waited on it at exit. `pytest` printed its summary in 20 s and then the process hung for
about three minutes. That is exactly what a runner reports as a timeout.

| Item | What changed |
|---|---|
| Leaked connection on a failed read-only open | `Database.connect()` closes the connection and clears it before re-raising; regression test asserts no driver thread survives. Suite wall-clock went from ~200 s to ~22 s |
| A hang can no longer masquerade as a slow suite | Autouse watchdog per test (SIGALRM, 120 s default, `COLLECTOR_TEST_TIMEOUT` to raise it) — a stalled test fails with its own name |
| Constrained environments | `tests/run_suite.py` runs one file at a time with a per-file cap and a PASS/FAIL line per file |
| Long suite on slow hardware | Chaos, self-test and timing tests marked `slow`; `pytest -q -m "not slow"` is the quick pass |

Also worth noting: the third review ran `python -m collector selftest` and reported 13/13
passing, which is the first time this project's behaviour has been verified outside my own
environment.

Tests: 188 → 189.

### 1.3.0 — third-review follow-up

The review rated 1.2.0 at 8.5/10 and named three remaining items: album edge cases needing
real-world tests, the inherent ambiguity of external delivery, and — the one that kept
recurring — that the reviewer could not run the tests because `aiosqlite` was not installed
and there was no network access.

| Item | What changed |
|---|---|
| "Could not run the tests" | Two answers. **A stdlib fallback driver**: if `aiosqlite` is missing the collector uses `sqlite3` in a worker thread, so it runs on a bare Python install. And **`python -m collector selftest`** now covers 13 invariants including a chaos run — verified working with `aiosqlite` deliberately blocked |
| "Album handling needs real-world tests" | Unit coverage extended to the restart case: a member discovered while the unit is still `pending` is merged (even across a restart), a unit in retry backoff is not merged, an in-flight unit is not merged. The chaos harness also exercises albums as units. Real Telegram behaviour still needs the integration scenarios — that is not something code can self-certify |
| "Ambiguous forwarding cannot be completely eliminated" | True, and still stated as such. But the window is now smaller: **verify-before-retry** checks the destination before re-sending a possibly-delivered unit, so a dropped response no longer becomes a duplicate. Startup also sweeps *all* leases (a fresh process holds none), so in-flight units are reconciled immediately instead of waiting out the lease |
| Crash consistency, generally | New `collector/chaos.py`: drives the real queue/forwarder/reconcile code through simulated process deaths before and after Telegram accepts a forward, restarting and reconciling each round. 20 seeds × 30 messages × 8 crash cycles: **0 lost, 0 duplicated**. The suite also asserts the harness *detects* loss and duplicates when recovery is deliberately broken |

Tests: 175 → 188.

### 1.2.0 — second-audit fixes

Follow-up review rated the previous build 7.5/10 and listed five remaining issues. Each was
reproduced against the code before changing it; the first one was reproduced as a failing
scenario (`checkpoint = 2`, carried member never queued, still lost after restart).

| # | Severity | Finding | Fix |
|---|---|---|---|
| 1 | high | The import checkpoint advanced past an album member still held in memory, so a crash between batches skipped it permanently | The durable checkpoint never moves past an uncommitted member: it stops at `carry[0].id - 1`, so the member is re-read on resume. Regression test uses a `BaseException` crash so no cleanup path can mask it |
| 2 | high | `reconcile` derived the destination id from `destination.id` instead of the canonical helper | Uses `canonical_id(destination)`; test uses a channel whose raw id differs from its marked id |
| 3 | medium-high | A late album member always formed a new unit, splitting albums discovered by both history and live paths | Members join the unit while it is still `pending` (priority upgraded to the higher one); `leased`/`sending`/`done`/`ambiguous` units are never merged into |
| 4 | medium | The reconciliation window was bounded with no way to widen it, and forced retries could silently duplicate | `--limit N` (default from config), `--limit 0` scans the whole history, the window is reported in the summary, and `--retry-unresolved` prints an explicit duplicate warning |
| 5 | medium | Album member→destination mapping was unverified against real Telegram behaviour | Never guessed: when Telegram returns fewer messages than it was given, only the first id is stored and a warning is logged (tested) |

Also in this release: **`python -m collector selftest`** — 12 invariant checks on a throwaway
database, requiring only the runtime dependencies. It exists because both audits could not
install pytest and therefore could not verify the suite; this gives a reviewer a way to prove
the persistence and recovery logic on their own host. It earned its place immediately by
catching a real bug introduced during this round (`scan_window` used before assignment in
`reconcile`), which no existing test covered.

Tests: 161 → 175.

### 1.1.0 — audit fixes

Full-code audit follow-up. Every finding was reproduced against the code before fixing.

| # | Severity | Finding | Fix |
|---|---|---|---|
| 1 | critical | Chat ids were raw `entity.id` for sources but marked ids for events → live messages ignored, dedup/reconcile inconsistent | `collector/ids.py` is now the only id source; all ids are marked peer ids |
| 2 | critical | Crash after Telegram accepted a forward → row returned to `pending` → re-forwarded | `leased` vs `sending` states; expired `sending` leases become `ambiguous`, which workers never claim |
| 3 | high | Validation errors detected but not enforced (English text matching) | Structured `Problem` list; `load_config` raises on every error |
| 4 | high | A transient error ended the background import permanently | Supervisor retries with backoff; watchdog resumes incomplete imports |
| 5 | high | `HISTORY_MAX_COUNT` reset on every restart | Persisted scan counter; the limit is a total budget |
| 6 | high | Albums: partial discovery, only one destination id | Late members form their own unit; per-member destination ids stored |
| 7 | high | Rate limiter leaked its lock when cancelled → deadlock | Cancellation-safe acquisition; regression test |
| 8 | high | Dashboard interpolated unescaped values into HTML | `html.escape` on every value |
| 9 | high | Dashboard opened the database read-write (broke `:ro` mounts) | `Database(read_only=True)`: `mode=ro`, no journal change, no migrations |
| 10 | high | Session/DB permissions only partially restricted | umask `0077`, `0700` dirs, `0600` files including `-wal`/`-shm` |
| 11 | high | Destination id formats inconsistent between forwarder and reconcile | Canonical helper used for sources, destination, events and reconciliation |
| 12 | medium | Annotation could be posted twice on retry | Annotation tracked per queue row |
| 13 | high | Reconciliation could not resolve every ambiguous delivery | Canonical ids, all album ids stored, configurable window, inconclusive results recorded |
| 14 | medium | Invite links did not distinguish joined vs joinable | `InviteNotJoined` vs expired/invalid with distinct messages |
| 15 | medium | Scan checkpoint lagged behind skipped messages | Separate `last_scanned_message_id`, plus seen/queued/skipped counters |
| 16 | medium | Date-window behaviour unverified | Inclusive start/end documented and tested |
| 17 | medium | Copy mode untested | Tests for text and media; documented limitations |
| 18 | medium | Unknown exceptions retried like network errors | `UNKNOWN` class with a 2-attempt budget, then ambiguous |
| 19 | medium | Missing SQLite operational safeguards | `db-maintenance` (integrity check, WAL checkpoint, pruning, vacuum), backup/restore docs, size in `status` |
| 20 | medium | Startup failure could leak resources | Structured startup with full teardown on any failure; tested |
| 21 | medium | `/health` claimed health without checking anything | `/health` = liveness, `/ready` = real health with 503 |
| 22 | medium | Insufficient observability | Per-source stats, oldest-pending age, live-event times, import attempts, heartbeat |

Tests went from 65 to 161, including every regression the audit asked for. Test count is
not the point — the two critical ones were reproduced as failing behaviour first, then
fixed.

### 1.0.0 — initial release
