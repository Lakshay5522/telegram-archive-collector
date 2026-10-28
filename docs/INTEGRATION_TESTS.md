# Real-Telegram integration tests

These tests exercise the collector against real Telegram infrastructure. They need a live
session, so they are **not** part of the default `pytest` run (which must stay fully mocked
and offline — it currently has 188 passing tests). Start with the portable check, which
needs no third-party packages at all:

```bash
python -m collector selftest      # 13 invariant checks + a crash/restart chaos run
```

The final check in that run kills the process mid-forward, restarts it, reconciles, and
asserts every message reached the destination exactly once. That is the same property the
scenarios below verify against real Telegram.

Everything here is done with accounts and channels **you control**. Do not point these
tests at groups you do not own or administer, and do not use them to test forwarding into
channels you are not allowed to post in.

## 1. Prepare the fixtures

1. Create a **test source group** (private is fine) and add your account.
2. Post ~20 messages of mixed types: text, a photo, a 3-message album, a document, a voice
   note, a sticker, and one service message (e.g. pin something).
3. Create a **test destination channel** that you own; make sure the account can post.
4. Use a **separate session and database** so production state is untouched:

```bash
mkdir -p /tmp/tac-test
cat > /tmp/tac-test/.env <<'EOF'
API_ID=...
API_HASH=...
DESTINATION_CHAT=-100...            # test destination channel
SOURCE_GROUPS=-100...|test source   # test source group
DATA_DIR=/tmp/tac-test/data
SESSION_PATH=/tmp/tac-test/data/test.session
DATABASE_PATH=/tmp/tac-test/data/test.sqlite3
LOG_DIR=/tmp/tac-test/data/logs
WORKER_CONCURRENCY=1
MIN_FORWARD_INTERVAL=1.0
HEARTBEAT_INTERVAL=10
IMPORT_RECHECK_INTERVAL=60
LOG_LEVEL=DEBUG
EOF
chmod 600 /tmp/tac-test/.env
export ENVF="--env-file /tmp/tac-test/.env"
python -m collector $ENVF login
python -m collector $ENVF validate-config --check-telegram
```

## 2. Scenarios

### A. Canonical chat ids (the audit's critical item 1)

```bash
python -m collector $ENVF list-groups | head -20
```

Expect: the `chat id` column is a **marked** id (`-100…` for channels/supergroups, `-…` for
basic groups). Paste that value into `SOURCE_GROUPS`, restart, then post a message in the
source group and confirm it appears in the destination **within seconds**. If live messages
are silently ignored, the configured id is a raw id — this is the exact bug the audit found.

Also confirm `status` shows the same id in "source groups" and that the `last live event`
timestamp advances after each posted message.

### B. History import is chronological and idempotent

```bash
python -m collector $ENVF import-history
python -m collector $ENVF status
python -m collector $ENVF import-history --force   # re-scan
```

Expect: destination receives the messages oldest → newest; the album stays grouped; the
second import adds **zero** new queue rows (dedup on `(chat, message_id)`).

### C. A message arriving during the historical import is not lost

1. Start the import on a group with a few hundred messages.
2. While it runs, post a new message in the source group.
3. Expect: forwarded promptly (live priority 10 jumps the history queue at priority 100),
   exactly once.

### D. Crash after forwarding but before the database records success

This is the audit's critical item 2 — the duplicate-forwarding window.

```bash
# simulate: a unit was claimed, marked sending, and the process died
python - <<'PY'
import sqlite3, time
db = sqlite3.connect("/tmp/tac-test/data/test.sqlite3")
row = db.execute("SELECT id, message_key FROM processing_queue WHERE state='pending' LIMIT 1").fetchone()
db.execute("UPDATE processing_queue SET state='sending', lease_until=?, worker_id='dead' WHERE id=?",
           (time.time() - 5, row[0]))
db.execute("UPDATE processed_messages SET status='processing' WHERE unit_key=?", (row[1],))
db.commit()
print("parked", row)
PY

python -m collector $ENVF status                 # expect queue: sending=1
# restart the collector: the lease expires and the unit becomes ambiguous, NOT pending
python -m collector $ENVF run --no-history &
sleep 10
python -m collector $ENVF status                 # expect queue: ambiguous=1, pending unchanged
python -m collector $ENVF reconcile --limit 200 --dry-run
```

Expect: the unit is reported ambiguous; `reconcile` finds the matching forwarded message in
the destination and marks it `forwarded` **without sending a duplicate**. Then run it
without `--dry-run` and confirm `ambiguous` drops to 0.

Critical negative check: at no point does the destination receive that message twice.

### E. Rate limits

Set `MIN_FORWARD_INTERVAL=0` and `WORKER_CONCURRENCY=4` against a large history. Expect
`FloodWaitError` in the logs, a global pause (all workers), and units in `retry_pending` —
never a crash, never a burst of parallel forwards.

### F. Protected content

Forward a message from a channel that restricts forwarding (or enable "restrict saving
content" in the test group). Expect `permanently_failed` with
`ChatForwardsRestrictedError`, and `retry-failed` does not produce a forward.

### G. Graceful shutdown, then resume

```bash
python -m collector $ENVF run
# press Ctrl-C while messages are flowing
python -m collector $ENVF status
```

Expect: logs show "shutting down" and a "shutdown sweep" line; `status` shows remaining work
as `queued` (not stranded as `processing` or `leased`); a restart continues from there. Any
unit that was genuinely mid-request appears as `ambiguous` — that is correct, not a bug.

### H. Album forwarding

Post a 5-photo album. Expect exactly one `forward_messages` call carrying all ids, a grouped
album at the destination, and a destination id recorded **per member**:

```bash
sqlite3 /tmp/tac-test/data/test.sqlite3 \
  "SELECT source_message_id, destination_message_id FROM processed_messages
   WHERE grouped_id IS NOT NULL ORDER BY source_message_id;"
```

### I. Dashboard safety (audit items 8 and 9)

```bash
# malicious-looking title: rename the test group to <script>alert(1)</script>
python -m collector $ENVF dashboard      # with DASHBOARD_* set
curl -u user:pass http://127.0.0.1:8080/ | grep -c "<script>alert"   # expect 0
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8080/health   # 200 (liveness)
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8080/ready    # 200 ok / 503 degraded
```

Then stop the collector and re-check `/ready`: it must return **503** with a "heartbeat is
stale" problem — a dashboard that keeps saying "ok" while the collector is dead is worse
than no dashboard.

Also verify the read-only mount path: `chmod 400` the database (or mount it `:ro`) and
confirm the dashboard still renders.

### J. Album members carried across a batch boundary (1.2.0, issue 1)

Post a 4-photo album, then kill the collector with `kill -9` (not SIGTERM) while it is
importing a long history, so a batch boundary lands on an album member.

```bash
python -m collector $ENVF status | grep -A3 "historical import"   # note last_scanned
# restart, let it finish, then compare source and destination
sqlite3 /tmp/tac-test/data/test.sqlite3 \
  "SELECT source_message_id, status FROM processed_messages
   WHERE grouped_id IS NOT NULL ORDER BY source_message_id;"
```

Expect: every album member ends up `forwarded`. Before 1.2.0 the member that was still held
in memory when the process died was skipped permanently.

### K. Album discovered by both history and live paths (1.2.0, issue 3)

1. Pause forwarding (`WORKER_CONCURRENCY=1`, `MIN_FORWARD_INTERVAL=10`) so units stay
   `pending`.
2. Start a history import of a group that already contains albums, and post a new album
   while it runs.
3. Expect: one `forward_messages` call per album (grouped at the destination), not two
   partial albums. A unit that is already `sending` is never merged into — that would be a
   lost member, not a merged album.

### L. Reconciliation window (1.2.0, issue 4)

```bash
python -m collector $ENVF reconcile --limit 5 --dry-run        # narrow window
python -m collector $ENVF reconcile --limit 0 --dry-run        # whole history
python -m collector $ENVF reconcile --retry-unresolved         # prints a duplicate warning
```

Expect: the summary reports the window it used; unresolved units stay `ambiguous` unless you
force a retry, and forcing one prints an explicit warning that it can duplicate a forward
made outside the window.

### M. Restart during a historical import with albums pending (1.3.0)

1. Post a multi-photo album, then start `run` on a group with a long history.
2. `sudo systemctl restart telegram-collector` while the import is mid-flight (or `kill -9`
   to make it a hard crash).
3. After restart, expect: the album arrives at the destination **once, grouped**, and
   `status` shows no leftover `ambiguous` units. A member discovered after delivery started
   forms its own unit — check the destination for a second, smaller album only in that case.

### N. Dropped response on retry (1.3.0, verify-before-retry)

Hard to force by hand; the closest approximation is to interrupt network access
(`iptables -A OUTPUT -p tcp --dport 443 -j DROP`) right after a forward is issued, then
restore it. Expect: the log shows `retry_verified_delivered` and the destination does **not**
receive a second copy. If `VERIFY_BEFORE_RETRY=false`, the retry proceeds and a duplicate is
possible — that is the documented trade-off.

### O. VM reboot and restart behaviour

1. `sudo reboot`, then after boot: `systemctl status telegram-collector`,
   `python -m collector status`.
2. Expect: session reused (no login prompt), queue recovered, import resumed or completed,
   `/ready` returns 200 within a heartbeat interval.

## 3. What to record

For each scenario record: command, expected result, observed result, and any log excerpt.
Anything that fails here should be turned into a mocked unit test in `tests/` so the
regression is caught without Telegram.

## 4. What these tests still do not prove

Multi-day soak stability, behaviour under sustained FloodWait pressure, and Telegram-side
policy changes. Run the collector against the test fixtures for a week before trusting it
with production archives, and check `status` daily for `ambiguous` or `permanently_failed`
growth.
