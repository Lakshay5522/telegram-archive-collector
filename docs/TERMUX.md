# Running the collector on Android with Termux

Termux is a terminal emulator for Android with its own package manager. It runs a real
Linux userland, so this collector — pure Python, no compiled extensions in its core
dependencies — runs on a phone without modification.

This is a genuinely usable host: a phone that is already on and charging costs nothing,
has a battery-backed power supply, and holds a network connection. It is not a server:
there is no uptime guarantee, Android may kill processes, and the phone is doing this in
addition to being a phone.

**Install Termux from [F-Droid](https://f-droid.org/packages/com.termux/), not the Play
Store.** The Play Store build was deprecated and stops receiving package updates, which
breaks `pkg install`.

---

## 1. Quick start

Open Termux and run:

```bash
pkg update && pkg upgrade -y
pkg install -y git
git clone https://github.com/Lakshay2322306/telegram-archive-collector.git
cd telegram-archive-collector
bash scripts/termux-setup.sh
```

The script installs `python`, `git` and `termux-services`, creates a virtualenv in
`~/telegram-archive-collector/.venv`, installs the package, seeds `.env` from
`.env.example` with `0600` permissions, and registers the runit services.

Then fill in the configuration and log in:

```bash
nano ~/telegram-archive-collector/.env
cd ~/telegram-archive-collector
.venv/bin/python -m collector login
.venv/bin/python -m collector validate-config --check-telegram
.venv/bin/python -m collector list-groups
.venv/bin/python -m collector import-history
```

`list-groups` is the real test: if it prints your source chats with their marked ids, the
session and the network path both work. `import-history` then drains the backlog before
you start live monitoring.

---

## 2. What the script does, and why

### Dependencies are installed without the `[standard]` extra

`uvicorn[standard]` pulls in `uvloop`, whose bundled libuv fails to build on Android
(libuv's `./configure` exits non-zero under Bionic), and `watchfiles`, which needs a Rust
toolchain. Both failures are well documented and neither is fixable from here.

Plain `uvicorn` is enough: it falls back to the standard asyncio event loop, and the
dashboard is a read-only page view — it does not need a fast loop. That is why the script
installs `fastapi` and `uvicorn` explicitly instead of the `[dashboard]` extra, and why
`pyproject.toml` no longer pins `uvicorn[standard]` for the dashboard.

The collector itself needs only `telethon`, `aiosqlite` and `python-dotenv`, all pure
Python, so the core install never compiles anything.

### State lives inside Termux's private storage

Keep `DATA_DIR` under `$HOME` (the default `~/telegram-archive-collector/data`). Do **not**
point it at `/sdcard` or any other shared-storage path: SQLite locking is unreliable over
Android's FUSE layer, and the session file would be readable by other apps. Termux's
private directory is already owner-only, which is what the collector's `RESTRICT_PERMISSIONS`
setting expects.

You do not need `termux-setup-storage` for this.

### The service holds a wake lock

`termux-wake-lock` tells Android to keep the CPU awake for the process. Without it, Doze
suspends the app when the screen is off and the collector stops receiving updates. The run
script acquires the lock on start; it is released when the service stops.

Also exempt Termux from battery optimisation, or Android will eventually freeze it anyway:

* Settings → Apps → Termux → Battery → **Unrestricted** (the exact wording varies by
  vendor), and
* on MIUI/ColorOS/OriginOS/One UI, enable **Autostart** for Termux in the vendor's
  security app. These skins kill background apps more aggressively than stock Android.

---

## 3. Autostart

Two separate things, and you want both:

**Start with Termux.** `termux-services` (runit) supervises the collector, restarts it if
it crashes, and starts it whenever Termux starts. The setup script registers it; enable it
with `sv-enable telegram-collector`. Useful commands:

```bash
sv up telegram-collector       # start now
sv down telegram-collector     # stop
sv restart telegram-collector  # restart
sv status telegram-collector   # is it running?
sv-disable telegram-collector  # stop starting with Termux
```

Logs go to `$PREFIX/var/log/sv/telegram-collector/current`:

```bash
tail -f $PREFIX/var/log/sv/telegram-collector/current
```

**Start at boot.** `termux-services` only starts when the Termux app starts. To start it
when the phone boots, install
[Termux:Boot](https://f-droid.org/packages/com.termux.boot/) from F-Droid, open it once,
then create `~/.termux/boot/start-services.sh`:

```bash
mkdir -p ~/.termux/boot
cat > ~/.termux/boot/start-services.sh <<'EOF'
#!/data/data/com.termux/files/usr/bin/sh
termux-wake-lock
EOF
chmod +x ~/.termux/boot/start-services.sh
```

Opening Termux once after boot is what actually starts `runsvdir`; the boot script makes
sure the wake lock is held even before that.

---

## 4. The dashboard on the phone

The dashboard is optional and binds to `127.0.0.1`, so it is reachable only from the phone
itself — which is usually what you want, since exposing it needs TLS and a reverse proxy.

Set credentials in `.env`, then:

```bash
rm $PREFIX/var/service/telegram-collector-dashboard/down
sv up telegram-collector-dashboard
```

Open `http://127.0.0.1:8080` in the phone's browser and log in with
`DASHBOARD_USERNAME` / `DASHBOARD_PASSWORD`.

---

## 5. Battery and thermal reality

Be honest with yourself about what a phone server means:

* **Keep it on the charger.** A held wake lock plus a persistent Telegram connection will
  drain a battery in a few hours.
* **Charging + running 24/7 is hard on the battery.** On a phone you care about, consider a
  charge limit (many OEMs have "protect battery" / 80% modes) and expect the battery to age
  faster. An old phone you no longer use is the ideal candidate.
* **Expect a small steady drain on the network**: the client keeps a socket open and
  reconnects periodically. On mobile data this is negligible in volume, but it is not zero.
* **Android may still kill the process.** The wake lock, battery exemption and vendor
  autostart settings make it much less likely; nothing makes it impossible. That is why the
  collector is designed to resume: it checkpoints imports, leases work in the database, and
  reconciles ambiguous deliveries on restart. A kill costs you time, not messages.

---

## 6. Known limits on this platform

* **No `uvloop`.** The event loop is the stdlib asyncio loop. Irrelevant at this scale.
* **Shared storage is off limits** for the database and session, as explained above.
* **No systemd.** Supervision is runit via `termux-services`; the `deploy/*.service` unit in
  this repo does not apply.
* **The dashboard cannot be exposed publicly without extra work** — use a tunnel or a
  reverse proxy, and remember the session file next to it is a credential.
* **Android updates can break Termux.** Termux keeps working after most updates, but a
  major Android release occasionally requires reinstalling packages (`pkg upgrade`). Your
  data survives: it is in `~/telegram-archive-collector/data`.

---

## 7. Moving to a VPS later

Everything you set up here is portable. Copy the data directory — session, database, logs —
to the server, then run the same commands under systemd or Docker (README sections 10
and 13):

```bash
# on the phone
tar czf collector-data.tgz -C ~/telegram-archive-collector data

# on the VPS, after installing the collector
scp collector-data.tgz user@server:/srv/telegram-collector/
tar xzf collector-data.tgz
chmod 600 data/collector.session data/collector.sqlite3
```

Then point `DATA_DIR`, `SESSION_PATH` and `DATABASE_PATH` at the copied directory. The
session file carries the login, so no new Telegram code is needed — and because it is a
credential, move it over SSH and delete the tarball afterwards.

Do not run the phone copy and the VPS copy at the same time against the same destination
channel. Two collectors forwarding the same sources will double-post; stop the Termux
service (`sv down telegram-collector`) before starting the VPS one.
