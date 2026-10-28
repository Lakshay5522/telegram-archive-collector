#!/data/data/com.termux/files/usr/bin/bash
#
# One-shot Termux setup for the Telegram Archive Collector.
#
#   bash scripts/termux-setup.sh
#
# What it does, in order:
#   1. checks it is really running inside Termux
#   2. installs python, git and termux-services
#   3. clones (or updates) the repo into $HOME/telegram-archive-collector
#   4. creates a virtualenv and installs the collector with the dashboard
#   5. seeds .env from .env.example with owner-only permissions
#   6. registers two runit services: telegram-collector (enabled) and
#      telegram-collector-dashboard (present but disabled)
#
# It is safe to re-run: existing checkouts are updated, an existing .env is
# never overwritten, and services are only rewritten.
#
# It does NOT log you in to Telegram. Run `python -m collector login` yourself
# afterwards -- that needs your phone number and the code Telegram sends you.

set -eu

REPO_URL="${REPO_URL:-https://github.com/Lakshay2322306/telegram-archive-collector.git}"
DIR="${DIR:-$HOME/telegram-archive-collector}"
SERVICE="telegram-collector"
DASH_SERVICE="telegram-collector-dashboard"

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[33mwarning:\033[0m %s\n' "$*" >&2; }
die() { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------- sanity check
if [ -z "${PREFIX:-}" ] || [ "${PREFIX#*com.termux}" = "$PREFIX" ]; then
    die "this script must run inside Termux (PREFIX=${PREFIX:-<unset>} is not a Termux prefix)"
fi

say "Termux detected (PREFIX=${PREFIX})"

# ------------------------------------------------------------------- packages
say "Installing packages (python, git, termux-services)"
pkg update -y >/dev/null
pkg install -y python git termux-services

# ----------------------------------------------------------------------- repo
if [ -d "$DIR/.git" ]; then
    say "Updating existing checkout in $DIR"
    git -C "$DIR" pull --ff-only
elif [ -e "$DIR" ]; then
    die "$DIR exists but is not a git checkout; move it aside or set DIR=..."
else
    say "Cloning into $DIR"
    git clone --depth 1 "$REPO_URL" "$DIR"
fi

cd "$DIR"

# ------------------------------------------------------------------ virtualenv
if [ ! -x "$DIR/.venv/bin/python" ]; then
    say "Creating virtualenv"
    python -m venv "$DIR/.venv"
fi

say "Installing the collector and dashboard dependencies"
"$DIR/.venv/bin/python" -m pip install --no-cache-dir --upgrade pip setuptools wheel >/dev/null
# Core package. Editable install keeps `python -m collector` working from $DIR.
"$DIR/.venv/bin/python" -m pip install --no-cache-dir -e .
# Dashboard without the [standard] extra: that extra pulls uvloop, whose bundled
# libuv cannot build on Android, and watchfiles, which needs a Rust toolchain.
# Plain uvicorn falls back to the standard asyncio loop, which is plenty for a
# read-only dashboard.
"$DIR/.venv/bin/python" -m pip install --no-cache-dir fastapi 'uvicorn>=0.29,<1.0'

# ------------------------------------------------------------------------ env
if [ -f "$DIR/.env" ]; then
    say "Keeping existing .env (not overwritten)"
else
    say "Creating .env from .env.example"
    cp "$DIR/.env.example" "$DIR/.env"
    chmod 600 "$DIR/.env"
    warn "edit $DIR/.env and fill in API_ID, API_HASH, DESTINATION_CHAT and SOURCE_GROUPS"
fi

# -------------------------------------------------------------------- service
# runit service: the run script must stay in the foreground and log to stdout.
say "Registering runit service '$SERVICE'"
mkdir -p "$PREFIX/var/service/$SERVICE/log"
ln -sf "$PREFIX/share/termux-services/svlogger" "$PREFIX/var/service/$SERVICE/log/run"
cat > "$PREFIX/var/service/$SERVICE/run" <<EOF
#!/data/data/com.termux/files/usr/bin/sh
# Hold a wake lock so Android does not freeze the collector when the screen
# turns off. Released automatically when the service stops.
termux-wake-lock
cd "$DIR" || exit 1
exec "$DIR/.venv/bin/python" -m collector --env-file "$DIR/.env" run 2>&1
EOF
chmod +x "$PREFIX/var/service/$SERVICE/run"

say "Registering runit service '$DASH_SERVICE' (disabled by default)"
mkdir -p "$PREFIX/var/service/$DASH_SERVICE/log"
ln -sf "$PREFIX/share/termux-services/svlogger" "$PREFIX/var/service/$DASH_SERVICE/log/run"
cat > "$PREFIX/var/service/$DASH_SERVICE/run" <<EOF
#!/data/data/com.termux/files/usr/bin/sh
cd "$DIR" || exit 1
exec "$DIR/.venv/bin/python" -m collector --env-file "$DIR/.env" dashboard 2>&1
EOF
chmod +x "$PREFIX/var/service/$DASH_SERVICE/run"
# A service is "disabled" while its `down` file exists.
touch "$PREFIX/var/service/$DASH_SERVICE/down"

# ------------------------------------------------------------------- next step
if command -v sv >/dev/null 2>&1; then
    say "Enabling '$SERVICE' so it starts with Termux"
    sv-enable "$SERVICE" || warn "could not start the service yet; restart Termux and run: sv up $SERVICE"
else
    warn "'sv' not found yet -- restart Termux so runsvdir starts, then: sv up $SERVICE"
fi

cat <<EOF

Setup finished.

Next steps (in this order):

  1. edit the configuration:
       nano $DIR/.env

  2. log in once (needs your phone number + the code Telegram sends you):
       cd $DIR && .venv/bin/python -m collector login

  3. prove the session can see your groups:
       .venv/bin/python -m collector validate-config --check-telegram
       .venv/bin/python -m collector list-groups

  4. import history, then start live forwarding:
       .venv/bin/python -m collector import-history
       sv up $SERVICE          # or: sv restart $SERVICE
       sv status $SERVICE

  Logs:    tail -f $PREFIX/var/log/sv/$SERVICE/current
  Status:  cd $DIR && .venv/bin/python -m collector status

Optional dashboard (set DASHBOARD_USERNAME/PASSWORD in .env first):
       rm $PREFIX/var/service/$DASH_SERVICE/down && sv up $DASH_SERVICE
       # then open http://127.0.0.1:8080 on the phone

Keep the phone plugged in. See docs/TERMUX.md for battery, autostart-on-boot
and the known limits of running this on Android.
EOF
