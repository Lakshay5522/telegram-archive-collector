"""ASGI entrypoint for hosting the read-only dashboard.

This module exists so a platform that looks for a module-level ASGI object
(Vercel, Render, Fly, plain ``uvicorn app:app``) can serve the dashboard
without knowing about the collector's CLI.

Important: this exposes **the dashboard only**. The collector itself is a
long-running daemon — it holds a Telethon session, keeps a live message
handler open and forwards into your channel — and cannot run on a serverless
platform. Deploy the collector as a service (systemd/Docker) and point this
dashboard at the database it writes.

Configuration is read from the environment exactly as the CLI reads it:

* ``DASHBOARD_USERNAME`` / ``DASHBOARD_PASSWORD`` — required; without them the
  dashboard refuses to serve data (no anonymous access is ever allowed).
* ``DATA_DIR`` / ``DATABASE_PATH`` — where the SQLite ledger lives. On a
  read-only serverless filesystem this defaults to ``/tmp/data``, which means
  the dashboard will show an empty archive; set ``DATABASE_PATH`` to a mounted
  volume or a synced copy to see real data.

Telegram credentials are deliberately not required here: the dashboard never
talks to Telegram.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from collector.config import Config, load_config
from collector.db import Database, DatabaseError
from collector.dashboard import create_app

#: Where the dashboard looks for the ledger when the platform gives us no
#: writable, persistent directory.
_SERVERLESS_DATA_DIR = "/tmp/data"


def _config() -> Config:
    """Build the dashboard config, defaulting storage to a writable path."""

    env = dict(os.environ)
    if env.get("VERCEL") and not env.get("DATA_DIR"):
        env["DATA_DIR"] = _SERVERLESS_DATA_DIR
    # Credentials are not needed to read the ledger.
    return load_config(env, dotenv_path=None, require_credentials=False)


def _ensure_database(path: Path | None) -> None:
    """Create an empty ledger if none exists, so pages render an empty state.

    The dashboard opens the database read-only and refuses to create it, which
    is the right default for a service that must not mutate the archive. On a
    fresh host there is nothing to open yet, so bootstrap the schema once.
    """

    if path is None or path.exists():
        return

    async def _bootstrap() -> None:
        db = Database(path)
        try:
            await db.connect()
        finally:
            await db.close()

    try:
        asyncio.run(_bootstrap())
    except (RuntimeError, DatabaseError):
        # Already inside an event loop, or the filesystem is read-only:
        # /ready will report the missing database rather than pretend.
        pass


def _unconfigured_app() -> object:
    """Minimal app that explains the missing credentials instead of 500ing."""

    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, JSONResponse

    app = FastAPI(title="Telegram Archive Collector", docs_url=None, redoc_url=None)

    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse({"status": "ok", "dashboard": "unconfigured"})

    @app.get("/{path:path}")
    async def setup_notice(path: str) -> HTMLResponse:
        return HTMLResponse(
            "<!doctype html><title>Dashboard not configured</title>"
            "<h1>Dashboard not configured</h1>"
            "<p>Set <code>DASHBOARD_USERNAME</code> and "
            "<code>DASHBOARD_PASSWORD</code> in the environment, then redeploy.</p>"
            "<p>The dashboard is never served without authentication.</p>",
            status_code=503,
        )

    return app


config = _config()
_ensure_database(config.database_path)

if config.dashboard_username and config.dashboard_password:
    app = create_app(config)
else:  # pragma: no cover - deployment misconfiguration
    app = _unconfigured_app()

__all__ = ["app", "config"]
