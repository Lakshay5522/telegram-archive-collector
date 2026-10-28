"""Optional read-only FastAPI dashboard.

Security and correctness rules baked in here:

* **All** interpolated values are HTML-escaped (source identifiers, titles,
  error text, the status report) — no untrusted text reaches the DOM as markup.
* The database is opened in **read-only mode** (``mode=ro``): no journal-mode
  change, no migrations, no writes, so a ``:ro`` Docker mount works.
* Authentication is mandatory; the app refuses to start without credentials and
  compares them in constant time.
* ``/health`` is liveness only; ``/ready`` reports real collector health
  (heartbeat freshness, queue movement, ambiguous deliveries) and returns 503
  when the collector is not actually healthy.
* No Telegram credentials or session data ever reach the browser.
"""

from __future__ import annotations

import html
import logging
import secrets
from typing import Any

from .config import Config
from .db import Database, DatabaseError
from .status import build_report, health_snapshot

log = logging.getLogger(__name__)


def _e(value: Any, default: str = "-") -> str:
    """Escape any value for HTML interpolation."""

    if value is None or value == "":
        return html.escape(default)
    return html.escape(str(value))


def create_app(config: Config) -> Any:
    try:
        from fastapi import Depends, FastAPI, HTTPException, status
        from fastapi.responses import HTMLResponse, JSONResponse
        from fastapi.security import HTTPBasic, HTTPBasicCredentials
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "The dashboard needs optional dependencies: `pip install -e .[dashboard]`"
        ) from exc

    if not config.dashboard_username or not config.dashboard_password:
        raise RuntimeError(
            "DASHBOARD_USERNAME and DASHBOARD_PASSWORD must be set; refusing to expose "
            "an unauthenticated dashboard."
        )

    app = FastAPI(title="Telegram Archive Collector", docs_url=None, redoc_url=None)
    security = HTTPBasic()

    def require_auth(credentials: "HTTPBasicCredentials" = Depends(security)) -> str:
        username_ok = secrets.compare_digest(
            credentials.username.encode(), (config.dashboard_username or "").encode()
        )
        password_ok = secrets.compare_digest(
            credentials.password.encode(), (config.dashboard_password or "").encode()
        )
        if not (username_ok and password_ok):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid credentials",
                headers={"WWW-Authenticate": "Basic"},
            )
        return credentials.username

    def open_db() -> Database:
        return Database(config.database_path, read_only=True)

    async def snapshot() -> dict[str, Any]:
        async with open_db() as db:
            return {
                "counts": await db.status_counts(),
                "queue_size": await db.queue_size(),
                "sources": await db.stats_by_source(),
                "import_progress": await db.list_import_progress(),
                "recent_failures": await db.recent_failures(limit=10),
                "recent_events": await db.recent_events(limit=20),
                "last_forwarded": await db.last_forwarded(),
            }

    @app.get("/health")
    async def health() -> JSONResponse:
        """Liveness: the HTTP process is up.  Says nothing about the collector."""

        return JSONResponse({"status": "ok", "component": "http"})

    @app.get("/ready")
    async def ready() -> JSONResponse:
        """Readiness: is the collector actually collecting right now?"""

        try:
            async with open_db() as db:
                health_info = await health_snapshot(db, config)
        except DatabaseError as exc:
            return JSONResponse(
                status_code=503, content={"state": "unavailable", "problems": [str(exc)]}
            )
        code = 200 if health_info["state"] == "ok" else 503
        return JSONResponse(status_code=code, content=health_info)

    @app.get("/api/status")
    async def api_status(_: str = Depends(require_auth)) -> JSONResponse:
        return JSONResponse(await snapshot())

    @app.get("/api/events")
    async def api_events(_: str = Depends(require_auth)) -> JSONResponse:
        async with open_db() as db:
            return JSONResponse({"events": await db.recent_events(limit=100)})

    @app.get("/", response_class=HTMLResponse)
    async def index(_: str = Depends(require_auth)) -> HTMLResponse:
        async with open_db() as db:
            report = await build_report(db, config, connection_state="see `collector status`")
            data = await snapshot()
            health_info = await health_snapshot(db, config)

        rows = "".join(
            "<tr>"
            f"<td>{_e(s.get('identifier'))}</td>"
            f"<td>{_e(s.get('chat_id'))}</td>"
            f"<td>{_e(s.get('title'))}</td>"
            f"<td>{'yes' if s.get('enabled') else 'no'}</td>"
            f"<td>{_e(s.get('forwarded', 0))}</td>"
            f"<td>{_e(s.get('pending', 0))}</td>"
            f"<td>{_e(s.get('failed', 0))}</td>"
            f"<td>{_e(s.get('ambiguous', 0))}</td>"
            f"<td>{_e(s.get('last_live_event_at'))}</td>"
            "</tr>"
            for s in data["sources"]
        ) or '<tr><td colspan="9">none</td></tr>'

        failures = "".join(
            f"<li>{_e(f['source_chat_id'])}:{_e(f['source_message_id'])} — "
            f"{_e(f['status'])} — {_e((f['last_error'] or '')[:160])}</li>"
            for f in data["recent_failures"]
        ) or "<li>none</li>"

        imports = "".join(
            f"<li>chat {_e(row['source_chat_id'])}: {_e(row['status'])} — "
            f"scanned {_e(row['messages_seen'])}, queued {_e(row['messages_queued'])}, "
            f"last scanned {_e(row.get('last_scanned_message_id'))}</li>"
            for row in data["import_progress"]
        ) or "<li>none</li>"

        problems = "".join(f"<li>{_e(p)}</li>" for p in health_info["problems"]) or "<li>none</li>"
        counts = data["counts"]
        html_doc = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Collector dashboard</title>
<style>
 body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1b1b1b; }}
 table {{ border-collapse: collapse; margin: 1rem 0; }}
 th, td {{ border: 1px solid #ddd; padding: .35rem .6rem; text-align: left; font-size: .85rem; }}
 h1 {{ font-size: 1.3rem; }} h2 {{ font-size: 1.05rem; margin-top: 1.6rem; }}
 .kpi {{ display: inline-block; margin-right: 1.5rem; }}
 .kpi b {{ display: block; font-size: 1.4rem; }}
 pre {{ background: #f6f6f6; padding: 1rem; overflow: auto; font-size: .8rem; }}
 .state-ok {{ color: #146c2e; }} .state-degraded {{ color: #9a3412; }}
</style></head><body>
<h1>Telegram Archive Collector</h1>
<p>health: <b class="state-{_e(health_info['state'])}">{_e(health_info['state'])}</b>
   · heartbeat {_e(health_info['heartbeat_at'])}</p>
<ul>{problems}</ul>
<div>
  <span class="kpi"><b>{_e(counts.get('forwarded', 0))}</b>forwarded</span>
  <span class="kpi"><b>{_e(data['queue_size'])}</b>queued</span>
  <span class="kpi"><b>{_e(counts.get('retry_pending', 0))}</b>retry pending</span>
  <span class="kpi"><b>{_e(counts.get('permanently_failed', 0))}</b>failed</span>
  <span class="kpi"><b>{_e(counts.get('ambiguous', 0))}</b>ambiguous</span>
</div>
<h2>Source groups</h2>
<table><tr><th>identifier</th><th>chat id</th><th>title</th><th>enabled</th><th>forwarded</th>
<th>pending</th><th>failed</th><th>ambiguous</th><th>last live event</th></tr>{rows}</table>
<h2>Historical imports</h2><ul>{imports}</ul>
<h2>Recent failures</h2><ul>{failures}</ul>
<h2>Full status</h2><pre>{_e(report)}</pre>
</body></html>"""
        return HTMLResponse(html_doc)

    return app


def run_dashboard(config: Config) -> None:  # pragma: no cover - needs uvicorn
    import uvicorn

    if not config.dashboard_username or not config.dashboard_password:
        raise RuntimeError("DASHBOARD_USERNAME and DASHBOARD_PASSWORD must be set")
    log.info("starting dashboard on %s:%s", config.dashboard_host, config.dashboard_port)
    uvicorn.run(
        create_app(config),
        host=config.dashboard_host,
        port=config.dashboard_port,
        log_level=config.log_level.lower(),
    )
