"""Dashboard: HTML escaping, read-only database, honest health endpoints."""

from __future__ import annotations

import time

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from collector.config import Config
from collector.dashboard import create_app
from collector.db import Database

CHAT = -1001234567890
AUTH = ("admin", "a-very-long-password")


@pytest.fixture
def dash_config(tmp_path) -> Config:
    config = Config(
        api_id=1,
        api_hash="c" * 32,
        destination_chat="@dest",
        sources=[],
        data_dir=tmp_path,
        dashboard_enabled=True,
        dashboard_username=AUTH[0],
        dashboard_password=AUTH[1],
    )
    config.ensure_directories()
    return config


async def seed(config: Config, *, title: str = "Group") -> None:
    async with Database(config.database_path) as db:
        await db.sync_source_groups([type("S", (), {"identifier": "@x", "alias": None, "enabled": True})()])
        await db.set_source_resolved("@x", CHAT, title)
        await db.discover_unit(
            chat_id=CHAT, messages=[{"message_id": 1, "date": None, "grouped_id": None}]
        )
        await db.heartbeat()


def test_dashboard_refuses_to_start_without_credentials(dash_config: Config):
    dash_config.dashboard_password = None
    with pytest.raises(RuntimeError):
        create_app(dash_config)


async def test_unauthenticated_requests_are_rejected(dash_config: Config):
    await seed(dash_config)
    app = create_app(dash_config)
    with TestClient(app) as client:
        assert client.get("/").status_code == 401
        assert client.get("/api/status").status_code == 401
        assert client.get("/api/events").status_code == 401


async def test_authenticated_requests_work(dash_config: Config):
    await seed(dash_config)
    app = create_app(dash_config)
    with TestClient(app) as client:
        assert client.get("/", auth=AUTH).status_code == 200
        assert client.get("/api/status", auth=AUTH).status_code == 200


async def test_wrong_credentials_are_rejected(dash_config: Config):
    await seed(dash_config)
    app = create_app(dash_config)
    with TestClient(app) as client:
        assert client.get("/", auth=("admin", "wrong")).status_code == 401


async def test_html_is_escaped(dash_config: Config):
    """Audit item 8: untrusted text must never be rendered as markup."""

    payload = '<script>alert("xss")</script>'
    await seed(dash_config, title=payload)
    app = create_app(dash_config)
    with TestClient(app) as client:
        body = client.get("/", auth=AUTH).text

    assert "<script>alert" not in body
    assert "&lt;script&gt;" in body


async def test_health_is_liveness_only(dash_config: Config):
    await seed(dash_config)
    app = create_app(dash_config)
    with TestClient(app) as client:
        response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["component"] == "http"


async def test_ready_reports_collector_health(dash_config: Config):
    await seed(dash_config)
    app = create_app(dash_config)
    with TestClient(app) as client:
        response = client.get("/ready")
    assert response.status_code == 200
    assert response.json()["state"] == "ok"


async def test_ready_is_503_when_heartbeat_is_stale(dash_config: Config):
    dash_config.stale_heartbeat_seconds = 1
    await seed(dash_config)
    async with Database(dash_config.database_path) as db:
        await db.set_setting("heartbeat_at", "2020-01-01T00:00:00+00:00")
    app = create_app(dash_config)
    with TestClient(app) as client:
        response = client.get("/ready")
    assert response.status_code == 503
    assert any("stale" in problem for problem in response.json()["problems"])


async def test_ready_is_503_when_database_is_missing(tmp_path):
    config = Config(
        api_id=1,
        api_hash="c" * 32,
        destination_chat="@dest",
        sources=[],
        data_dir=tmp_path / "empty",
        dashboard_username=AUTH[0],
        dashboard_password=AUTH[1],
    )
    app = create_app(config)
    with TestClient(app) as client:
        response = client.get("/ready")
    assert response.status_code == 503


async def test_dashboard_never_writes_to_the_database(dash_config: Config):
    """Audit item 9: a read-only mount must be enough for the dashboard."""

    await seed(dash_config)
    path = dash_config.database_path
    before = path.stat().st_mtime_ns
    app = create_app(dash_config)
    with TestClient(app) as client:
        client.get("/", auth=AUTH)
        client.get("/api/status", auth=AUTH)
        client.get("/ready")
    assert path.stat().st_mtime_ns == before
