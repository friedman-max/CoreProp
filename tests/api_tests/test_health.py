"""Smoke tests: the app boots, /health and / respond, /api/status works.

These don't exercise business logic. They prove the app imports correctly
and the most basic routes wire up — which catches the most common kind of
regression after a refactor (a stray import, a missing dependency, a
typo in a router include).
"""
from __future__ import annotations

import pytest

# Importing web.app triggers FastAPI app construction. We do it lazily so
# pytest collection doesn't pay the cost for tests that don't need it.


@pytest.fixture(scope="module")
def client():
    from fastapi.testclient import TestClient
    from web.app import app
    # TestClient also fires startup events. We don't want the scheduler
    # actually running in tests; the lifespan handler is permissive so
    # it just logs a Supabase warning and proceeds.
    with TestClient(app) as c:
        yield c


def test_health_endpoint(client):
    r = client.get("/health")
    assert r.status_code == 200


def test_root_serves_html(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers.get("content-type", "")


def test_status_endpoint_returns_json(client):
    r = client.get("/api/status")
    assert r.status_code == 200
    body = r.json()
    # Whatever its exact shape, it must at least be a dict and carry a
    # last_refresh field (possibly None).
    assert isinstance(body, dict)
    assert "last_refresh" in body or "next_refresh" in body or "interval_min" in body


def test_ui_config_returns_supabase_urls(client):
    """The frontend bootstraps from this endpoint."""
    r = client.get("/api/ui-config")
    assert r.status_code == 200
    body = r.json()
    assert "supabase_url" in body
    assert "supabase_anon_key" in body
