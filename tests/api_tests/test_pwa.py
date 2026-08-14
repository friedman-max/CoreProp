"""The installable-PWA contract.

A service worker can only control URLs at or below its own path, so the worker
MUST be served from the root ("/sw.js") with `Service-Worker-Allowed: /`. If it
ever moves under /static, its scope silently narrows and it stops intercepting
navigations and — the reason push exists here — stops receiving Web Push. The
manifest must also stay installable (standalone + a start_url + a 512px icon).
"""
from __future__ import annotations

import pytest


@pytest.fixture()
def client():
    from fastapi.testclient import TestClient
    from web.app import app

    with TestClient(app) as c:
        yield c


def test_service_worker_served_at_root_with_open_scope(client):
    r = client.get("/sw.js")
    assert r.status_code == 200
    assert "javascript" in r.headers.get("content-type", "")
    # Root scope is the whole point — a "/"-scoped register() fails without it.
    assert r.headers.get("service-worker-allowed") == "/"
    # It is the actual worker (has a push handler), not the SPA shell HTML.
    assert "addEventListener" in r.text and "push" in r.text


def test_manifest_is_standalone_installable(client):
    r = client.get("/static/site.webmanifest")
    assert r.status_code == 200
    body = r.json()
    assert body["display"] == "standalone"
    assert body["start_url"] == "/"
    assert any(i["sizes"] == "512x512" for i in body["icons"])
