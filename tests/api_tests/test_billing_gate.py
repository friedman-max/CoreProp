"""Regression test for the billing paywall gap.

The subscription gate (`_user_has_access`) was only applied to
/api/bootstrap/core, while /api/bets, /api/matched, /api/prizepicks,
/api/fanduel, /api/draftkings and /api/pinnacle served the same paid +EV
data with no check — a non-subscriber could pull everything from those.

This test forces access-denied and asserts every one of those endpoints
returns 402, i.e. the gate is actually wired on each.
"""
from __future__ import annotations

import pytest

GATED_ENDPOINTS = [
    "/api/bets",
    "/api/matched",
    "/api/bootstrap/core",
    "/api/prizepicks",
    "/api/fanduel",
    "/api/draftkings",
    "/api/pinnacle",
]


@pytest.fixture()
def client():
    from fastapi.testclient import TestClient
    from web.app import app
    with TestClient(app) as c:
        yield c


def test_all_data_endpoints_enforce_the_billing_gate(client, monkeypatch):
    import web.app as appmod

    # Simulate an enforced-billing deployment where this caller has no access.
    monkeypatch.setattr(appmod, "_user_has_access", lambda user: False)

    for path in GATED_ENDPOINTS:
        r = client.get(path)
        assert r.status_code == 402, f"{path} did not enforce the billing gate (got {r.status_code})"


def test_data_endpoints_open_when_access_granted(client, monkeypatch):
    import web.app as appmod

    # Default posture (enforcement off) grants everyone access — endpoints
    # must serve normally, not 402.
    monkeypatch.setattr(appmod, "_user_has_access", lambda user: True)

    for path in GATED_ENDPOINTS:
        r = client.get(path)
        assert r.status_code == 200, f"{path} unexpectedly blocked when access granted (got {r.status_code})"
