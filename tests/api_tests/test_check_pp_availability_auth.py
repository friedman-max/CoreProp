"""`/api/check-pp-availability` must not be an open, unbounded compute endpoint.

The audit found it declared with no auth dependency and taking an unbounded
`legs: list[dict]`, so any anonymous caller could POST an arbitrarily long list
and drive an O(legs x catalogue) fuzzy match on the single-worker dyno. The
endpoint only ever serves the logged-in client (the extension/frontend
pre-flight), so it should (1) require an authenticated Supabase user and
(2) cap the number of legs a request may carry.
"""
from __future__ import annotations

import pytest


@pytest.fixture()
def client():
    from fastapi.testclient import TestClient
    from web.app import app
    from web.auth import get_current_user

    with TestClient(app) as c:
        yield c, app, get_current_user
    app.dependency_overrides.clear()


def _one_leg():
    return {"player": "Aaron Judge", "prop": "hits", "line": 1.5, "side": "over"}


def test_unauthenticated_request_is_rejected(client):
    c, app, gcu = client
    # No auth override and no Authorization header -> get_current_user 401s.
    r = c.post("/api/check-pp-availability", json={"legs": [_one_leg()]})
    assert r.status_code == 401, (
        "check-pp-availability answered an anonymous caller "
        f"(status {r.status_code}); it must require an authenticated user."
    )


def test_authenticated_request_is_allowed(client):
    c, app, gcu = client
    app.dependency_overrides[gcu] = lambda: {"id": "u1", "email": "u1@t", "jwt": "x"}
    r = c.post("/api/check-pp-availability", json={"legs": [_one_leg()]})
    assert r.status_code == 200
    body = r.json()
    assert "available" in body and "legs" in body
    assert len(body["legs"]) == 1


def test_oversized_leg_list_is_rejected(client):
    c, app, gcu = client
    app.dependency_overrides[gcu] = lambda: {"id": "u1", "email": "u1@t", "jwt": "x"}
    # A real PrizePicks slip is at most ~6 legs; 50 is a compute-abuse payload.
    r = c.post("/api/check-pp-availability", json={"legs": [_one_leg() for _ in range(50)]})
    assert r.status_code == 400, (
        "check-pp-availability accepted an oversized legs list "
        f"(status {r.status_code}); it must bound the request size."
    )
