"""Regression tests for the pending-slip cross-user leak.

Before the token fix, GET /api/pending-slip returned the globally most-recent
slip across ALL users, so one user's browser extension could pick up and
auto-build another user's slip. The fix keys storage by an unguessable token
issued at POST time and passed back through the opened PrizePicks URL; GET
returns a slip ONLY when the caller presents the matching token.
"""
from __future__ import annotations

import pytest


@pytest.fixture()
def client():
    from fastapi.testclient import TestClient
    from web.app import app
    from web.auth import get_current_user
    import web.state as state

    # Start each test from a clean pending-slip store.
    with state._pending_slips_lock:
        state._pending_slips.clear()

    with TestClient(app) as c:
        yield c, app, get_current_user

    with state._pending_slips_lock:
        state._pending_slips.clear()


def _as_user(app, get_current_user, uid):
    """Override the auth dependency so POST /api/pending-slip runs as `uid`."""
    app.dependency_overrides[get_current_user] = lambda: {"id": uid, "email": f"{uid}@t", "jwt": "x"}


def test_get_without_token_returns_empty_even_when_a_slip_exists(client):
    c, app, gcu = client
    _as_user(app, gcu, "userA")
    r = c.post("/api/pending-slip", json={"legs": [{"player": "A"}], "slip_type": "Power", "n_legs": 1})
    assert r.status_code == 200
    assert r.json().get("token")
    app.dependency_overrides.clear()

    # No token -> no slip, even though one is stored.
    assert c.get("/api/pending-slip").json() == {}


def test_token_scopes_read_to_the_issuing_user(client):
    c, app, gcu = client
    # User A queues a slip, gets token A.
    _as_user(app, gcu, "userA")
    tokA = c.post("/api/pending-slip", json={"legs": [{"player": "A"}], "slip_type": "Power", "n_legs": 1}).json()["token"]
    # User B queues a slip LATER (greater expiry), gets token B.
    _as_user(app, gcu, "userB")
    tokB = c.post("/api/pending-slip", json={"legs": [{"player": "B"}], "slip_type": "Flex", "n_legs": 1}).json()["token"]
    app.dependency_overrides.clear()

    # Each token returns ONLY its own slip. Pre-fix, A's poll returned B's
    # slip (most-recent-wins); now A's token must never yield B's legs.
    a = c.get("/api/pending-slip", params={"cp_slip": tokA}).json()
    b = c.get("/api/pending-slip", params={"cp_slip": tokB}).json()
    assert a["legs"] == [{"player": "A"}]
    assert a["user_id"] == "userA"
    assert b["legs"] == [{"player": "B"}]
    assert b["user_id"] == "userB"


def test_delete_only_clears_the_given_token(client):
    c, app, gcu = client
    _as_user(app, gcu, "userA")
    tokA = c.post("/api/pending-slip", json={"legs": [{"player": "A"}], "slip_type": "Power", "n_legs": 1}).json()["token"]
    _as_user(app, gcu, "userB")
    tokB = c.post("/api/pending-slip", json={"legs": [{"player": "B"}], "slip_type": "Power", "n_legs": 1}).json()["token"]
    app.dependency_overrides.clear()

    # Clearing A's token must not wipe B's still-pending slip.
    c.delete("/api/pending-slip", params={"cp_slip": tokA})
    assert c.get("/api/pending-slip", params={"cp_slip": tokA}).json() == {}
    assert c.get("/api/pending-slip", params={"cp_slip": tokB}).json()["legs"] == [{"player": "B"}]
