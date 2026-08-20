"""When the extension reports a leg it could not stage, the server must act on
it rather than log it: the dead leg leaves the candidate pool, and the slip it
belonged to is dissolved so its surviving legs return to the pool for the next
scrape to regroup.
"""
from __future__ import annotations

import pytest

from web import app as app_mod
import web.state as state


class _FakeTable:
    def __init__(self, store, name):
        self.store, self.name, self.filters = store, name, {}

    def select(self, *_a, **_k):
        return self

    def delete(self):
        self.store["deleted"].append(self.name)
        return self

    def eq(self, col, val):
        self.filters[col] = val
        return self

    def execute(self):
        if self.name == "legs":
            rows = [r for r in self.store["legs"]
                    if all(r.get(k) == v for k, v in self.filters.items())]
            if self.name in self.store["deleted"]:
                self.store["legs"] = [r for r in self.store["legs"] if r not in rows]
            return type("R", (), {"data": rows})()
        return type("R", (), {"data": []})()


class _FakeDB:
    def __init__(self, store):
        self.store = store

    def table(self, name):
        return _FakeTable(self.store, name)


@pytest.fixture()
def client(monkeypatch):
    from fastapi.testclient import TestClient
    from web.app import app
    from web.auth import get_current_user

    with state._pending_slips_lock:
        state._pending_slips.clear()
    with app_mod._unavailable_lock:
        app_mod._unavailable_legs.clear()

    store = {"legs": [{"slip_id": "SLIP1", "user_id": "u1", "leg_num": i} for i in (1, 2, 3)],
             "deleted": []}
    import engine.database as dbmod
    monkeypatch.setattr(dbmod, "get_db", lambda: _FakeDB(store))

    with TestClient(app) as c:
        yield c, app, get_current_user, store

    app.dependency_overrides.clear()
    with state._pending_slips_lock:
        state._pending_slips.clear()
    with app_mod._unavailable_lock:
        app_mod._unavailable_legs.clear()


def _queue(c, app, gcu, slip_id="SLIP1"):
    app.dependency_overrides[gcu] = lambda: {"id": "u1", "email": "u1@t", "jwt": "x"}
    r = c.post("/api/pending-slip", json={
        "legs": [{"player": "A", "prop": "Points", "line": 1.5, "side": "over"},
                 {"player": "B", "prop": "Hits", "line": 0.5, "side": "over"}],
        "slip_type": "Power", "n_legs": 2, "slip_id": slip_id,
    })
    assert r.status_code == 200
    app.dependency_overrides.clear()
    return r.json()["token"]


def test_failed_leg_leaves_the_candidate_pool(client):
    c, app, gcu, _ = client
    token = _queue(c, app, gcu)
    dead = {"player": "A", "prop": "Points", "line": 1.5, "side": "over"}

    assert app_mod.is_leg_unavailable(dead) is False
    r = c.post(f"/api/pending-slip/status?cp_slip={token}", json={
        "legs_total": 2, "legs_staged": 1, "failures": [dead], "version": "2.4.1",
    })
    assert r.status_code == 200
    assert app_mod.is_leg_unavailable(dead) is True


def test_the_slip_is_dissolved_so_survivors_return_to_the_pool(client):
    c, app, gcu, store = client
    token = _queue(c, app, gcu)
    c.post(f"/api/pending-slip/status?cp_slip={token}", json={
        "legs_total": 2, "legs_staged": 1,
        "failures": [{"player": "A", "prop": "Points", "line": 1.5, "side": "over"}],
    })
    # Both the legs and the slip header must go — a header with no legs is an
    # orphan row that still renders on the Backtest tab.
    assert "legs" in store["deleted"]
    assert "slips" in store["deleted"]


def test_a_fully_staged_slip_is_left_completely_alone(client):
    """No failures ⇒ nothing is blocked and nothing is deleted. This is the
    common path and must not touch the pool or the user's backtest."""
    c, app, gcu, store = client
    token = _queue(c, app, gcu)
    r = c.post(f"/api/pending-slip/status?cp_slip={token}", json={
        "legs_total": 2, "legs_staged": 2, "failures": [],
    })
    assert r.status_code == 200
    assert store["deleted"] == []
    assert app_mod.is_leg_unavailable(
        {"player": "A", "prop": "Points", "line": 1.5, "side": "over"}) is False


def test_missing_slip_id_still_blocks_the_leg(client):
    """Older extensions / slips queued before this change carry no slip_id.
    The leg must still leave the pool even though no slip can be dissolved."""
    c, app, gcu, store = client
    app.dependency_overrides[gcu] = lambda: {"id": "u1", "email": "u1@t", "jwt": "x"}
    r = c.post("/api/pending-slip", json={
        "legs": [{"player": "A", "prop": "Points", "line": 1.5, "side": "over"}],
        "slip_type": "Power", "n_legs": 1,
    })
    token = r.json()["token"]
    app.dependency_overrides.clear()

    c.post(f"/api/pending-slip/status?cp_slip={token}", json={
        "legs_total": 1, "legs_staged": 0,
        "failures": [{"player": "A", "prop": "Points", "line": 1.5, "side": "over"}],
    })
    assert app_mod.is_leg_unavailable(
        {"player": "A", "prop": "Points", "line": 1.5, "side": "over"}) is True
    assert store["deleted"] == []
