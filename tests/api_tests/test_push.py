"""Web Push: subscription endpoints + the per-user send path.

Isolation is the load-bearing property: a subscription row is bound to the
authenticated user, and the cross-user send path reads a single user's rows.
The whole feature is a no-op unless VAPID keys are configured. Delivery is
stubbed via engine.push._deliver so these run without pywebpush installed.
"""
from __future__ import annotations

import pytest

import config as cfg


@pytest.fixture()
def client():
    from fastapi.testclient import TestClient
    from web.app import app
    from web.auth import get_current_user

    with TestClient(app) as c:
        yield c, app, get_current_user
    app.dependency_overrides.clear()


def _as_user(app, gcu, uid):
    app.dependency_overrides[gcu] = lambda: {"id": uid, "email": f"{uid}@t", "jwt": "x"}


_SUB = {"endpoint": "https://push.example/abc", "keys": {"p256dh": "p", "auth": "a"}}


# ── endpoints ────────────────────────────────────────────────────────────────

def test_subscribe_requires_auth(client):
    c, app, gcu = client
    r = c.post("/api/push/subscribe", json=_SUB)
    assert r.status_code == 401


def test_subscribe_503_when_push_unconfigured(client, monkeypatch):
    c, app, gcu = client
    monkeypatch.setattr(cfg, "VAPID_PUBLIC_KEY", "")
    monkeypatch.setattr(cfg, "VAPID_PRIVATE_KEY", "")
    _as_user(app, gcu, "userA")
    r = c.post("/api/push/subscribe", json=_SUB)
    assert r.status_code == 503


def test_subscribe_binds_row_to_authenticated_user(client, monkeypatch):
    c, app, gcu = client
    monkeypatch.setattr(cfg, "VAPID_PUBLIC_KEY", "pub")
    monkeypatch.setattr(cfg, "VAPID_PRIVATE_KEY", "priv")
    captured = {}

    class _W:
        def table(self, name):
            captured["table"] = name
            return self

        def upsert(self, row, **kw):
            captured["row"] = row
            captured["kw"] = kw
            return self

        def execute(self):
            return None

    import web.routers.push as pushrouter
    monkeypatch.setattr(pushrouter, "writer", lambda purpose: _W())

    _as_user(app, gcu, "userA")
    r = c.post("/api/push/subscribe", json=_SUB)
    assert r.status_code == 200
    assert captured["table"] == "push_subscriptions"
    # The row is tied to the caller's id — this is what scopes it to the user.
    assert captured["row"]["user_id"] == "userA"
    assert captured["row"]["endpoint"] == _SUB["endpoint"]
    assert captured["kw"].get("on_conflict") == "endpoint"


# ── send path ─────────────────────────────────────────────────────────────────

def test_send_to_user_noop_when_unconfigured(monkeypatch):
    import engine.push as push
    monkeypatch.setattr(cfg, "VAPID_PUBLIC_KEY", "")
    monkeypatch.setattr(cfg, "VAPID_PRIVATE_KEY", "")
    assert push.send_to_user("userA", "T", "B") == 0


def _fake_db(rows):
    class _Q:
        def select(self, *a, **k): return self
        def eq(self, *a, **k): return self
        def execute(self):
            return type("R", (), {"data": rows})()

    class _DB:
        def table(self, name): return _Q()

    return _DB()


def test_send_to_user_delivers_to_each_subscription(monkeypatch):
    import engine.push as push
    monkeypatch.setattr(cfg, "VAPID_PUBLIC_KEY", "pub")
    monkeypatch.setattr(cfg, "VAPID_PRIVATE_KEY", "priv")
    subs = [
        {"endpoint": "https://e1", "p256dh": "a", "auth": "b"},
        {"endpoint": "https://e2", "p256dh": "c", "auth": "d"},
    ]
    monkeypatch.setattr(push, "get_db", lambda: _fake_db(subs))
    delivered = []
    monkeypatch.setattr(push, "_deliver", lambda info, payload: delivered.append(info["endpoint"]))

    n = push.send_to_user("userA", "CoreProp", "2 new +EV slips logged", "/")
    assert n == 2
    assert delivered == ["https://e1", "https://e2"]


def test_send_to_user_prunes_dead_subscriptions(monkeypatch):
    import engine.push as push
    monkeypatch.setattr(cfg, "VAPID_PUBLIC_KEY", "pub")
    monkeypatch.setattr(cfg, "VAPID_PRIVATE_KEY", "priv")
    subs = [{"endpoint": "https://gone", "p256dh": "a", "auth": "b"}]
    monkeypatch.setattr(push, "get_db", lambda: _fake_db(subs))

    class _Dead(Exception):
        def __init__(self):
            self.response = type("r", (), {"status_code": 410})()

    def _boom(info, payload):
        raise _Dead()

    monkeypatch.setattr(push, "_deliver", _boom)

    pruned = {}

    class _WT:
        def delete(self): return self
        def in_(self, col, vals):
            pruned["col"] = col
            pruned["vals"] = list(vals)
            return self
        def execute(self): return None

    class _W:
        def table(self, name): return _WT()

    monkeypatch.setattr(push, "writer", lambda purpose: _W())

    n = push.send_to_user("userA", "T", "B")
    assert n == 0
    # A 410/404 endpoint is expired and must be removed so it isn't retried.
    assert pruned["vals"] == ["https://gone"]
