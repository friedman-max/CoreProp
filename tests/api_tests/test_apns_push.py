"""APNs (native iOS) push: registration endpoints + the per-user send path.

Mirrors test_push.py for the Web Push side. Isolation is the load-bearing
property: a token row is bound to the authenticated user, and the cross-user
send path reads a single user's rows. The whole feature is a no-op unless the
APNS_* keys are configured. Delivery is stubbed via engine.push._deliver_apns so
these run without h2/HTTP-2 or a network.
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


def _configure(monkeypatch):
    monkeypatch.setattr(cfg, "APNS_AUTH_KEY", "-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----")
    monkeypatch.setattr(cfg, "APNS_KEY_ID", "ABC123DEF0")
    monkeypatch.setattr(cfg, "APNS_TEAM_ID", "TEAMID1234")
    monkeypatch.setattr(cfg, "APNS_BUNDLE_ID", "me.coreprop.app")


_TOKEN = "a1b2c3" * 10 + "abcd"   # hex, 64 chars


# ── endpoints ────────────────────────────────────────────────────────────────

def test_register_requires_auth(client):
    c, app, gcu = client
    r = c.post("/api/push/apns/register", json={"device_token": _TOKEN})
    assert r.status_code == 401


def test_register_503_when_apns_unconfigured(client, monkeypatch):
    c, app, gcu = client
    monkeypatch.setattr(cfg, "APNS_AUTH_KEY", "")   # any missing key ⇒ unconfigured
    _as_user(app, gcu, "userA")
    r = c.post("/api/push/apns/register", json={"device_token": _TOKEN})
    assert r.status_code == 503


def test_register_400_on_malformed_token(client, monkeypatch):
    c, app, gcu = client
    _configure(monkeypatch)
    _as_user(app, gcu, "userA")
    for bad in ["", "nothex!!", "z" * 300]:
        r = c.post("/api/push/apns/register", json={"device_token": bad})
        assert r.status_code == 400, bad


def test_register_binds_row_to_authenticated_user(client, monkeypatch):
    c, app, gcu = client
    _configure(monkeypatch)
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
    r = c.post("/api/push/apns/register",
               json={"device_token": _TOKEN, "environment": "sandbox", "bundle_id": "me.coreprop.app"})
    assert r.status_code == 200
    assert captured["table"] == "apns_tokens"
    assert captured["row"]["user_id"] == "userA"
    assert captured["row"]["device_token"] == _TOKEN
    assert captured["row"]["environment"] == "sandbox"
    assert captured["kw"].get("on_conflict") == "device_token"


def test_register_defaults_bad_environment_to_production(client, monkeypatch):
    c, app, gcu = client
    _configure(monkeypatch)
    captured = {}

    class _W:
        def table(self, name): return self
        def upsert(self, row, **kw): captured["row"] = row; return self
        def execute(self): return None

    import web.routers.push as pushrouter
    monkeypatch.setattr(pushrouter, "writer", lambda purpose: _W())
    _as_user(app, gcu, "userA")
    r = c.post("/api/push/apns/register", json={"device_token": _TOKEN, "environment": "bogus"})
    assert r.status_code == 200
    assert captured["row"]["environment"] == "production"


# ── send path ─────────────────────────────────────────────────────────────────

def test_send_apns_noop_when_unconfigured(monkeypatch):
    import engine.push as push
    monkeypatch.setattr(cfg, "APNS_AUTH_KEY", "")
    assert push.send_apns_to_user("userA", "T", "B") == 0


def _fake_db(rows):
    class _Q:
        def select(self, *a, **k): return self
        def eq(self, *a, **k): return self
        def execute(self):
            return type("R", (), {"data": rows})()

    class _DB:
        def table(self, name): return _Q()

    return _DB()


def test_send_apns_delivers_to_each_token(monkeypatch):
    import engine.push as push
    _configure(monkeypatch)
    monkeypatch.setattr(push, "_apns_jwt", lambda: "jwt-token")
    tokens = [
        {"device_token": "t1", "environment": "production"},
        {"device_token": "t2", "environment": "sandbox"},
    ]
    monkeypatch.setattr(push, "get_db", lambda: _fake_db(tokens))
    delivered = []
    monkeypatch.setattr(push, "_deliver_apns",
                        lambda tok, env, payload, jwt_token: (delivered.append((tok, env)) or (200, None)))

    n = push.send_apns_to_user("userA", "CoreProp", "2 new +EV slips logged")
    assert n == 2
    assert delivered == [("t1", "production"), ("t2", "sandbox")]


def test_send_apns_prunes_unregistered_token(monkeypatch):
    import engine.push as push
    _configure(monkeypatch)
    monkeypatch.setattr(push, "_apns_jwt", lambda: "jwt-token")
    monkeypatch.setattr(push, "get_db", lambda: _fake_db([{"device_token": "gone", "environment": "production"}]))
    monkeypatch.setattr(push, "_deliver_apns", lambda *a, **k: (410, "Unregistered"))

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

    n = push.send_apns_to_user("userA", "T", "B")
    assert n == 0
    assert pruned["vals"] == ["gone"]   # 410 Unregistered ⇒ prune


# ── provider JWT (ES256) ────────────────────────────────────────────────────

def test_apns_jwt_is_valid_es256(monkeypatch):
    """Sign a real ES256 provider token with a freshly generated P-256 key and
    verify its header (kid) and issuer claim decode correctly."""
    import jwt
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import serialization

    key = ec.generate_private_key(ec.SECP256R1())
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    pub_pem = key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()

    monkeypatch.setattr(cfg, "APNS_AUTH_KEY", pem)
    monkeypatch.setattr(cfg, "APNS_KEY_ID", "ABC123DEF0")
    monkeypatch.setattr(cfg, "APNS_TEAM_ID", "TEAMID1234")

    import engine.push as push
    # Clear the module cache so this key is used, not a prior test's token.
    monkeypatch.setattr(push, "_APNS_JWT_CACHE", {"token": None, "iat": 0.0})

    tok = push._apns_jwt()
    header = jwt.get_unverified_header(tok)
    assert header["kid"] == "ABC123DEF0"
    assert header["alg"] == "ES256"
    claims = jwt.decode(tok, pub_pem, algorithms=["ES256"], options={"verify_aud": False})
    assert claims["iss"] == "TEAMID1234"
    assert "iat" in claims
