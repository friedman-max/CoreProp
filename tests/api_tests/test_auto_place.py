"""Auto-place: the arming endpoint + the on-failure reform helper.

Arming (POST /api/user/auto-place-prefs) records the user's intent to auto-place
for a set stake; it is gated by AUTO_PLACE_ENABLED and requires explicit consent.
Reform (_reform_failed_slip) is the "if placement fails, drop the unstable legs
and re-form the rest" behavior — verified here against a fake DB + stub pp_lines.
"""
from __future__ import annotations

import pytest

import config as cfg  # noqa: F401  (kept for parity with sibling tests)
import web.app as app_mod


@pytest.fixture()
def client():
    from fastapi.testclient import TestClient
    from web.app import app
    from web.auth import get_current_user
    with TestClient(app) as c:
        yield c, app, get_current_user
    app.dependency_overrides.clear()


def _as_user(app, gcu, uid="userA"):
    app.dependency_overrides[gcu] = lambda: {"id": uid, "email": f"{uid}@t", "jwt": "x"}


class _UpsertDB:
    def __init__(self): self.upserted = None
    def table(self, name): self._t = name; return self
    def upsert(self, payload, **kw): self.upserted = payload; return self
    def execute(self): return type("R", (), {"data": []})()


# ── arming endpoint ───────────────────────────────────────────────────────────

def test_arm_requires_server_flag(client, monkeypatch):
    c, app, gcu = client
    monkeypatch.setattr(app_mod, "AUTO_PLACE_ENABLED", False)
    _as_user(app, gcu)
    r = c.post("/api/user/auto-place-prefs", json={"mode": "live", "consent": True})
    assert r.status_code == 403


def test_arm_rejects_bad_mode(client, monkeypatch):
    c, app, gcu = client
    monkeypatch.setattr(app_mod, "AUTO_PLACE_ENABLED", True)
    _as_user(app, gcu)
    r = c.post("/api/user/auto-place-prefs", json={"mode": "wild"})
    assert r.status_code == 400


def test_arm_requires_consent(client, monkeypatch):
    c, app, gcu = client
    monkeypatch.setattr(app_mod, "AUTO_PLACE_ENABLED", True)
    _as_user(app, gcu)
    r = c.post("/api/user/auto-place-prefs", json={"mode": "live", "consent": False})
    assert r.status_code == 400


def test_arm_rejects_out_of_range_stake(client, monkeypatch):
    c, app, gcu = client
    monkeypatch.setattr(app_mod, "AUTO_PLACE_ENABLED", True)
    db = _UpsertDB()
    monkeypatch.setattr("engine.database.get_user_db", lambda jwt: db)
    _as_user(app, gcu)
    for bad in (0, -5, 1001):
        r = c.post("/api/user/auto-place-prefs", json={"mode": "live", "consent": True, "stake": bad})
        assert r.status_code == 400, bad


def test_arm_live_persists_stake_cap_and_consent(client, monkeypatch):
    c, app, gcu = client
    monkeypatch.setattr(app_mod, "AUTO_PLACE_ENABLED", True)
    db = _UpsertDB()
    monkeypatch.setattr("engine.database.get_user_db", lambda jwt: db)
    _as_user(app, gcu, "userZ")

    r = c.post("/api/user/auto-place-prefs",
               json={"mode": "live", "stake": 10, "daily_cap": 50, "consent": True})
    assert r.status_code == 200
    p = db.upserted
    assert p["user_id"] == "userZ"
    assert p["auto_place_mode"] == "live"
    # The set dollar amount is both the stake and its ceiling.
    assert p["auto_place_stake"] == 10 and p["auto_place_max_stake"] == 10
    assert p["auto_place_daily_cap"] == 50
    assert p["auto_place_consent_at"]                # consent timestamp recorded
    assert p["auto_place_fail_streak"] == 0          # re-arm clears a prior disarm


def test_disarm_off_needs_no_consent(client, monkeypatch):
    c, app, gcu = client
    monkeypatch.setattr(app_mod, "AUTO_PLACE_ENABLED", True)
    db = _UpsertDB()
    monkeypatch.setattr("engine.database.get_user_db", lambda jwt: db)
    _as_user(app, gcu)
    r = c.post("/api/user/auto-place-prefs", json={"mode": "off"})
    assert r.status_code == 200
    assert db.upserted["auto_place_mode"] == "off"
    assert "auto_place_consent_at" not in db.upserted   # disarm leaves prior consent intact


# ── reform on failure ─────────────────────────────────────────────────────────

class _ReformDB:
    """Fake Supabase client for _reform_failed_slip. Records delete/insert ops;
    returns configured legs + slip_type on select."""
    def __init__(self, legs, slip_type="Power"):
        self.ops = []
        self._select = {"legs": legs, "slips": [{"slip_type": slip_type}]}

    def table(self, name):
        return _ReformQ(self, name)


class _ReformQ:
    def __init__(self, db, name): self.db = db; self.name = name; self.op = "select"; self.payload = None
    def select(self, *a, **k): self.op = "select"; return self
    def eq(self, *a, **k): return self
    def order(self, *a, **k): return self
    def limit(self, *a, **k): return self
    def insert(self, payload): self.db.ops.append(("insert", self.name, payload)); return self
    def delete(self, *a, **k): self.db.ops.append(("delete", self.name)); return self
    def execute(self):
        if self.op == "select":
            return type("R", (), {"data": self.db._select.get(self.name, [])})()
        return type("R", (), {"data": []})()


def _pp(player, stat, line, side="both"):
    return {"player_name": player, "stat_type": stat, "line_score": line, "side": side}


def _leg(n, player, prop, line, side):
    return {"leg_num": n, "player": player, "league": "NBA", "prop": prop,
            "line": line, "side": side, "true_prob": 0.6, "ind_ev_pct": 0.05, "game_start": ""}


def test_reform_drops_unstable_and_reforms(monkeypatch):
    legs = [
        _leg(1, "LeBron James", "Points", 25.5, "over"),
        _leg(2, "Nikola Jokic", "Rebounds", 12.5, "under"),
        _leg(3, "Luka Doncic", "Assists", 9.5, "over"),   # will be UNstable (no pp match)
    ]
    # pp_lines cover legs 1 & 2 but NOT leg 3 (its line moved / was pulled).
    pp = [_pp("LeBron James", "Points", 25.5), _pp("Nikola Jokic", "Rebounds", 12.5)]
    monkeypatch.setitem(app_mod._state, "pp_lines", pp)

    captured = {}
    monkeypatch.setattr("engine.backtest.insert_legs_idempotent",
                        lambda db, sid, db_legs: captured.update({"sid": sid, "legs": db_legs}))

    db = _ReformDB(legs, slip_type="Flex")
    out = app_mod._reform_failed_slip(db, "userA", "OLDSLIP1")

    assert out and out["reformed"] is True
    assert out["dropped"] == 1 and out["kept"] == 2
    # The failed slip's legs + header were deleted.
    assert ("delete", "legs") in db.ops and ("delete", "slips") in db.ops
    # A new slip header was inserted with only the 2 stable legs.
    slip_inserts = [o for o in db.ops if o[0] == "insert" and o[1] == "slips"]
    assert slip_inserts and slip_inserts[0][2]["n_legs"] == 2
    assert slip_inserts[0][2]["slip_type"] == "Flex"
    # The 2 stable legs were re-logged via the idempotent inserter.
    assert captured["legs"] and len(captured["legs"]) == 2
    kept_players = {l["player"] for l in captured["legs"]}
    assert kept_players == {"LeBron James", "Nikola Jokic"}   # Luka (unstable) dropped


def test_reform_noop_when_all_legs_available(monkeypatch):
    legs = [
        _leg(1, "LeBron James", "Points", 25.5, "over"),
        _leg(2, "Nikola Jokic", "Rebounds", 12.5, "under"),
    ]
    pp = [_pp("LeBron James", "Points", 25.5), _pp("Nikola Jokic", "Rebounds", 12.5)]
    monkeypatch.setitem(app_mod._state, "pp_lines", pp)
    db = _ReformDB(legs)
    out = app_mod._reform_failed_slip(db, "userA", "SLIP")
    # Transient failure (nothing unstable) — must NOT delete/reform (would loop).
    assert out is None
    assert not any(o[0] in ("delete", "insert") for o in db.ops)


def test_reform_drops_slip_when_too_few_stable(monkeypatch):
    legs = [
        _leg(1, "LeBron James", "Points", 25.5, "over"),   # stable
        _leg(2, "Luka Doncic", "Assists", 9.5, "over"),    # unstable
    ]
    pp = [_pp("LeBron James", "Points", 25.5)]
    monkeypatch.setitem(app_mod._state, "pp_lines", pp)
    db = _ReformDB(legs)
    out = app_mod._reform_failed_slip(db, "userA", "SLIP")
    # 1 stable leg < 2 → delete the failed slip, don't reform (can't make a slip).
    assert out and out["reformed"] is False and out["kept"] == 1
    assert ("delete", "legs") in db.ops and ("delete", "slips") in db.ops
    assert not any(o[0] == "insert" for o in db.ops)
