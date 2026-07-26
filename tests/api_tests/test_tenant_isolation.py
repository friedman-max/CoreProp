"""Cross-user data isolation: the app layer must filter by user_id itself.

Today every user-scoped read goes through `get_user_db(jwt)` and issues an
UNFILTERED query — `db.table("slips").select(...)` with no `.eq("user_id", ...)`.
That returns only the caller's rows *solely* because Supabase RLS
(`user_id = auth.uid()`, migration_001) scopes visibility. RLS is therefore a
single point of failure with no second layer: disable it, misconfigure a policy,
or hand `get_user_db` anything with service-role privileges (which BYPASSES RLS)
and those endpoints start serving every user's history. `SUPABASE_ANON_KEY`
used to fall back to the service key, which made that last one a single missing
env var — see `test_anon_key_exposure.py`, which now pins the refusal.

These tests pin the defense-in-depth requirement. `_UnscopedDB` emulates a
Postgres with RLS OFF: it holds both users' rows and honors ONLY the filters
the application actually passes. A query that forgets `user_id` therefore sees
its neighbour's rows, and the assertion below fails.

Note this is not a test of RLS itself (that lives in the database and is
verified by migration_018 / test_rls_migration.py) — it's a test that the
application does not *depend* on RLS for correctness.
"""
from __future__ import annotations

import pytest


class _Res:
    def __init__(self, data):
        self.data = data


class _Query:
    """PostgREST query builder that applies the filters it is given, and
    nothing more. No implicit tenant scoping — that's the point."""

    def __init__(self, rows: list[dict]):
        self._rows = list(rows)
        self._eq: list[tuple[str, object]] = []
        self._in: list[tuple[str, list]] = []
        self._lo = None
        self._hi = None
        self._limit = None
        self._op = "select"

    # ── filters ───────────────────────────────────────────────────────────
    def select(self, *a, **k):
        return self

    def eq(self, col, val):
        self._eq.append((col, val))
        return self

    def in_(self, col, vals):
        self._in.append((col, list(vals)))
        return self

    def gte(self, *a, **k):
        return self

    def order(self, *a, **k):
        return self

    def limit(self, n):
        self._limit = n
        return self

    def range(self, lo, hi):
        self._lo, self._hi = lo, hi
        return self

    def delete(self, *a, **k):
        self._op = "delete"
        return self

    def insert(self, rows, *a, **k):
        self._op = "insert"
        self._payload = rows
        return self

    # ── execution ─────────────────────────────────────────────────────────
    def _matching(self) -> list[dict]:
        out = []
        for r in self._rows:
            if any(r.get(c) != v for c, v in self._eq):
                continue
            if any(r.get(c) not in vals for c, vals in self._in):
                continue
            out.append(r)
        return out

    def execute(self):
        rows = self._matching()
        if self._op == "delete":
            # Record what a real DELETE would have removed so the test can
            # assert a cross-user delete touches nothing.
            for r in rows:
                self._rows.remove(r)
            return _Res(rows)
        if self._op == "insert":
            return _Res([])
        if self._lo is not None:
            rows = rows[self._lo : self._hi + 1]
        elif self._limit is not None:
            rows = rows[: self._limit]
        return _Res(rows)


class _UnscopedDB:
    """A database with RLS switched off: every caller sees every row unless
    the query itself filters."""

    def __init__(self, tables: dict[str, list[dict]]):
        self.tables = tables

    def table(self, name):
        return _Query(self.tables.setdefault(name, []))


# Two users' worth of history living in the same tables.
def _two_user_fixture() -> _UnscopedDB:
    return _UnscopedDB(
        {
            "slips": [
                {
                    "id": "slipA",
                    "user_id": "userA",
                    "timestamp": "2026-07-01T20:00:00Z",
                    "slip_type": "power",
                    "n_legs": 2,
                    "proj_slip_ev_pct": 5.0,
                },
                {
                    "id": "slipB",
                    "user_id": "userB",
                    "timestamp": "2026-07-02T20:00:00Z",
                    "slip_type": "power",
                    "n_legs": 2,
                    "proj_slip_ev_pct": 6.0,
                },
            ],
            "legs": [
                {
                    "slip_id": "slipA",
                    "user_id": "userA",
                    "leg_num": 1,
                    "player": "A-Player-1",
                    "league": "MLB",
                    "prop": "Hits",
                    "line": 1.5,
                    "side": "over",
                    "true_prob": 0.6,
                    "result": "hit",
                    "stat_actual": 2,
                    "game_start": "2026-07-01T23:00:00Z",
                    "closing_prob": 0.61,
                    "clv_pct": 1.0,
                },
                {
                    "slip_id": "slipA",
                    "user_id": "userA",
                    "leg_num": 2,
                    "player": "A-Player-2",
                    "league": "MLB",
                    "prop": "Hits",
                    "line": 1.5,
                    "side": "over",
                    "true_prob": 0.6,
                    "result": "hit",
                    "stat_actual": 2,
                    "game_start": "2026-07-01T23:00:00Z",
                    "closing_prob": 0.61,
                    "clv_pct": 1.0,
                },
                {
                    "slip_id": "slipB",
                    "user_id": "userB",
                    "leg_num": 1,
                    "player": "B-Player-1",
                    "league": "MLB",
                    "prop": "Hits",
                    "line": 1.5,
                    "side": "over",
                    "true_prob": 0.7,
                    "result": "hit",
                    "stat_actual": 3,
                    "game_start": "2026-07-02T23:00:00Z",
                    "closing_prob": 0.71,
                    "clv_pct": 1.0,
                },
                {
                    "slip_id": "slipB",
                    "user_id": "userB",
                    "leg_num": 2,
                    "player": "B-Player-2",
                    "league": "MLB",
                    "prop": "Hits",
                    "line": 1.5,
                    "side": "over",
                    "true_prob": 0.7,
                    "result": "miss",
                    "stat_actual": 0,
                    "game_start": "2026-07-02T23:00:00Z",
                    "closing_prob": 0.71,
                    "clv_pct": 1.0,
                },
            ],
        }
    )


@pytest.fixture()
def client():
    from fastapi.testclient import TestClient
    from web.app import app

    with TestClient(app) as c:
        yield c, app
    app.dependency_overrides.clear()


def _as_user(app, uid):
    from web.auth import get_current_user

    app.dependency_overrides[get_current_user] = lambda: {
        "id": uid,
        "email": f"{uid}@t",
        "jwt": f"jwt-{uid}",
    }


@pytest.fixture()
def unscoped_db(monkeypatch):
    db = _two_user_fixture()
    import engine.database as dbmod
    import engine.calibration as cal

    monkeypatch.setattr(dbmod, "get_user_db", lambda jwt: db)
    monkeypatch.setattr(cal, "get_user_db", lambda jwt: db)
    return db


def test_backtest_slips_returns_only_the_callers_slips(client, unscoped_db):
    """GET /api/backtest/slips must scope to the caller, not rely on RLS."""
    c, app = client
    _as_user(app, "userA")

    body = c.get("/api/backtest/slips").json()
    ids = {s["id"] for s in body["slips"]}

    assert ids == {"slipA"}, f"leaked another user's slips: {ids}"
    players = {l["player"] for s in body["slips"] for l in s["legs"]}
    assert all(p.startswith("A-") for p in players), f"leaked another user's legs: {players}"


def test_backtest_slips_scopes_legs_even_when_slip_ids_are_known(client, unscoped_db):
    """The legs fetch keys off slip_id. It must ALSO scope by user_id so a
    known/guessed slip_id from another tenant can't pull that tenant's legs."""
    c, app = client
    _as_user(app, "userB")

    body = c.get("/api/backtest/slips").json()
    players = {l["player"] for s in body["slips"] for l in s["legs"]}

    assert players == {"B-Player-1", "B-Player-2"}, f"leaked legs: {players}"


def test_delete_slip_cannot_touch_another_users_slip(client, unscoped_db):
    """DELETE /api/backtest/slip/{id} verifies ownership by SELECTing the row.
    Without a user_id filter that check passes for any slip id in the table."""
    c, app = client
    _as_user(app, "userA")

    r = c.delete("/api/backtest/slip/slipB")

    assert r.status_code == 404, (
        f"userA was allowed to delete userB's slip (status {r.status_code})"
    )
    # And userB's rows must still be there.
    remaining = {s["id"] for s in unscoped_db.tables["slips"]}
    assert "slipB" in remaining, "another user's slip row was deleted"
    remaining_legs = {l["slip_id"] for l in unscoped_db.tables["legs"]}
    assert "slipB" in remaining_legs, "another user's legs were deleted"


def test_analytics_counts_only_the_callers_legs(client, unscoped_db):
    """Analytics/calibration aggregate every leg the DB hands back. Unscoped,
    userA's Brier/hit-rate silently folds in userB's outcomes."""
    c, app = client
    _as_user(app, "userA")

    body = c.get("/api/analytics").json()

    # userA has 2 resolved legs (both hits). userB adds 2 more (1 hit, 1 miss),
    # so an unscoped read reports n_resolved=4 and hit_rate=0.75.
    assert body.get("n_resolved") == 2, (
        f"analytics counted another user's legs: n_resolved={body.get('n_resolved')}"
    )
    assert body.get("hit_rate") == pytest.approx(1.0), (
        f"hit rate is polluted by another user's miss: {body.get('hit_rate')}"
    )


def test_calibration_counts_only_the_callers_legs(client, unscoped_db):
    c, app = client
    _as_user(app, "userA")

    body = c.get("/api/calibration").json()

    assert body.get("n_resolved") == 2, (
        f"calibration counted another user's legs: n_resolved={body.get('n_resolved')}"
    )
    assert body.get("n_won") == 2, (
        f"calibration counted another user's outcomes: n_won={body.get('n_won')}"
    )
