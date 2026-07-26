"""The correlation fit must not pair a market against its own complement.

`engine/observatory.py` logs EVERY priced side with raw prob >= OBS_MIN_PROB
(0.30). So any market devigging into [0.30, 0.70] — the bulk of props — writes
TWO rows: the over and the under. `results_checker.grade_leg` makes the under
row the exact complement of the over row (one hits iff the other misses, absent
a push).

`update_correlation_map` selected `player, league, game_start, result, prop` —
with NO `side` — and then enumerated all within-game pairs. Every market was
therefore paired against its own mirror image, which is a PERFECT NEGATIVE
correlation, and those pairs swamped the real signal:

    NBA|same_game    -> phi  0.000, rho  0.000  (correct heuristic: 0.144)
    NBA|same_player  -> phi -0.333, rho -0.500  (correct heuristic: 0.360)

Both cleared MIN_PAIR_OBS easily, because the same double-logging inflates n.
`_pair_correlation` prefers a "trusted" empirical bucket over the heuristic, so
the garbage won.

Downstream: with same_game rho == 0, `calculate_slip` sees np.allclose(corr, I)
and short-circuits to the independence formulas — so the correlation model
silently did nothing at all. For PrizePicks that understates same-game stacks
(positive correlation HELPS Power/Flex, where all-hit is the payout condition),
so the cost is missed opportunity rather than losses.

These tests assert the fit reads `side` and only pairs like-for-like.
"""
from __future__ import annotations

import engine.correlation as corr


class _Res:
    def __init__(self, data):
        self.data = data


class _Query:
    def __init__(self, rows):
        self._rows = rows
        self.selected = ""
        self.ranged = False

    def select(self, cols, *a, **k):
        self.selected = cols
        return self

    def in_(self, *a, **k):
        return self

    def or_(self, *a, **k):
        return self

    def order(self, *a, **k):
        return self

    def range(self, lo, hi):
        self.ranged = True
        self._lo, self._hi = lo, hi
        return self

    def execute(self):
        if self.ranged:
            return _Res(self._rows[self._lo : self._hi + 1])
        return _Res(self._rows)


class _DB:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def table(self, name):
        q = _Query(self.rows)
        self.queries.append(q)
        return q


def _both_sided_rows(n_games: int = 60) -> list[dict]:
    """Two players per game, each with one market logged on BOTH sides.

    Within a game both players hit together (a genuine POSITIVE same-game
    correlation). The complement 'under' rows are the mirror of each 'over'.
    """
    rows = []
    for g in range(n_games):
        gs = f"2026-07-{(g % 28) + 1:02d}T23:00:00Z"
        outcome = "hit" if g % 2 == 0 else "miss"       # both players share it
        for p in ("alice", "bob"):
            rows.append({"player": p, "league": "NBA", "game_start": gs,
                         "prop": "POINTS", "side": "over", "result": outcome})
            # The complement row the observatory also logs.
            rows.append({"player": p, "league": "NBA", "game_start": gs,
                         "prop": "POINTS", "side": "under",
                         "result": "miss" if outcome == "hit" else "hit"})
    return rows


def test_fit_requests_the_side_column(monkeypatch):
    db = _DB(_both_sided_rows())
    import engine.database as dbmod
    monkeypatch.setattr(dbmod, "get_db", lambda: db)
    monkeypatch.setattr(corr, "_persist_correlation_map", lambda *a, **k: None,
                        raising=False)
    corr.update_correlation_map()

    assert db.queries, "no query was issued"
    selected = db.queries[0].selected
    assert "side" in selected, (
        "the correlation fit does not select `side`, so it cannot tell an over "
        f"from its complementary under. Selected: {selected!r}"
    )


def test_same_game_correlation_is_positive_not_cancelled(monkeypatch):
    """The headline symptom: complement pairs cancel the real signal to ~0."""
    db = _DB(_both_sided_rows())
    import engine.database as dbmod
    monkeypatch.setattr(dbmod, "get_db", lambda: db)
    monkeypatch.setattr(corr, "_persist_correlation_map", lambda *a, **k: None,
                        raising=False)

    out = corr.update_correlation_map()
    assert out, "fit returned nothing"
    buckets = out.get("buckets") or {}

    same_game = None
    for key, val in buckets.items():
        if key.upper().startswith("NBA") and "SAME_GAME" in key.upper() and "|POINTS" not in key.upper():
            same_game = val
            break
    assert same_game is not None, f"no NBA same_game bucket in {list(buckets)[:8]}"

    rho = same_game.get("rho_latent") if isinstance(same_game, dict) else same_game
    assert rho is not None, f"bucket has no rho: {same_game}"
    assert rho > 0.05, (
        f"same-game rho is {rho:.4f}. The fixture has players hitting TOGETHER, "
        "so rho must be clearly positive; a value near zero means complementary "
        "under-rows cancelled the signal — and rho==0 makes calculate_slip "
        "short-circuit to the independence formula, disabling the model."
    )


def test_fit_paginates_the_observatory_read(monkeypatch):
    """PostgREST silently caps unbounded selects at 1000 rows. Every other
    observatory reader in the repo paginates; this one did not, so the fit saw
    an arbitrary sample — and pairs scale as n^2, so the loss is severe."""
    db = _DB(_both_sided_rows(n_games=400))    # 1600 rows, past the cap
    import engine.database as dbmod
    monkeypatch.setattr(dbmod, "get_db", lambda: db)
    monkeypatch.setattr(corr, "_persist_correlation_map", lambda *a, **k: None,
                        raising=False)
    corr.update_correlation_map()

    assert any(q.ranged for q in db.queries), (
        "the correlation fit issues an unpaginated select; beyond 1000 rows it "
        "silently fits on an arbitrary subset"
    )
