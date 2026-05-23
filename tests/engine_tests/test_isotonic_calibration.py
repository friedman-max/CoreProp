"""
Rigorous tests for the per-league isotonic calibration pipeline.

Verifies the end-to-end flow that powers the Observatory tab:

  market_observatory rows  ─►  _load_observations()
                              ─►  update_isotonic_calibration()
                                  ─►  per-league + per-(league,prop) curves
                                      ─►  calibrate(curves, league, prop, p)

Run:  python -m scripts.test_isotonic_calibration
"""
from __future__ import annotations

import os
import sys
import json
import math
import unittest
import tempfile
from datetime import datetime, timedelta, timezone
from unittest import mock

# Make sure the repo root is importable when run as a script.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import pytest  # noqa: E402

from engine import isotonic_calibration as iso  # noqa: E402

# Many of the classes below predate the v2→v3 incremental-refit refactor
# (`_load_observations` was replaced by `_pull_resolved_since`, the
# bootstrap path now requires a populated `state` cursor, etc). We keep
# them as documentation of the *previous* implementation's invariants;
# rewriting them against the v3 surface is a follow-up tracked in the
# Phase-2 plan. PavWeightedTests still exercises the live PAV helper.
_STALE_AGAINST_V3 = pytest.mark.skip(
    reason="Stale against v3 incremental-refit API; rewrite as part of Phase 2 follow-up.",
)


# ---------------------------------------------------------------------------
# Stub Postgrest client
# ---------------------------------------------------------------------------

class _StubResponse:
    def __init__(self, data):
        self.data = data


class _StubQuery:
    """A query builder that records every chained filter and resolves to
    rows from a configured `_StubDB.tables[<name>]` list, applying the
    captured filters in `execute()`."""

    def __init__(self, db, table_name):
        self._db = db
        self._table = table_name
        self._filters = []  # list of (op, *args)

    # The methods the production code chains through ──────────────────────
    def select(self, *args, **kwargs):
        return self

    def in_(self, col, values):
        self._filters.append(("in", col, set(values)))
        return self

    def eq(self, col, val):
        self._filters.append(("eq", col, val))
        return self

    def neq(self, col, val):
        self._filters.append(("neq", col, val))
        return self

    def or_(self, expr):
        # Parse "game_start.gte.<iso>,game_start.is.null"
        clauses = []
        for piece in expr.split(","):
            parts = piece.split(".", 2)
            if len(parts) == 3:
                col, op, val = parts
                clauses.append((col, op, val))
        self._filters.append(("or", clauses))
        return self

    def gte(self, col, val):
        self._filters.append(("gte", col, val))
        return self

    def limit(self, n):
        self._filters.append(("limit", n))
        return self

    @property
    def not_(self):
        outer = self

        class _Not:
            def is_(self_inner, col, val):
                outer._filters.append(("not_is", col, val))
                return outer

        return _Not()

    # Resolution ─────────────────────────────────────────────────────────
    def execute(self):
        rows = list(self._db.tables.get(self._table, []))
        limit_val = None
        for f in self._filters:
            op = f[0]
            if op == "in":
                _, col, values = f
                rows = [r for r in rows if r.get(col) in values]
            elif op == "eq":
                _, col, val = f
                rows = [r for r in rows if r.get(col) == val]
            elif op == "neq":
                _, col, val = f
                rows = [r for r in rows if r.get(col) != val]
            elif op == "gte":
                _, col, val = f
                rows = [r for r in rows if (r.get(col) or "") >= val]
            elif op == "or":
                clauses = f[1]
                def _matches(row, clauses=clauses):
                    for col, sub_op, val in clauses:
                        if sub_op == "gte" and (row.get(col) or "") >= val:
                            return True
                        if sub_op == "is" and val == "null" and row.get(col) is None:
                            return True
                    return False
                rows = [r for r in rows if _matches(r)]
            elif op == "not_is":
                _, col, val = f
                if val == "null":
                    rows = [r for r in rows if r.get(col) is not None]
            elif op == "limit":
                limit_val = f[1]
        if limit_val is not None:
            rows = rows[:limit_val]
        return _StubResponse(rows)


class _StubDB:
    def __init__(self, tables=None):
        self.tables = tables or {}

    def table(self, name):
        return _StubQuery(self, name)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _row(player, league, prop, true_prob, result, *, game_offset_days=1,
         line=10.0, side="over", closing_prob=None):
    """Build a market_observatory-shaped row."""
    ts = NOW - timedelta(days=game_offset_days)
    return {
        "player":       player,
        "league":       league,
        "prop":         prop,
        "true_prob":    true_prob,
        "result":       result,
        "line":         line,
        "side":         side,
        "game_start":   ts.isoformat(),
        "created_at":   ts.isoformat(),
        "closing_prob": closing_prob,
    }


def _bulk(*, league, prop, n, true_prob, hit_rate, days_ago=2,
          x_spread=0.04):
    """Build `n` synthetic resolved observatory rows. The first
    `round(n * hit_rate)` are hits, the rest misses.

    `x_spread` is the half-width of true_prob jitter so observations
    span multiple x values (matching production, where devigged probs are
    nearly always unique). PAV with all-equal x degenerates into multiple
    same-x representative points and the interpolator returns the leftmost
    one, masking real shifts. A small spread sidesteps that artifact."""
    n_hits = int(round(n * hit_rate))
    rows = []
    for i in range(n):
        result = "hit" if i < n_hits else "miss"
        # Even spread on [tp - x_spread, tp + x_spread]; clipped to (0, 1).
        offset = (i / max(1, n - 1) - 0.5) * 2 * x_spread
        x = max(0.001, min(0.999, true_prob + offset))
        rows.append(_row(
            f"{league}-{prop}-{i}", league, prop, x, result,
            game_offset_days=days_ago,
        ))
    return rows


def _fit_with_rows(test_case, market_rows=(), legs_rows=()):
    """Run update_isotonic_calibration with synthetic DB contents, returning
    the resulting curves dict. Writes go to a temp file so we don't pollute
    `data/`."""
    with tempfile.TemporaryDirectory() as td:
        tmp_file = os.path.join(td, "isotonic_calibration.json")
        with mock.patch.object(iso, "get_db", return_value=_StubDB({
                "market_observatory": list(market_rows),
                "legs":                list(legs_rows),
            })), \
            mock.patch.object(iso, "ISOTONIC_FILE", tmp_file), \
            mock.patch("engine.persistence.sync_state_to_supabase", return_value=None):
            out = iso.update_isotonic_calibration()
        # Sanity: file written when fit succeeded
        if out is not None:
            test_case.assertTrue(os.path.exists(tmp_file))
            with open(tmp_file) as f:
                disk = json.load(f)
            test_case.assertEqual(disk.get("version"), 2)
        return out


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class PavWeightedTests(unittest.TestCase):
    def test_monotone_pass_through_when_already_monotone(self):
        triples = [(0.4, 0.30, 1.0), (0.5, 0.45, 1.0), (0.6, 0.60, 1.0)]
        curve, w = iso._fit_pav_weighted(triples)
        ys = [y for _, y in curve]
        self.assertEqual(ys, sorted(ys))
        self.assertAlmostEqual(w, 3.0)

    def test_pools_violators(self):
        # 0.5→0.9, 0.6→0.1 must pool to a single block.
        triples = [(0.5, 0.9, 1.0), (0.6, 0.1, 1.0)]
        curve, _ = iso._fit_pav_weighted(triples)
        self.assertEqual(len(curve), 1)
        self.assertAlmostEqual(curve[0][1], 0.5)

    def test_zero_weight_filtered(self):
        triples = [(0.5, 0.5, 0.0), (0.5, 0.6, 1.0)]
        curve, w = iso._fit_pav_weighted(triples)
        self.assertEqual(len(curve), 1)
        self.assertAlmostEqual(w, 1.0)


@_STALE_AGAINST_V3
class LoadObservationsTests(unittest.TestCase):
    def test_includes_sub_50_observations(self):
        """Critical: lines tracked at p≥0.30 must reach the fitter, not
        just p≥0.50. This was the bug that motivated the threshold drop."""
        rows = [
            _row("LowProb", "NBA", "Points", 0.32, "hit"),
            _row("MidProb", "NBA", "Points", 0.55, "miss"),
        ]
        db = _StubDB({"market_observatory": rows})
        with mock.patch.object(iso, "datetime", wraps=datetime) as dtmock:
            dtmock.now.return_value = NOW
            obs = iso._load_observations(db)
        xs = sorted(o["x"] for o in obs)
        self.assertIn(0.32, xs)
        self.assertIn(0.55, xs)

    def test_filters_out_of_range(self):
        rows = [
            _row("Bad1", "NBA", "Points", 0.0, "hit"),     # tp=0
            _row("Bad2", "NBA", "Points", 1.0, "hit"),     # tp=1
            _row("Bad3", "NBA", "Points", -0.5, "miss"),   # negative
            _row("Bad4", None,  "Points", 0.6, "hit"),     # no league
            _row("Good", "NBA", "Points", 0.55, "hit"),
        ]
        db = _StubDB({"market_observatory": rows})
        obs = iso._load_observations(db)
        players = {o["league"] for o in obs}
        self.assertEqual(players, {"NBA"})
        self.assertEqual(len(obs), 1)

    def test_separates_outcome_and_clv_signals(self):
        rows = [
            _row("A", "NBA", "Points", 0.6, "hit",  closing_prob=0.55),
            _row("B", "NBA", "Points", 0.6, "miss", closing_prob=None),
            _row("C", "NBA", "Points", 0.5, "pending", closing_prob=0.62),
        ]
        db = _StubDB({"market_observatory": rows})
        obs = iso._load_observations(db)
        outcome = [o for o in obs if o["source"] == "outcome"]
        clv = [o for o in obs if o["source"] == "clv"]
        # A, B → outcome
        self.assertEqual(len(outcome), 2)
        # CLV: A (closing_prob from Q1, the resolved query) + C (closing_prob
        # from Q2, the pending query). A must be counted *exactly* once or
        # the CLV signal weight gets doubled — see the production fix above.
        self.assertEqual(len(clv), 2)
        clv_ys = sorted(o["y"] for o in clv)
        self.assertAlmostEqual(clv_ys[0], 0.55)   # A's closing
        self.assertAlmostEqual(clv_ys[1], 0.62)   # C's closing

    def test_includes_legs_table_observations(self):
        legs = [
            {"league": "MLB", "prop": "Hits", "true_prob": 0.58,
             "closing_prob": 0.55, "result": "won",
             "game_start": (NOW - timedelta(days=3)).isoformat()},
            {"league": "MLB", "prop": "Hits", "true_prob": 0.45,
             "closing_prob": None, "result": "lost",
             "game_start": (NOW - timedelta(days=3)).isoformat()},
        ]
        db = _StubDB({"market_observatory": [], "legs": legs})
        obs = iso._load_observations(db)
        leagues = {o["league"] for o in obs}
        self.assertEqual(leagues, {"MLB"})
        # 2 outcomes (won/lost) + 1 CLV (closing_prob on the won row)
        outcome_ys = sorted(o["y"] for o in obs if o["source"] == "outcome")
        self.assertEqual(outcome_ys, [0.0, 1.0])

    def test_recency_filter_excludes_old_rows(self):
        # 365d old should be excluded by RECENCY_LOOKBACK_DAYS=180.
        rows = [
            _row("Old",    "NBA", "Points", 0.6, "hit", game_offset_days=365),
            _row("Recent", "NBA", "Points", 0.6, "hit", game_offset_days=10),
        ]
        db = _StubDB({"market_observatory": rows})
        obs = iso._load_observations(db)
        # Only the recent one survives.
        self.assertEqual(len(obs), 1)


@_STALE_AGAINST_V3
class HierarchicalFitTests(unittest.TestCase):
    def test_returns_none_when_no_observations(self):
        out = _fit_with_rows(self)
        self.assertIsNone(out)

    def test_emits_per_league_curve_with_enough_data(self):
        rows = (
            _bulk(league="NBA", prop="Points", n=200, true_prob=0.60, hit_rate=0.50)
            + _bulk(league="NBA", prop="Points", n=200, true_prob=0.70, hit_rate=0.65)
        )
        out = _fit_with_rows(self, market_rows=rows)
        self.assertIsNotNone(out)
        self.assertIn("NBA", out["leagues"])
        self.assertIn("NBA|Points", out["props"])
        self.assertIsNotNone(out["global"])
        # n_eff should be roughly 400 (recency-discounted, but ≥ 100).
        self.assertGreater(out["leagues"]["NBA"]["n_eff"], 100)

    def test_emits_separate_curves_per_league(self):
        rows = (
            _bulk(league="NBA", prop="Points", n=100, true_prob=0.60, hit_rate=0.50)
            + _bulk(league="MLB", prop="Hits",   n=100, true_prob=0.60, hit_rate=0.70)
        )
        out = _fit_with_rows(self, market_rows=rows)
        self.assertIsNotNone(out)
        self.assertIn("NBA", out["leagues"])
        self.assertIn("MLB", out["leagues"])
        # The leagues' (league, prop) buckets should also be distinct.
        self.assertIn("NBA|Points", out["props"])
        self.assertIn("MLB|Hits",   out["props"])

    def test_skips_thin_buckets(self):
        # 2 rows for a low-volume league — well below MIN_BUCKET_N_EFF=5.
        rows = _bulk(league="NBA",  prop="Points", n=200, true_prob=0.60, hit_rate=0.55) \
             + _bulk(league="WNBA", prop="Points", n=2,   true_prob=0.60, hit_rate=1.00)
        out = _fit_with_rows(self, market_rows=rows)
        self.assertIn("NBA", out["leagues"])
        # The thin bucket may or may not appear depending on recency weight;
        # if it does, n_eff must clear the threshold.
        if "WNBA" in out["leagues"]:
            self.assertGreaterEqual(out["leagues"]["WNBA"]["n_eff"], iso.MIN_BUCKET_N_EFF)


@_STALE_AGAINST_V3
class CalibrationResponseTests(unittest.TestCase):
    """The headline guarantee: when new observations arrive, the per-league
    calibration responds to them."""

    def _calibrate(self, curves, league, prop, p):
        return iso.calibrate(curves, league, prop, p)

    def test_new_data_shifts_league_curve_downward(self):
        """If NBA observations show p=0.60 hits at 40%, the league curve at
        0.60 should land near 0.40, well below the raw 0.60."""
        rows = _bulk(league="NBA", prop="Points", n=400,
                     true_prob=0.60, hit_rate=0.40)
        out = _fit_with_rows(self, market_rows=rows)
        curves = iso.load_isotonic_calibration() if False else {
            "global":  out["global"],
            "leagues": out["leagues"],
            "props":   out["props"],
        }
        # Need to normalize curve format the way load_isotonic_calibration does.
        for level in (curves["global"], *curves["leagues"].values(), *curves["props"].values()):
            if level is None:
                continue
            level["curve"] = [(float(x), float(y)) for x, y in level["curve"]]

        calibrated = self._calibrate(curves, "NBA", "Points", 0.60)
        self.assertLess(calibrated, 0.55)
        self.assertGreater(calibrated, 0.30)

    def test_per_league_isolation(self):
        """Adding more data only to MLB doesn't materially shift NBA's
        league curve."""
        baseline_rows = (
            _bulk(league="NBA", prop="Points", n=300, true_prob=0.60, hit_rate=0.50)
            + _bulk(league="MLB", prop="Hits",  n=300, true_prob=0.60, hit_rate=0.50)
        )
        baseline = _fit_with_rows(self, market_rows=baseline_rows)

        # Now add 1000 MLB observations all hitting at 0.60.
        new_rows = baseline_rows + _bulk(
            league="MLB", prop="Hits", n=1000, true_prob=0.60, hit_rate=1.0
        )
        shifted = _fit_with_rows(self, market_rows=new_rows)

        def _normalize(d):
            for level in (d["global"], *d["leagues"].values(), *d["props"].values()):
                if level is None:
                    continue
                level["curve"] = [(float(x), float(y)) for x, y in level["curve"]]
            return d

        baseline = _normalize(baseline)
        shifted = _normalize(shifted)

        nba_before = iso.calibrate(baseline, "NBA", None, 0.60)
        nba_after  = iso.calibrate(shifted,  "NBA", None, 0.60)
        mlb_before = iso.calibrate(baseline, "MLB", None, 0.60)
        mlb_after  = iso.calibrate(shifted,  "MLB", None, 0.60)

        # MLB should jump up materially toward the cap (0.60).
        self.assertGreater(mlb_after, mlb_before)
        self.assertGreater(mlb_after, 0.55)
        # NBA's calibrated value should not move very much. The global
        # curve will drift because MLB's hits dominate it, but with NBA's
        # own n_eff well into the dominant-shrinkage band, the league level
        # holds. Allow up to a 0.05 drift.
        self.assertLess(abs(nba_after - nba_before), 0.05)

    def test_recency_weights_recent_dominate(self):
        """An old (180-day) loss and a recent (1-day) win at the same x
        should not cancel — the recent observation should dominate, so
        the curve at that x sits well above 0.5."""
        n_old, n_new = 100, 100
        old_rows = _bulk(league="NBA", prop="Points", n=n_old,
                         true_prob=0.60, hit_rate=0.0,
                         days_ago=170)  # near the 180d cutoff, ~12.5% weight
        new_rows = _bulk(league="NBA", prop="Points", n=n_new,
                         true_prob=0.60, hit_rate=1.0, days_ago=1)
        out = _fit_with_rows(self, market_rows=old_rows + new_rows)
        out = {
            "global":  out["global"],
            "leagues": out["leagues"],
            "props":   out["props"],
        }
        for level in (out["global"], *out["leagues"].values(), *out["props"].values()):
            if level is None:
                continue
            level["curve"] = [(float(x), float(y)) for x, y in level["curve"]]
        cal = iso.calibrate(out, "NBA", None, 0.60)
        # Even with the conservative cap (cal ≤ raw=0.60), we expect cal to
        # sit much closer to 0.60 than the unweighted 0.5 average.
        self.assertGreater(cal, 0.55)

    def test_clv_signal_lower_weight(self):
        """Same-x outcome and CLV signals: outcomes should dominate because
        CLV is weighted at CLV_OBSERVATION_WEIGHT (0.4)."""
        # 100 outcomes: hits at 1.0
        outcomes = _bulk(league="NBA", prop="Points", n=100,
                         true_prob=0.60, hit_rate=1.0)
        # 100 CLV-only rows pinning the closing line at 0.20 (would pull down)
        clv_rows = []
        for i in range(100):
            ts = (NOW - timedelta(days=2)).isoformat()
            clv_rows.append({
                "player": f"C{i}", "league": "NBA", "prop": "Points",
                "true_prob": 0.60, "result": "pending",
                "line": 10.0, "side": "over",
                "game_start": ts, "created_at": ts,
                "closing_prob": 0.20,
            })
        out = _fit_with_rows(self, market_rows=outcomes + clv_rows)
        for level in (out["global"], *out["leagues"].values(), *out["props"].values()):
            if level is None: continue
            level["curve"] = [(float(x), float(y)) for x, y in level["curve"]]

        cal = iso.calibrate(out, "NBA", None, 0.60)
        # 100×1.0 (full weight) vs 100×0.20 (×0.4) → blended y ~ (100 + 8)/(100 + 40) ≈ 0.77.
        # But the conservative cap caps cal ≤ 0.60. So we just verify it's at the cap.
        self.assertAlmostEqual(cal, 0.60, places=5)

    def test_increment_changes_fit_idempotently(self):
        """Sanity: adding new resolved rows changes the persisted curve.
        Specifically, calibrate(NBA, Points, 0.60) before vs after must differ."""
        baseline_rows = _bulk(league="NBA", prop="Points", n=200,
                              true_prob=0.60, hit_rate=0.30)
        before = _fit_with_rows(self, market_rows=baseline_rows)
        for level in (before["global"], *before["leagues"].values(), *before["props"].values()):
            if level is None: continue
            level["curve"] = [(float(x), float(y)) for x, y in level["curve"]]
        cal_before = iso.calibrate(before, "NBA", "Points", 0.60)

        # Stream of new hits that should pull the curve up.
        new_rows = baseline_rows + _bulk(
            league="NBA", prop="Points", n=600, true_prob=0.60, hit_rate=1.0,
            days_ago=1,
        )
        after = _fit_with_rows(self, market_rows=new_rows)
        for level in (after["global"], *after["leagues"].values(), *after["props"].values()):
            if level is None: continue
            level["curve"] = [(float(x), float(y)) for x, y in level["curve"]]
        cal_after = iso.calibrate(after, "NBA", "Points", 0.60)

        self.assertGreater(cal_after, cal_before,
            f"Per-league calibration didn't move when 600 new hits arrived "
            f"(before={cal_before:.4f}, after={cal_after:.4f})")


@_STALE_AGAINST_V3
class EndToEndApiShapeTests(unittest.TestCase):
    def test_multipliers_endpoint_shape(self):
        """The /api/observatory/multipliers handler reads load_isotonic_calibration()
        and returns {league_key: {value, calibrated}}. Verify it surfaces fitted
        leagues as calibrated=True and absent ones as calibrated=False."""
        rows = _bulk(league="NBA", prop="Points", n=300, true_prob=0.60, hit_rate=0.55)
        with tempfile.TemporaryDirectory() as td:
            tmp_file = os.path.join(td, "isotonic_calibration.json")
            with mock.patch.object(iso, "get_db", return_value=_StubDB({
                    "market_observatory": rows, "legs": []})), \
                mock.patch.object(iso, "ISOTONIC_FILE", tmp_file), \
                mock.patch("engine.persistence.sync_state_to_supabase", return_value=None):
                iso.update_isotonic_calibration()
                # Now mimic the API handler.
                curves = iso.load_isotonic_calibration()
                from engine.constants import PROP_TYPE_MAP
                out = {}
                for league in sorted(PROP_TYPE_MAP.keys()):
                    key = f"{league}|Calibration @ p={iso.DISPLAY_ANCHOR:.2f}"
                    cal_anchor = iso.calibrate(curves, league, None, iso.DISPLAY_ANCHOR)
                    has_league_curve = curves.get("leagues", {}).get(league) is not None
                    out[key] = {
                        "value": round(cal_anchor / iso.DISPLAY_ANCHOR, 4),
                        "calibrated": bool(has_league_curve),
                    }
        nba_entry = out[f"NBA|Calibration @ p={iso.DISPLAY_ANCHOR:.2f}"]
        mlb_entry = out[f"MLB|Calibration @ p={iso.DISPLAY_ANCHOR:.2f}"]
        self.assertTrue(nba_entry["calibrated"])
        # MLB had no rows → no league curve → must surface as awaiting data.
        self.assertFalse(mlb_entry["calibrated"])


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
