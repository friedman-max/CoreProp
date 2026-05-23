"""Calibration-pipeline regression tests.

Two invariants guarded here:

1. The hourly isotonic refit must NOT feed `legs.closing_prob` back into
   itself. That field is the *calibrated* closing-line probability
   (CLVTracker writes the post-isotonic value so the CLV % displayed
   to users is apples-to-apples with the stored true_prob). Feeding it
   to PAV as a target makes the calibrator learn raw → calibrated
   instead of raw → outcome — a drift toward an identity-through-
   calibration fixed point. The fix: pass None for the legs CLV signal.
   `market_observatory.closing_prob` is the *raw* value and still
   contributes, so the calibrator keeps the closing-line signal where
   it's clean.

2. Apply-time calibration output is bounded in [0.001, 0.999] on every
   surface (BetResult / CLVTracker / observatory). Without a floor, a
   degenerate PAV interpolant or a deliberately-extreme raw prob could
   feed log-loss / EV math a zero and crash the slip evaluator.
"""
from __future__ import annotations

import unittest
from unittest.mock import patch
from datetime import datetime, timezone


class CalibratorIgnoresLegsClosingProb(unittest.TestCase):
    """Walk the legs ingest loop directly and confirm closing_prob is
    not propagated into the per-bucket sufficient statistics.

    We test by patching `_ingest_resolved_row` and recording every call
    the legs loop makes; the `closing_prob` argument must be None for
    every legs row even when the row dict itself carries one. This is
    the cheapest regression check that doesn't need a live database."""

    def test_legs_loop_passes_none_for_closing_prob(self):
        from engine import isotonic_calibration as iso

        # Build two legs rows that look like they came from the DB.
        # closing_prob is set so we can prove it's stripped.
        leg_rows = [
            {
                "league": "NBA", "prop": "Points", "side": "over",
                "raw_true_prob": 0.55, "true_prob": 0.58,
                "result": "hit",
                "closing_prob": 0.62,            # calibrated value — must be ignored
                "resolved_at": "2026-05-22T10:00:00+00:00",
            },
            {
                "league": "MLB", "prop": "Hits", "side": "under",
                "raw_true_prob": 0.48, "true_prob": 0.50,
                "result": "miss",
                "closing_prob": 0.45,
                "resolved_at": "2026-05-22T11:00:00+00:00",
            },
        ]

        # Capture every ingest call so we can inspect the closing_prob
        # passed for each row (positional arg index matches the function
        # signature: state, league, prop, side, true_prob, result,
        # closing_prob, ra_dt, now, confidence_weight=...).
        captured: list[dict] = []

        def _spy(state, league, prop, side, true_prob, result,
                 closing_prob, ra_dt, now, confidence_weight=1.0):
            captured.append({
                "league": league, "prop": prop, "side": side,
                "raw": true_prob, "result": result,
                "closing_prob": closing_prob,
            })

        # Patch the ingest helper and the row-pull helpers so the
        # update routine runs entirely against in-memory fixtures.
        # _pull_resolved_since returns the leg fixtures and an empty
        # list for the observatory side; the function ignores DB
        # connectivity once both lists are populated.
        def _fake_pull(db, table, hwm, cols):
            if table == "legs":
                return leg_rows
            return []

        class _FakeDB:
            def __init__(self):
                pass

        # save_state etc. are exercised at the end of update; stub them
        # so the test doesn't need a real disk or network. We don't
        # care about the persisted artefact — only the in-loop behavior.
        with patch.object(iso, "get_db", return_value=_FakeDB()), \
             patch.object(iso, "_pull_resolved_since", side_effect=_fake_pull), \
             patch.object(iso, "_ingest_resolved_row", side_effect=_spy), \
             patch.object(iso, "_save_state", lambda *_a, **_k: None), \
             patch("engine.persistence.sync_state_to_supabase",
                   lambda *_a, **_k: None), \
             patch("builtins.open", create=True), \
             patch("os.makedirs", lambda *_a, **_k: None), \
             patch("json.dump", lambda *_a, **_k: None):
            iso.update_isotonic_calibration()

        self.assertGreaterEqual(len(captured), 2,
            "Expected both legs rows to flow through _ingest_resolved_row")
        for row in captured:
            self.assertIsNone(
                row["closing_prob"],
                f"legs ingest fed closing_prob={row['closing_prob']!r} "
                f"into the calibrator (feedback loop) for "
                f"{row['league']}|{row['prop']}|{row['side']}",
            )


class CalibratedProbabilityIsBounded(unittest.TestCase):
    """BetResult must clip the calibrated probability to [0.001, 0.999].
    Below the floor breaks log-loss (log(0) = -inf); above the ceiling
    breaks the optimal-EV math (1.0 implies infinite Kelly fraction)."""

    def test_floor_applied_when_calibration_returns_zero(self):
        from engine import ev_calculator as evc
        from engine.ev_calculator import BetResult

        # Force the apply path to return 0.0 — easy way is to swap the
        # _apply_calibration symbol used inside the BetResult init.
        with patch.object(evc, "_apply_calibration", return_value=0.0):
            bet = BetResult(
                bet_id="t1", player_name="Test", league="NBA",
                prop_type="Points", pp_line=20.5, fd_line=20.5,
                side="over", true_prob=0.55, over_odds=-110,
                under_odds=-110, both_sided=True, pp_player_id="p1",
            )
        self.assertGreaterEqual(
            bet.true_prob, 0.001,
            "Apply-time floor missing — calibrated prob would feed 0 "
            "into downstream log-loss / EV math",
        )

    def test_ceiling_applied_when_calibration_returns_one(self):
        from engine import ev_calculator as evc
        from engine.ev_calculator import BetResult

        with patch.object(evc, "_apply_calibration", return_value=1.0):
            bet = BetResult(
                bet_id="t2", player_name="Test", league="NBA",
                prop_type="Points", pp_line=20.5, fd_line=20.5,
                side="over", true_prob=0.55, over_odds=-110,
                under_odds=-110, both_sided=True, pp_player_id="p1",
            )
        self.assertLessEqual(bet.true_prob, 0.999)

    def test_typical_value_passes_through(self):
        """Sanity: the clip must not perturb in-range outputs."""
        from engine import ev_calculator as evc
        from engine.ev_calculator import BetResult

        with patch.object(evc, "_apply_calibration", return_value=0.55):
            bet = BetResult(
                bet_id="t3", player_name="Test", league="NBA",
                prop_type="Points", pp_line=20.5, fd_line=20.5,
                side="over", true_prob=0.55, over_odds=-110,
                under_odds=-110, both_sided=True, pp_player_id="p1",
            )
        self.assertAlmostEqual(bet.true_prob, 0.55, places=6)


if __name__ == "__main__":
    unittest.main()
