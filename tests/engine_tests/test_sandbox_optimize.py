"""`StrategyTester.optimize_threshold` — validation gates + threshold sweep.

The optimizer sweeps min_prob over [0.53, 0.58] and returns the ROI-best
point. Before it touches data it runs a battery of cheap config
validations; those guard the user from nonsense configs (2-leg flex,
zero stake, bad slip type) and are the most likely thing a future refactor
breaks silently. We pin every branch, plus one end-to-end sweep over
patched data.
"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd

from tests.engine_tests._sandbox_fixtures import (
    obs_row, make_tester, StrategyConfig, StrategyTester,
)

UTC = timezone.utc


class OptimizeValidationTests(unittest.TestCase):
    """Every guard returns {"error": ...} and never reaches the DB."""

    def setUp(self):
        # db=object() is truthy so we pass the "Database not connected"
        # gate and reach the config validations.
        self.t = make_tester(db=object())

    def test_no_db_short_circuits(self):
        res = make_tester(db=None).optimize_threshold(StrategyConfig())
        self.assertEqual(res["error"], "Database not connected")

    def test_bad_slip_size(self):
        res = self.t.optimize_threshold(StrategyConfig(slip_size=7))
        self.assertIn("slip_size must be one of 2..6", res["error"])

    def test_bad_slip_type(self):
        res = self.t.optimize_threshold(
            StrategyConfig(slip_size=3, slip_type="parlay"))
        self.assertIn("slip_type", res["error"])

    def test_non_positive_bankroll(self):
        res = self.t.optimize_threshold(
            StrategyConfig(slip_size=3, slip_type="power", bankroll=0))
        self.assertIn("bankroll must be positive", res["error"])

    def test_non_positive_bet_size_without_kelly(self):
        res = self.t.optimize_threshold(StrategyConfig(
            slip_size=3, slip_type="power", bankroll=100,
            bet_size=0, use_kelly=False))
        self.assertIn("bet_size must be positive", res["error"])

    def test_zero_bet_allowed_when_kelly_on(self):
        # With Kelly the bet_size gate is skipped; the run proceeds far
        # enough to hit the (patched-away) data layer. We only assert it
        # did NOT trip the bet_size validation.
        with patch.object(StrategyTester, "_fetch_resolved_observatory",
                          return_value=pd.DataFrame()):
            res = self.t.optimize_threshold(StrategyConfig(
                slip_size=3, slip_type="power", bankroll=100,
                bet_size=0, use_kelly=True))
        self.assertNotIn("bet_size must be positive", res.get("error", ""))

    def test_two_leg_flex_rejected(self):
        res = self.t.optimize_threshold(
            StrategyConfig(slip_size=2, slip_type="flex", bankroll=100, bet_size=1.0))
        self.assertEqual(res["error"], "Flex slips require at least 3 legs.")

    def test_empty_observatory_reports_no_data(self):
        with patch.object(StrategyTester, "_fetch_resolved_observatory",
                          return_value=pd.DataFrame()):
            res = self.t.optimize_threshold(StrategyConfig(
                slip_size=3, slip_type="power", bankroll=100, bet_size=1.0))
        self.assertIn("No resolved data", res["error"])


class OptimizeSweepTests(unittest.TestCase):
    """A full sweep over hand-built data returns a best threshold and a
    per-threshold ROI table."""

    def _winning_df(self):
        # Many slates of 3 high-prob legs so slips form at most thresholds
        # in [0.53, 0.58]. Distinct calendar days → distinct slates.
        base = datetime(2026, 4, 1, 19, 0, tzinfo=UTC)
        rows = []
        for d in range(8):
            game = base + timedelta(days=d)
            seen = game - timedelta(hours=2)
            for i in range(3):
                rows.append(obs_row(
                    player=f"D{d}_P{i}", team=f"T{i}",
                    first_seen=seen, last_seen=seen + timedelta(minutes=5),
                    game_start=game, true_prob=0.66, result="hit"))
        return pd.DataFrame(rows)

    def test_sweep_returns_best_threshold_and_results(self):
        t = make_tester(db=object())
        df = self._winning_df()
        # Use the legacy slate-based strategy ("top_ev") so the optimizer's
        # _simulate_at_threshold slate loop builds slips (the sweep path
        # does not call _replay_live_auto_builder).
        with patch.object(StrategyTester, "_fetch_resolved_observatory",
                          return_value=df), \
             patch.object(t, "_apply_current_calibration",
                          side_effect=lambda d: d.assign(
                              calibrated_prob=pd.to_numeric(d["true_prob"]))):
            res = t.optimize_threshold(StrategyConfig(
                leagues=["NBA"], slip_size=3, slip_type="power",
                bankroll=100, bet_size=1.0, use_kelly=False,
                slip_strategy="top_ev", use_calibration=True))
        self.assertNotIn("error", res, res.get("error"))
        self.assertIn("best_threshold", res)
        self.assertIn("best_roi", res)
        self.assertIn("all_results", res)
        self.assertTrue(len(res["all_results"]) >= 1)
        for row in res["all_results"]:
            self.assertIn("threshold", row)
            self.assertIn("roi", row)
            self.assertIn("slips", row)


if __name__ == "__main__":
    unittest.main()
