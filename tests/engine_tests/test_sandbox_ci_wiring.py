"""Backend wiring tests for the sandbox bootstrap-CI feature.

Every metric card carries a 95% bootstrap CI so a user can tell a
+5% ROI [+3%, +7%] (decision-grade) from a +5% ROI [-3%, +13%] (noise).
The server computes it in `StrategyTester._bootstrap_metrics` when
bootstrap:true is on the request.

These tests pin the CI dict shape and units the React sandbox consumes
(`ci.roi_pct.lo/hi`, etc.). The frontend side of this wiring — the React
component in web/static/page-sandbox.jsx — is covered by
tests/frontend/test_sandbox_live.mjs (the old vanilla-JS app.js the
previous DOM-contract tests asserted against was removed in the React
migration).
"""
from __future__ import annotations

import unittest

from engine.strategy_tester import StrategyTester


# ── Backend: strategy_tester._bootstrap_metrics ──────────────────────

def _bet_slip(profit: float) -> dict:
    """Minimal slip shape `_bootstrap_metrics` reads."""
    bet = 1.0
    payout = bet + profit
    return {"bet_size": bet, "payout": payout, "profit": profit}


class BootstrapShapeTests(unittest.TestCase):
    """The frontend reads `ci.roi_pct.lo / hi`, `ci.win_rate_pct.lo / hi`,
    `ci.max_drawdown_pct.lo / hi`. A drift in any of those keys would
    silently blank out the cards. Pin the shape down."""

    def test_returns_all_expected_keys_with_lo_hi_pairs(self):
        slips = [_bet_slip(p) for p in [5, -1, 5, -1, 5, -1, -1, 5, -1, -1] * 6]
        ci = StrategyTester._bootstrap_metrics(slips, n_resamples=200)
        self.assertIsInstance(ci, dict)
        for k in ("roi_pct", "win_rate_pct", "max_drawdown_pct"):
            self.assertIn(k, ci, f"missing top-level key {k!r}")
            self.assertIn("lo", ci[k], f"missing lo on {k!r}")
            self.assertIn("hi", ci[k], f"missing hi on {k!r}")
            self.assertLessEqual(ci[k]["lo"], ci[k]["hi"],
                f"lo > hi on {k!r}: {ci[k]}")
        self.assertIn("n_resamples", ci)
        self.assertEqual(ci["n_resamples"], 200)

    def test_empty_input_returns_empty_dict(self):
        ci = StrategyTester._bootstrap_metrics([], n_resamples=50)
        self.assertEqual(ci, {})

    def test_single_slip_returns_empty_dict(self):
        # With only one bet slip every resample picks the same row, so
        # the CI degenerates to a single point. The UI renders that as
        # "+200%, +200%" — vacuous and misleading. Skip and let the UI
        # show "—" instead.
        ci = StrategyTester._bootstrap_metrics(
            [_bet_slip(5)], n_resamples=200,
        )
        self.assertEqual(ci, {},
            "n<2 should suppress the CI to avoid degenerate "
            "[+x%, +x%] bands")

    def test_brackets_point_estimate_for_strong_signal(self):
        # 100 slips, every one wins +5u. ROI = 500%. The 95% CI from
        # bootstrap resampling MUST include the point estimate.
        slips = [_bet_slip(5) for _ in range(100)]
        ci = StrategyTester._bootstrap_metrics(slips, n_resamples=200)
        self.assertGreater(ci["roi_pct"]["lo"], 0,
            "ROI lo must be > 0 for an all-wins strategy")
        self.assertLessEqual(ci["roi_pct"]["lo"], 500.0)
        self.assertGreaterEqual(ci["roi_pct"]["hi"], 500.0,
            "ROI hi must reach the empirical 500% on resamples")

    def test_noisy_strategy_straddles_zero(self):
        # 30 coin-flip slips: +1u / -1u alternating. Empirical ROI ≈ 0;
        # bootstrap CI must contain 0 — that's the "don't trust this"
        # signal users need.
        slips = [_bet_slip(1 if i % 2 == 0 else -1) for i in range(30)]
        ci = StrategyTester._bootstrap_metrics(slips, n_resamples=300)
        self.assertLessEqual(ci["roi_pct"]["lo"], 0.0)
        self.assertGreaterEqual(ci["roi_pct"]["hi"], 0.0)


if __name__ == "__main__":
    unittest.main()
