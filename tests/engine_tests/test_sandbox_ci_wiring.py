"""End-to-end wiring tests for the sandbox bootstrap-CI feature.

The single most important sandbox change: every metric card now carries
a 95% bootstrap CI so a user can tell a +5% ROI [+3%, +7%] (decision-
grade) from a +5% ROI [-3%, +13%] (noise). The compute path:

  1. Server bootstrap when bootstrap:true is on the request
     (`StrategyTester._bootstrap_metrics`).
  2. Client recompute on a date-range filter (`_bootstrapSandbox`).
  3. DOM render under each card.

These tests guard each link of that chain:

  * The strategy_tester bootstrap produces a CI dict with the expected
    shape, in the expected units (percent for roi/winrate/drawdown).
  * The CI brackets the empirical point estimate of its input.
  * Degenerate inputs (n<2) return an empty dict, not garbage.
  * The frontend has subvalue DOM nodes for every metric card (so the
    JS write target exists) and the bootstrap flag is true in
    `_readSandboxControls`.
"""
from __future__ import annotations

import re
import unittest
from pathlib import Path

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


# ── Frontend: DOM contract & control payload ─────────────────────────

_REPO = Path(__file__).resolve().parents[2]
_INDEX_HTML = (_REPO / "web" / "static" / "index.html").read_text(encoding="utf-8")
_APP_JS     = (_REPO / "web" / "static" / "app.js"    ).read_text(encoding="utf-8")


class FrontendWiringTests(unittest.TestCase):
    """If any of these IDs are missing the JS write paths become no-ops
    and the user silently sees no CI. Cheaper than a Selenium test."""

    def test_every_metric_card_has_a_subvalue_node(self):
        # Each card's CI / sub-text element id, declared in index.html.
        expected = [
            "sb-metric-slips-sub",
            "sb-metric-profit-sub",
            "sb-metric-roi-ci",
            "sb-metric-winrate-ci",
            "sb-metric-drawdown-ci",
        ]
        for eid in expected:
            self.assertIn(f'id="{eid}"', _INDEX_HTML,
                f"sandbox card subvalue node #{eid} missing from index.html")

    def test_run_payload_requests_bootstrap_true(self):
        # `_readSandboxControls` must set bootstrap:true; otherwise the
        # server returns no CI on the first response and only client-
        # filtered ranges get bands, which is exactly the regression
        # this whole feature is meant to prevent.
        m = re.search(
            r"function\s+_readSandboxControls[\s\S]{0,2000}?bootstrap:\s*true",
            _APP_JS,
        )
        self.assertIsNotNone(
            m, "_readSandboxControls() no longer sets bootstrap:true — "
               "server CI will not be returned on /api/sandbox/run.",
        )

    def test_render_writes_ci_text_into_subvalue_nodes(self):
        # Sanity: the renderer references each CI subvalue id at least
        # once. Catches a rename in JS that leaves index.html dangling.
        for eid in (
            "sb-metric-roi-ci",
            "sb-metric-winrate-ci",
            "sb-metric-drawdown-ci",
        ):
            self.assertIn(eid, _APP_JS,
                f"renderSandboxResults no longer writes to #{eid}")

    def test_fmt_ci_helper_exists(self):
        self.assertIn("function _fmtCI(", _APP_JS,
            "_fmtCI helper missing — CI text would fall back to raw numbers")

    def test_client_bootstrap_helper_exists(self):
        self.assertIn("function _bootstrapSandbox(", _APP_JS,
            "_bootstrapSandbox missing — date-range filters would lose CI")

    def test_filter_function_recomputes_ci(self):
        # _filterSandboxData must call _bootstrapSandbox so a date-range
        # toggle replaces the stale full-window CI with one matching the
        # visible slips.
        m = re.search(
            r"function\s+_filterSandboxData[\s\S]{0,3500}?_bootstrapSandbox\s*\(",
            _APP_JS,
        )
        self.assertIsNotNone(
            m, "_filterSandboxData no longer recomputes CI on filter — "
               "cards would show CIs from the wrong window.",
        )


if __name__ == "__main__":
    unittest.main()
