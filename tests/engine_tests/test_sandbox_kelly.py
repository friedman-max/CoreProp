"""Quarter-Kelly stake sizing: `StrategyTester._calculate_kelly_fraction`.

When the user enables Kelly, this function decides what fraction of
bankroll to stake on a slip given the per-leg probabilities. It must:
  * return 0 on a -EV slip (never stake into a losing bet),
  * return a positive fraction on a +EV slip,
  * stay quarter-Kelly (scaled by 0.25) and clamped to [0, 1],
  * grow as the edge grows.

The function enumerates all 2^n outcomes, so it's exact for the
independent-Bernoulli model the simulator assumes.
"""
from __future__ import annotations

import unittest

from tests.engine_tests._sandbox_fixtures import make_tester


class KellyEdgeGateTests(unittest.TestCase):
    """A -EV slip must size to zero — Kelly never bets into a loss."""

    def test_negative_ev_power_slip_returns_zero(self):
        t = make_tester()
        # Power-2 at 0.50 each: 0.25 * 3 - 1 = -0.25 EV < 0.
        self.assertEqual(t._calculate_kelly_fraction([0.5, 0.5], 2, "power"), 0.0)

    def test_just_below_break_even_returns_zero(self):
        t = make_tester()
        # Power-2 true break-even is (1/3)^0.5 = 0.57735…. Just below it
        # the slip is -EV, so Kelly must size to zero. (Note: the
        # tabulated BREAK_EVEN value 0.5774 rounds *up* past the true
        # root, so it is marginally +EV — that's why we test 0.577 here.)
        f = t._calculate_kelly_fraction([0.577, 0.577], 2, "power")
        self.assertEqual(f, 0.0)

    def test_positive_ev_power_slip_sizes_up(self):
        t = make_tester()
        # Power-2 at 0.80: 0.64 * 3 - 1 = +0.92 EV.
        f = t._calculate_kelly_fraction([0.8, 0.8], 2, "power")
        self.assertGreater(f, 0.0)
        self.assertLessEqual(f, 1.0)


class KellyMonotonicityTests(unittest.TestCase):
    """A bigger edge should never size smaller (within the +EV region)."""

    def test_stronger_edge_stakes_more(self):
        t = make_tester()
        weak = t._calculate_kelly_fraction([0.70, 0.70], 2, "power")
        strong = t._calculate_kelly_fraction([0.85, 0.85], 2, "power")
        self.assertGreater(weak, 0.0)
        self.assertGreaterEqual(strong, weak)

    def test_fraction_never_exceeds_one(self):
        t = make_tester()
        # Near-certain legs — the raw Kelly number is large; the 0.25
        # scale and the min(...,1.0) clamp must still cap it at 1.0.
        f = t._calculate_kelly_fraction([0.99, 0.99, 0.99], 3, "power")
        self.assertLessEqual(f, 1.0)
        self.assertGreaterEqual(f, 0.0)


class KellyFlexTests(unittest.TestCase):
    """Flex uses the partial-payout tiers, so a slip can be +EV even with
    a leg that occasionally misses."""

    def test_positive_ev_flex_slip_sizes_up(self):
        t = make_tester()
        # 3-leg flex at 0.70 each: pays at k=2 (1.0x) and k=3 (3.0x).
        f = t._calculate_kelly_fraction([0.7, 0.7, 0.7], 3, "flex")
        self.assertGreater(f, 0.0)
        self.assertLessEqual(f, 1.0)

    def test_quarter_kelly_is_smaller_than_full_kelly(self):
        # The implementation multiplies by 0.25. We can't see full Kelly
        # directly, but quarter-Kelly on a strong edge must be well under
        # the full-Kelly ceiling (sanity: a modest fraction, not ~1.0).
        t = make_tester()
        f = t._calculate_kelly_fraction([0.75, 0.75], 2, "power")
        self.assertLess(f, 0.5, "quarter-Kelly on a moderate edge should stay modest")


if __name__ == "__main__":
    unittest.main()
