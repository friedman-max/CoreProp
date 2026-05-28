"""Push/DNP-aware payout math: `StrategyTester._payout_with_push`.

This is the function that turns a slip's per-leg results into the decimal
multiplier the user is paid. It mirrors the live PrizePicks API
(web/app.py:2446-2458). Every dollar of simulated P&L flows through here,
so its edge cases — pushes, DNPs, the 2-leg-flex-is-power-2 quirk, the
n_eff==1 refund rule — are worth pinning hard.

Returns (payout_mult, n_eff, hits_eff, n_pushed):
  * payout_mult: decimal multiplier on stake. 1.0 == refund, >1 == win.
  * n_eff:   legs that weren't pushed/DNP'd (push and DNP shrink the slip).
  * hits_eff: legs that actually hit.
  * n_pushed: legs removed by push or DNP.
"""
from __future__ import annotations

import unittest

from engine.strategy_tester import StrategyTester
from engine.constants import POWER_PAYOUTS, FLEX_PAYOUTS

PWP = StrategyTester._payout_with_push


class PowerPayoutTests(unittest.TestCase):
    def test_all_hit_pays_the_power_table(self):
        for n in (2, 3, 4, 5, 6):
            mult, n_eff, hits, pushed = PWP(["hit"] * n, "power")
            self.assertEqual(mult, POWER_PAYOUTS[n], f"power-{n} all-hit")
            self.assertEqual((n_eff, hits, pushed), (n, n, 0))

    def test_one_miss_zeroes_a_power_slip(self):
        mult, n_eff, hits, pushed = PWP(["hit", "hit", "miss"], "power")
        self.assertEqual(mult, 0.0)
        self.assertEqual((n_eff, hits, pushed), (3, 2, 0))

    def test_win_aliases_count_as_hits(self):
        # The live data uses "hit"/"won"/"win" interchangeably.
        mult, _n_eff, hits, _p = PWP(["won", "win", "hit"], "power")
        self.assertEqual(mult, POWER_PAYOUTS[3])
        self.assertEqual(hits, 3)


class FlexPayoutTests(unittest.TestCase):
    def test_flex_partial_tiers_pay_out(self):
        # 5-leg flex, 4 correct → FLEX_PAYOUTS[5][4] = 2.0.
        mult, n_eff, hits, _p = PWP(["hit"] * 4 + ["miss"], "flex")
        self.assertEqual(mult, FLEX_PAYOUTS[5][4])
        self.assertEqual((n_eff, hits), (5, 4))

    def test_flex_below_lowest_paying_tier_is_zero(self):
        # 6-leg flex pays only at k>=4; 3 hits is below the tier → 0.
        mult, _n_eff, hits, _p = PWP(["hit"] * 3 + ["miss"] * 3, "flex")
        self.assertEqual(mult, 0.0)
        self.assertEqual(hits, 3)

    def test_two_leg_flex_is_treated_as_power_two(self):
        # PrizePicks degenerates a 2-leg flex to Power-2 (3x). The slip
        # builder forbids 2-leg flex, but _payout_with_push can still see
        # n_eff==2 after pushes shrink a larger flex.
        mult, n_eff, hits, _p = PWP(["hit", "hit"], "flex")
        self.assertEqual(mult, POWER_PAYOUTS[2])
        self.assertEqual((n_eff, hits), (2, 2))

    def test_two_leg_flex_one_miss_is_zero(self):
        mult, _n_eff, hits, _p = PWP(["hit", "miss"], "flex")
        self.assertEqual(mult, 0.0)
        self.assertEqual(hits, 1)  # n_eff==1 path, see below


class PushAndDnpTests(unittest.TestCase):
    def test_all_pushed_refunds_in_full(self):
        mult, n_eff, hits, pushed = PWP(["push", "push", "push"], "power")
        self.assertEqual(mult, 1.0)
        self.assertEqual((n_eff, hits, pushed), (0, 0, 3))

    def test_dnp_is_treated_like_push(self):
        # 3-leg power, one DNP, two hits → n_eff shrinks to 2, both hit,
        # so it pays the Power-2 table (3x).
        mult, n_eff, hits, pushed = PWP(["hit", "hit", "dnp"], "power")
        self.assertEqual(mult, POWER_PAYOUTS[2])
        self.assertEqual((n_eff, hits, pushed), (2, 2, 1))

    def test_push_shrinks_flex_size_changing_the_tier(self):
        # 6-leg flex with 2 pushes → effective 4-leg flex. 3 of the 4
        # remaining hit → FLEX_PAYOUTS[4][3] = 1.5.
        results = ["hit", "hit", "hit", "miss", "push", "push"]
        mult, n_eff, hits, pushed = PWP(results, "flex")
        self.assertEqual(n_eff, 4)
        self.assertEqual(hits, 3)
        self.assertEqual(pushed, 2)
        self.assertEqual(mult, FLEX_PAYOUTS[4][3])

    def test_single_effective_leg_hit_refunds(self):
        # n_eff == 1 is neither a power nor flex payout; PP refunds a
        # winning lone leg (1.0) and zeroes a losing one.
        mult, n_eff, hits, pushed = PWP(["hit", "push", "push"], "power")
        self.assertEqual((mult, n_eff, hits, pushed), (1.0, 1, 1, 2))

    def test_single_effective_leg_miss_is_zero(self):
        mult, n_eff, hits, pushed = PWP(["miss", "push", "dnp"], "flex")
        self.assertEqual((mult, n_eff, hits, pushed), (0.0, 1, 0, 2))

    def test_n_pushed_counts_both_push_and_dnp(self):
        _m, n_eff, _h, pushed = PWP(["hit", "hit", "push", "dnp"], "power")
        self.assertEqual(n_eff, 2)
        self.assertEqual(pushed, 2)


if __name__ == "__main__":
    unittest.main()
