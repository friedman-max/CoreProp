"""Integrity of the payout / break-even constants the sandbox is built on.

These tables are the bedrock: BREAK_EVEN gates every slip, POWER_PAYOUTS /
FLEX_PAYOUTS pay every win. The sandbox UI mirrors BREAK_EVEN in JS
(SB_BREAK_EVEN), and tests/frontend/test_sandbox_live.mjs asserts parity
against this file — so a silent edit here would surface as a frontend test
failure too. These tests pin the internal consistency (power break-evens
are the closed-form geometric root of their payout) and the structural
rules (no 2-leg flex) directly.
"""
from __future__ import annotations

import math
import unittest

from engine.constants import (
    BREAK_EVEN, POWER_PAYOUTS, FLEX_PAYOUTS,
    OPTIMAL_IMPLIED_DECIMAL, OPTIMAL_BREAK_EVEN,
)


class PowerBreakEvenClosedFormTests(unittest.TestCase):
    """Power break-even is p such that p^n * payout == 1, i.e.
    p = (1/payout)^(1/n). Each tabulated value must match to 4 dp."""

    def test_power_break_evens_match_geometric_root(self):
        for n, payout in POWER_PAYOUTS.items():
            expected = (1.0 / payout) ** (1.0 / n)
            tabulated = BREAK_EVEN[(str(n), "power")]
            self.assertAlmostEqual(
                tabulated, expected, places=4,
                msg=f"power-{n} BE {tabulated} != (1/{payout})^(1/{n})={expected:.4f}",
            )

    def test_power_two_is_577(self):
        self.assertAlmostEqual(BREAK_EVEN[("2", "power")], 0.5774, places=4)

    def test_power_six_matches_optimal_break_even(self):
        # Power-6 is the most efficient single-leg implied decimal; its
        # break-even should track OPTIMAL_BREAK_EVEN (1/1.849).
        self.assertAlmostEqual(BREAK_EVEN[("6", "power")], 0.5407, places=4)
        self.assertAlmostEqual(OPTIMAL_BREAK_EVEN, 1.0 / OPTIMAL_IMPLIED_DECIMAL, places=6)
        self.assertAlmostEqual(OPTIMAL_BREAK_EVEN, 0.5408, places=4)


class FlexBreakEvenTests(unittest.TestCase):
    def test_no_two_leg_flex(self):
        # A 2-leg flex degenerates to Power-2 and is not a real product.
        self.assertNotIn(("2", "flex"), BREAK_EVEN)

    def test_flex_defined_for_three_through_six(self):
        for n in (3, 4, 5, 6):
            self.assertIn((str(n), "flex"), BREAK_EVEN, f"flex-{n} missing")

    def test_flex_break_evens_in_plausible_range(self):
        # Every break-even probability must sit strictly between a coin
        # flip and certainty — anything outside that is a data-entry bug.
        for (size, typ), p in BREAK_EVEN.items():
            self.assertTrue(0.5 < p < 0.6, f"{(size, typ)} BE {p} out of range")


class PayoutTableTests(unittest.TestCase):
    def test_power_payouts_strictly_increasing(self):
        sizes = sorted(POWER_PAYOUTS)
        payouts = [POWER_PAYOUTS[s] for s in sizes]
        self.assertEqual(payouts, sorted(payouts))
        self.assertTrue(all(a < b for a, b in zip(payouts, payouts[1:])))

    def test_flex_top_tier_is_full_correct(self):
        # The highest-paying tier in each flex table must be the
        # all-correct count (k == n).
        for n, tiers in FLEX_PAYOUTS.items():
            top_k = max(tiers, key=lambda k: tiers[k])
            self.assertEqual(top_k, n, f"flex-{n} top payout should be at k={n}")

    def test_flex_payout_keys_are_subsets_of_leg_count(self):
        for n, tiers in FLEX_PAYOUTS.items():
            for k in tiers:
                self.assertLessEqual(k, n, f"flex-{n} has impossible tier k={k}")
                self.assertGreaterEqual(k, 0)

    def test_every_break_even_size_has_a_payout(self):
        for (size, typ) in BREAK_EVEN:
            n = int(size)
            if typ == "power":
                self.assertIn(n, POWER_PAYOUTS)
            else:
                self.assertIn(n, FLEX_PAYOUTS)


if __name__ == "__main__":
    unittest.main()
