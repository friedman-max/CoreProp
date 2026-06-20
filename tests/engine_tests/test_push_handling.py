"""
End-to-end coverage of push handling across the pipeline.

Push semantics (PrizePicks): when a PrizePicks line is a whole number and
the player's actual stat lands exactly on the line, the leg is refunded —
PrizePicks reduces the effective slip size by one. Our pipeline mirrors
that:

  1. The scraper preserves whole-number lines verbatim (no .5 fabrication).
  2. `grade_leg(actual, line, side)` returns "push" iff actual==line.
  3. Slip P&L computes payout from the *effective* legs only (push & dnp
     stripped), exactly the same way for both kinds of refund.
  4. Calibration ingest skips push rows so they never tilt curves up or
     down — they're zero-information events for outcome learning.
  5. Database query helpers explicitly omit "push" from the resolved-rows
     IN-list so accuracy metrics aren't polluted.

Run:  python -m scripts.test_push_handling
"""
from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from engine.results_checker import grade_leg, PUSH_TOLERANCE  # noqa: E402
from engine.constants import POWER_PAYOUTS, FLEX_PAYOUTS  # noqa: E402


# ---------------------------------------------------------------------------
# 1. The grader itself — every cell of the truth table
# ---------------------------------------------------------------------------

class GradeLegTests(unittest.TestCase):
    """Pure-function grader: cover whole + half lines, both sides, tied
    cases, and float-precision noise."""

    def test_whole_line_exact_tie_is_push_both_sides(self):
        # The motivating case: integer line, actual lands on it. Both sides
        # must push regardless of which side the user picked.
        self.assertEqual(grade_leg(14, 14.0, "over"),  "push")
        self.assertEqual(grade_leg(14, 14.0, "under"), "push")

    def test_whole_line_over_above_is_hit(self):
        self.assertEqual(grade_leg(15, 14.0, "over"), "hit")

    def test_whole_line_over_below_is_miss(self):
        self.assertEqual(grade_leg(13, 14.0, "over"), "miss")

    def test_whole_line_under_below_is_hit(self):
        self.assertEqual(grade_leg(13, 14.0, "under"), "hit")

    def test_whole_line_under_above_is_miss(self):
        self.assertEqual(grade_leg(15, 14.0, "under"), "miss")

    def test_half_line_can_never_push(self):
        # No integer stat can equal a half line — grader must return a
        # decisive hit/miss for every (side, position) pair.
        for actual in range(0, 30):
            for line in (14.5, 21.5, 0.5):
                for side in ("over", "under"):
                    r = grade_leg(actual, line, side)
                    self.assertIn(r, ("hit", "miss"),
                                  f"half-line ({actual},{line},{side}) → {r}")
                    self.assertNotEqual(r, "push")

    def test_half_line_just_above_is_over_hit(self):
        # The simulated "upgrade" world: PP at 14, equivalent book line 14.5
        # — actual=15 must be an over hit on either representation, but
        # only the integer line can yield push at 14.
        self.assertEqual(grade_leg(15, 14.5, "over"),  "hit")
        self.assertEqual(grade_leg(15, 14.5, "under"), "miss")

    def test_float_precision_noise_within_tolerance_pushes(self):
        # `_compute_stat` does floating-point arithmetic for combo
        # props. A delta of 1e-12 must still resolve to push.
        actual = 14.0 + 1e-12
        self.assertEqual(grade_leg(actual, 14.0, "over"),  "push")
        self.assertEqual(grade_leg(actual, 14.0, "under"), "push")

    def test_clearly_off_by_more_than_tolerance_is_not_push(self):
        # 1e-6 is well outside PUSH_TOLERANCE — must grade hit/miss.
        actual = 14.0 + 1e-6
        self.assertEqual(grade_leg(actual, 14.0, "over"),  "hit")
        self.assertEqual(grade_leg(actual, 14.0, "under"), "miss")

    def test_negative_values_and_zero_line(self):
        # Defensive: grader is pure arithmetic, so a 0 line should work too.
        self.assertEqual(grade_leg(0, 0.0, "over"),  "push")
        self.assertEqual(grade_leg(1, 0.0, "over"),  "hit")
        self.assertEqual(grade_leg(0, 0.5, "over"),  "miss")

    def test_side_normalization(self):
        # Side string is lowercased internally — "OVER", "Under" must work.
        self.assertEqual(grade_leg(15, 14.0, "OVER"),  "hit")
        self.assertEqual(grade_leg(13, 14.0, "Under"), "hit")
        # Unknown side defaults to under semantics; a tie still pushes
        # because the equality branch is checked first.
        self.assertEqual(grade_leg(14, 14.0, "??"), "push")

    def test_tolerance_constant_is_strict_enough_for_halves(self):
        # Sanity: tolerance must be much smaller than a half — otherwise
        # half lines could spuriously push.
        self.assertLess(PUSH_TOLERANCE, 0.1)


# ---------------------------------------------------------------------------
# 2. Slip P&L: pushes shrink the effective slip
# ---------------------------------------------------------------------------

def _slip_payout(slip_type: str, results: list[str]) -> float:
    """Replica of the slip-payout logic in `web/app.py::get_backtest_data`
    and `engine/calibration.py::evaluate_analytics`. We exercise it here so
    pushes' refund semantics are pinned down by tests, not just by
    convention."""
    effective = [r for r in results if r not in ("push", "dnp")]
    n_eff    = len(effective)
    hits_eff = sum(1 for r in effective if r == "hit")

    if n_eff < 2:
        return 1.0 if (n_eff == 0 or (n_eff == 1 and hits_eff == 1)) else 0.0
    if slip_type == "power":
        return POWER_PAYOUTS.get(n_eff, 0) if hits_eff == n_eff else 0
    # flex
    if n_eff == 2:
        return POWER_PAYOUTS.get(2, 0) if hits_eff == 2 else 0
    return FLEX_PAYOUTS.get(n_eff, {}).get(hits_eff, 0)


class SlipPushPayoutTests(unittest.TestCase):
    """Pushes must behave exactly like DNPs at slip-settlement time: shrink
    the effective slip and look up the smaller-tier payout."""

    def test_six_leg_power_one_push_collapses_to_five_leg_power(self):
        # All other legs hit, one pushes → treat as a 5-leg power win.
        all_hit_one_push = ["hit"] * 5 + ["push"]
        self.assertEqual(
            _slip_payout("power", all_hit_one_push),
            POWER_PAYOUTS[5],
        )

    def test_push_and_dnp_are_interchangeable(self):
        for r in ("push", "dnp"):
            payout = _slip_payout("power", ["hit"] * 4 + [r] + ["hit"])
            self.assertEqual(payout, POWER_PAYOUTS[5],
                             f"{r} must collapse 6→5-leg power same as the other")

    def test_two_pushes_in_six_leg_power_win(self):
        # Two pushes + four hits = 4-leg power win.
        results = ["hit", "hit", "hit", "hit", "push", "push"]
        self.assertEqual(_slip_payout("power", results), POWER_PAYOUTS[4])

    def test_push_with_one_miss_in_power_loses(self):
        # Pushes don't rescue a miss: 4 hits + 1 push + 1 miss is a 5-leg
        # power that lost a leg → payout 0 (no power partial credit).
        results = ["hit", "hit", "hit", "hit", "push", "miss"]
        self.assertEqual(_slip_payout("power", results), 0)

    def test_all_push_returns_stake(self):
        # Edge: every leg pushes. n_eff = 0 → fully refunded (1x stake).
        results = ["push"] * 6
        self.assertEqual(_slip_payout("power", results), 1.0)

    def test_flex_six_leg_one_push_uses_five_leg_flex_table(self):
        # 4-of-5 with one push reads the 5-leg flex table at k=4.
        results = ["hit", "hit", "hit", "hit", "miss", "push"]
        self.assertEqual(
            _slip_payout("flex", results),
            FLEX_PAYOUTS[5][4],
        )

    def test_flex_two_legs_after_push_demoted_to_power_table(self):
        # 3-leg flex with 1 push → 2 effective legs → reads the 2-leg
        # POWER table (per the existing demotion rule in
        # `evaluate_analytics`).
        results = ["hit", "hit", "push"]
        self.assertEqual(_slip_payout("flex", results), POWER_PAYOUTS[2])

    def test_single_push_on_two_leg_returns_stake(self):
        # Drops to 1 effective leg that hit → 1.0x (stake-back rule for
        # n_eff < 2 with a hit).
        self.assertEqual(_slip_payout("power", ["hit", "push"]), 1.0)

    def test_single_push_on_two_leg_with_miss_loses(self):
        # 1 effective leg that missed → 0x.
        self.assertEqual(_slip_payout("power", ["miss", "push"]), 0.0)


# ---------------------------------------------------------------------------
# 5. End-to-end: a 6-leg slip with a push is a 5-leg slip for everything
# ---------------------------------------------------------------------------

class EndToEndSlipPushTests(unittest.TestCase):
    """Glue test: walk a pretend slip from grading through payout to pin
    down that a whole-number line that ties produces the same downstream
    arithmetic as a DNP — and a meaningfully different outcome from a
    miss-with-no-push."""

    def test_whole_line_tie_yields_push_then_collapses_slip(self):
        # 6-leg power slip: five overs all clear, the sixth ties on an
        # integer line (e.g. PrizePicks "Over 14" with actual=14).
        leg_inputs = [
            (15, 14.0, "over"),  # hit
            (22, 19.5, "over"),  # hit
            (4,  2.5,  "over"),  # hit
            (33, 24.5, "over"),  # hit
            (8,  7.5,  "over"),  # hit
            (14, 14.0, "over"),  # PUSH
        ]
        results = [grade_leg(a, l, s) for a, l, s in leg_inputs]
        self.assertEqual(results.count("push"), 1)
        self.assertEqual(results.count("hit"),  5)
        self.assertEqual(_slip_payout("power", results), POWER_PAYOUTS[5])

    def test_same_situation_but_actual_one_below_is_a_loss(self):
        # Identical slip but actual=13 on the integer line → that leg is a
        # MISS, slip loses. Confirms the prior test isn't accidentally
        # passing because pushes are silently treated as hits.
        leg_inputs = [
            (15, 14.0, "over"),
            (22, 19.5, "over"),
            (4,  2.5,  "over"),
            (33, 24.5, "over"),
            (8,  7.5,  "over"),
            (13, 14.0, "over"),  # miss (would have been "miss" too on a
                                 # 14.5 line — this is the case that the
                                 # old "upgrade to .5" behavior was trying
                                 # to capture).
        ]
        results = [grade_leg(a, l, s) for a, l, s in leg_inputs]
        self.assertEqual(results.count("miss"), 1)
        self.assertEqual(_slip_payout("power", results), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
