"""Regression: the +EV tab decision number is the HONEST consensus, not the
pessimistic worst_case min.

worst_case is computed per-side as the minimum across books AND across devig
methods. Applied per side it does not sum to 1 across over/under, so for a
balanced or mildly-disagreeing market BOTH sides can land below 0.50 — which
made near-50/50 PrizePicks lines never surface. We gate/display on
consensus_prob (the vig-stripped market mean) instead.
"""
from __future__ import annotations

from engine.consensus import BookOdds, compute_true_probability

MIN_DISPLAY = 0.50


def _both(over, under, n=3, book="fanduel"):
    names = ["fanduel", "draftkings", "pinnacle"]
    return [BookOdds(names[i % 3], over, under, True) for i in range(n)]


def test_balanced_line_consensus_is_about_half():
    co, _wo, _ = compute_true_probability(_both(-110, -110), "over", league="NBA", prop="Points")
    assert abs(co - 0.50) < 0.01
    # ...and it reaches the display floor, so balanced lines still appear.
    assert co >= MIN_DISPLAY - 1e-9


def test_favored_side_surfaces_via_consensus():
    # A genuinely favored over (~57%). Consensus must clear a real edge bar.
    books = [BookOdds("fanduel", -150, 125, True),
             BookOdds("draftkings", -145, 120, True),
             BookOdds("pinnacle", -155, 130, True)]
    co, _wo, _ = compute_true_probability(books, "over", league="NBA", prop="Points")
    cu, _wu, _ = compute_true_probability(books, "under", league="NBA", prop="Points")
    assert co > 0.55          # the favored side surfaces
    assert cu < 0.45          # the other side does not
    assert abs((co + cu) - 1.0) < 0.02   # honest devig sums to ~1


def test_worst_case_can_sink_both_sides_below_half():
    # Why we do NOT gate on worst_case: when books disagree, the per-side min
    # comes from different books, so over+under < 1 and both can be < 0.50.
    books = [BookOdds("fanduel", -120, 100, True),
             BookOdds("draftkings", 100, -120, True)]  # mirror-image disagreement
    _co, wo, _ = compute_true_probability(books, "over", league="NBA", prop="Points")
    _cu, wu, _ = compute_true_probability(books, "under", league="NBA", prop="Points")
    assert wo < 0.50 and wu < 0.50          # both below 50% -> would never surface
    assert (wo + wu) < 1.0
