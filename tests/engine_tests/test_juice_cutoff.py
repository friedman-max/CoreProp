"""simplify-v1 single-sided juice guardrail (engine/consensus.py).

A book posting an extreme one-way price (e.g. -10000) is protecting itself, not
quoting a real probability. Those legs must be dropped, and any single-sided
devigged prob is capped at config.SINGLE_SIDED_PROB_CAP.
"""
from __future__ import annotations

import config as cfg
from engine.consensus import (
    BookOdds,
    _devig_book_worst_case,
    _single_sided_too_juiced,
    compute_true_probability,
)


def _one_way(over_odds):
    return BookOdds(book_name="fanduel", over_odds=over_odds, under_odds=None, both_sided=False)


def test_extreme_juice_is_flagged_and_dropped():
    juiced = _one_way(-10000)  # ~99% implied — not a credible probability
    assert _single_sided_too_juiced(juiced) is True
    assert _devig_book_worst_case(juiced, "over") is None


def test_reasonable_one_way_is_kept_and_capped():
    ok = _one_way(-300)  # ~75% implied
    assert _single_sided_too_juiced(ok) is False
    p = _devig_book_worst_case(ok, "over")
    assert p is not None
    assert p <= cfg.SINGLE_SIDED_PROB_CAP + 1e-9


def test_two_sided_book_is_never_flagged():
    # A normal two-sided market is unaffected by the single-sided guardrail.
    two_sided = BookOdds(book_name="pinnacle", over_odds=-110, under_odds=-110, both_sided=True)
    assert _single_sided_too_juiced(two_sided) is False


def test_pipeline_drops_lone_juiced_book():
    # Only price available is a -10000 one-way line -> no decision probability.
    consensus, worst_case, meta = compute_true_probability(
        [_one_way(-10000)], "over", league="NBA", prop="Points",
    )
    assert worst_case is None


def test_pipeline_caps_lone_reasonable_book():
    consensus, worst_case, meta = compute_true_probability(
        [_one_way(-300)], "over", league="NBA", prop="Points",
    )
    assert worst_case is not None
    assert worst_case <= cfg.SINGLE_SIDED_PROB_CAP + 1e-9
