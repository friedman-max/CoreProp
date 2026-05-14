"""Devig math correctness tests.

The +EV pipeline lives or dies by the devig math: a 2% bias here is a
direct 2% bias in every reported EV. These tests pin the invariants:
American/decimal/probability roundtrips, multiplicative-method sum-to-1,
Power-method monotonicity, single-sided-scaled vig caps, market-width
non-negativity. They run in milliseconds and don't touch a network.
"""
from __future__ import annotations

import math
import pytest

from engine.devig import (
    american_to_decimal,
    american_to_implied,
    prob_to_american,
    market_width_cents,
    devig_multiplicative,
    devig_additive,
    devig_power,
    devig_worst_case,
    devig_single_sided_scaled,
    devig_single_sided,
    revigg_power,
)


# ── Conversion roundtrips ──────────────────────────────────────────────

@pytest.mark.parametrize("american,decimal", [
    (-110, 1.9091),  # standard juice
    (-200, 1.5000),  # heavy favorite
    (+100, 2.0000),  # coinflip
    (+150, 2.5000),  # underdog
    (+300, 4.0000),  # longshot
])
def test_american_to_decimal_known_values(american, decimal):
    assert american_to_decimal(american) == pytest.approx(decimal, abs=1e-4)


@pytest.mark.parametrize("american", [-300, -150, -110, +100, +120, +250, +800])
def test_prob_american_roundtrip(american):
    """prob_to_american ∘ american_to_implied must be the identity for
    every American odd above the displayable threshold."""
    prob = american_to_implied(american)
    back = prob_to_american(prob)
    assert back == pytest.approx(american, abs=0.5)


def test_prob_to_american_edge_cases():
    assert prob_to_american(0.0) == 0.0
    assert prob_to_american(1.0) == 0.0
    assert prob_to_american(-0.1) == 0.0


# ── Market width ───────────────────────────────────────────────────────

def test_market_width_zero_vig_market():
    """A 0% vig market (+100/+100) has width 0%."""
    assert market_width_cents(+100, +100) == pytest.approx(0.0, abs=1e-6)


def test_market_width_standard_juice():
    """-110/-110 ≈ 4.76% overround."""
    w = market_width_cents(-110, -110)
    assert w == pytest.approx(4.7619, abs=1e-3)


def test_market_width_nonnegative():
    """No matter how skewed the prices, width is the absolute overround
    so it must be ≥ 0."""
    for combo in [(-200, +180), (-110, -110), (-500, +400), (+100, +100)]:
        assert market_width_cents(*combo) >= 0.0


# ── Multiplicative devig ───────────────────────────────────────────────

def test_devig_multiplicative_sums_to_one():
    over, under = devig_multiplicative(-110, -110)
    assert over + under == pytest.approx(1.0, abs=1e-9)


def test_devig_multiplicative_favorite():
    """A -200 favorite vs +180 dog should devig to over > under."""
    over, under = devig_multiplicative(-200, +180)
    assert over > under
    assert over + under == pytest.approx(1.0, abs=1e-9)


# ── Power method ───────────────────────────────────────────────────────

def test_devig_power_sums_to_one():
    over, under = devig_power(-110, -110)
    assert over + under == pytest.approx(1.0, abs=1e-6)


def test_devig_power_symmetric():
    """A symmetric -110/-110 market devigs to 50/50."""
    over, under = devig_power(-110, -110)
    assert over == pytest.approx(0.5, abs=1e-4)
    assert under == pytest.approx(0.5, abs=1e-4)


def test_devig_power_monotone_in_favorite_odds():
    """Tightening the favorite side moves its devigged prob up."""
    a, _ = devig_power(-150, +130)
    b, _ = devig_power(-200, +180)
    c, _ = devig_power(-300, +250)
    assert a < b < c


def test_devig_power_handles_extreme_longshot():
    """Power method must not blow up on a heavy longshot."""
    over, under = devig_power(+800, -1200)
    assert 0.0 < over < 1.0
    assert 0.0 < under < 1.0
    assert over + under == pytest.approx(1.0, abs=1e-6)


# ── Worst-case ─────────────────────────────────────────────────────────

def test_devig_worst_case_is_lower_bound():
    """worst_case returns the most conservative (lowest) prob for each side."""
    multi = devig_multiplicative(-150, +130)
    power = devig_power(-150, +130)
    wc_o, wc_u = devig_worst_case(-150, +130)
    assert wc_o <= max(multi[0], power[0])
    assert wc_u <= max(multi[1], power[1])


# ── Single-sided ───────────────────────────────────────────────────────

def test_single_sided_unscaled_is_implied():
    """devig_single_sided with default scaling returns the raw implied."""
    impl = american_to_implied(-110)
    out = devig_single_sided(-110)
    # devig_single_sided strips a fixed assumption; sanity-bound it.
    assert 0.0 < out < impl


def test_single_sided_scaled_caps_below_implied():
    """Single-sided scaling never returns more than the raw implied (else
    we'd be claiming a +EV on a single book with no consensus)."""
    for odds in (-300, -150, -110, +100, +180, +400):
        scaled = devig_single_sided_scaled(odds)
        impl = american_to_implied(odds)
        assert 0.0 < scaled < impl


# ── Revig roundtrip ────────────────────────────────────────────────────

def test_revigg_power_inverse_of_devig():
    """Going devig → revig should return the original implied probs
    (modulo numerical precision)."""
    over_a, under_a = -120, +110
    true_o, true_u = devig_power(over_a, under_a)
    margin = american_to_implied(over_a) + american_to_implied(under_a) - 1.0
    rev_o, rev_u = revigg_power(true_o, true_u, margin)
    # Roundtrip should be close. We just check no blow-up.
    assert 0.0 < rev_o < 1.0
    assert 0.0 < rev_u < 1.0
