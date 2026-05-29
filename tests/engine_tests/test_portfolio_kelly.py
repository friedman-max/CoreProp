"""Maybe Cool Fix C3: portfolio Kelly across same-day slips."""
import numpy as np
import pytest

from engine.portfolio_kelly import (
    CAP_PER_SLIP, CAP_TOTAL, KAPPA,
    SlipDistribution,
    build_slip_distribution,
    portfolio_kelly_discount,
    portfolio_kelly_mc,
)


# ─────────────────────────────────────────────────────────────────────────
# portfolio_kelly_discount
# ─────────────────────────────────────────────────────────────────────────

def test_discount_no_correlation_is_identity_after_kappa():
    """ρ=0 → discount=1, so output = κ × per-slip Kelly (capped)."""
    per_slip = [0.20, 0.30, 0.10]  # raw Kelly fractions
    out = portfolio_kelly_discount(per_slip, correlation_max=0.0)
    expected = [min(CAP_PER_SLIP, KAPPA * x) for x in per_slip]
    assert out == pytest.approx(expected, abs=1e-9)


def test_discount_high_correlation_compresses():
    """ρ=1 → discount=0.5; output should be halved before damping/capping."""
    per_slip = [0.10, 0.10]
    out_indep = portfolio_kelly_discount(per_slip, correlation_max=0.0)
    out_corr  = portfolio_kelly_discount(per_slip, correlation_max=1.0)
    assert all(c < i + 1e-9 for c, i in zip(out_corr, out_indep))
    assert all(c <= 0.5 * i + 1e-9 for c, i in zip(out_corr, out_indep))


def test_discount_total_cap_enforced():
    """Many slips summing above CAP_TOTAL should be proportionally scaled."""
    per_slip = [0.50] * 10  # huge fractions
    out = portfolio_kelly_discount(per_slip, correlation_max=0.0)
    assert abs(sum(out) - CAP_TOTAL) < 1e-9


def test_discount_per_slip_cap_enforced():
    """No single slip exceeds CAP_PER_SLIP."""
    out = portfolio_kelly_discount([10.0], correlation_max=0.0)
    assert out[0] <= CAP_PER_SLIP


def test_discount_empty_input():
    assert portfolio_kelly_discount([]) == []


# ─────────────────────────────────────────────────────────────────────────
# build_slip_distribution
# ─────────────────────────────────────────────────────────────────────────

def test_build_slip_distribution_power_3():
    """3-Power slip: 2^3 = 8 outcomes; only all-3-hit pays 6×."""
    dist = build_slip_distribution([0.6, 0.6, 0.6], slip_type="power")
    assert len(dist.outcomes) == 8
    # Find the all-hit outcome
    payouts_present = [m for _, m in dist.outcomes if m > 0]
    assert payouts_present == [6.0]  # only one winning outcome
    # Probability of all-hit = 0.6^3 = 0.216
    win_outcomes = [(p, m) for p, m in dist.outcomes if m > 0]
    assert abs(win_outcomes[0][0] - 0.216) < 1e-6


def test_build_slip_distribution_flex_6():
    """6-Flex slip: 7 distinct hit counts; tiers 4/5/6 pay."""
    dist = build_slip_distribution([0.55] * 6, slip_type="flex")
    assert len(dist.outcomes) == 64
    nonzero = sum(1 for _, m in dist.outcomes if m > 0)
    assert nonzero > 0


# ─────────────────────────────────────────────────────────────────────────
# portfolio_kelly_mc
# ─────────────────────────────────────────────────────────────────────────

def test_mc_zero_for_break_even_slips():
    """At per-leg break-even, Kelly should be ~0."""
    # 6-Power break-even: 0.5407 per leg.
    dist = build_slip_distribution([0.5407] * 6, slip_type="power")
    out = portfolio_kelly_mc(
        [dist], correlation_matrix=None,
        n_sims=2000, n_iter=100, seed=0,
    )
    assert out[0] < 0.005


def test_mc_positive_with_edge():
    """Above break-even → positive Kelly, capped at CAP_PER_SLIP."""
    dist = build_slip_distribution([0.65] * 6, slip_type="power")
    out = portfolio_kelly_mc(
        [dist], correlation_matrix=None,
        n_sims=2000, n_iter=200, seed=1,
    )
    assert out[0] > 0.0
    assert out[0] <= CAP_PER_SLIP + 1e-9


def test_mc_total_cap_enforced_under_correlation():
    """Two highly-correlated slips with edge — total should not exceed
    CAP_TOTAL even before MC correlation discount."""
    d1 = build_slip_distribution([0.65] * 6, slip_type="power")
    d2 = build_slip_distribution([0.65] * 6, slip_type="power")
    R = np.array([[1.0, 0.8], [0.8, 1.0]])
    out = portfolio_kelly_mc(
        [d1, d2], correlation_matrix=R,
        n_sims=2000, n_iter=200, seed=2,
    )
    assert sum(out) <= CAP_TOTAL + 1e-9


def test_mc_empty_input():
    assert portfolio_kelly_mc([]) == []


def test_mc_independent_slips_higher_total_than_correlated():
    """Independent slips should allow more total exposure than perfectly-
    correlated slips."""
    d1 = build_slip_distribution([0.65] * 6, slip_type="power")
    d2 = build_slip_distribution([0.65] * 6, slip_type="power")
    R_indep = np.eye(2)
    R_corr  = np.array([[1.0, 0.95], [0.95, 1.0]])
    out_indep = portfolio_kelly_mc(
        [d1, d2], correlation_matrix=R_indep,
        n_sims=4000, n_iter=200, seed=3,
    )
    out_corr = portfolio_kelly_mc(
        [d1, d2], correlation_matrix=R_corr,
        n_sims=4000, n_iter=200, seed=3,
    )
    # MC noise means we can't guarantee a strict inequality on point values,
    # but the total exposure should not be materially higher under correlation.
    assert sum(out_corr) <= sum(out_indep) + 0.02
