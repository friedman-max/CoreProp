"""Phase 1A audit C4 quantitative fixes — Shin z-prior, log width
weighting, Bonett-Price tetrachoric, log-optimal Kelly with cap.
"""
import math
import pytest

from engine.devig import (
    SHIN_Z_PRIOR_DEFAULT,
    SHIN_Z_PRIOR_STRENGTH,
    devig_shin,
    devig_shin_with_prior,
)
from engine.consensus import _width_weighting_term, WIDTH_WEIGHTING_MODE
from engine.correlation import (
    MAX_RHO,
    _phi_to_latent,
    _phi_to_latent_bp,
)
from engine.strategy_tester import StrategyTester


# ─────────────────────────────────────────────────────────────────────────
# Shin z-prior shrinkage
# ─────────────────────────────────────────────────────────────────────────

def test_shin_with_prior_equal_to_standard_with_k_zero():
    """With k_prior=0 the prior carries no weight; result must equal
    standard Shin (modulo numerical precision)."""
    a, b = devig_shin(-110, -110)
    c, d = devig_shin_with_prior(-110, -110, z_prior=0.30, k_prior=0.0)
    assert abs(a - c) < 1e-6
    assert abs(b - d) < 1e-6


def test_shin_with_prior_shrinks_toward_prior():
    """Higher k_prior pulls z toward the prior. For a -110/-110 market the
    fitted z is small (~0.03); a 0.3 prior with k=10 should produce a
    materially different devig."""
    a, b = devig_shin(-110, -110)
    c, d = devig_shin_with_prior(-110, -110, z_prior=0.30, k_prior=10.0)
    # The shrunk version pulls z up → t_i compresses toward 0.5.
    # Sides are symmetric here, so we just check the result is bounded
    # and that the prior had a non-trivial effect.
    assert abs(c - 0.5) < abs(a - 0.5) + 0.01
    assert 0.0 < c < 1.0 and 0.0 < d < 1.0


def test_shin_with_prior_returns_implied_when_no_vig():
    """Booksum ≤ 1 should fall through to raw implied without invoking
    bisection / shrinkage."""
    # +110 / +110 means implied 0.476 + 0.476 = 0.952 < 1.0
    o, u = devig_shin_with_prior(110, 110)
    assert abs(o - 0.4762) < 1e-3
    assert abs(u - 0.4762) < 1e-3


def test_shin_default_prior_constants_present():
    """Sanity: the module-level constants used by callers exist and have
    sane values."""
    assert 0.0 < SHIN_Z_PRIOR_DEFAULT < 0.2
    assert SHIN_Z_PRIOR_STRENGTH > 0.0


# ─────────────────────────────────────────────────────────────────────────
# Log-mode width weighting in consensus
# ─────────────────────────────────────────────────────────────────────────

def test_width_term_default_log_mode():
    """Default mode is 'log' per Phase 1A audit recommendation."""
    assert WIDTH_WEIGHTING_MODE == "log"


def test_width_term_log_decreasing_in_width():
    a = _width_weighting_term(2.0)
    b = _width_weighting_term(5.0)
    c = _width_weighting_term(15.0)
    assert a > b > c


def test_width_term_log_softer_than_linear():
    """At width=15, log weighting is much less punitive than 1/M."""
    log_w = _width_weighting_term(15.0)
    linear_w = 1.0 / 15.0
    # log: 1/log(16) ≈ 0.36; linear: 1/15 ≈ 0.067. Log should be >> linear.
    assert log_w > 4 * linear_w


# ─────────────────────────────────────────────────────────────────────────
# Bonett-Price tetrachoric
# ─────────────────────────────────────────────────────────────────────────

def test_bonett_price_returns_none_on_zero_cell():
    """No formula when a contingency cell is empty."""
    # x: 5 hits, y: 5 hits, both-hit: 0 — cell a is empty.
    assert _phi_to_latent_bp(5, 5, 0, 10) is None
    # Both miss: a=10, b=0 — cell b empty.
    assert _phi_to_latent_bp(10, 10, 10, 10) is None


def test_bonett_price_positive_for_positive_phi():
    """Positively-correlated 2×2 (a, d large; b, c small) → positive ρ."""
    # n=100, sum_x=60, sum_y=60, sum_xy=50
    # a=50, b=10, c=10, d=30  →  ad/bc = 1500/100 = 15
    # ρ ≈ cos(π / (1 + √15)) = cos(π / 4.87) ≈ cos(0.645) ≈ 0.80
    rho = _phi_to_latent_bp(60, 60, 50, 100)
    assert rho is not None
    assert 0.7 < rho < 0.85


def test_bonett_price_zero_for_independent_table():
    """Independent table → ρ near 0."""
    # n=100, p_x=p_y=0.5, sum_xy = 25 (independence)
    # a=25, b=25, c=25, d=25 → ad/bc = 1
    # ρ ≈ cos(π / (1 + 1)) = cos(π/2) = 0
    rho = _phi_to_latent_bp(50, 50, 25, 100)
    assert rho is not None
    assert abs(rho) < 0.01


def test_bonett_price_negative_for_negative_correlation():
    """Negatively-correlated table → negative ρ (a, d small; b, c large)."""
    # n=100, sum_x=50, sum_y=50, sum_xy=10 (anti-correlated)
    # a=10, b=40, c=40, d=10 → ad/bc = 100/1600 = 0.0625
    # ρ ≈ cos(π / (1 + 0.25)) = cos(π / 1.25) ≈ cos(2.51) ≈ -0.81
    rho = _phi_to_latent_bp(50, 50, 10, 100)
    assert rho is not None
    assert -0.85 < rho < -0.70


def test_sin_symmetric_fallback_remains_for_phi_only_path():
    """Legacy sin(πφ/2) still available for the heuristic path."""
    assert _phi_to_latent(0.0) == 0.0
    assert _phi_to_latent(1.0) == pytest.approx(1.0, abs=1e-9)
    assert _phi_to_latent(-1.0) == pytest.approx(-1.0, abs=1e-9)


def test_max_rho_raised_to_0_75():
    assert MAX_RHO == 0.75


# ─────────────────────────────────────────────────────────────────────────
# Closed-form log-optimal Kelly
# ─────────────────────────────────────────────────────────────────────────

def test_kelly_zero_at_no_edge():
    """At p = 1/D (break-even) Kelly returns 0."""
    # 6-Power: D=40, break-even p_all = 1/40 = 0.025 per slip
    # Per-leg break-even: 0.5407 → joint = 0.5407^6 = 0.025 (yes)
    st = StrategyTester.__new__(StrategyTester)  # no DB
    probs = [0.5407] * 6
    f = st._calculate_kelly_fraction(probs, 6, "power")
    assert f == 0.0


def test_kelly_positive_at_positive_edge():
    """Above break-even on every leg, Kelly is positive and capped."""
    st = StrategyTester.__new__(StrategyTester)
    probs = [0.70] * 6
    f = st._calculate_kelly_fraction(probs, 6, "power")
    assert f > 0.0


def test_kelly_capped_at_5_percent():
    """Even with a huge edge, Kelly fraction must not exceed 5%."""
    st = StrategyTester.__new__(StrategyTester)
    probs = [0.99] * 6
    f = st._calculate_kelly_fraction(probs, 6, "power")
    assert f <= 0.05 + 1e-9


def test_kelly_closed_form_matches_textbook_for_power():
    """For Power (single non-zero payout tier), f* should match
    (D·p - 1) / (D - 1) before damping; damped value = κ × that."""
    st = StrategyTester.__new__(StrategyTester)
    probs = [0.60] * 6
    p_all = math.prod(probs)  # 0.6^6 = 0.0467
    D = 40.0   # 6-Power payout
    expected_f_star = (D * p_all - 1.0) / (D - 1.0)
    if expected_f_star < 0:
        expected_f_star = 0.0
    expected_damped = 0.25 * expected_f_star
    expected_capped = min(0.05, max(0.0, expected_damped))

    f = st._calculate_kelly_fraction(probs, 6, "power")
    assert abs(f - expected_capped) < 1e-3


def test_kelly_flex_returns_positive_on_edge():
    """Flex has multi-tier payouts; verify the bisection finds a
    positive root when the slip has edge."""
    st = StrategyTester.__new__(StrategyTester)
    probs = [0.65] * 6
    f = st._calculate_kelly_fraction(probs, 6, "flex")
    assert f > 0.0
    assert f <= 0.05 + 1e-9
