"""
Advanced de-vig utilities.

Implements multiple margin-removal methodologies for sports proposition markets:

- **Power Method**: Nonlinear exponentiation that naturally handles the
  favorite-longshot bias.  Solves  π_over^k + π_under^k = 1  via bisection.
- **Multiplicative**: Proportional normalization (baseline / fallback).
- **Additive**: Equal subtraction of overround.
- **Worst-Case**: Runs all methods and returns the lowest (most conservative)
  true probability for each side.
- **Scaled Single-Sided**: For one-way / unmatched markets, applies a vig
  assumption that scales with odds magnitude — longshots get heavier vig.
"""
from __future__ import annotations

import math
from typing import Optional





# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------

def american_to_decimal(american: int | float) -> float:
    """Convert American odds to decimal odds."""
    if american > 0:
        return (american / 100.0) + 1.0
    else:
        return (100.0 / abs(american)) + 1.0


def american_to_implied(american: int | float) -> float:
    """Raw (vigged) implied probability from American odds."""
    return 1.0 / american_to_decimal(american)


def prob_to_american(prob: float) -> float:
    """Convert true probability to American odds."""
    if prob <= 0 or prob >= 1:
        return 0.0
    decimal = 1.0 / prob
    if decimal >= 2.0:
        return (decimal - 1.0) * 100.0
    else:
        return -100.0 / (decimal - 1.0)


def market_width_cents(over_american: int | float, under_american: int | float) -> float:
    """
    Calculate market width in cents.

    Width = |implied_over + implied_under - 1| expressed as a percentage.
    A 20-cent market (-110/-110) has width ≈ 4.76%.
    A 70-cent market (-120/-150) has much wider width.
    We return the raw overround percentage (e.g. 4.76 for a -110/-110 market).
    """
    implied_over = american_to_implied(over_american)
    implied_under = american_to_implied(under_american)
    return (implied_over + implied_under - 1.0) * 100.0


# ---------------------------------------------------------------------------
# Multiplicative de-vig (baseline)
# ---------------------------------------------------------------------------

def devig_multiplicative(
    over_american: int | float,
    under_american: int | float,
) -> tuple[float, float]:
    """
    Multiplicative de-vig.  Returns (true_over_prob, true_under_prob).
    Scales each implied probability proportionally so they sum to 1.
    """
    implied_over = american_to_implied(over_american)
    implied_under = american_to_implied(under_american)
    total = implied_over + implied_under
    if total <= 0:
        return 0.5, 0.5
    return implied_over / total, implied_under / total


# ---------------------------------------------------------------------------
# Additive de-vig
# ---------------------------------------------------------------------------

def devig_additive(
    over_american: int | float,
    under_american: int | float,
) -> tuple[float, float]:
    """
    Additive de-vig.  Subtracts an equal share of the overround from each
    outcome.  Can produce negative probabilities for extreme longshots —
    those are clamped to a small ε.
    """
    implied_over = american_to_implied(over_american)
    implied_under = american_to_implied(under_american)
    overround = implied_over + implied_under - 1.0
    half_or = overround / 2.0

    p_over = max(implied_over - half_or, 0.001)
    p_under = max(implied_under - half_or, 0.001)

    # Re-normalise so they sum to 1
    total = p_over + p_under
    return p_over / total, p_under / total


# ---------------------------------------------------------------------------
# Power Method de-vig
# ---------------------------------------------------------------------------

def _power_objective(k: float, pi_over: float, pi_under: float) -> float:
    """Evaluate  π_over^k + π_under^k  — target is 1.0."""
    return pi_over ** k + pi_under ** k


def devig_power(
    over_american: int | float,
    under_american: int | float,
    *,
    tol: float = 1e-9,
    max_iter: int = 200,
) -> tuple[float, float]:
    """
    Power Method de-vig.  Solves for exponent *k* such that:

        π_over^k + π_under^k = 1

    Because k > 1 in a market with vig, raising the fractional implied
    probability to *k* reduces the overall probability.  Crucially, smaller
    probabilities (longshots) are reduced by a *larger relative proportion*
    than larger probabilities (favorites), mirroring the favorite-longshot
    bias that bookmakers embed.

    Falls back to multiplicative if bisection fails to converge.
    """
    pi_over = american_to_implied(over_american)
    pi_under = american_to_implied(under_american)

    booksum = pi_over + pi_under
    if booksum <= 1.0:
        # No vig detected — return implied directly (already fair)
        return pi_over, pi_under

    # Bisection for k in [1, k_max]
    k_lo, k_hi = 1.0, 20.0

    # Ensure bracket validity
    f_lo = _power_objective(k_lo, pi_over, pi_under) - 1.0  # positive (booksum > 1)
    f_hi = _power_objective(k_hi, pi_over, pi_under) - 1.0

    if f_hi > 0:
        # k_hi too small — fall back to multiplicative
        return devig_multiplicative(over_american, under_american)

    for _ in range(max_iter):
        k_mid = (k_lo + k_hi) / 2.0
        f_mid = _power_objective(k_mid, pi_over, pi_under) - 1.0

        if abs(f_mid) < tol:
            break

        if f_mid > 0:
            k_lo = k_mid
        else:
            k_hi = k_mid

    k = (k_lo + k_hi) / 2.0
    p_over = pi_over ** k
    p_under = pi_under ** k

    # Normalise for floating-point safety
    total = p_over + p_under
    if total <= 0:
        return devig_multiplicative(over_american, under_american)

    return p_over / total, p_under / total


# ---------------------------------------------------------------------------
# Shin (1993) de-vig
# ---------------------------------------------------------------------------
#
# Power Method assumes a uniform favorite-longshot bias across all markets.
# Shin's model derives the bias from the *individual* market's overround
# under the assumption that a fraction `z` of trade volume comes from
# insiders (who know the true outcome). The bookmaker shades prices to
# protect against insider losses; the resulting overround depends on z and
# the dispersion of true probabilities. Solving for z that's consistent
# with the observed prices gives a per-market bias estimate.
#
# Inversion (2-outcome) — for each side:
#
#     t_i  =  ( sqrt( z² + 4·(1−z)·π_i² ) − z ) / ( 2·(1−z) )
#
# We bisect z ∈ [0, 1) until t_over + t_under = 1. At z=0 the formula
# returns t_i = π_i (no devig); at z→1 the formula compresses both sides
# toward 1/2 (heavy devig). The unique root is monotonic in z for any
# overround > 0.
#
# Reference: Shin, H.S. (1993), "Measuring the incidence of insider
# trading in a market for state-contingent claims." The 2-outcome
# closed-form bisection here matches Strumbelj (2014) and gives
# results within 1e-6 of his closed-form approximation while being
# robust against extreme overrounds.

def _shin_true(pi: float, z: float) -> float:
    """Single-side Shin inversion: π → t given z."""
    if z >= 1.0 - 1e-9:
        # Degenerate: all weight on insiders; return a uniform fallback.
        return 0.5
    return (math.sqrt(z * z + 4.0 * (1.0 - z) * pi * pi) - z) / (2.0 * (1.0 - z))


# ---------------------------------------------------------------------------
# Shin z-prior for shrinkage (Phase 1A audit C4)
# ---------------------------------------------------------------------------
# Per Strumbelj (2014) and follow-ups on the Shin family, player-prop markets
# exhibit insider-trade parameter z in the 0.02-0.08 band. A single-market
# bisection on a single book pair has high variance — z is fit from one
# (overround, dispersion) sample. Shrinking the per-market z toward a
# (league, prop) prior with strength `k` stabilizes thin markets:
#
#   z_final = (k · z_prior + z_fitted) / (k + 1)
#
# With k=3 the prior carries 75% weight on each market estimate. The prior
# itself is refit from realized CLV residuals; see
# engine/sharpness_calibration.py for the analogous fit on book weights.
SHIN_Z_PRIOR_DEFAULT = 0.05
SHIN_Z_PRIOR_STRENGTH = 3.0


def devig_shin(
    over_american: int | float,
    under_american: int | float,
    *,
    tol: float = 1e-9,
    max_iter: int = 200,
) -> tuple[float, float]:
    """
    Shin (1993) Method de-vig for two-outcome markets.

    Returns (true_over_prob, true_under_prob). Falls back to multiplicative
    de-vig if bisection fails to converge or the booksum is ≤ 1.0 (no vig
    detected).
    """
    pi_o = american_to_implied(over_american)
    pi_u = american_to_implied(under_american)

    booksum = pi_o + pi_u
    if booksum <= 1.0:
        return pi_o, pi_u

    # At z=0 the sum equals booksum (>1); we need it to equal 1.
    # The function s(z) = t_o(z) + t_u(z) is monotonically decreasing in z
    # over (0, 1), so straight bisection works.
    lo, hi = 0.0, 1.0 - 1e-6
    for _ in range(max_iter):
        mid = 0.5 * (lo + hi)
        s = _shin_true(pi_o, mid) + _shin_true(pi_u, mid)
        if abs(s - 1.0) < tol:
            break
        if s > 1.0:
            lo = mid
        else:
            hi = mid

    z = 0.5 * (lo + hi)
    t_o = _shin_true(pi_o, z)
    t_u = _shin_true(pi_u, z)

    total = t_o + t_u
    if total <= 0:
        return devig_multiplicative(over_american, under_american)
    return t_o / total, t_u / total


def devig_shin_with_prior(
    over_american: int | float,
    under_american: int | float,
    *,
    z_prior: float = SHIN_Z_PRIOR_DEFAULT,
    k_prior: float = SHIN_Z_PRIOR_STRENGTH,
    tol: float = 1e-9,
    max_iter: int = 200,
) -> tuple[float, float]:
    """Shin devig with shrinkage toward a (league, prop) z prior.

    Returns (true_over_prob, true_under_prob). Falls back to multiplicative
    when the booksum < 1.

    The fitted z (from bisection on observed booksum) is blended with the
    prior via:

        z_final = (k_prior · z_prior + z_fitted) / (k_prior + 1)

    For typical k_prior=3 and z_prior=0.05, this anchors thin markets
    around 0.05 while still letting a market with a clearly outlying
    overround push z up to 0.08-0.10.
    """
    pi_o = american_to_implied(over_american)
    pi_u = american_to_implied(under_american)

    booksum = pi_o + pi_u
    if booksum <= 1.0:
        return pi_o, pi_u

    # Fit z to this market (same bisection as devig_shin).
    lo, hi = 0.0, 1.0 - 1e-6
    for _ in range(max_iter):
        mid = 0.5 * (lo + hi)
        s = _shin_true(pi_o, mid) + _shin_true(pi_u, mid)
        if abs(s - 1.0) < tol:
            break
        if s > 1.0:
            lo = mid
        else:
            hi = mid
    z_fitted = 0.5 * (lo + hi)

    # Shrink toward prior.
    z_blend = (k_prior * z_prior + z_fitted) / (k_prior + 1.0)
    z_blend = max(0.0, min(1.0 - 1e-6, z_blend))

    t_o = _shin_true(pi_o, z_blend)
    t_u = _shin_true(pi_u, z_blend)
    total = t_o + t_u
    if total <= 0:
        return devig_multiplicative(over_american, under_american)
    return t_o / total, t_u / total


# ---------------------------------------------------------------------------
# Inverse Power Method: re-vig fair probabilities → bookmaker odds
# ---------------------------------------------------------------------------

def revigg_power(
    p_over: float,
    p_under: float,
    target_margin: float,
    *,
    tol: float = 1e-9,
    max_iter: int = 200,
) -> tuple[float, float]:
    """
    Inverse of the Power Method de-vig.  Given fair probabilities and a
    target overround (margin), produce realistically vigged implied
    probabilities that sum to 1 + target_margin.

    The Power Method relationship:
        π_i = p_i^(1/k)
    where k > 1 is chosen so that  Σ π_i = 1 + margin.

    Because 1/k < 1, raising probabilities to 1/k inflates them, with
    lower probabilities (longshots) inflated proportionally MORE than
    higher probabilities (favorites).  This mirrors the favorite-longshot
    bias that real sportsbooks embed.

    Args:
        p_over:  Fair (devigged) probability for the over side.
        p_under: Fair (devigged) probability for the under side.
        target_margin: Desired overround as a fraction (e.g. 0.07 for 7%).

    Returns:
        (implied_over, implied_under) — vigged implied probabilities
        that sum to approximately 1 + target_margin.
    """
    if target_margin <= 0:
        # No vig requested — return fair probs directly
        return p_over, p_under

    # Clamp probs to avoid math errors
    p_over = max(p_over, 1e-6)
    p_under = max(p_under, 1e-6)
    total_p = p_over + p_under
    if abs(total_p - 1.0) > 0.01:
        # Renormalize if they don't sum to 1
        p_over /= total_p
        p_under /= total_p

    target_sum = 1.0 + target_margin

    # Bisection for k in [1, k_max]
    # At k=1: p_over^1 + p_under^1 = 1.0 (no vig) — too small
    # At k→∞: both terms → 1.0, sum → 2.0 — too large
    # We need sum = target_sum ∈ (1.0, 2.0)
    k_lo, k_hi = 1.0, 50.0

    f_lo = p_over ** (1.0 / k_lo) + p_under ** (1.0 / k_lo) - target_sum  # ≈ -margin
    f_hi = p_over ** (1.0 / k_hi) + p_under ** (1.0 / k_hi) - target_sum

    if f_hi < 0:
        # target_margin too large for bisection range — fall back to multiplicative
        scale = target_sum
        return p_over * scale, p_under * scale

    for _ in range(max_iter):
        k_mid = (k_lo + k_hi) / 2.0
        f_mid = p_over ** (1.0 / k_mid) + p_under ** (1.0 / k_mid) - target_sum

        if abs(f_mid) < tol:
            break

        if f_mid < 0:
            # Sum too small → need larger k (more vig)
            k_lo = k_mid
        else:
            # Sum too large → need smaller k (less vig)
            k_hi = k_mid

    k = (k_lo + k_hi) / 2.0
    inv_k = 1.0 / k

    return p_over ** inv_k, p_under ** inv_k


# ---------------------------------------------------------------------------
# Worst-Case de-vig (defensive failsafe)
# ---------------------------------------------------------------------------

def devig_worst_case(
    over_american: int | float,
    under_american: int | float,
) -> tuple[float, float]:
    """
    Run Power, Multiplicative, and Additive methods in parallel.
    Return the **lowest** true probability for each side across all methods.

    This provides the most mathematically conservative estimate, aggressively
    protecting against unobservable margins and variance.
    """
    methods = [devig_shin, devig_power, devig_multiplicative, devig_additive]

    over_probs = []
    under_probs = []

    for method in methods:
        try:
            p_o, p_u = method(over_american, under_american)
            over_probs.append(p_o)
            under_probs.append(p_u)
        except Exception:
            continue

    if not over_probs:
        return 0.5, 0.5

    return min(over_probs), min(under_probs)


# ---------------------------------------------------------------------------
# Scaled single-sided de-vig
# ---------------------------------------------------------------------------

# Base vig for standard lines near even money (-110 / +100)
_BASE_VIG = 0.05

# Additional vig per unit of "longshot distance" — at +500 this adds ~5%
_LONGSHOT_SLOPE = 0.15


def devig_single_sided_scaled(american: int | float) -> float:
    """
    Improved single-sided de-vig with scaled vig assumption.

    Instead of a flat vig (e.g. 7%), the assumed vig increases with odds
    magnitude.  Longshots carry astronomically higher unobservable holds,
    so we penalise them more aggressively:

        implied_prob      vig_assumed
        ──────────────    ───────────
        ~50% (even)       ~5%
        ~33% (+200)       ~7%
        ~20% (+400)       ~9%
        ~17% (+500)       ~10%
        ~9%  (+1000)      ~12%

    Formula:
        distance = max(0,  0.50 - implied_prob)  # how far from 50-50
        vig = BASE_VIG + LONGSHOT_SLOPE × distance
        true_prob = implied_prob / (1 + vig)
    """
    implied = american_to_implied(american)

    # Distance from 50-50 — longshots are further
    distance = max(0.0, 0.50 - implied)

    vig = _BASE_VIG + _LONGSHOT_SLOPE * distance
    return implied / (1.0 + vig)


# Vig that a single-sided FAVORITE milestone carries. The longshot model
# above only adds hold for implied < 0.5, so a -500 "1+ shots" over devigs
# to ~0.79 with just the 5% base removed — but one-way milestone markets
# carry a fat hold on the heavy side too. This is the bias the 2026-06-14
# soccer audit flagged. The favorite-aware variant below applies the hold
# symmetrically: distance is measured from 0.5 in EITHER direction.
_FAVORITE_SLOPE = 0.20  # steeper than the longshot slope — heavy milestone
                        # favorites hide more hold than symmetric longshots


def devig_single_sided_favorite_aware(american: int | float) -> float:
    """Single-sided de-vig that corrects FAVORITES as well as longshots.

    `devig_single_sided_scaled` only removes extra hold below 0.5, so it
    leaves favorite milestones biased HIGH. This variant scales the assumed
    vig with distance from 0.5 in both directions, with a steeper slope on
    the favorite side:

        implied 0.50  -> ~5% hold
        implied 0.20  -> ~9.5% hold   (longshot, same as scaled)
        implied 0.80  -> ~11% hold    (favorite — scaled gave only 5%)
        implied 0.91  -> ~13% hold

    Heuristic, not calibrated to a two-sided market (single-sided props
    don't expose one) — but directionally correct where the old model was
    flatly wrong. Used only by the SOCCER single-sided anchor path.
    """
    implied = american_to_implied(american)
    if implied >= 0.50:
        vig = _BASE_VIG + _FAVORITE_SLOPE * (implied - 0.50)
    else:
        vig = _BASE_VIG + _LONGSHOT_SLOPE * (0.50 - implied)
    return implied / (1.0 + vig)


def devig_single_sided(american: int | float) -> float:
    """
    Legacy single-sided de-vig.  Kept as a compatibility shim that now
    delegates to the improved scaled version.
    """
    return devig_single_sided_scaled(american)


# ---------------------------------------------------------------------------
# Single-source uncertainty discount
# ---------------------------------------------------------------------------

def apply_single_source_discount(
    prob: float,
    american_odds: int | float,
) -> float:
    """
    When only one sportsbook offers a line, apply a scaled uncertainty
    discount.  The discount increases with odds magnitude because isolated
    longshot markets carry the highest, most predatory unobservable holds.

        ~even money (+100):  ~5% discount   → multiply by 0.95
        +200–+400:           ~7% discount   → multiply by 0.93
        +500+:               ~10% discount  → multiply by 0.90

    Formula:
        discount_factor = 0.95 - 0.05 × clamp((|american| - 100) / 400, 0, 1)
    """
    abs_odds = abs(american_odds)
    # Linear ramp from 0.95 (at ±100) → 0.90 (at ±500+)
    ratio = max(0.0, min(1.0, (abs_odds - 100.0) / 400.0))
    discount_factor = 0.95 - 0.05 * ratio

    return prob * discount_factor
