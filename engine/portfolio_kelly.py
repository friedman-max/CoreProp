"""
Portfolio Kelly across same-day slips (Maybe Cool Fix C3).

Per-slip independent Kelly oversizes the total stake when slips have
correlated outcomes — same-player props in different slips, multiple
slips drawing from the same game's pace/score regime, slips logged from
the same model state on the same slate. Phase 1A audit Stage 7 finding:
"Independent Bernoulli enumeration ... doesn't account for the joint
distribution of all slips on a given day."

Two solvers in this module:

  1. portfolio_kelly_discount(per_slip_kelly, correlation_max)
       Closed-form fast path. Applies a multiplicative discount
       1/(1+ρ_max) to every per-slip Kelly so total exposure compresses
       as cross-slip correlation rises. Conservative — pessimistic
       upper bound on the joint Kelly when slips are partially
       correlated.

  2. portfolio_kelly_mc(slip_dists, correlation_matrix, n_sims, ...)
       Monte Carlo joint Kelly: sample N×n_sims correlated outcomes,
       solve for f* via gradient ascent on E[log(1 + Σ fᵢ·Xᵢ)]. Slower
       but more accurate when ρ matters and slips have multi-tier
       payoffs (Flex).

Both solvers apply κ=0.25 damping and per-slip + total caps.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

import numpy as np

# Damping and caps — match StrategyTester's per-slip Kelly.
KAPPA: float = 0.25
CAP_PER_SLIP: float = 0.05
CAP_TOTAL: float = 0.20    # never expose more than 20% of bankroll per day


# ─────────────────────────────────────────────────────────────────────────
# Discount-based fast path
# ─────────────────────────────────────────────────────────────────────────

def portfolio_kelly_discount(
    per_slip_kelly: list[float],
    correlation_max: float = 0.0,
    *,
    kappa: float = KAPPA,
    cap_per_slip: float = CAP_PER_SLIP,
    cap_total: float = CAP_TOTAL,
) -> list[float]:
    """Apply a correlation-aware discount to a list of independent per-slip
    Kelly fractions.

    The discount is 1/(1 + ρ_max). At ρ_max=0 (independent slips) this is
    a no-op; at ρ_max=1 (perfect correlation, effectively one slip) the
    discount halves the per-slip stakes. Conservative — accurate when
    slips are pairwise independent and provides a defensive upper bound
    when they aren't.

    `per_slip_kelly` is the list of UNDAMPED Kelly fractions (the output
    of StrategyTester._kelly_log_optimal before κ-multiplication). κ and
    caps are applied here so callers don't double-damp.
    """
    if not per_slip_kelly:
        return []
    rho = max(0.0, min(1.0, float(correlation_max)))
    discount = 1.0 / (1.0 + rho)

    # Pre-damp + per-slip cap.
    f = [max(0.0, min(cap_per_slip, kappa * x * discount)) for x in per_slip_kelly]

    # Total cap.
    total = sum(f)
    if total > cap_total and total > 0:
        scale = cap_total / total
        f = [x * scale for x in f]
    return f


# ─────────────────────────────────────────────────────────────────────────
# Monte-Carlo joint Kelly
# ─────────────────────────────────────────────────────────────────────────

@dataclass
class SlipDistribution:
    """Container for a single slip's payoff distribution under independence
    of its constituent legs. `outcomes` is the list of (probability,
    payoff_multiplier) tuples covering every 2^n_legs combination."""
    outcomes: list[tuple[float, float]]


def _expected_log_wealth(
    f: np.ndarray,
    sampled_payoffs: np.ndarray,
) -> float:
    """E[log(1 + Σᵢ fᵢ (Xᵢ - 1))] estimated from MC samples.

    sampled_payoffs: (n_sims, N) array where each cell is the multiplier
    (>=0) of slip i in sample t. f: length-N stake fractions.
    """
    net = sampled_payoffs - 1.0  # net profit per unit stake
    portfolio = 1.0 + net @ f
    # Clip to keep log finite; portfolio < epsilon means ruin on this sample.
    portfolio = np.maximum(portfolio, 1e-12)
    return float(np.mean(np.log(portfolio)))


def _sample_correlated_payoffs(
    slip_dists: list[SlipDistribution],
    correlation_matrix: Optional[np.ndarray],
    n_sims: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Draw n_sims correlated payoff samples for each of N slips.

    For independent slips: sample each slip's payoff distribution
    directly. For correlated slips: use a Gaussian copula on the cumulative
    distribution of each slip's payoffs.
    """
    N = len(slip_dists)
    samples = np.zeros((n_sims, N), dtype=float)

    if correlation_matrix is None or N == 1:
        # Independent path — sample each slip directly.
        for i, dist in enumerate(slip_dists):
            probs = np.array([p for p, _ in dist.outcomes])
            payoffs = np.array([m for _, m in dist.outcomes])
            probs /= probs.sum()
            idxs = rng.choice(len(payoffs), size=n_sims, p=probs)
            samples[:, i] = payoffs[idxs]
        return samples

    # Gaussian copula path.
    # 1. Draw correlated standard normals.
    try:
        L = np.linalg.cholesky(
            correlation_matrix + 1e-10 * np.eye(N),
        )
    except np.linalg.LinAlgError:
        # Fall back to independent.
        L = np.eye(N)
    Z = rng.standard_normal((n_sims, N))
    X = Z @ L.T

    # 2. Convert standard normal to uniform via Φ, then invert each
    # slip's payoff CDF.
    from statistics import NormalDist
    nd = NormalDist()
    for i, dist in enumerate(slip_dists):
        probs = np.array([p for p, _ in dist.outcomes])
        payoffs = np.array([m for _, m in dist.outcomes])
        # Sort outcomes by payoff to build a CDF.
        order = np.argsort(payoffs)
        cdf = np.cumsum(probs[order])
        cdf /= cdf[-1]
        # Convert latent normal to uniform via Φ.
        u = np.array([nd.cdf(x) for x in X[:, i]])
        # Inverse CDF lookup.
        idxs = np.searchsorted(cdf, u, side="left")
        idxs = np.clip(idxs, 0, len(payoffs) - 1)
        samples[:, i] = payoffs[order][idxs]
    return samples


def portfolio_kelly_mc(
    slip_dists: list[SlipDistribution],
    correlation_matrix: Optional[np.ndarray] = None,
    *,
    n_sims: int = 5000,
    learning_rate: float = 0.05,
    n_iter: int = 200,
    kappa: float = KAPPA,
    cap_per_slip: float = CAP_PER_SLIP,
    cap_total: float = CAP_TOTAL,
    seed: Optional[int] = None,
) -> list[float]:
    """Joint Kelly via Monte-Carlo gradient ascent.

    Returns a list of N fractions, each already κ-damped and capped.
    """
    N = len(slip_dists)
    if N == 0:
        return []
    rng = np.random.default_rng(seed)
    payoffs = _sample_correlated_payoffs(
        slip_dists, correlation_matrix, n_sims, rng,
    )
    net = payoffs - 1.0   # (n_sims, N)

    # Gradient ascent on E[log(1 + net @ f)] subject to 0 ≤ f_i ≤ 1.
    f = np.full(N, 0.01)
    for _ in range(n_iter):
        portfolio = 1.0 + net @ f
        portfolio = np.maximum(portfolio, 1e-9)
        grad = (net / portfolio[:, None]).mean(axis=0)
        f = f + learning_rate * grad
        # Project to [0, 1]
        f = np.clip(f, 0.0, 1.0)

    # κ damping + caps.
    f *= kappa
    f = np.minimum(f, cap_per_slip)
    total = float(np.sum(f))
    if total > cap_total and total > 0:
        f *= (cap_total / total)
    return f.tolist()


# ─────────────────────────────────────────────────────────────────────────
# Convenience: builds SlipDistribution from a list of leg probabilities
# ─────────────────────────────────────────────────────────────────────────

def build_slip_distribution(
    leg_probs: list[float],
    slip_type: str = "power",
) -> SlipDistribution:
    """Build a SlipDistribution from independent leg probabilities + slip
    type. Same enumeration as StrategyTester._payoff_distribution; kept
    here so this module is import-light."""
    from engine.constants import POWER_PAYOUTS, FLEX_PAYOUTS
    import itertools
    n = len(leg_probs)
    out: list[tuple[float, float]] = []
    for outcome in itertools.product([0, 1], repeat=n):
        prob = 1.0
        for i in range(n):
            prob *= leg_probs[i] if outcome[i] == 1 else (1.0 - leg_probs[i])
        hits = sum(outcome)
        if slip_type == "power":
            mult = POWER_PAYOUTS.get(n, 0.0) if hits == n else 0.0
        else:
            mult = FLEX_PAYOUTS.get(n, {}).get(hits, 0.0)
        out.append((prob, mult))
    return SlipDistribution(outcomes=out)
