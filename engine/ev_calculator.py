"""
EV calculation engine.

- Per-leg individual EV% (theoretical, using optimal 5/6-Flex break-even)
- Slip EV% for Power (exact) and Flex (full enumeration over all leg combinations)
- Correlation-aware Monte Carlo EV for same-game stacks
"""
import statistics
from itertools import product as itertools_product
from typing import Optional

import numpy as np

from engine.constants import (
    OPTIMAL_BREAK_EVEN,
    OPTIMAL_IMPLIED_DECIMAL,
    POWER_PAYOUTS,
    FLEX_PAYOUTS,
)
from engine.devig import prob_to_american
from engine.isotonic_calibration import (
    load_isotonic_calibration, calibrate as _apply_isotonic_calibration,
)
# RWBC drop-in alternative — only consulted when config.USE_RWBC=true.
# Returns Optional[float]: a number means "calibrated prob", None means
# "cell untradeable" (circuit breaker fired). The wrapper below picks
# the calibrator at import time.
from engine import rwbc_calibration as _rwbc
from config import USE_RWBC as _USE_RWBC, USE_RAW_CONSENSUS_ONLY as _USE_RAW
from engine.correlation import build_correlation_matrix, legs_metadata_from_bets

# Module-level calibration curves (refreshed on import or via reload_calibration).
# We switched from the Beta-Binomial empirical table to the hierarchical
# isotonic curves so a sparse (league, prop, side) bucket borrows strength
# from neighbouring raw-prob bins instead of falling off a hard band edge.
_calibration_curves: dict = load_isotonic_calibration()


# ---------------------------------------------------------------------------
# Individual bet result
# ---------------------------------------------------------------------------

def reload_calibration():
    """Reload the isotonic calibration curves from disk (called after each refit)."""
    global _calibration_curves
    _calibration_curves = load_isotonic_calibration()


class BetResult:
    __slots__ = (
        "bet_id", "player_name", "league", "prop_type",
        "pp_line", "fd_line", "side",
        "raw_true_prob", "true_prob", "true_odds", "edge", "individual_ev_pct",
        "over_odds", "under_odds", "both_sided",
        "pp_player_id", "start_time", "market_width", "team",
        # RWBC: set True when the calibration cell halted this leg
        # (w_cell < W_CELL_HALT_THRESHOLD or unknown cell). Auto-backtest
        # worker filters legs with this=True from the slip pool.
        "calibration_halted",
    )

    def __init__(
        self,
        bet_id: str,
        player_name: str,
        league: str,
        prop_type: str,
        pp_line: float,
        fd_line: float,
        side: str,            # "over" or "under"
        true_prob: float,
        over_odds: Optional[int],
        under_odds: Optional[int],
        both_sided: bool,
        pp_player_id: str,
        start_time: str = "",
        market_width: Optional[float] = None,
        team: str = "",
    ):
        self.bet_id = bet_id
        self.player_name = player_name
        self.league = league
        self.prop_type = prop_type
        self.pp_line = pp_line
        self.fd_line = fd_line
        self.side = side
        self.raw_true_prob = true_prob
        self.over_odds = over_odds
        self.under_odds = under_odds
        self.both_sided = both_sided
        self.pp_player_id = pp_player_id
        self.start_time = start_time
        self.market_width = market_width
        self.team = team or ""

        # Calibration. Two code paths gated by config.USE_RWBC:
        #
        # USE_RWBC=false (default) — Hierarchical isotonic with Bayesian
        #   shrinkage: global PAV curve combined with the (league, prop,
        #   side) curve via κ-shrinkage. Strictly more flexible than the
        #   Beta-Binomial bands because PAV borrows monotone signal across
        #   neighbouring raw-prob bins instead of cliffing at hard band
        #   edges. The [0.001, 0.999] window is a numerical guard for
        #   downstream log-loss / EV math; the calibrated number is
        #   allowed to push above or below raw_prob when the data warrants.
        #
        # USE_RWBC=true — Reliability-Weighted Bayesian Calibration. Per
        #   (league, prop, side) cell with hard circuit breaker. A None
        #   return from _rwbc.calibrate() means the cell is untradeable;
        #   we still need to populate self.true_prob for display surfaces
        #   (the Lines tab, +EV table tooltip), so we fall back to
        #   raw_true_prob for display while setting calibration_halted=True
        #   so the auto-backtester can filter the leg out of slip pools.
        self.calibration_halted = False
        if _USE_RAW:
            # Diagnostic mode — bypass *both* calibrators. true_prob is
            # set to the vig-stripped book consensus exactly. Useful for
            # A/B against the calibrated paths. Halt flag is always False
            # because there's no model layer that could halt.
            calibrated_prob = max(0.001, min(0.999, true_prob))
        elif _USE_RWBC:
            _rwbc_prob = _rwbc.calibrate(true_prob, league, prop_type, side)
            if _rwbc_prob is None:
                # Cell halted — display surfaces see raw model output but
                # auto-backtest skips. edge/individual_ev_pct stay at the
                # raw values; the auto-backtest worker is the gate.
                self.calibration_halted = True
                calibrated_prob = max(0.001, min(0.999, true_prob))
            else:
                calibrated_prob = _rwbc_prob
        else:
            _raw_calibrated = _apply_isotonic_calibration(
                _calibration_curves, league, prop_type, side, true_prob,
            )
            calibrated_prob = max(0.001, min(0.999, _raw_calibrated))

        self.true_prob = calibrated_prob
        self.true_odds = prob_to_american(calibrated_prob)

        self.edge = round(calibrated_prob - OPTIMAL_BREAK_EVEN, 6)
        self.individual_ev_pct = round((calibrated_prob * OPTIMAL_IMPLIED_DECIMAL) - 1.0, 6)

    def score_for(self, slip_n: int = 6, slip_type: str = "power") -> float:
        """Context-aware per-leg EV%. Returns the leg's EV as a fraction of
        stake given the slip (size, type) it will deploy in. Use this when
        ranking legs for a *specific* slip; the legacy `individual_ev_pct`
        assumes Power-6 and misranks legs intended for shorter slips."""
        from engine.constants import score_leg
        return score_leg(self.true_prob, slip_n=slip_n, slip_type=slip_type)

    def tier(self) -> str:
        """Phase 1A tier label for this leg. 'A' / 'B' / 'C' / 'REJECT'."""
        from engine.tier import tier_for_prob
        return tier_for_prob(self.true_prob, calibration_halted=self.calibration_halted)

    def to_dict(self) -> dict:
        return {
            "bet_id":            self.bet_id,
            "player_name":       self.player_name,
            "league":            self.league,
            "prop_type":         self.prop_type,
            "pp_line":           self.pp_line,
            "fd_line":           self.fd_line,
            "side":              self.side,
            "true_prob":         round(self.true_prob, 4),
            "raw_true_prob":     round(self.raw_true_prob, 4),
            "market_width":      round(self.market_width, 4) if self.market_width is not None else None,
            "team":              self.team or "",
            "true_odds":         self.true_odds,
            "edge":              round(self.edge, 4),
            "individual_ev_pct": round(self.individual_ev_pct, 4),
            "over_odds":         self.over_odds,
            "under_odds":        self.under_odds,
            "both_sided":        self.both_sided,
            "start_time":        self.start_time,
            # RWBC circuit breaker flag. True iff the calibration cell for
            # this bet had w_cell < W_CELL_HALT_THRESHOLD at scoring time
            # (auto-backtest worker filters legs with this=True).
            "calibration_halted": bool(getattr(self, "calibration_halted", False)),
            # Phase 1A tier label. The auto-backtest worker reads this to
            # decide which (slip_type, n_legs) combos this leg may enter.
            "tier": self.tier(),
        }


# ---------------------------------------------------------------------------
# (Removed: `_evaluate_same_line`, `_get_true_prob_for_side`,
# `compute_bet_true_prob_raw`, `evaluate_match`.)
#
# These were the v1 EV path that only consulted FanDuel + DraftKings, so
# Pinnacle prices were silently ignored even though the matcher captured
# them. The live pipeline switched to `engine.consensus.compute_true_
# probability` (which includes Pinnacle with proper sharpness weights)
# back in the consensus migration; the legacy functions stayed in the
# file as dead code with stale doctrings, creating real risk that a
# future caller would rewire them and quietly drop a third of the
# price-discovery signal. Removed entirely.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Slip EV calculations
# ---------------------------------------------------------------------------

def power_slip_ev(true_probs: list[float]) -> Optional[float]:
    """
    EV for a Power slip: all legs must hit.
    Returns None if the pick count isn't supported.
    """
    n = len(true_probs)
    payout = POWER_PAYOUTS.get(n)
    if payout is None:
        return None
    combined = 1.0
    for p in true_probs:
        combined *= p
    return combined * payout - 1.0


def flex_slip_ev(true_probs: list[float]) -> Optional[float]:
    """
    EV for a Flex slip using full enumeration over all 2^n outcome combinations.
    Each leg has its own true_prob (independent).
    Returns None if the pick count isn't supported.
    """
    n = len(true_probs)
    payout_tiers = FLEX_PAYOUTS.get(n)
    if payout_tiers is None:
        return None

    ev = -1.0  # cost of the bet
    for outcome in itertools_product([0, 1], repeat=n):
        prob = 1.0
        for i, hit in enumerate(outcome):
            prob *= true_probs[i] if hit else (1.0 - true_probs[i])
        k = sum(outcome)
        payout = payout_tiers.get(k, 0.0)
        ev += prob * payout

    return ev


_MC_N_SIMS_DEFAULT: int = 20_000


def _sample_correlated_bernoullis(
    probs: np.ndarray,
    corr_matrix: np.ndarray,
    n_sims: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Draw `n_sims` correlated Bernoulli vectors with the given marginal
    probabilities and latent-scale correlation matrix via a Gaussian copula.

    Returns an (n_sims, n_legs) int8 array of 0/1 outcomes."""
    n = probs.shape[0]
    # Invert the standard-normal CDF once per leg. P(X_i < τ_i) = p_i where
    # X_i ~ N(0, 1). Using stdlib inv_cdf avoids a scipy dependency.
    thresholds = np.array(
        [statistics.NormalDist().inv_cdf(float(p)) for p in probs],
        dtype=float,
    )

    # Cholesky with mild jitter for numerical stability on near-singular
    # matrices (e.g. when two legs have ρ ≈ 1).
    jitter = 1e-10
    for _ in range(4):
        try:
            L = np.linalg.cholesky(corr_matrix + jitter * np.eye(n))
            break
        except np.linalg.LinAlgError:
            jitter *= 100
    else:
        # Absolute fallback: treat as independent. Better to lose the
        # correlation signal than to crash the slip evaluation.
        L = np.eye(n)

    Z = rng.standard_normal((n_sims, n))
    X = Z @ L.T
    hits = (X < thresholds[np.newaxis, :]).astype(np.int8)
    return hits


def power_slip_ev_corr(
    probs: list[float],
    corr_matrix: np.ndarray,
    n_sims: int = _MC_N_SIMS_DEFAULT,
    seed: Optional[int] = None,
) -> Optional[float]:
    """Correlation-aware Power EV via Gaussian copula Monte Carlo.

    Independence-assuming Π(p_i) systematically overstates Power EV on
    same-game stacks. This function samples correlated outcomes and computes
    P(all hit) directly, which at default n_sims has a standard error of
    ~0.7% on a 5% probability.
    """
    n = len(probs)
    payout = POWER_PAYOUTS.get(n)
    if payout is None:
        return None
    if n == 0:
        return None
    if n == 1:
        return probs[0] * payout - 1.0

    rng = np.random.default_rng(seed)
    p_arr = np.asarray(probs, dtype=float)
    hits = _sample_correlated_bernoullis(p_arr, corr_matrix, n_sims, rng)
    all_hit_rate = float((hits.sum(axis=1) == n).mean())
    return all_hit_rate * payout - 1.0


def flex_slip_ev_corr(
    probs: list[float],
    corr_matrix: np.ndarray,
    n_sims: int = _MC_N_SIMS_DEFAULT,
    seed: Optional[int] = None,
) -> Optional[float]:
    """Correlation-aware Flex EV via Gaussian copula Monte Carlo.

    Flex payouts depend on the number of hits, so we compute the full
    distribution of hit counts under the correlation structure rather than
    enumerating 2^n independent outcomes.
    """
    n = len(probs)
    payout_tiers = FLEX_PAYOUTS.get(n)
    if payout_tiers is None:
        return None

    rng = np.random.default_rng(seed)
    p_arr = np.asarray(probs, dtype=float)
    hits = _sample_correlated_bernoullis(p_arr, corr_matrix, n_sims, rng)
    hit_counts = hits.sum(axis=1)

    ev = -1.0  # cost of the bet
    for k, pay in payout_tiers.items():
        if pay == 0.0:
            continue
        ev += float((hit_counts == k).mean()) * pay
    return ev


def calculate_slip(
    bet_results: list[BetResult],
    bankroll: float,
    n_sims: int = _MC_N_SIMS_DEFAULT,
    seed: Optional[int] = None,
) -> dict:
    """
    Given a list of selected BetResults and a bankroll, compute Power and Flex EV.
    Returns a dict with slip stats ready for the frontend.

    Uses correlation-aware Monte Carlo when two or more legs share a game
    (latent-scale ρ > 0). If the correlation matrix is the identity (all
    legs from different games), we short-circuit to the exact independence
    formulas — no MC noise on unrelated slips.
    """
    n = len(bet_results)
    true_probs = [b.true_prob for b in bet_results]

    corr_matrix = build_correlation_matrix(legs_metadata_from_bets(bet_results))
    has_correlation = n >= 2 and not np.allclose(corr_matrix, np.eye(n), atol=1e-8)

    if has_correlation:
        power_ev = power_slip_ev_corr(true_probs, corr_matrix, n_sims=n_sims, seed=seed)
        flex_ev  = flex_slip_ev_corr(true_probs,  corr_matrix, n_sims=n_sims, seed=seed)
    else:
        power_ev = power_slip_ev(true_probs)
        flex_ev  = flex_slip_ev(true_probs)

    # Determine the better play
    best_type = None
    best_ev   = None
    if power_ev is not None and flex_ev is not None:
        if power_ev >= flex_ev:
            best_type, best_ev = "Power", power_ev
        else:
            best_type, best_ev = "Flex", flex_ev
    elif power_ev is not None:
        best_type, best_ev = "Power", power_ev
    elif flex_ev is not None:
        best_type, best_ev = "Flex", flex_ev

    expected_profit = bankroll * best_ev if best_ev is not None else None

    return {
        "n_picks":          n,
        "power_ev_pct":     round(power_ev, 4) if power_ev is not None else None,
        "flex_ev_pct":      round(flex_ev, 4)  if flex_ev  is not None else None,
        "best_play_type":   best_type,
        "best_ev_pct":      round(best_ev, 4)  if best_ev  is not None else None,
        "expected_profit":  round(expected_profit, 2) if expected_profit is not None else None,
        "bankroll":         bankroll,
        "legs": [
            {
                "player_name": b.player_name,
                "prop_type":   b.prop_type,
                "pp_line":     b.pp_line,
                "side":        b.side,
                "true_prob":   round(b.true_prob, 4),
                "ind_ev_pct":  round(b.individual_ev_pct, 4),
            }
            for b in bet_results
        ],
    }
