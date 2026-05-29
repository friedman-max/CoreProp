"""
Beta calibration with shade conditioning (Maybe Cool Fix C2).

Replaces the hierarchical isotonic apply path with a parametric beta
calibration ([Kull et al. 2017](https://proceedings.mlr.press/v54/kull17a/kull17a.pdf))
plus an optional fourth feature for the PP-shade signal from
engine/shade_signal.py:

    q = σ(a·log(s) + b·log(1−s) + c·shade + d)         (shade-conditioned)
    q = σ(a·log(s) + b·log(1−s) + d)                   (no-shade fallback)

Why beta over isotonic on top of RWBC:
  - Phase 1A audit found that PAV is mathematically incapable of
    representing inverted signal (FINDINGS NHL α=-0.616, NBA UNDER
    α=-0.49). RWBC's circuit breaker halts those cells but for cells
    that are *almost* well-behaved (positive but distorted), PAV emits
    a flat segment instead of a downward-sloping fit.
  - Parametric beta has three (or four) parameters that can independently
    shape the curve, including the non-monotone-in-s case via opposite
    signs on (a, b).
  - Boldness: PAV refuses to extrapolate beyond observed buckets; beta
    extrapolates the parametric form. For our D1 overconfidence-at-
    high-prob finding, this is exactly what we need — a calibrator that
    can pull q DOWN in the high-prob tail even when the high-prob bucket
    is thin.

Fit method: weighted Newton-Raphson on the standard logistic-regression
likelihood. Closes in 5-15 iterations on the cell sizes we care about
(n_eff > 40). Falls back to identity on convergence failure.

Apply path: drop-in alternative to isotonic_calibration.calibrate(). Gated
on config.USE_BETA_CAL; when off, the calibrator is unchanged.

Note on shade collinearity: in the current apply path shade is computed
as pp_shade(s) = s - 0.5408, a deterministic function of s. The c
coefficient is therefore NOT identifiable independently of (a, b, d) —
the Newton fit will absorb shade's effect into (a, b, d). The shade
feature only adds signal when fed an INDEPENDENT shade value (e.g.,
shade at log time vs. shade at close, or shade from a different book).
This is a Phase 3 follow-up: the comparison logger should pass an
out-of-sample shade snapshot rather than the input-derived shade.
"""
from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

BETA_CAL_FILE = os.path.join(
    os.path.dirname(__file__), "..", "data", "beta_calibration.json",
)

# Minimum cell n_eff to bother fitting beta-cal. Below this, the apply
# path falls back to RWBC's posterior or to identity.
MIN_FIT_N = 40.0

# Numerical guards
_EPS = 1e-6
_LOG_CLIP = 30.0  # cap |a·log(s)+...| for sigmoid stability


# ─────────────────────────────────────────────────────────────────────────
# Persisted state
# ─────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class BetaCalParams:
    """Fitted params for one (league, prop, side) cell."""
    a: float   # coefficient on log(s)
    b: float   # coefficient on log(1-s)
    c: float   # coefficient on pp_shade (0 when shade-disabled)
    d: float   # intercept
    n_eff: float
    has_shade: bool


# ─────────────────────────────────────────────────────────────────────────
# Fit
# ─────────────────────────────────────────────────────────────────────────

def _design(s: np.ndarray, shade: np.ndarray | None) -> np.ndarray:
    """Build the design matrix from raw probabilities (and optional shade)."""
    s = np.clip(s, _EPS, 1.0 - _EPS)
    x1 = np.log(s)
    x2 = np.log(1.0 - s)
    if shade is None:
        return np.column_stack([x1, x2, np.ones_like(s)])
    return np.column_stack([x1, x2, shade, np.ones_like(s)])


def _identity_params(has_shade: bool, n_eff: float) -> BetaCalParams:
    """The (a=1, b=-1, c=0, d=0) parameter set reduces to q = s exactly
    (because log(s) - log(1-s) = log(s/(1-s)) = logit(s); σ(logit(s))=s).
    """
    return BetaCalParams(a=1.0, b=-1.0, c=0.0, d=0.0,
                         n_eff=n_eff, has_shade=has_shade)


def fit_beta_cal(
    s: np.ndarray,
    y: np.ndarray,
    w: Optional[np.ndarray] = None,
    shade: Optional[np.ndarray] = None,
    *,
    max_iter: int = 25,
    tol: float = 1e-7,
    ridge: float = 1e-4,
) -> BetaCalParams:
    """Newton-Raphson weighted logistic regression. Returns identity params
    on convergence failure so the apply path never blows up the leg.

    Args:
        s:     raw probabilities to be calibrated (n,).
        y:     0/1 outcomes (n,).
        w:     observation weights (n,); default 1.
        shade: pp_shade values (n,) for shade-conditioned fit; None to omit.
        max_iter / tol: Newton stop criteria.
        ridge: tiny regularizer for Hessian stability; protects against
               perfectly-separable cells.
    """
    n = len(s)
    if n < MIN_FIT_N:
        return _identity_params(shade is not None, float(n))
    if w is None:
        w = np.ones(n, dtype=float)
    s = np.asarray(s, dtype=float)
    y = np.asarray(y, dtype=float)
    w = np.asarray(w, dtype=float)
    shade_arr = np.asarray(shade, dtype=float) if shade is not None else None

    X = _design(s, shade_arr)
    p_dim = X.shape[1]
    theta = np.zeros(p_dim)

    for _ in range(max_iter):
        z = np.clip(X @ theta, -_LOG_CLIP, _LOG_CLIP)
        q = 1.0 / (1.0 + np.exp(-z))
        residual = w * (q - y)
        grad = X.T @ residual

        wt = w * q * (1.0 - q)
        # Hessian + ridge for numerical stability
        H = X.T @ (X * wt[:, None]) + ridge * np.eye(p_dim)
        try:
            step = np.linalg.solve(H, grad)
        except np.linalg.LinAlgError:
            return _identity_params(shade is not None, float(np.sum(w)))
        theta -= step
        if float(np.linalg.norm(step)) < tol:
            break

    n_eff = float(np.sum(w))
    if shade_arr is None:
        a, b, d = theta.tolist()
        c = 0.0
    else:
        a, b, c, d = theta.tolist()
    return BetaCalParams(
        a=a, b=b, c=c, d=d, n_eff=n_eff,
        has_shade=shade_arr is not None,
    )


# ─────────────────────────────────────────────────────────────────────────
# Apply
# ─────────────────────────────────────────────────────────────────────────

def apply_beta_cal(
    params: BetaCalParams,
    s: float,
    shade: float = 0.0,
) -> float:
    """Compute the beta-calibrated probability for a single leg."""
    s_c = max(_EPS, min(1.0 - _EPS, float(s)))
    z = params.a * math.log(s_c) + params.b * math.log(1.0 - s_c) + params.d
    if params.has_shade:
        z += params.c * float(shade)
    z = max(-_LOG_CLIP, min(_LOG_CLIP, z))
    q = 1.0 / (1.0 + math.exp(-z))
    # Mirror the downstream [0.001, 0.999] guard so log-loss / EV math
    # never sees a 0 or 1 from the calibrator.
    return max(0.001, min(0.999, q))


# ─────────────────────────────────────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────────────────────────────────────

# Process-local cache: (league, prop, side) → BetaCalParams.
_cache: dict[tuple[str, str, str], BetaCalParams] = {}


def save_params(by_cell: dict[tuple[str, str, str], BetaCalParams]) -> bool:
    """Persist a per-cell params dict to disk + cache. Returns True on success."""
    global _cache
    payload = {
        "version": 1,
        "params": {
            f"{lg}|{prop}|{side}": {
                "a": p.a, "b": p.b, "c": p.c, "d": p.d,
                "n_eff": p.n_eff, "has_shade": p.has_shade,
            }
            for (lg, prop, side), p in by_cell.items()
        },
    }
    try:
        os.makedirs(os.path.dirname(BETA_CAL_FILE), exist_ok=True)
        with open(BETA_CAL_FILE, "w") as f:
            json.dump(payload, f)
    except Exception as exc:
        logger.warning("beta_cal save failed: %s", exc)
        return False
    _cache = dict(by_cell)
    return True


def load_params() -> dict[tuple[str, str, str], BetaCalParams]:
    """Load the persisted beta-cal params from disk into the cache."""
    global _cache
    try:
        if not os.path.exists(BETA_CAL_FILE):
            _cache = {}
            return _cache
        with open(BETA_CAL_FILE) as f:
            raw = json.load(f)
    except Exception as exc:
        logger.warning("beta_cal load failed: %s", exc)
        _cache = {}
        return _cache

    out: dict[tuple[str, str, str], BetaCalParams] = {}
    for key, p in (raw.get("params") or {}).items():
        parts = key.split("|")
        if len(parts) != 3:
            continue
        try:
            out[(parts[0], parts[1], parts[2])] = BetaCalParams(
                a=float(p["a"]), b=float(p["b"]),
                c=float(p["c"]), d=float(p["d"]),
                n_eff=float(p.get("n_eff", 0.0)),
                has_shade=bool(p.get("has_shade", False)),
            )
        except (KeyError, TypeError, ValueError):
            continue
    _cache = out
    return _cache


# Auto-load at import.
_cache = load_params()


def calibrate(
    league: str | None,
    prop: str | None,
    side: str | None,
    raw_prob: float,
    *,
    shade: float = 0.0,
) -> Optional[float]:
    """Apply path: look up the cell's beta-cal params and emit the
    calibrated probability. Returns None when no params exist for the
    cell — caller falls back to RWBC or isotonic."""
    side_n = (side or "").lower()
    if not league or not prop or side_n not in ("over", "under"):
        return None
    key = (league, prop, side_n)
    params = _cache.get(key)
    if params is None:
        return None
    return apply_beta_cal(params, raw_prob, shade=shade)


def cache_stats() -> dict:
    """Diagnostic snapshot for monitoring."""
    return {
        "n_cells": len(_cache),
        "mean_n_eff": (
            sum(p.n_eff for p in _cache.values()) / len(_cache)
            if _cache else 0.0
        ),
        "n_with_shade": sum(1 for p in _cache.values() if p.has_shade),
    }
