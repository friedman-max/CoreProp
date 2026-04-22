"""
Leg-to-leg correlation model for slip EV.

Independence is a poor assumption for Power/Flex slips. Two legs from the
same game (same player even more so) are positively correlated: a blowout,
high-pace game, or a hot shooting night lifts all overs on that side; a
low-scoring game sinks them together. Treating those as independent inflates
P(all hit) and overstates Power EV, sometimes by 20-30% on same-game stacks.

This module produces an n×n correlation matrix on the latent (Gaussian
copula) scale. The EV calculator consumes it via Monte Carlo simulation:
draw correlated standard normals, threshold each at Φ⁻¹(p_i), and count
hits — which preserves both marginal probabilities AND pairwise correlations.

Correlation values come from two sources:
  1. **Empirical**: pairwise hit-correlations fit nightly from
     `market_observatory`. A bucket (league, same_player_flag) is trusted
     once it accumulates MIN_PAIR_OBS resolved pairs.
  2. **Heuristic**: fixed constants, used for buckets that haven't crossed
     the observation threshold yet (cold start / new leagues / sparse
     prop types).

The empirical layer takes over automatically as data accumulates — no
config change or code switch needed. See `update_correlation_map()`.
"""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime
from typing import Iterable, Optional

import numpy as np

logger = logging.getLogger(__name__)


# Latent-scale (Gaussian copula) correlation magnitudes. These are NOT
# Pearson correlations of {0,1} hit outcomes — they're the correlations of
# the underlying standard-normal variates. A latent ρ of 0.25 produces a
# Bernoulli-scale correlation of roughly 0.15-0.20 for marginals near 0.5.
BASE_SAME_PLAYER: float = 0.30
BASE_SAME_GAME:   float = 0.12

# Per-league multiplier on the base correlations. Pace-driven sports (all
# basketball variants) have stronger same-game performance correlation than
# discrete plate-appearance sports (MLB). Unknown leagues default to 1.0.
LEAGUE_PACE_MULTIPLIER: dict[str, float] = {
    "NBA":        1.20,
    "WNBA":       1.20,
    "NCAAB":      1.15,
    "NBL":        1.15,
    "EUROLEAGUE": 1.15,
    "NFL":        1.00,
    "NCAAF":      1.00,
    "NHL":        0.95,
    "MLB":        0.60,
    "SOCCER":     0.90,
}

# Absolute ceiling on any single ρ entry. Latent ρ close to 1 makes the
# Cholesky factorisation near-singular and makes the slip behaviour
# degenerate (all legs move together). 0.5 is conservative.
MAX_RHO: float = 0.50

# Empirical correlation map — persisted JSON. Auto-loaded at module import
# and refreshed by the hourly scheduler job via `update_correlation_map()`.
CORRELATION_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "correlation_map.json")

# Minimum number of resolved pair observations required to trust an
# empirical correlation bucket. Below this threshold we fall back to the
# heuristic default — noise from small samples would be worse than the
# conservative prior. ~100 pairs gives a standard error of ~0.10 on ρ.
MIN_PAIR_OBS: int = 100

# Empirical map populated by `load_correlation_map()` at import time and
# mutated by `reload_correlation()` after each scheduler run.
#
# Shape: {"NBA|same_player": {"n": 450, "phi": 0.18, "rho_latent": 0.28}, ...}
_empirical_map: dict = {}


def _league_multiplier(league: str) -> float:
    if not league:
        return 1.0
    return LEAGUE_PACE_MULTIPLIER.get(league.upper(), 1.0)


def _bucket_key(league: str, same_player: bool, prop_a: str = "", prop_b: str = "") -> str:
    """Empirical-map bucket key for a pair (league-scoped, player-joined flag).
    Optionally includes sorted prop-pair names for granular fitting.
    """
    league = (league or "UNKNOWN").upper()
    base = "same_player" if same_player else "same_game"
    if prop_a and prop_b:
        p1, p2 = sorted([prop_a.upper(), prop_b.upper()])
        return f"{league}|{base}|{p1}|{p2}"
    return f"{league}|{base}"


def _heuristic_rho(league: str, same_player: bool) -> float:
    mult = _league_multiplier(league)
    base = BASE_SAME_PLAYER if same_player else BASE_SAME_GAME
    return base * mult


def _pair_correlation(a: dict, b: dict) -> float:
    """Latent-scale correlation for a single leg pair.

    Consults the empirical bucket first. If the bucket has crossed
    MIN_PAIR_OBS resolutions, use its fitted ρ; otherwise fall back to the
    heuristic. This makes the transition to data-driven correlations
    automatic — no flag to flip.
    """
    game_a = a.get("game_key") or ""
    game_b = b.get("game_key") or ""
    if not game_a or game_a != game_b:
        return 0.0

    pid_a = a.get("player_id") or ""
    pid_b = b.get("player_id") or ""
    same_player = bool(pid_a) and pid_a == pid_b
    league = a.get("league", "")
    prop_a = a.get("prop", "")
    prop_b = b.get("prop", "")

    # 1. Specific prop-pair bucket (e.g. NBA|same_game|POINTS|ASSISTS)
    key_spec = _bucket_key(league, same_player, prop_a, prop_b)
    bucket_spec = _empirical_map.get(key_spec)

    # 2. General bucket fallback (e.g. NBA|same_game)
    key_gen = _bucket_key(league, same_player)
    bucket_gen = _empirical_map.get(key_gen)

    if bucket_spec and bucket_spec.get("n", 0) >= MIN_PAIR_OBS:
        rho = float(bucket_spec.get("rho_latent", 0.0))
    elif bucket_gen and bucket_gen.get("n", 0) >= MIN_PAIR_OBS:
        rho = float(bucket_gen.get("rho_latent", 0.0))
    else:
        rho = _heuristic_rho(league, same_player)

    return float(np.clip(rho, -MAX_RHO, MAX_RHO))


def build_correlation_matrix(legs: list[dict]) -> np.ndarray:
    """Return an n×n latent-scale correlation matrix for the given legs.

    Each leg is a dict with (all optional but recommended):
      - league:     str, upper-case preferred — drives the pace multiplier.
      - game_key:   str, identifies the game (e.g. f"{league}|{start_time}").
      - player_id:  str, identifies the player (same-player legs).

    Any leg missing a `game_key` is treated as an independent contract
    (all off-diagonal entries involving it are zero), which is the safe
    default when upstream metadata is incomplete.

    The returned matrix is symmetric with ones on the diagonal, ρ ∈ [-0.5, 0.5]
    off-diagonal, and is projected to the nearest PSD matrix in the (rare)
    case the heuristic produces one that isn't positive semi-definite.
    """
    n = len(legs)
    if n == 0:
        return np.zeros((0, 0), dtype=float)

    R = np.eye(n, dtype=float)
    for i in range(n):
        for j in range(i + 1, n):
            rho = _pair_correlation(legs[i], legs[j])
            R[i, j] = rho
            R[j, i] = rho

    return _project_to_psd(R)


def _project_to_psd(R: np.ndarray) -> np.ndarray:
    """Project a symmetric matrix onto the PSD cone by zeroing negative
    eigenvalues, then renormalising the diagonal to 1. Cheap for n ≤ 6."""
    try:
        # Quick path: Cholesky succeeds → already PSD.
        np.linalg.cholesky(R + 1e-10 * np.eye(R.shape[0]))
        return R
    except np.linalg.LinAlgError:
        pass

    # Eigendecomposition fallback.
    w, V = np.linalg.eigh(R)
    w_clipped = np.clip(w, 1e-8, None)
    R_psd = (V * w_clipped) @ V.T
    # Renormalise so the diagonal stays 1 (unit variances on the latent scale).
    d = np.sqrt(np.diag(R_psd))
    d_outer = np.outer(d, d)
    d_outer[d_outer == 0] = 1.0
    R_norm = R_psd / d_outer
    np.fill_diagonal(R_norm, 1.0)
    return R_norm


def _field(b, *names, default: str = "") -> str:
    """Look up the first non-empty attribute or dict key from `names`."""
    for n in names:
        if hasattr(b, n):
            val = getattr(b, n)
            if val:
                return str(val)
        elif isinstance(b, dict) and n in b:
            val = b[n]
            if val:
                return str(val)
    return default


# ---------------------------------------------------------------------------
# Empirical correlation fitting
# ---------------------------------------------------------------------------

def _bernoulli_phi(sum_x: int, sum_y: int, sum_xy: int, n: int) -> Optional[float]:
    """Pearson correlation (== Bernoulli phi coefficient) for two 0/1 series
    given running totals. Returns None if either marginal has zero variance."""
    if n <= 1:
        return None
    mean_x = sum_x / n
    mean_y = sum_y / n
    var_x = mean_x - mean_x ** 2  # Bernoulli variance = p(1-p)
    var_y = mean_y - mean_y ** 2
    if var_x <= 0 or var_y <= 0:
        return None
    cov = (sum_xy / n) - mean_x * mean_y
    return cov / math.sqrt(var_x * var_y)


def _phi_to_latent(phi: float) -> float:
    """Approximate Bernoulli phi → latent Gaussian-copula ρ.

    Exact for marginals at p = 0.5: ρ = sin(π · phi / 2) (the tetrachoric
    correlation at the symmetric point). Within a few percent across the
    0.3-0.7 probability range we care about in sports betting, which is
    well within the sampling noise of a 100-pair bucket."""
    return math.sin(phi * math.pi / 2.0)


def update_correlation_map() -> Optional[dict]:
    """Fit pairwise hit-correlations from resolved `market_observatory`
    rows and persist as JSON. Called hourly by the scheduler.

    Groups resolved observations by (league, game_start), enumerates all
    within-game pairs, and accumulates 0/1 hit outcomes into per-bucket
    running totals. Buckets with < MIN_PAIR_OBS pairs are written too
    (for visibility / diagnostics) but the lookup in `_pair_correlation`
    only trusts them once they cross the threshold.
    """
    # Import here to avoid a circular import at module load time.
    from engine.database import get_db

    db = get_db()
    if not db:
        return None

    try:
        res = (
            db.table("market_observatory")
              .select("player, league, game_start, result, prop")
              .in_("result", ["hit", "miss"])
              .execute()
        )
    except Exception as exc:
        logger.warning("Correlation fit: query failed: %s", exc)
        return None

    rows = res.data or []
    if not rows:
        logger.info("Correlation fit: no resolved observations yet.")
        return None

    # Group by (league, game_start) so we can enumerate within-game pairs.
    games: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        league = (r.get("league") or "").upper()
        gs = r.get("game_start") or ""
        if not league or not gs:
            continue
        hit = 1 if str(r.get("result", "")).lower() == "hit" else 0
        games.setdefault((league, gs), []).append({
            "player": (r.get("player") or "").strip().lower(),
            "prop": (r.get("prop") or "UNKNOWN").strip().upper(),
            "hit": hit,
        })

    # Running totals per bucket: n, Σx, Σy, Σxy.
    buckets: dict[str, dict[str, int]] = {}
    for (league, _gs), legs in games.items():
        k = len(legs)
        if k < 2:
            continue
        for i in range(k):
            for j in range(i + 1, k):
                a, b = legs[i], legs[j]
                same_player = a["player"] != "" and a["player"] == b["player"]
                
                # Accumulate both the specific prop-pair bucket AND the general bucket.
                # This ensures we have aggregate data for the sport even if specific
                # prop combinations are sparse.
                keys = [
                    _bucket_key(league, same_player, a["prop"], b["prop"]),
                    _bucket_key(league, same_player)
                ]
                for key in keys:
                    bk = buckets.setdefault(key, {"n": 0, "sum_x": 0, "sum_y": 0, "sum_xy": 0})
                    bk["n"] += 1
                    bk["sum_x"] += a["hit"]
                    bk["sum_y"] += b["hit"]
                    bk["sum_xy"] += a["hit"] * b["hit"]

    # Derive phi and latent ρ per bucket.
    fitted: dict = {}
    for key, s in buckets.items():
        phi = _bernoulli_phi(s["sum_x"], s["sum_y"], s["sum_xy"], s["n"])
        if phi is None:
            continue
        rho_latent = _phi_to_latent(phi)
        fitted[key] = {
            "n": s["n"],
            "phi": round(phi, 4),
            "rho_latent": round(float(np.clip(rho_latent, -MAX_RHO, MAX_RHO)), 4),
        }

    payload = {
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "min_pair_obs": MIN_PAIR_OBS,
        "buckets": fitted,
    }

    try:
        os.makedirs(os.path.dirname(CORRELATION_FILE), exist_ok=True)
        with open(CORRELATION_FILE, "w") as f:
            json.dump(payload, f, indent=2)
    except Exception as exc:
        logger.warning("Correlation fit: write failed: %s", exc)
        return payload  # still return for in-memory reload

    n_trusted = sum(1 for v in fitted.values() if v["n"] >= MIN_PAIR_OBS)
    logger.info(
        "Correlation fit: %d buckets fitted (%d above trust threshold of %d pairs).",
        len(fitted), n_trusted, MIN_PAIR_OBS,
    )
    return payload


def load_correlation_map() -> dict:
    """Read the persisted empirical map from disk into the plain-dict form
    used by `_pair_correlation`. Returns {} if the file is missing."""
    if not os.path.exists(CORRELATION_FILE):
        return {}
    try:
        with open(CORRELATION_FILE, "r") as f:
            payload = json.load(f)
        return payload.get("buckets", {}) or {}
    except Exception as exc:
        logger.warning("Correlation load: %s", exc)
        return {}


def reload_correlation() -> int:
    """Refresh the module-level empirical map from disk. Called after each
    hourly `update_correlation_map()` run so in-flight slip evaluations see
    the new ρ values immediately."""
    global _empirical_map
    _empirical_map = load_correlation_map()
    return sum(1 for v in _empirical_map.values() if v.get("n", 0) >= MIN_PAIR_OBS)


# Auto-load at import time so unit-test / first-request paths already have
# whatever empirical data has been persisted.
_empirical_map = load_correlation_map()


def legs_metadata_from_bets(bets: Iterable) -> list[dict]:
    """Extract the minimal leg metadata the correlation model needs.

    Accepts either a list of BetResult-like objects (attributes: `league`,
    `start_time`, `pp_player_id`) OR a list of dicts coming from the
    frontend (keys: `league`, `start_time`, `pp_player_id` or `player_id`).
    """
    out: list[dict] = []
    for b in bets:
        league = _field(b, "league").strip()
        start_time = _field(b, "start_time").strip()
        player_id = _field(b, "pp_player_id", "player_id").strip()
        prop = _field(b, "prop_type", "stat_type", "prop").strip()
        game_key = f"{league.upper()}|{start_time}" if (league and start_time) else ""
        out.append({
            "league":    league,
            "game_key":  game_key,
            "player_id": player_id,
            "prop":      prop,
        })
    return out
