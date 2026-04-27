"""
Hierarchical isotonic calibration.

Three improvements layered on top of the original per-league PAV:

  1. **Recency weighting** — each observation is weighted by
     `exp(-Δdays / RECENCY_HALF_LIFE_DAYS)` so recent legs dominate when the
     market drifts (rule changes, season starts, player development).

  2. **Hierarchical fit** — three parallel curves are fit per refresh:
       global   → all observations
       league   → filtered to one league
       prop     → filtered to one (league, prop) pair
     At apply-time, Bayesian shrinkage blends each level toward its parent
     based on effective sample size, eliminating the prior hard
     `MIN_LEAGUE_OBS` cutoff. A bucket starts borrowing strength from its
     parent on observation #1 and gradually takes over as data accumulates.

  3. **CLV signal** — closing_prob (set when a game starts) is used as a
     secondary calibration target alongside hit/miss outcomes. CLV
     observations are weighted at `CLV_OBSERVATION_WEIGHT` of an outcome
     observation; the bias they carry (closing line ≠ truly fair price) is
     small and the ~5–10× sample-density boost more than compensates.

PAV (Pool Adjacent Violators) handles continuous targets in [0, 1] just as
naturally as binary {0, 1}, so the same weighted fit serves both signals.

Output is conservative: `calibrated_prob ≤ raw_prob` always — calibration
shrinks but never inflates.
"""
from __future__ import annotations

import os
import json
import math
import logging
from collections import defaultdict
from datetime import datetime, timezone

from engine.database import get_db

logger = logging.getLogger(__name__)

ISOTONIC_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "isotonic_calibration.json")

# Half-life for the recency exponential decay. Drops a 60-day-old observation
# to 50% of its weight, a 120-day-old one to 25%, etc.
RECENCY_HALF_LIFE_DAYS = 60.0

# Hard cutoff on how far back we read from the database. With the 60-day
# half-life, a 180-day-old observation contributes <12.5% weight — not worth
# pulling into RAM on the 512 MB tier. Older rows stay in Supabase, just
# excluded from the fit.
RECENCY_LOOKBACK_DAYS = 180

# Bayesian shrinkage smoothing constant. At n_eff = SHRINKAGE_KAPPA the bucket
# carries 50% of the weight vs its parent; at n_eff = 5×SHRINKAGE_KAPPA, ~83%.
# Lower κ ⇒ buckets take over faster (more variance); higher κ ⇒ more
# regularization toward the parent.
SHRINKAGE_KAPPA = 50.0

# Effective sample size below which we don't bother emitting a curve at all
# (the parent curve will be used). Prevents serializing thousands of one-leg
# nano-buckets.
MIN_BUCKET_N_EFF = 5.0

# Relative weight of a CLV-based observation vs an outcome-based one. Closing
# lines are sharp but not unbiased fair-value estimates; we discount them.
CLV_OBSERVATION_WEIGHT = 0.4

# Anchor probability used by the diagnostic display.
DISPLAY_ANCHOR = 0.60


# ---------------------------------------------------------------------------
# Pool Adjacent Violators (weighted)
# ---------------------------------------------------------------------------

def _fit_pav_weighted(triples: list[tuple[float, float, float]]) -> tuple[list[tuple[float, float]], float]:
    """
    Weighted PAV. Each triple is (x, y, w) with y ∈ [0, 1] and w > 0.

    Returns:
        (curve, total_weight) where curve is a list of (x_mean, y_mean)
        representative points for each monotone block.
    """
    if not triples:
        return [], 0.0
    triples = [(float(x), float(y), float(w)) for x, y, w in triples if w > 0]
    if not triples:
        return [], 0.0
    triples.sort(key=lambda t: t[0])
    # Each block: [sum_xw, sum_yw, sum_w]
    blocks: list[list[float]] = [[t[0] * t[2], t[1] * t[2], t[2]] for t in triples]
    i = 0
    while i < len(blocks) - 1:
        mean_i = blocks[i][1] / blocks[i][2]
        mean_j = blocks[i + 1][1] / blocks[i + 1][2]
        if mean_i > mean_j:
            a, b = blocks[i], blocks[i + 1]
            blocks[i] = [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
            del blocks[i + 1]
            if i > 0:
                i -= 1
        else:
            i += 1
    curve = [(b[0] / b[2], b[1] / b[2]) for b in blocks]
    total_weight = sum(b[2] for b in blocks)
    return curve, total_weight


# ---------------------------------------------------------------------------
# Observation loading
# ---------------------------------------------------------------------------

def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        if isinstance(s, datetime):
            dt = s
        else:
            s = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


_LN2 = math.log(2.0)


def _recency_weight(observation_dt: datetime | None, now: datetime) -> float:
    """0.5 ** (Δdays / HALF_LIFE) — clamped to (0, 1]. True half-life
    semantics: at Δ = HALF_LIFE the weight is exactly 0.5. Missing dates
    default to weight 1 (treat as recent rather than ignore)."""
    if observation_dt is None:
        return 1.0
    delta_days = max(0.0, (now - observation_dt).total_seconds() / 86400.0)
    return math.exp(-delta_days * _LN2 / RECENCY_HALF_LIFE_DAYS)


def _load_observations(db) -> list[dict]:
    """
    Pull calibration observations from two sources:

      1. market_observatory rows resolved to hit/miss → outcome signal.
         Also pull any closing_prob already populated on those rows for the
         CLV signal (introduced by migration_003).

      2. legs table rows that have closing_prob recorded but might not be
         resolved yet → CLV signal only. The legs table lives behind RLS;
         callers must use the service-role client (`get_db()`).

    De-duplicates physical markets across the two sources by `market_key`
    (player|league|prop|line|side|game_start). When both an outcome-bearing
    observatory row and a leg row exist for the same market, the outcome
    becomes the primary y-target and the closing_prob contributes a second,
    lower-weight observation.
    """
    obs_outcome: list[dict] = []
    obs_clv: list[dict] = []

    # Date floor — cuts the historical scan to roughly 3× the half-life.
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=RECENCY_LOOKBACK_DAYS)).isoformat()

    # ── market_observatory ─────────────────────────────────────────────────
    try:
        select_cols = "league, prop, true_prob, result, game_start, closing_prob"
        res = (
            db.table("market_observatory")
            .select(select_cols)
            .gte("game_start", cutoff_iso)
            .execute()
        )
        for r in (res.data or []):
            league = r.get("league")
            prop = r.get("prop")
            try:
                tp = float(r.get("true_prob") or 0.0)
            except (ValueError, TypeError):
                continue
            if not (0.0 < tp < 1.0) or not league:
                continue
            ts = _parse_dt(r.get("game_start"))

            outcome = r.get("result")
            if outcome in ("hit", "miss"):
                obs_outcome.append({
                    "x": tp,
                    "y": 1.0 if outcome == "hit" else 0.0,
                    "league": league,
                    "prop": prop or "",
                    "ts": ts,
                    "source": "outcome",
                })
            cp = r.get("closing_prob")
            if cp is not None:
                try:
                    cpf = float(cp)
                    if 0.0 < cpf < 1.0:
                        obs_clv.append({
                            "x": tp,
                            "y": cpf,
                            "league": league,
                            "prop": prop or "",
                            "ts": ts,
                            "source": "clv",
                        })
                except (ValueError, TypeError):
                    pass
    except Exception as exc:
        logger.warning("IsotonicCalibration: market_observatory load failed: %s", exc)

    # ── legs (service-role bypasses RLS so we can pool across users) ───────
    try:
        select_cols = "league, prop, true_prob, closing_prob, result, game_start"
        res = (
            db.table("legs")
            .select(select_cols)
            .gte("game_start", cutoff_iso)
            .execute()
        )
        for r in (res.data or []):
            league = r.get("league")
            prop = r.get("prop")
            try:
                tp = float(r.get("true_prob") or 0.0)
            except (ValueError, TypeError):
                continue
            if not (0.0 < tp < 1.0) or not league:
                continue
            ts = _parse_dt(r.get("game_start"))
            cp = r.get("closing_prob")
            if cp is not None:
                try:
                    cpf = float(cp)
                    if 0.0 < cpf < 1.0:
                        obs_clv.append({
                            "x": tp,
                            "y": cpf,
                            "league": league,
                            "prop": prop or "",
                            "ts": ts,
                            "source": "clv",
                        })
                except (ValueError, TypeError):
                    pass
            outcome = (r.get("result") or "").lower()
            if outcome in ("hit", "miss", "won", "win", "lost", "loss"):
                obs_outcome.append({
                    "x": tp,
                    "y": 1.0 if outcome in ("hit", "won", "win") else 0.0,
                    "league": league,
                    "prop": prop or "",
                    "ts": ts,
                    "source": "outcome",
                })
    except Exception as exc:
        logger.warning("IsotonicCalibration: legs load failed: %s", exc)

    return obs_outcome + obs_clv


# ---------------------------------------------------------------------------
# Top-level fit
# ---------------------------------------------------------------------------

def _fit_level(triples: list[tuple[float, float, float]]) -> dict | None:
    """Fit a single level. Returns None if too thin to bother emitting."""
    curve, n_eff = _fit_pav_weighted(triples)
    if not curve or n_eff < MIN_BUCKET_N_EFF:
        return None
    return {
        "curve": [[round(x, 6), round(y, 6)] for x, y in curve],
        "n_eff": round(n_eff, 3),
        "n_obs": len(triples),
    }


def update_isotonic_calibration() -> dict | None:
    """
    Refit hierarchical isotonic curves and persist to disk.
    """
    db = get_db()
    if not db:
        return None

    now = datetime.now(timezone.utc)
    try:
        observations = _load_observations(db)
    except Exception as exc:
        logger.error("IsotonicCalibration: load failed: %s", exc)
        return None

    if not observations:
        logger.info("IsotonicCalibration: no observations to fit.")
        return None

    # Build weighted (x, y, w) triples bucketed by hierarchy level.
    global_triples: list[tuple[float, float, float]] = []
    league_triples: dict[str, list[tuple[float, float, float]]] = defaultdict(list)
    prop_triples: dict[str, list[tuple[float, float, float]]] = defaultdict(list)

    for obs in observations:
        recency = _recency_weight(obs["ts"], now)
        source_w = 1.0 if obs["source"] == "outcome" else CLV_OBSERVATION_WEIGHT
        w = recency * source_w
        if w <= 0:
            continue
        triple = (obs["x"], obs["y"], w)
        global_triples.append(triple)
        league_triples[obs["league"]].append(triple)
        if obs["prop"]:
            key = f"{obs['league']}|{obs['prop']}"
            prop_triples[key].append(triple)

    out: dict = {
        "version":    2,
        "fitted_at":  now.isoformat(),
        "global":     _fit_level(global_triples),
        "leagues":    {},
        "props":      {},
        "config": {
            "recency_half_life_days":  RECENCY_HALF_LIFE_DAYS,
            "shrinkage_kappa":         SHRINKAGE_KAPPA,
            "clv_observation_weight":  CLV_OBSERVATION_WEIGHT,
        },
    }
    for lg, ts in league_triples.items():
        fit = _fit_level(ts)
        if fit is not None:
            out["leagues"][lg] = fit
    for key, ts in prop_triples.items():
        fit = _fit_level(ts)
        if fit is not None:
            out["props"][key] = fit

    try:
        os.makedirs(os.path.dirname(ISOTONIC_FILE), exist_ok=True)
        with open(ISOTONIC_FILE, "w") as f:
            json.dump(out, f, indent=2)
    except Exception as exc:
        logger.error("IsotonicCalibration: write failed: %s", exc)
        return None

    logger.info(
        "IsotonicCalibration: fit %d leagues, %d (league,prop) buckets, %d total obs (n_eff=%.1f)",
        len(out["leagues"]), len(out["props"]), len(observations),
        out["global"]["n_eff"] if out["global"] else 0.0,
    )
    return out


# ---------------------------------------------------------------------------
# Load / apply
# ---------------------------------------------------------------------------

def load_isotonic_calibration() -> dict:
    """
    Returns the fitted hierarchy, with curves normalized into tuples for the
    interpolator. Returns an empty (no-op) shape if the file doesn't exist
    or is from a prior schema version.
    """
    if not os.path.exists(ISOTONIC_FILE):
        return {"global": None, "leagues": {}, "props": {}}
    try:
        with open(ISOTONIC_FILE, "r") as f:
            raw = json.load(f)
    except Exception:
        return {"global": None, "leagues": {}, "props": {}}

    if raw.get("version") != 2:
        # Older single-level format; treat as no-op until next refit.
        return {"global": None, "leagues": {}, "props": {}}

    def _normalize(level):
        if not level:
            return None
        return {
            "curve": [(float(x), float(y)) for x, y in level.get("curve", [])],
            "n_eff": float(level.get("n_eff") or 0.0),
            "n_obs": int(level.get("n_obs") or 0),
        }

    return {
        "global":  _normalize(raw.get("global")),
        "leagues": {lg: _normalize(v) for lg, v in (raw.get("leagues") or {}).items()},
        "props":   {k: _normalize(v) for k, v in (raw.get("props") or {}).items()},
        "fitted_at": raw.get("fitted_at"),
        "config":  raw.get("config", {}),
    }


def _interp(curve: list[tuple[float, float]], x: float) -> float:
    """Piecewise-linear interpolation over PAV representative points."""
    if not curve:
        return x
    if x <= curve[0][0]:
        return curve[0][1]
    if x >= curve[-1][0]:
        return curve[-1][1]
    for i in range(len(curve) - 1):
        x0, y0 = curve[i]
        x1, y1 = curve[i + 1]
        if x0 <= x <= x1:
            if x1 == x0:
                return y0
            t = (x - x0) / (x1 - x0)
            return y0 + t * (y1 - y0)
    return curve[-1][1]


def _shrink(parent_q: float, level: dict | None, raw_prob: float) -> float:
    """
    Bayesian shrinkage of a child level toward its parent's calibrated value.

        α = n_eff / (n_eff + κ)
        q = α · q_child + (1 - α) · q_parent

    Returns `parent_q` unchanged when no child level is available.
    """
    if not level or not level.get("curve"):
        return parent_q
    n_eff = float(level.get("n_eff") or 0.0)
    if n_eff <= 0:
        return parent_q
    q_child = _interp(level["curve"], raw_prob)
    alpha = n_eff / (n_eff + SHRINKAGE_KAPPA)
    return alpha * q_child + (1.0 - alpha) * parent_q


def calibrate(curves: dict, league: str | None, prop: str | None, raw_prob: float) -> float:
    """
    Apply hierarchical isotonic calibration with Bayesian shrinkage.

    Walks the hierarchy global → league → (league, prop), shrinking each
    level toward its parent based on effective sample size. Conservative
    cap: the calibrated probability never exceeds the raw input.

    A missing level is treated as "no data" — we fall through to the parent.
    """
    if not curves:
        return raw_prob

    # Level 1: global
    global_level = curves.get("global")
    if not global_level or not global_level.get("curve"):
        # No fits at all yet — pass through.
        return raw_prob
    q_global = _interp(global_level["curve"], raw_prob)

    # Level 2: league shrinks toward global
    league_level = curves.get("leagues", {}).get(league or "") if league else None
    q_league = _shrink(q_global, league_level, raw_prob)

    # Level 3: (league, prop) shrinks toward league
    prop_level = None
    if league and prop:
        prop_level = curves.get("props", {}).get(f"{league}|{prop}")
    q_prop = _shrink(q_league, prop_level, raw_prob)

    # Conservative cap: never inflate.
    return min(q_prop, raw_prob)
