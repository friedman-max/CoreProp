"""
Hierarchical isotonic calibration with incremental sufficient statistics.

Design notes:

  1. **Recency weighting** — each observation contributes `exp(-Δdays / H)`
     where Δ is measured against `resolved_at` (when we learned the
     outcome). H = RECENCY_HALF_LIFE_DAYS.

  2. **Two fitted levels, one apply path** — global and (league, prop)
     curves are fit. The per-league curve is also fit (and surfaced in the
     Observatory diagnostic) but is intentionally NOT a parent in
     `calibrate()`: prop performance must not be diluted by the league
     average. (league, prop) shrinks directly toward global.

  3. **CLV signal** — closing_prob contributes a second observation per
     row at `CLV_OBSERVATION_WEIGHT` × the outcome weight. PAV handles
     continuous targets identically to binary ones. Outcome-only
     accumulators are also tracked separately so the diagnostic heatmap
     can report a true hit rate uncontaminated by CLV.

  4. **Incremental, bounded state** — instead of pulling all rows on every
     refit, we maintain bucket-level sufficient statistics
     `(sum_w, sum_yw, sum_xw, outcome_w, outcome_hits_w)` per (level, x_bin).
     Exponential decay commutes with addition, so on each tick we multiply
     all accumulators by `exp(-Δt·ln2/H)` and fold in only the rows resolved
     since the previous cursor (`resolved_at` from migration_005). State
     size scales with #(distinct buckets), not with history depth — no row
     limits needed.

Output is symmetric: a (league, prop) bucket whose empirical hit rate
exceeds the raw devig probability at high n_eff is allowed to push the
calibrated number above raw. The Bayesian shrinkage (κ=SHRINKAGE_KAPPA)
is the only guardrail against overfitting — thin buckets pool toward
global, well-attested ones move the number meaningfully in either
direction. Stake-sizing (quarter-Kelly) is where variance control lives.
"""
from __future__ import annotations

import os
import json
import math
import logging
from datetime import datetime, timedelta, timezone

from engine.database import get_db

logger = logging.getLogger(__name__)

ISOTONIC_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "isotonic_calibration.json")

# Sufficient-statistics state for the incremental refit. Stored separately
# from the curves file because consumers only ever read the curves; the
# state is internal bookkeeping that doesn't need to round-trip through
# `load_isotonic_calibration()`.
ISOTONIC_STATE_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "isotonic_state.json")

# Width of each raw-probability bin used by the sufficient-statistics
# accumulator. 0.5% = 200 bins across [0, 1] — fine enough that PAV on bin
# means is indistinguishable from PAV on raw observations, coarse enough
# that state stays small (<1 MB even with thousands of (league, prop) keys).
_BIN_COUNT = 200

# Drop accumulator buckets whose decayed weight has fallen below this floor.
# Without pruning, every bucket ever touched would persist forever; with
# pruning the state stays bounded as old activity decays out.
_BUCKET_WEIGHT_FLOOR = 0.01

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
# (the parent curve will be used). Set low so per-league/(league,prop) curves
# surface in the Observatory chart as soon as we have any signal — Bayesian
# shrinkage already pulls thin buckets back toward their parent, so emitting
# them early is safe and lets the chart densify naturally as data grows.
MIN_BUCKET_N_EFF = 2.0

# Default relative weight of a CLV-based observation vs an outcome-based
# one. This is the cold-start fallback used until the dynamic estimator
# (see _fit_clv_weight() below) has enough data to produce its own value.
# Closing lines are sharp but not unbiased fair-value estimates; we
# discount them.
CLV_OBSERVATION_WEIGHT = 0.4

# The fitted CLV weight is clamped into this range. Below CLV_WEIGHT_MIN
# we treat the closing signal as noise; above CLV_WEIGHT_MAX it would
# dominate the (already lower-variance) outcome signal and the calibrator
# would chase the closing line instead of empirical hit rate.
CLV_WEIGHT_MIN = 0.05
CLV_WEIGHT_MAX = 1.50

# Minimum number of (raw, closing, outcome) triples needed before we
# trust the dynamic CLV-weight estimator. Below this we keep the
# CLV_OBSERVATION_WEIGHT default.
CLV_FIT_MIN_OBS = 200

# Anchor probability used by the diagnostic display.
DISPLAY_ANCHOR = 0.60

# ── Market-width confidence weighting ──────────────────────────────────────
# Tight markets (low overround) carry more information per observation than
# wide ones, so we scale each observation's weight by a factor derived from
# its market width. Centred on 1.0 at WIDTH_PIVOT so the prior tuning
# (SHRINKAGE_KAPPA, MIN_BUCKET_N_EFF) doesn't shift.
#
#   cw = clamp( WIDTH_PIVOT / max(width, _MIN_WIDTH_FLOOR),
#               WIDTH_CONFIDENCE_MIN, WIDTH_CONFIDENCE_MAX )
#
# Examples (WIDTH_PIVOT=5):
#   width=2  → cw=2.5 (clamped to 2.0)
#   width=5  → cw=1.0
#   width=15 → cw≈0.33 (clamped to 0.5)
#   None     → cw=1.0 (legacy / unknown)
WIDTH_PIVOT = 5.0
WIDTH_CONFIDENCE_MIN = 0.5
WIDTH_CONFIDENCE_MAX = 2.0
_MIN_WIDTH_FLOOR = 0.5  # avoid runaway weights on near-zero widths


def _confidence_weight(market_width) -> float:
    """Map a market_width (overround %) into a multiplicative weight on the
    observation. Returns 1.0 for None/invalid inputs so legacy rows stay
    neutral instead of dropping out."""
    if market_width is None:
        return 1.0
    try:
        w = float(market_width)
    except (ValueError, TypeError):
        return 1.0
    if w <= 0:
        return 1.0
    raw = WIDTH_PIVOT / max(w, _MIN_WIDTH_FLOOR)
    if raw < WIDTH_CONFIDENCE_MIN:
        return WIDTH_CONFIDENCE_MIN
    if raw > WIDTH_CONFIDENCE_MAX:
        return WIDTH_CONFIDENCE_MAX
    return raw


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


# ---------------------------------------------------------------------------
# Incremental refit: bucket-level sufficient statistics
# ---------------------------------------------------------------------------
#
# Each refit maintains three flat dicts of accumulators keyed by
# (level_key, x_bin) → [sum_w, sum_yw, sum_xw]. The math:
#
#   PAV with weights only depends on (sum_w, sum_yw) per x-bin; sum_xw is
#   carried so we can use the empirical bin-mean as the PAV input rather
#   than the bin centre. Three quantities per bin = full sufficient
#   statistics for weighted isotonic regression.
#
#   Recency decay is exponential, so it commutes with addition:
#     decay( Σᵢ wᵢ·xᵢ )  =  Σᵢ decay·wᵢ·xᵢ
#   which means we can apply Δt of decay to the entire accumulator in O(1)
#   per bucket between refits and add new observations on top, instead of
#   rebuilding from scratch. This is what makes the loader bounded: state
#   size scales with #(distinct buckets) rather than with history depth.
#
# Cursor: we read newly-resolved rows since the last refit using the
# `resolved_at` column added in migration_005. No history scan, no row
# limits — only the delta. Pending rows that haven't resolved yet contribute
# nothing this tick; they'll arrive on the tick after their game finishes.

def _bin(x: float) -> int:
    b = int(round(x * _BIN_COUNT))
    if b < 0:
        return 0
    if b > _BIN_COUNT:
        return _BIN_COUNT
    return b


def _accum_bin_set(by_bin: dict, x: float, y: float, w: float, *, is_outcome: bool = False) -> None:
    """Fold one weighted observation into a {x_bin → cell} dict.

    Cell layout: [sum_w, sum_yw, sum_xw, outcome_w, outcome_hits_w].
    The first three drive PAV (all signals, including CLV); the last two
    are outcome-only and feed the diagnostic heatmap so it shows true
    hit rates rather than CLV-blended values.
    """
    if w <= 0:
        return
    b = _bin(x)
    cell = by_bin.get(b)
    if cell is None:
        cell = [0.0, 0.0, 0.0, 0.0, 0.0]
        by_bin[b] = cell
    cell[0] += w
    cell[1] += y * w
    cell[2] += x * w
    if is_outcome:
        cell[3] += w
        cell[4] += y * w  # y is exactly 0 or 1 for outcomes


def _accum(stats: dict, key: str, x: float, y: float, w: float, *, is_outcome: bool = False) -> None:
    """Fold one weighted observation into a {key → {x_bin → cell}} dict."""
    if w <= 0:
        return
    _accum_bin_set(stats.setdefault(key, {}), x, y, w, is_outcome=is_outcome)


def _decay_bin_set(by_bin: dict, factor: float) -> None:
    """Multiply one {x_bin → cell} dict in-place by `factor`; prune cells
    that fall below the weight floor."""
    for b, cell in list(by_bin.items()):
        for i in range(len(cell)):
            cell[i] *= factor
        if cell[0] < _BUCKET_WEIGHT_FLOOR:
            del by_bin[b]


def _decay_stats(stats: dict, factor: float) -> None:
    """Decay a {key → {x_bin → cell}} two-level dict in place. Drops outer
    keys whose bin set goes empty so the state can't grow without bound."""
    if factor >= 1.0:
        return
    for key, by_bin in list(stats.items()):
        _decay_bin_set(by_bin, factor)
        if not by_bin:
            del stats[key]


def _level_triples(by_bin: dict) -> list[tuple[float, float, float]]:
    """Convert a level's bucket accumulators into PAV input triples."""
    triples: list[tuple[float, float, float]] = []
    for cell in by_bin.values():
        sw, syw, sxw = cell[0], cell[1], cell[2]
        if sw <= 0:
            continue
        triples.append((sxw / sw, syw / sw, sw))
    return triples


def _empty_state() -> dict:
    return {
        "version": 3,
        "fitted_at": None,
        "hwm_observatory": None,
        "hwm_legs": None,
        "global": {},   # {x_bin: [sum_w, sum_yw, sum_xw]}
        "leagues": {},  # {league: {x_bin: [...]}}
        "props": {},    # {"league|prop": {x_bin: [...]}}
        # Sufficient stats for the dynamic CLV-weight fit. We track the
        # weighted mean squared error of (raw - outcome) and (closing -
        # outcome). The ratio is the inverse-variance estimate of the
        # optimal CLV weight; recency-decayed identically to the curve
        # accumulators above.
        "clv_fit": {
            "sum_w_raw":     0.0,   # Σ w
            "sum_w_clv":     0.0,
            "sum_w_err_raw": 0.0,   # Σ w·(raw - y)²
            "sum_w_err_clv": 0.0,
            "n_raw":         0,
            "n_clv":         0,
            "weight":        CLV_OBSERVATION_WEIGHT,
        },
    }


def _coerce_cell(v) -> list[float]:
    """Pad legacy 3-element cells out to the current 5-element layout so
    state written by an earlier deploy still loads cleanly."""
    cell = [float(x) for x in (v or [])]
    while len(cell) < 5:
        cell.append(0.0)
    return cell


def _normalize_state_bins(state: dict) -> None:
    """JSON keys are strings; coerce x_bin keys back to int after a load."""
    for level_key in ("global", "leagues", "props"):
        node = state.get(level_key) or {}
        if level_key == "global":
            state[level_key] = {int(k): _coerce_cell(v) for k, v in node.items()}
        else:
            state[level_key] = {
                outer: {int(k): _coerce_cell(v) for k, v in inner.items()}
                for outer, inner in node.items()
            }


def _load_state() -> dict:
    """Load incremental state from disk → Supabase fallback. Returns an
    empty state when nothing usable exists (a bootstrap will then run)."""
    raw: dict | None = None
    if os.path.exists(ISOTONIC_STATE_FILE):
        try:
            with open(ISOTONIC_STATE_FILE, "r") as f:
                raw = json.load(f)
        except Exception:
            raw = None

    if raw is None:
        try:
            from engine.persistence import load_state_from_supabase
            mirrored, _ = load_state_from_supabase("isotonic_state")
            if isinstance(mirrored, dict):
                raw = mirrored
        except Exception:
            raw = None

    if not isinstance(raw, dict) or raw.get("version") != 3:
        return _empty_state()

    state = _empty_state()
    state["fitted_at"] = raw.get("fitted_at")
    state["hwm_observatory"] = raw.get("hwm_observatory")
    state["hwm_legs"] = raw.get("hwm_legs")
    state["global"] = raw.get("global") or {}
    state["leagues"] = raw.get("leagues") or {}
    state["props"] = raw.get("props") or {}
    # Carry forward CLV-fit stats if present; otherwise the empty-state
    # defaults stand and the next refit will populate them from new rows.
    raw_clv = raw.get("clv_fit")
    if isinstance(raw_clv, dict):
        for k, default in state["clv_fit"].items():
            v = raw_clv.get(k, default)
            try:
                state["clv_fit"][k] = type(default)(v)
            except (TypeError, ValueError):
                state["clv_fit"][k] = default
    _normalize_state_bins(state)
    return state


def _save_state(state: dict) -> None:
    try:
        os.makedirs(os.path.dirname(ISOTONIC_STATE_FILE), exist_ok=True)
        with open(ISOTONIC_STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as exc:
        logger.warning("IsotonicCalibration: state write failed: %s", exc)
        return
    try:
        from engine.persistence import sync_state_to_supabase
        sync_state_to_supabase("isotonic_state", state)
    except Exception as exc:
        logger.warning("IsotonicCalibration: state mirror failed: %s", exc)


def _ingest_into_state(state: dict, league: str, prop: str, side: str,
                       x: float, y: float, w: float, *, is_outcome: bool = False) -> None:
    """Add one observation to all three hierarchy levels.

    The (league, prop) bucket is keyed by side too — over and under tend to
    be miscalibrated asymmetrically (books often shade one side for variance
    protection), and pooling them washes the signal out. `side` must be
    "over" or "under"; anything else (legacy "both", missing, etc.) is
    skipped at the prop level but still contributes to global and league.
    """
    if w <= 0 or not (0.0 < x < 1.0):
        return
    _accum_bin_set(state["global"], x, y, w, is_outcome=is_outcome)
    _accum(state["leagues"], league, x, y, w, is_outcome=is_outcome)
    if prop and side in ("over", "under"):
        _accum(state["props"], f"{league}|{prop}|{side}", x, y, w, is_outcome=is_outcome)


def _ingest_resolved_row(state: dict, league: str, prop: str, side: str,
                         true_prob: float, result: str,
                         closing_prob: float | None,
                         resolved_at: datetime | None,
                         now: datetime,
                         confidence_weight: float = 1.0) -> None:
    """Fold a single resolved row's signals into the accumulators.

    `resolved_at` (not game_start) drives the recency weight: the moment we
    learned the outcome is what matters for "how old is this evidence."
    `confidence_weight` is a multiplicative factor (default 1.0) derived
    from the market width — tight markets carry more weight per observation.

    Side effects on `state["clv_fit"]`: when both an outcome and a
    closing_prob are present we fold (raw - y)² and (closing - y)² into
    the running MSE estimators used by `_compute_clv_weight()`.
    """
    rec_w = _recency_weight(resolved_at, now) * float(confidence_weight or 1.0)
    if rec_w <= 0:
        return

    r = (result or "").lower()
    outcome_y: float | None = None
    if r in ("hit", "won", "win"):
        outcome_y = 1.0
    elif r in ("miss", "lost", "loss"):
        outcome_y = 0.0
    # push/dnp contribute no outcome signal but do contribute CLV below.

    if outcome_y is not None:
        _ingest_into_state(state, league, prop, side, true_prob, outcome_y, rec_w, is_outcome=True)

    cp: float | None = None
    if closing_prob is not None:
        try:
            cp_f = float(closing_prob)
        except (ValueError, TypeError):
            cp_f = None
        if cp_f is not None and 0.0 < cp_f < 1.0:
            cp = cp_f

    if cp is not None:
        clv_w = _current_clv_weight(state)
        _ingest_into_state(state, league, prop, side, true_prob, cp,
                           rec_w * clv_w, is_outcome=False)

    # Update the CLV-weight sufficient statistics. Only triples that have
    # *both* an outcome and a closing_prob contribute, so the two MSE
    # estimators are computed on the same row population.
    if outcome_y is not None and cp is not None:
        clv_stats = state.setdefault("clv_fit", {
            "sum_w_raw": 0.0, "sum_w_clv": 0.0,
            "sum_w_err_raw": 0.0, "sum_w_err_clv": 0.0,
            "n_raw": 0, "n_clv": 0,
            "weight": CLV_OBSERVATION_WEIGHT,
        })
        clv_stats["sum_w_raw"]     += rec_w
        clv_stats["sum_w_clv"]     += rec_w
        clv_stats["sum_w_err_raw"] += rec_w * (true_prob - outcome_y) ** 2
        clv_stats["sum_w_err_clv"] += rec_w * (cp - outcome_y) ** 2
        clv_stats["n_raw"]         += 1
        clv_stats["n_clv"]         += 1


def _current_clv_weight(state: dict) -> float:
    """Read the most recently fitted CLV weight off `state['clv_fit']`,
    falling back to the static default. We read here (rather than only
    refitting at the end of update) so the closing observations within
    a single tick use the latest available estimate."""
    clv = state.get("clv_fit") or {}
    try:
        w = float(clv.get("weight", CLV_OBSERVATION_WEIGHT))
    except (TypeError, ValueError):
        w = CLV_OBSERVATION_WEIGHT
    return max(CLV_WEIGHT_MIN, min(CLV_WEIGHT_MAX, w))


def _compute_clv_weight(state: dict) -> float:
    """Inverse-variance estimate of the optimal CLV observation weight.

    Intuition: the calibrator is averaging two noisy estimators of the
    true outcome — `raw_true_prob` and `closing_prob`. The optimal
    weight on the closing-prob signal (relative to outcome) is

        w_clv  =  Var(raw - y) / Var(closing - y)

    where Var here is the per-observation MSE around the binary
    outcome. If the closing line is a much better predictor than raw
    (low MSE), the ratio rises and CLV gets more weight; if closing
    barely improves on raw, the ratio collapses to ~1.

    Falls back to the running default until CLV_FIT_MIN_OBS triples
    have accumulated. Always clamped to [CLV_WEIGHT_MIN, CLV_WEIGHT_MAX]
    so a noisy bucket can't blow up the calibrator."""
    clv = state.get("clv_fit") or {}
    n = int(clv.get("n_raw", 0) or 0)
    if n < CLV_FIT_MIN_OBS:
        return CLV_OBSERVATION_WEIGHT
    sw_raw = float(clv.get("sum_w_raw", 0.0) or 0.0)
    sw_clv = float(clv.get("sum_w_clv", 0.0) or 0.0)
    se_raw = float(clv.get("sum_w_err_raw", 0.0) or 0.0)
    se_clv = float(clv.get("sum_w_err_clv", 0.0) or 0.0)
    if sw_raw <= 0 or sw_clv <= 0 or se_clv <= 0:
        return CLV_OBSERVATION_WEIGHT
    mse_raw = se_raw / sw_raw
    mse_clv = se_clv / sw_clv
    if mse_clv <= 0:
        return CLV_WEIGHT_MAX
    raw_ratio = mse_raw / mse_clv
    return max(CLV_WEIGHT_MIN, min(CLV_WEIGHT_MAX, raw_ratio))


def _pull_resolved_since(db, table: str, hwm: str | None,
                         select_cols: str) -> list[dict]:
    """Pull rows whose `resolved_at` exceeds the high-water-mark. When the
    column or HWM doesn't yet exist, falls back to a one-time bootstrap on
    the recency lookback window — so the very first run, and any run after
    the migration but before the next resolution wave, still produces a
    correct fit."""
    # Page through without any silent caps. PostgREST tops out at 1000 per
    # page by default; we walk pages until exhausted so the fit can never
    # lose new data to a row limit. A fresh query is built per page so the
    # builder's internal range header isn't carried across requests.
    page_size = 1000
    rows: list[dict] = []
    offset = 0
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=RECENCY_LOOKBACK_DAYS)).isoformat()
    while True:
        try:
            q = db.table(table).select(select_cols).order("resolved_at", desc=False)
            if hwm:
                q = q.gt("resolved_at", hwm)
            else:
                q = q.gte("resolved_at", cutoff_iso)
            res = q.range(offset, offset + page_size - 1).execute()
        except Exception as exc:
            logger.warning(
                "IsotonicCalibration: %s pull failed (resolved_at missing? run migration_005.sql): %s",
                table, exc,
            )
            return rows
        chunk = res.data or []
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size
    return rows


def _stats_to_curves(state: dict, fitted_at: datetime) -> dict:
    """Run PAV against the current accumulators and assemble the curves
    payload that downstream consumers (clv_checker, ev_calculator, the
    Observatory chart) load via `load_isotonic_calibration()`."""
    fitted_clv_w = _current_clv_weight(state)
    out: dict = {
        "version":   2,   # consumer-facing curves schema unchanged
        "fitted_at": fitted_at.isoformat(),
        "global":    _fit_level(_level_triples(state["global"])),
        "leagues":   {},
        "props":     {},
        "config": {
            "recency_half_life_days":  RECENCY_HALF_LIFE_DAYS,
            "shrinkage_kappa":         SHRINKAGE_KAPPA,
            "clv_observation_weight":          CLV_OBSERVATION_WEIGHT,
            "clv_observation_weight_dynamic":  round(fitted_clv_w, 5),
            "clv_fit_n_obs":                   int((state.get("clv_fit") or {}).get("n_raw", 0) or 0),
        },
    }
    for lg, by_bin in state["leagues"].items():
        fit = _fit_level(_level_triples(by_bin))
        if fit is not None:
            out["leagues"][lg] = fit
    for key, by_bin in state["props"].items():
        fit = _fit_level(_level_triples(by_bin))
        if fit is not None:
            out["props"][key] = fit
    return out


def update_isotonic_calibration() -> dict | None:
    """
    Incrementally refit hierarchical isotonic curves.

    Each call:
      1. Loads bucket-level sufficient statistics from prior runs.
      2. Applies exponential recency decay since the last fit.
      3. Pulls only the rows resolved since the last cursor and folds them
         into the accumulators.
      4. Runs PAV from the bucket aggregates and persists the curves.

    State and curves are both mirrored to Supabase so a Render redeploy
    (which wipes the local disk) doesn't reset the calibration.
    """
    db = get_db()
    if not db:
        return None

    now = datetime.now(timezone.utc)
    state = _load_state()

    # ── 1. Decay existing accumulators ──────────────────────────────────
    if state.get("fitted_at"):
        prev = _parse_dt(state["fitted_at"])
        if prev is not None:
            dt_days = max(0.0, (now - prev).total_seconds() / 86400.0)
            decay = math.exp(-dt_days * _LN2 / RECENCY_HALF_LIFE_DAYS)
            if decay < 1.0:
                _decay_bin_set(state["global"], decay)
                _decay_stats(state["leagues"], decay)
                _decay_stats(state["props"], decay)
                # Decay the CLV-fit MSE accumulators in lockstep so the
                # weight estimate reflects the same recency horizon as the
                # curves it feeds.
                clv_stats = state.get("clv_fit")
                if isinstance(clv_stats, dict):
                    for k in ("sum_w_raw", "sum_w_clv",
                              "sum_w_err_raw", "sum_w_err_clv"):
                        try:
                            clv_stats[k] = float(clv_stats.get(k, 0.0)) * decay
                        except (TypeError, ValueError):
                            clv_stats[k] = 0.0

    # ── 2. Cursor pulls ─────────────────────────────────────────────────
    # We select both `true_prob` (calibrated, kept for backward compat) and
    # `raw_true_prob` (the pre-calibration consensus, added in
    # migration_006). The refit prefers raw_true_prob — training on the
    # calibrated value created a feedback loop where the calibrator was
    # learning the mapping from its own previous output to outcomes
    # instead of from the raw consensus to outcomes. `market_width` is
    # used as a confidence weight on each observation: tight markets
    # contribute more, wide markets less.
    obs_cols  = "league, prop, side, true_prob, raw_true_prob, market_width, result, closing_prob, resolved_at"
    legs_cols = "league, prop, side, true_prob, raw_true_prob, result, closing_prob, resolved_at"
    obs_rows  = _pull_resolved_since(db, "market_observatory", state.get("hwm_observatory"), obs_cols)
    if not obs_rows:
        # Pre-migration_006 shim — fall back to the original column set.
        obs_rows = _pull_resolved_since(
            db, "market_observatory", state.get("hwm_observatory"),
            "league, prop, side, true_prob, result, closing_prob, resolved_at",
        )
    leg_rows  = _pull_resolved_since(db, "legs", state.get("hwm_legs"), legs_cols)
    if not leg_rows:
        leg_rows = _pull_resolved_since(
            db, "legs", state.get("hwm_legs"),
            "league, prop, side, true_prob, result, closing_prob, resolved_at",
        )

    # ── 3. Fold into accumulators ───────────────────────────────────────
    new_hwm_obs  = state.get("hwm_observatory")
    new_hwm_legs = state.get("hwm_legs")
    n_ingested = 0

    def _row_raw_prob(r) -> float | None:
        """Prefer raw_true_prob; fall back to true_prob for legacy rows."""
        for key in ("raw_true_prob", "true_prob"):
            v = r.get(key)
            if v is None:
                continue
            try:
                f = float(v)
            except (ValueError, TypeError):
                continue
            if 0.0 < f < 1.0:
                return f
        return None

    for r in obs_rows:
        league = r.get("league")
        if not league:
            continue
        tp = _row_raw_prob(r)
        if tp is None:
            continue
        ra = r.get("resolved_at")
        ra_dt = _parse_dt(ra)
        cw = _confidence_weight(r.get("market_width"))
        _ingest_resolved_row(state, league, r.get("prop") or "",
                             (r.get("side") or "").lower(), tp,
                             r.get("result") or "", r.get("closing_prob"),
                             ra_dt, now, confidence_weight=cw)
        n_ingested += 1
        if ra and (new_hwm_obs is None or ra > new_hwm_obs):
            new_hwm_obs = ra

    for r in leg_rows:
        league = r.get("league")
        if not league:
            continue
        tp = _row_raw_prob(r)
        if tp is None:
            continue
        ra = r.get("resolved_at")
        ra_dt = _parse_dt(ra)
        # `legs` rows don't carry market_width yet — neutral confidence (1.0)
        # until the bet logger starts populating it on the legs path too.
        _ingest_resolved_row(state, league, r.get("prop") or "",
                             (r.get("side") or "").lower(), tp,
                             r.get("result") or "", r.get("closing_prob"),
                             ra_dt, now, confidence_weight=1.0)
        n_ingested += 1
        if ra and (new_hwm_legs is None or ra > new_hwm_legs):
            new_hwm_legs = ra

    state["hwm_observatory"] = new_hwm_obs
    state["hwm_legs"] = new_hwm_legs
    state["fitted_at"] = now.isoformat()

    # Refit the dynamic CLV observation weight against the freshly-folded
    # MSE accumulators. Subsequent ticks will use this on the apply path
    # via _current_clv_weight().
    new_clv_w = _compute_clv_weight(state)
    if isinstance(state.get("clv_fit"), dict):
        state["clv_fit"]["weight"] = round(new_clv_w, 5)

    # If we never managed to ingest a single observation and we've never
    # built a state before, there's nothing meaningful to fit yet — let the
    # next tick try again rather than overwriting the curves with empties.
    has_prior_signal = any(state["global"].values())
    if n_ingested == 0 and not has_prior_signal:
        logger.info("IsotonicCalibration: no observations to fit yet.")
        return None

    # ── 4. Refit PAV and persist ────────────────────────────────────────
    out = _stats_to_curves(state, now)

    try:
        os.makedirs(os.path.dirname(ISOTONIC_FILE), exist_ok=True)
        with open(ISOTONIC_FILE, "w") as f:
            json.dump(out, f, indent=2)
    except Exception as exc:
        logger.error("IsotonicCalibration: curves write failed: %s", exc)
        return None

    try:
        from engine.persistence import sync_state_to_supabase
        sync_state_to_supabase("isotonic_calibration", out)
    except Exception as exc:
        logger.warning("IsotonicCalibration: Supabase mirror failed: %s", exc)

    _save_state(state)

    logger.info(
        "IsotonicCalibration: incremental refit — +%d new rows, %d leagues, "
        "%d (league,prop) buckets, global n_eff=%.1f, clv_w=%.3f (n=%d)",
        n_ingested, len(out["leagues"]), len(out["props"]),
        out["global"]["n_eff"] if out["global"] else 0.0,
        out["config"].get("clv_observation_weight_dynamic", CLV_OBSERVATION_WEIGHT),
        out["config"].get("clv_fit_n_obs", 0),
    )
    return out


# ---------------------------------------------------------------------------
# Heatmap export (diagnostic: per-(league, prop) actual hit rate by 5%
# expected-probability buckets)
# ---------------------------------------------------------------------------

# 5% wide buckets covering the bet-relevant range.
HEATMAP_BUCKETS: list[tuple[float, float]] = [
    (round(lo / 100, 2), round((lo + 5) / 100, 2)) for lo in range(30, 100, 5)
]

# Minimum recency-weighted outcome n_eff for a (league, prop, side) row to
# qualify for the heatmap. Set low so something useful surfaces while data
# accumulates — buckets within a row still need MIN_BUCKET_N_EFF to render
# individually, so per-cell noise is already controlled. The row threshold
# only gates whether we show the row at all.
HEATMAP_MIN_ROW_N_EFF = 5.0

# How far back the DB-direct fallback reaches when state is empty or still
# in the legacy 2-key format. Matches the calibrator's recency lookback.
HEATMAP_FALLBACK_LOOKBACK_DAYS = RECENCY_LOOKBACK_DAYS


def _heatmap_from_state(state: dict) -> list[dict]:
    """Render heatmap rows directly from the incremental sufficient
    statistics. Only emits 3-key (league|prop|side) entries; legacy 2-key
    entries are skipped because they conflate over/under."""
    rows: list[dict] = []
    for bucket_key, by_bin in (state.get("props") or {}).items():
        parts = bucket_key.split("|")
        if len(parts) != 3:
            continue
        league, prop, side = parts
        if side not in ("over", "under"):
            continue

        cells: list[dict | None] = []
        total_n_eff = 0.0
        total_hits_w = 0.0
        total_expected_w = 0.0
        for lo, hi in HEATMAP_BUCKETS:
            bin_lo = int(round(lo * _BIN_COUNT))
            bin_hi = int(round(hi * _BIN_COUNT))
            sum_w = 0.0
            sum_hits_w = 0.0
            for b, cell in by_bin.items():
                if bin_lo <= b < bin_hi:
                    sum_w += cell[3]
                    sum_hits_w += cell[4]
            if sum_w >= MIN_BUCKET_N_EFF:
                cells.append({
                    "actual":   round(sum_hits_w / sum_w, 4),
                    "expected": round((lo + hi) / 2, 4),
                    "n_eff":    round(sum_w, 2),
                })
                total_n_eff += sum_w
                total_hits_w += sum_hits_w
                total_expected_w += ((lo + hi) / 2) * sum_w
            else:
                cells.append(None)

        if total_n_eff < HEATMAP_MIN_ROW_N_EFF:
            continue
        rows.append({
            "league":   league,
            "prop":     prop,
            "side":     side,
            "n_eff":    round(total_n_eff, 2),
            "actual":   round(total_hits_w / total_n_eff, 4),
            "expected": round(total_expected_w / total_n_eff, 4),
            "cells":    cells,
        })
    return rows


def _heatmap_from_db() -> list[dict]:
    """DB-direct fallback used when the incremental state is empty or only
    contains legacy 2-key entries (e.g., right after the per-side split was
    introduced, before any new outcomes have resolved into the 3-key state).

    Pulls resolved outcomes from `legs` and `market_observatory` within the
    recency lookback window, applies the same exponential decay used by the
    calibrator, and aggregates by (league, prop, side, expected-bucket).
    Returns a list shaped identically to `_heatmap_from_state` so callers
    are agnostic to the source.
    """
    db = get_db()
    if not db:
        return []

    now = datetime.now(timezone.utc)
    cutoff_iso = (now - timedelta(days=HEATMAP_FALLBACK_LOOKBACK_DAYS)).isoformat()

    # {(league, prop, side): {bucket_idx: [sum_w, sum_hits_w]}}
    aggs: dict[tuple[str, str, str], dict[int, list[float]]] = {}

    def _bucket_idx(prob: float) -> int | None:
        for i, (lo, hi) in enumerate(HEATMAP_BUCKETS):
            if lo <= prob < hi:
                return i
        return None

    def _ingest_rows(table: str) -> None:
        page_size = 1000
        offset = 0
        # Try the migration_006 column first; fall back to the legacy column
        # set if raw_true_prob doesn't exist on the deployed schema.
        select_cols_v2 = "league, prop, side, true_prob, raw_true_prob, result, resolved_at"
        select_cols_v1 = "league, prop, side, true_prob, result, resolved_at"
        cols = select_cols_v2
        while True:
            try:
                q = (db.table(table)
                       .select(cols)
                       .gte("resolved_at", cutoff_iso)
                       .order("resolved_at", desc=False)
                       .range(offset, offset + page_size - 1))
                res = q.execute()
            except Exception as exc:
                if cols == select_cols_v2:
                    cols = select_cols_v1
                    continue
                logger.warning(
                    "Heatmap fallback: %s pull failed (resolved_at missing?): %s",
                    table, exc,
                )
                return
            chunk = res.data or []
            for r in chunk:
                league = (r.get("league") or "").strip()
                prop = (r.get("prop") or "").strip()
                side = (r.get("side") or "").lower()
                if not league or not prop or side not in ("over", "under"):
                    continue
                # Prefer raw_true_prob; fall back to true_prob for legacy rows.
                tp = None
                for key in ("raw_true_prob", "true_prob"):
                    v = r.get(key)
                    if v is None:
                        continue
                    try:
                        tp = float(v)
                    except (ValueError, TypeError):
                        tp = None
                    if tp is not None:
                        break
                if tp is None:
                    continue
                if not (0.0 < tp < 1.0):
                    continue
                idx = _bucket_idx(tp)
                if idx is None:
                    continue
                result = (r.get("result") or "").lower()
                if result in ("hit", "won", "win"):
                    y = 1.0
                elif result in ("miss", "lost", "loss"):
                    y = 0.0
                else:
                    # push/dnp/pending: no outcome signal for the heatmap
                    continue
                w = _recency_weight(_parse_dt(r.get("resolved_at")), now)
                if w <= 0:
                    continue
                key = (league, prop, side)
                cells = aggs.setdefault(key, {})
                cell = cells.setdefault(idx, [0.0, 0.0])
                cell[0] += w
                cell[1] += y * w
            if len(chunk) < page_size:
                break
            offset += page_size

    _ingest_rows("legs")
    _ingest_rows("market_observatory")

    rows: list[dict] = []
    for (league, prop, side), per_bucket in aggs.items():
        cells: list[dict | None] = []
        total_n_eff = 0.0
        total_hits_w = 0.0
        total_expected_w = 0.0
        for i, (lo, hi) in enumerate(HEATMAP_BUCKETS):
            cell = per_bucket.get(i)
            if cell and cell[0] >= MIN_BUCKET_N_EFF:
                sum_w, sum_hits_w = cell
                cells.append({
                    "actual":   round(sum_hits_w / sum_w, 4),
                    "expected": round((lo + hi) / 2, 4),
                    "n_eff":    round(sum_w, 2),
                })
                total_n_eff += sum_w
                total_hits_w += sum_hits_w
                total_expected_w += ((lo + hi) / 2) * sum_w
            else:
                cells.append(None)
        if total_n_eff < HEATMAP_MIN_ROW_N_EFF:
            continue
        rows.append({
            "league":   league,
            "prop":     prop,
            "side":     side,
            "n_eff":    round(total_n_eff, 2),
            "actual":   round(total_hits_w / total_n_eff, 4),
            "expected": round(total_expected_w / total_n_eff, 4),
            "cells":    cells,
        })
    return rows


def export_heatmap() -> dict:
    """Build the per-(league, prop, side) × expected-probability heatmap.

    Primary path uses the incremental sufficient-statistics state so the
    cell values match the recency weighting the calibrator itself sees.
    When the state has no qualifying 3-key entries — which happens
    immediately after the legacy 2-key state is in place but before any
    new outcomes have resolved into per-side buckets — we fall back to a
    DB-direct aggregation over the same recency window. This keeps the
    panel populated rather than silently empty during state transitions
    or fresh deploys.
    """
    state = _load_state()
    rows = _heatmap_from_state(state)
    source = "state"

    if not rows:
        try:
            rows = _heatmap_from_db()
            if rows:
                source = "db-fallback"
        except Exception as exc:
            logger.warning("Heatmap DB fallback failed: %s", exc)

    rows.sort(key=lambda r: (r["league"], -r["n_eff"]))

    return {
        "buckets": [
            {"lo": lo, "hi": hi, "label": f"{int(lo*100)}-{int(hi*100)}%"}
            for lo, hi in HEATMAP_BUCKETS
        ],
        "rows": rows,
        "fitted_at": state.get("fitted_at"),
        "source": source,
    }


# ---------------------------------------------------------------------------
# Load / apply
# ---------------------------------------------------------------------------

def load_isotonic_calibration() -> dict:
    """
    Returns the fitted hierarchy, with curves normalized into tuples for the
    interpolator. Reads the local JSON file first; falls back to the
    Supabase-mirrored copy when the disk is empty (ephemeral-filesystem
    deploys). Returns an empty (no-op) shape if neither is available or
    the version is from a prior schema.
    """
    raw = None
    if os.path.exists(ISOTONIC_FILE):
        try:
            with open(ISOTONIC_FILE, "r") as f:
                raw = json.load(f)
        except Exception:
            raw = None

    if raw is None:
        try:
            from engine.persistence import load_state_from_supabase
            mirrored, _ = load_state_from_supabase("isotonic_calibration")
            if isinstance(mirrored, dict) and mirrored.get("version") == 2:
                # Hydrate the local file so subsequent reads skip the round trip.
                try:
                    os.makedirs(os.path.dirname(ISOTONIC_FILE), exist_ok=True)
                    with open(ISOTONIC_FILE, "w") as f:
                        json.dump(mirrored, f, indent=2)
                except Exception:
                    pass
                raw = mirrored
        except Exception as exc:
            logger.debug("IsotonicCalibration: Supabase fallback unavailable: %s", exc)

    if raw is None:
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


def calibrate(curves: dict, league: str | None, prop: str | None,
              side: str | None, raw_prob: float) -> float:
    """
    Apply hierarchical isotonic calibration with Bayesian shrinkage.

    Apply-path hierarchy: global → (league) → (league, prop, side).
    Each child level shrinks toward its parent's calibrated value with
    α = n_eff / (n_eff + κ). A thin (NBA, Pts+Rebs+Asts, over) bucket
    therefore borrows from "all NBA overs" first and from the global
    curve second, instead of skipping the league signal entirely.

    The (league, prop, side) bucket is keyed by side because
    miscalibration tends to be side-asymmetric — books often shade one
    side for variance protection. The (league) parent is NOT
    side-keyed: aggregating over/under at the league level is a
    smoothing aid, not a directional signal, and side-keying the parent
    would halve its sample size for no benefit at this tier.

    Symmetric output: when empirical hit rate exceeds raw at high
    n_eff, the result is allowed to exceed raw_prob. The Bayesian
    shrinkage (κ=SHRINKAGE_KAPPA) is the only guardrail; variance
    control belongs in stake sizing (quarter-Kelly), not the point
    estimate.
    """
    if not curves:
        return raw_prob

    global_level = curves.get("global")
    if not global_level or not global_level.get("curve"):
        return raw_prob
    q_global = _interp(global_level["curve"], raw_prob)

    # Tier 1: league posterior shrinks toward global. Provides a
    # league-wide bias correction that the prop bucket can lean on
    # before falling through to the global curve.
    league_level = curves.get("leagues", {}).get(league) if league else None
    q_league = _shrink(q_global, league_level, raw_prob)

    # Tier 2: (league, prop, side) bucket shrinks toward the league
    # posterior. Side-asymmetric calibration lives here.
    prop_level = None
    side_norm = (side or "").lower()
    if league and prop and side_norm in ("over", "under"):
        prop_level = curves.get("props", {}).get(f"{league}|{prop}|{side_norm}")
    return _shrink(q_league, prop_level, raw_prob)
