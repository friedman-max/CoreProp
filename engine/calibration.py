"""
Calibration metrics for evaluating probability estimation accuracy.

Implements:
- **Brier Score**: Mean squared error of predicted probabilities vs outcomes.
- **Log-Loss** (Cross-Entropy Loss): Penalises overconfident mispredictions
  asymmetrically — a 95% forecast that fails is punished far more than a 60%
  forecast that fails.

These metrics are computed against resolved backtest data to continuously
audit the predictive validity of the devigging and consensus algorithms.
"""
from __future__ import annotations

import math
import logging
from typing import Optional
from engine.database import get_user_db

logger = logging.getLogger(__name__)

# Clamp probabilities away from 0 and 1 to avoid log(0) in log-loss
_EPS = 1e-7

# DEPRECATED (Phase 1A audit C5): this gate sorted legs by slip_id — which
# is RANDOM HEX, not chronological — and discarded everything lexically
# before this id, dropping ~39% of CLV-tracked legs from the analytics
# average essentially at random by hex prefix. A leg from yesterday with
# id "0010B35C" was dropped; one from months ago with "9F..." was kept.
# The average CLV shown to users was therefore computed on a biased 61%
# subset. Replaced by `CLV_START_DATE` (a real chronological cutoff, None
# = all history) applied against the slip TIMESTAMP. Kept as a module
# attribute only so any external import doesn't break.
START_SLIP_ID = "5D3D2A96"

# Chronological CLV cutoff. None = include every leg that has a tracked
# closing line. Set to an ISO date string (e.g. "2026-04-01") to exclude
# the earliest period if its closing captures are known-unreliable. Unlike
# the old hex gate this is applied against the slip's actual timestamp, so
# it does what "start from date X" was always meant to do.
CLV_START_DATE: Optional[str] = None

# A clv_pct whose magnitude is below this is treated as a STALE capture
# (closing_prob == bet-time prob to 4 decimals) rather than a genuine
# 0% CLV. ~18% of historical legs are exactly 0.0 because the closing
# capture never re-fired after log time. We still report them (coverage)
# but separate "moved" CLV (real signal) from "stale" so the average
# isn't silently biased toward zero by frozen captures.
CLV_STALE_EPS = 1e-6


def _load_resolved_rows(user_jwt: str) -> list[dict]:
    """
    Read resolved rows from the Supabase database.
    """
    db = get_user_db(user_jwt)
    if not db:
        return []

    try:
        # Project only the columns we use; `select("*")` was pulling every
        # leg's worth of CLV/odds/timestamp baggage into RAM.
        cols = "result, true_prob, player, prop, side, league, slip_id"
        # Paginate — an unbounded select silently caps at 1000 rows, which
        # would drop resolved legs (and their slips' outcomes) from the
        # calibration/hit-rate metrics once a user exceeds ~1000 resolved legs.
        resolved_filter = ["won", "win", "hit", "1", "lost", "loss", "miss", "0"]
        _data: list = []
        _page_size = 1000
        _offset = 0
        while True:
            _page = (
                db.table("legs").select(cols).in_("result", resolved_filter)
                  .order("slip_id", desc=False)
                  .range(_offset, _offset + _page_size - 1)
                  .execute()
                  .data
            ) or []
            _data.extend(_page)
            if len(_page) < _page_size:
                break
            _offset += _page_size
        from engine.constants import is_excluded_league
        rows = []
        for r in _data:
            # Skip legacy legs whose league has since been removed.
            # Otherwise their resolved outcomes silently feed
            # the per-league calibration display.
            if is_excluded_league(r.get("league")):
                continue
            outcome = 1 if str(r.get("result")).lower() in ("won", "win", "hit", "1") else 0
            try:
                true_prob = float(r.get("true_prob", 0))
            except (ValueError, TypeError):
                continue
            if true_prob <= 0 or true_prob >= 1:
                continue
            rows.append({
                "true_prob": true_prob,
                "outcome":   outcome,
                "player":    r.get("player", ""),
                "prop":      r.get("prop", ""),
                "side":      r.get("side", ""),
                "league":    r.get("league", ""),
                "slip_id":   r.get("slip_id", ""),
            })
        return rows
    except Exception as e:
        logger.warning("Calibration: Supabase load failed: %s", e)
        return []


def _load_clv_rows(user_jwt: str) -> list[dict]:
    """Read every leg that has a tracked closing line.

    Phase 1A audit C5: this no longer applies the broken START_SLIP_ID
    hex-sort gate (which dropped ~39% of CLV legs at random by hex prefix).
    A leg without a genuine closing capture simply has clv_pct = NULL and
    self-excludes via the null check below, so the gate was both redundant
    and harmful. The optional CLV_START_DATE chronological cutoff is applied
    in evaluate_analytics (which has the slip timestamps); this lower-level
    loader returns all tracked rows.
    """
    db = get_user_db(user_jwt)
    if not db:
        return []

    try:
        # Paginate — an unbounded select silently caps at 1000 rows.
        rows = []
        _page_size = 1000
        _offset = 0
        while True:
            _page = (
                db.table("legs").select("closing_prob, clv_pct, slip_id")
                  .order("slip_id", desc=False)
                  .range(_offset, _offset + _page_size - 1)
                  .execute()
                  .data
            ) or []
            for r in _page:
                if r.get("closing_prob") is not None and r.get("clv_pct") is not None:
                    rows.append({"closing_prob": r["closing_prob"], "clv_pct": r["clv_pct"]})
            if len(_page) < _page_size:
                break
            _offset += _page_size
        return rows
    except Exception as e:
        logger.warning("Calibration: Supabase CLV load failed: %s", e)
        return []


def _summarize_clv(clv_rows: list[dict]) -> dict:
    """Compute the CLV summary used by the Analytics tab.

    Splits tracked legs into 'moved' (genuine CLV signal, |clv| > eps) and
    'stale' (closing == bet-time prob, capture never re-fired). Reports both
    the all-in average (what users intuitively expect) and the moved-only
    average (the honest signal), plus +CLV rate.
    """
    n = len(clv_rows)
    if n == 0:
        return {
            "n_clv_tracked":   0,
            "n_clv_moved":     0,
            "n_clv_stale":     0,
            "clv_plus_rate":   None,
            "avg_clv_pct":     None,
            "avg_clv_pct_moved": None,
        }
    vals = [float(r["clv_pct"]) for r in clv_rows]
    moved = [v for v in vals if abs(v) > CLV_STALE_EPS]
    n_stale = n - len(moved)
    n_plus = sum(1 for v in vals if v > CLV_STALE_EPS)
    return {
        "n_clv_tracked":     n,
        "n_clv_moved":       len(moved),
        "n_clv_stale":       n_stale,
        # +CLV rate is over MOVED legs — a stale 0.0 is neither a win nor a
        # loss against the close, so counting it dilutes the rate.
        "clv_plus_rate":     (n_plus / len(moved)) if moved else None,
        "avg_clv_pct":       sum(vals) / n,
        "avg_clv_pct_moved": (sum(moved) / len(moved)) if moved else None,
    }


def brier_score(rows: list[dict]) -> Optional[float]:
    """
    Brier Score = (1/N) × Σ(f_t - o_t)²

    Range: [0, 1].  Lower is better.
    - 0.0 = perfect calibration
    - 0.25 = random coin-flip baseline
    """
    if not rows:
        return None
    n = len(rows)
    total = sum((r["true_prob"] - r["outcome"]) ** 2 for r in rows)
    return total / n


def log_loss(rows: list[dict]) -> Optional[float]:
    """
    Log-Loss = -(1/N) × Σ[o_t × ln(f_t) + (1-o_t) × ln(1-f_t)]

    Lower is better.  Aggressively penalises overconfident mispredictions.
    - A 95% forecast that fails gets a massive penalty.
    - A 55% forecast that fails gets a moderate penalty.
    """
    if not rows:
        return None
    n = len(rows)
    total = 0.0
    for r in rows:
        p = max(_EPS, min(1 - _EPS, r["true_prob"]))
        o = r["outcome"]
        total += o * math.log(p) + (1 - o) * math.log(1 - p)
    return -total / n


def evaluate_calibration(user_jwt: str, _rows: Optional[list] = None, _clv_rows: Optional[list] = None) -> dict:
    """
    Compute calibration metrics from resolved backtest data.

    Returns a dict with:
      - brier_score: float | None
      - log_loss: float | None
      - n_resolved: int
      - n_won: int
      - n_lost: int
      - hit_rate: float | None (raw accuracy)
      - avg_predicted_prob: float | None
      - calibration_buckets: list of {bucket, predicted_avg, actual_avg, count}

    `_rows` is an optimization: when a caller already loaded resolved rows
    it can pass them in here to avoid a second round-trip to Supabase.
    """
    rows = _rows if _rows is not None else _load_resolved_rows(user_jwt)
    n = len(rows)

    if n == 0:
        return {
            "brier_score": None,
            "log_loss": None,
            "n_resolved": 0,
            "n_won": 0,
            "n_lost": 0,
            "hit_rate": None,
            "avg_predicted_prob": None,
            "calibration_buckets": [],
        }

    n_won = sum(1 for r in rows if r["outcome"] == 1)
    n_lost = n - n_won

    bs = brier_score(rows)
    ll = log_loss(rows)
    hit_rate = n_won / n if n > 0 else None
    avg_pred = sum(r["true_prob"] for r in rows) / n

    # Build calibration buckets (2% wide ranges: 30-32, 32-34, ..., 98-100)
    buckets = []
    for bucket_start in range(30, 100, 2):
        lo = bucket_start / 100.0
        hi = (bucket_start + 2) / 100.0
        bucket_rows = [r for r in rows if lo <= r["true_prob"] < hi]

        count = len(bucket_rows)
        if count > 0:
            pred_avg = sum(r["true_prob"] for r in bucket_rows) / count
            actual_avg = sum(r["outcome"] for r in bucket_rows) / count
        else:
            pred_avg = None
            actual_avg = None

        buckets.append({
            "bucket": f"{bucket_start}-{bucket_start+2}%",
            "predicted_avg": round(pred_avg, 4) if pred_avg is not None else None,
            "actual_avg": round(actual_avg, 4) if actual_avg is not None else None,
            "count": count,
        })

    # CLV — split moved (real signal) vs stale (frozen capture) per audit C5.
    clv_rows = _clv_rows if _clv_rows is not None else _load_clv_rows(user_jwt)
    clv = _summarize_clv(clv_rows)

    return {
        "brier_score": round(bs, 6) if bs is not None else None,
        "log_loss": round(ll, 6) if ll is not None else None,
        "n_resolved": n,
        "n_won": n_won,
        "n_lost": n_lost,
        "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
        "avg_predicted_prob": round(avg_pred, 4) if n > 0 else None,
        "calibration_buckets": buckets,
        "n_clv_tracked":     clv["n_clv_tracked"],
        "n_clv_moved":       clv["n_clv_moved"],
        "n_clv_stale":       clv["n_clv_stale"],
        "clv_plus_rate":     round(clv["clv_plus_rate"], 4) if clv["clv_plus_rate"] is not None else None,
        "avg_clv_pct":       round(clv["avg_clv_pct"], 4) if clv["avg_clv_pct"] is not None else None,
        "avg_clv_pct_moved": round(clv["avg_clv_pct_moved"], 4) if clv["avg_clv_pct_moved"] is not None else None,
    }


def evaluate_analytics(user_jwt: str) -> dict:
    """
    Analytics payload for the Analytics tab.

    Computes:
      - All of evaluate_calibration() (Brier, log-loss, hit-rate cards, CLV).
      - Cumulative P&L timeline: one point per resolved slip with the slip's
        timestamp, using PrizePicks payout tables and a 1-unit stake per
        slip. Positive values = profit.
      - resolved_legs / clv_legs: per-row data with timestamps so the
        frontend can recompute every metric for the chart's selected
        date range without another round trip.
    """
    from engine.constants import POWER_PAYOUTS, FLEX_PAYOUTS, slip_payout_factor

    # Fetch every leg; both the calibration metrics and the slip aggregation
    # read from this list. PostgREST silently caps an unbounded select at 1000
    # rows, so we MUST paginate — at 6 legs/slip that cap is only ~167 slips,
    # and once a user has more history than that the missing legs make whole
    # slips vanish from the P&L timeline (a won slip's legs never land in
    # legs_by_slip, so it's skipped) and from the hit-rate/calibration rows.
    # Order by slip_id so a given slip's legs are never split across the
    # truncation boundary of a partial final page.
    db = get_user_db(user_jwt)
    all_legs: list = []
    if db:
        _base_cols = "result, true_prob, player, prop, side, league, slip_id, leg_num, closing_prob, clv_pct"
        # odds_type (migration_018) may not exist yet on the deployed schema —
        # naming a missing column 400s and would blank Analytics. Try with it,
        # fall back without it (everything then scores as standard, factor 1.0).
        for cols in (_base_cols + ", odds_type", _base_cols):
            try:
                all_legs = []
                page_size = 1000
                offset = 0
                while True:
                    page = (
                        db.table("legs")
                          .select(cols)
                          .order("slip_id", desc=False)
                          .range(offset, offset + page_size - 1)
                          .execute()
                          .data
                    ) or []
                    all_legs.extend(page)
                    if len(page) < page_size:
                        break
                    offset += page_size
                break  # success
            except Exception as exc:
                msg = str(exc).lower()
                if "odds_type" in cols and "odds_type" in msg:
                    continue  # retry without the not-yet-migrated column
                logger.warning("Analytics: legs fetch failed: %s", exc)
                all_legs = []
                break

    # Dedup by (slip_id, leg_num). A slip corrupted by the old lost-response
    # double-insert bug (fixed in engine.backtest.insert_legs_idempotent) has
    # two rows per leg_num — which would otherwise double-count in the per-leg
    # Brier/hit-rate arrays AND inflate n_eff in the P&L payout math (a 6-leg
    # Power slip would look 12-leg and score as a guaranteed loss). Keep one
    # row per (slip_id, leg_num) so every metric counts each leg once.
    if all_legs:
        _seen_leg_keys = set()
        _deduped_legs = []
        for _l in all_legs:
            _ln = _l.get("leg_num")
            # Only dedup rows that carry a leg_num. The double-insert bug always
            # writes leg_num, so keying on it catches every duplicate; a row
            # without leg_num (legacy/edge) is never collapsed, so distinct
            # legs can't be lost if the column is absent.
            if _ln is not None:
                _k = (_l.get("slip_id"), _ln)
                if _k in _seen_leg_keys:
                    continue
                _seen_leg_keys.add(_k)
            _deduped_legs.append(_l)
        all_legs = _deduped_legs

    # Slip → timestamp map. Legs don't have their own timestamp; they
    # inherit it from the slip via slip_id. Pulled here once so both
    # the metric arrays and the P&L aggregation share one lookup.
    slip_ts: dict[str, str] = {}
    slip_meta: list = []
    if db:
        try:
            # Paginate — an unbounded select silently caps at 1000 rows, which
            # would drop the oldest slips from the P&L timeline entirely.
            _page_size = 1000
            _offset = 0
            while True:
                _page = (
                    db.table("slips")
                      .select("id, timestamp, slip_type")
                      .order("timestamp", desc=False)
                      .range(_offset, _offset + _page_size - 1)
                      .execute()
                      .data
                ) or []
                slip_meta.extend(_page)
                if len(_page) < _page_size:
                    break
                _offset += _page_size
            for s in slip_meta:
                if s.get("id") and s.get("timestamp"):
                    slip_ts[s["id"]] = s["timestamp"]
        except Exception as exc:
            logger.warning("Analytics: slip timestamp fetch failed: %s", exc)

    from engine.constants import is_excluded_league
    _RESOLVED = {"won", "win", "hit", "1", "lost", "loss", "miss", "0"}
    rows: list[dict] = []
    resolved_legs_with_ts: list[dict] = []
    for leg in all_legs:
        # Drop legs from excluded leagues so historical
        # rows can't surface in the per-league analytics breakdowns.
        if is_excluded_league(leg.get("league")):
            continue
        r = str(leg.get("result") or "").lower()
        if r not in _RESOLVED:
            continue
        try:
            true_prob = float(leg.get("true_prob") or 0)
        except (ValueError, TypeError):
            continue
        if true_prob <= 0 or true_prob >= 1:
            continue
        sid = leg.get("slip_id", "")
        outcome = 1 if r in ("won", "win", "hit", "1") else 0
        rows.append({
            "true_prob": true_prob,
            "outcome":   outcome,
            "player":    leg.get("player", ""),
            "prop":      leg.get("prop", ""),
            "side":      leg.get("side", ""),
            "league":    leg.get("league", ""),
            "slip_id":   sid,
        })
        # Compact per-leg array for the client-side recompute path. Only
        # the three fields the frontend actually needs (true_prob,
        # outcome, timestamp) so the response stays small.
        ts = slip_ts.get(sid)
        if ts:
            resolved_legs_with_ts.append({
                "true_prob": true_prob,
                "outcome":   outcome,
                "timestamp": ts,
            })

    # CLV rows — Phase 1A audit C5: no more START_SLIP_ID hex gate. Include
    # every leg with a tracked closing line; apply the optional chronological
    # CLV_START_DATE cutoff against the slip timestamp (the correct way to
    # "start from date X"). Also count coverage: how many of the bets we took
    # actually have a closing line (the "122 of 717" denominator OddsJam
    # shows), so the average is never silently computed on a partial subset.
    clv_rows: list[dict] = []
    clv_legs_with_ts: list[dict] = []
    n_logged_legs = 0          # all logged legs (the bets we took)
    n_logged_excluded_league = 0
    for leg in all_legs:
        if is_excluded_league(leg.get("league")):
            n_logged_excluded_league += 1
            continue
        sid = leg.get("slip_id", "")
        ts = slip_ts.get(sid)
        # Chronological cutoff (None = include everything).
        if CLV_START_DATE is not None and ts is not None and ts < CLV_START_DATE:
            continue
        n_logged_legs += 1
        cp = leg.get("closing_prob")
        cv = leg.get("clv_pct")
        if cp is not None and cv is not None:
            clv_rows.append({"closing_prob": cp, "clv_pct": cv})
            if ts:
                clv_legs_with_ts.append({
                    "closing_prob": cp,
                    "clv_pct":      cv,
                    "timestamp":    ts,
                })

    base = evaluate_calibration(user_jwt, _rows=rows, _clv_rows=clv_rows)
    # CLV coverage — fraction of the bets we took that have a tracked close.
    base["n_logged_legs"] = n_logged_legs
    base["clv_coverage_pct"] = (
        round(base["n_clv_tracked"] / n_logged_legs, 4)
        if n_logged_legs else None
    )

    # --- Cumulative P&L timeline --------------------------------------------
    pnl_timeline: list[dict] = []
    resolved_slips = won_slips = 0

    if db and slip_meta:
        try:
            legs_by_slip: dict[str, list] = {}
            for l in all_legs:
                legs_by_slip.setdefault(l["slip_id"], []).append(l)

            cum_pnl = 0.0
            for s in slip_meta:
                sid = s["id"]
                legs = legs_by_slip.get(sid, [])
                if not legs:
                    continue
                results = [str(l.get("result", "pending")).lower() for l in legs]
                completed = all(r in ("hit", "miss", "push", "dnp", "won", "win", "lost", "loss") for r in results)
                if not completed:
                    continue

                effective = [r for r in results if r not in ("push", "dnp")]
                n_eff = len(effective)
                hits_eff = sum(1 for r in effective if r in ("hit", "won", "win"))

                slip_type = (s.get("slip_type") or "").lower()
                if n_eff < 2:
                    payout = 1.0 if (n_eff == 0 or (n_eff == 1 and hits_eff == 1)) else 0.0
                elif slip_type == "power":
                    payout = POWER_PAYOUTS.get(n_eff, 0) if hits_eff == n_eff else 0
                else:
                    if n_eff == 2:
                        payout = POWER_PAYOUTS.get(2, 0) if hits_eff == 2 else 0
                    else:
                        payout = FLEX_PAYOUTS.get(n_eff, {}).get(hits_eff, 0)

                # Scale a winning payout by the goblin/demon factor from the
                # legs' odds_type (1.0 for an all-standard slip). Keeps realized
                # P&L consistent with the EV the slip was logged at.
                if payout:
                    payout = float(payout) * slip_payout_factor(
                        [l.get("odds_type") or "standard" for l in legs]
                    )

                pnl = float(payout) - 1.0  # 1-unit stake per slip
                cum_pnl += pnl
                resolved_slips += 1
                if payout > 1.0 or (hits_eff == n_eff and n_eff > 0):
                    won_slips += 1

                pnl_timeline.append({
                    "slip_id":   sid,
                    "timestamp": s.get("timestamp"),
                    "pnl":       round(pnl, 4),
                    "cum_pnl":   round(cum_pnl, 4),
                })
        except Exception as exc:
            logger.warning("Analytics: slip aggregation failed: %s", exc)

    roi = None
    if resolved_slips > 0 and pnl_timeline:
        roi = round(pnl_timeline[-1]["cum_pnl"] / resolved_slips, 4)

    base.update({
        "pnl_timeline":   pnl_timeline,
        "resolved_slips": resolved_slips,
        "won_slips":      won_slips,
        "roi_per_slip":   roi,
        # Per-leg arrays (with slip-inherited timestamps) so the
        # frontend can recompute Brier / log-loss / hit-rate / CLV
        # for whatever date range the chart is showing without
        # another network round trip.
        "resolved_legs":  resolved_legs_with_ts,
        "clv_legs":       clv_legs_with_ts,
    })
    return base
