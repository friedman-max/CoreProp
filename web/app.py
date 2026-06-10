"""
FastAPI backend with APScheduler auto-refresh.

Endpoints:
  GET  /api/bets        - All current +EV bets (sorted by ind_ev_pct desc)
  GET  /api/status      - Scrape status, last/next refresh time
  POST /api/slip        - Calculate slip EV for selected bet IDs
  GET  /api/config      - Current runtime config
  POST /api/config      - Update config (interval, min_ev, leagues)
"""
import gc
import hashlib
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Optional

# Aggressive GC thresholds — scrape cycles create lots of short-lived objects.
# Lower thresholds trigger collection earlier, keeping RSS closer to working-set
# size on the 512MB Render free tier.
gc.set_threshold(500, 5, 5)


def _intern(s):
    """Intern strings so repeated league/stat_type/side values share storage."""
    return sys.intern(s) if isinstance(s, str) else s

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.base import JobLookupError
from fastapi import FastAPI, HTTPException, Depends, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from engine.ev_calculator import reload_calibration
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import statistics
from web.auth import get_current_user, get_current_user_optional

import config as cfg
from engine.ev_calculator import BetResult, calculate_slip
from engine.matcher import match_props
from engine.backtest import BacktestLogger, make_bet_key
from engine.results_checker import ESPNResultsChecker
from engine.clv_checker import CLVTracker
from engine.persistence import sync_state_to_supabase, load_state_from_supabase
from engine.devig import (
    american_to_implied as _american_to_implied,
    devig_single_sided_scaled,
    prob_to_american as _prob_to_american,
    revigg_power,
)
from scrapers.fanduel import scrape_fanduel
from scrapers.prizepicks import scrape_prizepicks
from scrapers.draftkings import scrape_draftkings
from scrapers.pinnacle import scrape_pinnacle

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="CoreProp")

# Compress JSON responses >= 1KB. Typical line payloads are 100-500KB raw and
# compress 6-10x with gzip, dramatically reducing network time and response-
# buffer memory on the 512MB tier.
app.add_middleware(GZipMiddleware, minimum_size=1024)

# CORS: allow the CoreProp Chrome extension to call the pending-slip endpoints
# from app.prizepicks.com origin. Content scripts in MV3 inherit the page's
# origin and are subject to CORS, so we need explicit Access-Control headers.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://app.prizepicks.com", "https://prizepicks.com"],
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=False,
)

# Results checker / CLV singletons (stateless, run in background via service role)
_results_checker  = ESPNResultsChecker()
_clv_tracker      = CLVTracker()

# ---------------------------------------------------------------------------
# In-memory state — moved to web/state.py during the Phase-1 router split.
# Re-imported here so the bulk of this file's existing code can keep
# referencing `_state`, `_lock`, etc. without a wholesale rename.
# ---------------------------------------------------------------------------
from web.state import (
    _lock,
    _state,
    _payload_cache,
    _payload_lock,
    _pending_slips,
    _pending_slips_lock,
    _PENDING_SLIP_TTL_SEC,
    _analytics_cache,
    _analytics_cache_lock,
    _ANALYTICS_TTL_SEC,
    invalidate_analytics_cache as _invalidate_analytics_cache,
)


def _memory_snapshot() -> dict:
    """Best-effort memory diagnostics for the 512 MB tier. Safe to call from
    request handlers and post-pipeline logging hooks. Catches everything so a
    stat-collection failure can't bring the process down."""
    snap: dict = {}

    # ── RSS / peak / threads via psutil ───────────────────────────────────
    try:
        import psutil
        proc = psutil.Process()
        mi = proc.memory_info()
        snap["rss_mb"] = round(mi.rss / 1_048_576, 1)
        snap["vms_mb"] = round(mi.vms / 1_048_576, 1)
        try:
            full = proc.memory_full_info()
            if hasattr(full, "uss"):
                snap["uss_mb"] = round(full.uss / 1_048_576, 1)
        except Exception:
            pass
        # Peak RSS — Linux only. /proc/<pid>/status VmHWM is in kB.
        try:
            with open(f"/proc/{proc.pid}/status") as f:
                for line in f:
                    if line.startswith("VmHWM:"):
                        snap["peak_rss_mb"] = round(int(line.split()[1]) / 1024, 1)
                        break
        except Exception:
            pass
        try:
            snap["num_fds"] = proc.num_fds() if hasattr(proc, "num_fds") else None
        except Exception:
            pass
        snap["thread_count"] = proc.num_threads()
    except Exception as exc:
        snap["error"] = f"psutil unavailable: {exc}"
        snap["thread_count"] = threading.active_count()

    # ── Threads breakdown ─────────────────────────────────────────────────
    try:
        names = [t.name for t in threading.enumerate()]
        snap["threads"] = {"active": len(names), "names": names[:30]}
    except Exception:
        pass

    # ── Pre-serialized payload cache ──────────────────────────────────────
    try:
        with _payload_lock:
            sizes = {}
            total = 0
            for k, v in _payload_cache.items():
                if isinstance(v, (bytes, bytearray)):
                    sz = len(v)
                    sizes[k] = round(sz / 1024, 1)
                    total += sz
                else:
                    sizes[k] = None
            snap["payload_cache_kb"] = sizes
            snap["payload_cache_total_kb"] = round(total / 1024, 1)
    except Exception:
        pass

    # ── Per-user analytics cache ──────────────────────────────────────────
    try:
        with _analytics_cache_lock:
            snap["analytics_cache_users"] = len(_analytics_cache)
    except Exception:
        pass

    # ── _state list lengths (raw books / matches retained in memory) ──────
    try:
        with _lock:
            snap["state_lists"] = {
                "bets":      len(_state.get("bets") or []),
                "matches":   len(_state.get("matches") or []),
                "pp_lines":  len(_state.get("pp_lines") or []),
                "fd_lines":  len(_state.get("fd_lines") or []),
                "dk_lines":  len(_state.get("dk_lines") or []),
                "pin_lines": len(_state.get("pin_lines") or []),
            }
    except Exception:
        pass

    # ── Pandas DataFrame inventory (count + total memory_usage) ───────────
    try:
        import pandas as pd
        dfs = [o for o in gc.get_objects() if isinstance(o, pd.DataFrame)]
        total_bytes = 0
        for df in dfs:
            try:
                total_bytes += int(df.memory_usage(deep=True).sum())
            except Exception:
                pass
        snap["pandas_dataframes"] = {
            "count": len(dfs),
            "total_mb": round(total_bytes / 1_048_576, 2),
        }
    except Exception:
        pass

    # ── GC stats for context ──────────────────────────────────────────────
    try:
        snap["gc_objects"] = len(gc.get_objects())
        snap["gc_counts"] = list(gc.get_count())
    except Exception:
        pass

    return snap


def _build_etag(last_refresh) -> str:
    """Stable ETag derived from the last_refresh timestamp. Weak (W/) because
    gzip compression can mutate bytes at the transport layer."""
    seed = last_refresh.isoformat() if isinstance(last_refresh, datetime) else str(last_refresh)
    h = hashlib.md5(seed.encode("utf-8")).hexdigest()[:16]
    return f'W/"{h}"'


def _json_bytes(obj) -> bytes:
    """Compact JSON encoding. separators=(',',':') trims ~15% off list payloads."""
    return json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8")


def _serialize_one(key: str, data, last_iso: str, interval_min=None) -> bytes:
    """Encode a single dataset's response body as JSON bytes."""
    if key == "bets":
        payload = {"bets": data, "total": len(data), "is_scraping": False, "last_refresh": last_iso}
    elif key == "matches":
        payload = {"matches": data, "total": len(data), "is_scraping": False, "last_refresh": last_iso}
    elif key == "core":
        payload = {"bets": data, "total": len(data), "is_scraping": False, "last_refresh": last_iso, "interval_min": interval_min}
    else:  # pp_lines / fd_lines / dk_lines / pin_lines — line datasets
        payload = {"lines": data, "total": len(data), "is_scraping": False, "last_refresh": last_iso}
    return _json_bytes(payload)


def _refresh_payload_cache(
    serialized_bets,
    serialized_matches,
    serialized_pp,
    serialized_fd,
    serialized_dk,
    serialized_pin,
    last_refresh,
    interval_min,
):
    """Build all pre-serialized response bytes in one pass. Called from the
    pipeline with the just-built state so we never dumps() per-request."""
    last_iso = last_refresh.isoformat() if isinstance(last_refresh, datetime) else last_refresh
    etag = _build_etag(last_refresh)

    # Serialize outside the lock — encoding is CPU-bound and doesn't touch
    # shared state. Holding _payload_lock across six dumps() on the 512MB tier
    # was serializing concurrent GETs against the cache swap.
    bets_bytes    = _serialize_one("bets", serialized_bets, last_iso)
    matches_bytes = _serialize_one("matches", serialized_matches, last_iso)
    pp_bytes      = _serialize_one("pp_lines", serialized_pp, last_iso)
    fd_bytes      = _serialize_one("fd_lines", serialized_fd, last_iso)
    dk_bytes      = _serialize_one("dk_lines", serialized_dk, last_iso)
    pin_bytes     = _serialize_one("pin_lines", serialized_pin, last_iso)
    core_bytes    = _serialize_one("core", serialized_bets, last_iso, interval_min)

    with _payload_lock:
        _payload_cache["bets"]      = bets_bytes
        _payload_cache["matches"]   = matches_bytes
        _payload_cache["pp_lines"]  = pp_bytes
        _payload_cache["fd_lines"]  = fd_bytes
        _payload_cache["dk_lines"]  = dk_bytes
        _payload_cache["pin_lines"] = pin_bytes
        _payload_cache["core"]      = core_bytes
        _payload_cache["etag"]      = etag


def _update_one_payload(key: str, data, last_refresh):
    """Serialize and swap a single cache entry. Used by per-book refresh
    endpoints so they don't re-encode the other five datasets (which was
    allocating ~3–5MB transient buffers per unrelated book refresh)."""
    last_iso = last_refresh.isoformat() if isinstance(last_refresh, datetime) else last_refresh
    body = _serialize_one(key, data, last_iso)
    etag = _build_etag(last_refresh)
    with _payload_lock:
        _payload_cache[key] = body
        _payload_cache["etag"] = etag


def _rebuild_cache_from_state():
    """Rebuild the payload cache from whatever's currently in _state. Used
    at startup after seeding from Supabase."""
    with _lock:
        _refresh_payload_cache(
            _state["bets"],
            _state["matches"],
            _state["pp_lines"],
            _state["fd_lines"],
            _state["dk_lines"],
            _state["pin_lines"],
            _state["last_refresh"],
            _state["interval_min"],
        )


def _cached_response(key: str, request: Request) -> Response:
    """Serve a pre-serialized JSON payload with ETag/304 short-circuit. If the
    cache is empty (pre-first-scrape), returns an empty-shape JSON response."""
    with _payload_lock:
        body = _payload_cache.get(key)
        etag = _payload_cache.get("etag")

    if body is None:
        # Cold start — minimal empty shape, but still fast and cacheable.
        shape = {"bets": []} if key in ("bets", "core") else {"lines": [] if key != "matches" else None, "matches": []}
        empty = {
            "total": 0,
            "is_scraping": _state["is_scraping"],
            "last_refresh": _last_refresh_iso(),
        }
        if key == "core":
            empty["bets"] = []
            empty["interval_min"] = _state["interval_min"]
        elif key == "bets":
            empty["bets"] = []
        elif key == "matches":
            empty["matches"] = []
        else:
            empty["lines"] = []
        return JSONResponse(empty)

    if etag and request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers={"ETag": etag})

    headers = {"Cache-Control": "no-cache"}
    if etag:
        headers["ETag"] = etag
    return Response(content=body, media_type="application/json", headers=headers)


scheduler = BackgroundScheduler()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_book_overround(props: list) -> float:
    """
    Compute the median overround (margin) for a book from its own both-sided
    props.  Returns the overround as a fraction (e.g. 0.07 for 7%).
    Falls back to 0.07 if there aren't enough both-sided lines.
    """
    _DEFAULT_OVERROUND = 0.07
    margins = []
    for p in props:
        if getattr(p, "both_sided", False) and p.over_odds is not None and p.under_odds is not None:
            impl_o = _american_to_implied(p.over_odds)
            impl_u = _american_to_implied(p.under_odds)
            margin = impl_o + impl_u - 1.0
            if 0 < margin < 0.25:  # sanity: ignore bad data
                margins.append(margin)
    if len(margins) >= 3:
        return statistics.median(margins)
    return _DEFAULT_OVERROUND


def _display_odds(book, side: str, book_overround: float = 0.07):
    """
    Return the American odds to display for a book on a given side.

    If the book only has the opposite side, derive the missing side using:
      1. Devig the available side → true probability
      2. Complement → true probability for missing side
      3. Re-vig BOTH sides using the inverse Power Method with the book's
         own median overround, producing realistic vigged odds that honor
         the favorite-longshot bias.

    Returns None if the book is None or has no odds at all.
    """
    if book is None:
        return None
    direct = book.over_odds if side == "over" else book.under_odds
    if direct is not None:
        return direct
    # Derive from opposite side using the book's own overround
    opposite = book.under_odds if side == "over" else book.over_odds
    if opposite is not None:
        # Step 1-2: devig available side, complement for missing side
        available_true = devig_single_sided_scaled(opposite)
        missing_true = 1.0 - available_true
        if missing_true <= 0 or missing_true >= 1:
            return None
        # Step 3: re-vig both sides with the book's observed overround
        if side == "over":
            vigged_over, vigged_under = revigg_power(missing_true, available_true, book_overround)
            return round(_prob_to_american(vigged_over), 2)
        else:
            vigged_over, vigged_under = revigg_power(available_true, missing_true, book_overround)
            return round(_prob_to_american(vigged_under), 2)
    return None


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

# Auto-logging has been removed to support multi-tenancy.
# Users must manually log slips from the UI.



def run_pipeline():
    """Public pipeline entry. Delegates to _run_pipeline_body so that all
    large per-cycle locals (tens of thousands of dicts) go out of scope and
    can be reclaimed before we force a GC."""
    with _lock:
        if _state["is_scraping"]:
            logger.info("Scrape already in progress, skipping.")
            return
        _state["is_scraping"] = True
    try:
        _run_pipeline_body()
    finally:
        with _lock:
            _state["is_scraping"] = False
        # Large locals from _run_pipeline_body are now unreachable. Force a
        # full GC so the memory is released to the OS (critical on 512MB tier).
        gc.collect()


def _run_pipeline_body():
    errors = {}
    try:
        with _lock:
            leagues = dict(_state["active_leagues"])

        import concurrent.futures
        
        logger.info("Pipeline: kicking off scrapers concurrently...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_pp = executor.submit(scrape_prizepicks, active_leagues=leagues)
            future_fd = executor.submit(scrape_fanduel, active_leagues=leagues)
            future_dk = executor.submit(scrape_draftkings, active_leagues=leagues)
            future_pin = executor.submit(scrape_pinnacle, active_leagues=leagues)

            try:
                pp_lines = future_pp.result()
                if len(pp_lines) == 0: errors["prizepicks"] = "Empty response"
            except Exception as e:
                logger.error(f"PrizePicks scraper failed: {e}")
                pp_lines, errors["prizepicks"] = [], "Exception"

            try:
                fd_props = future_fd.result()
                if len(fd_props) == 0: errors["fanduel"] = "Empty response"
            except Exception as e:
                logger.error(f"FanDuel scraper failed: {e}")
                fd_props, errors["fanduel"] = [], "Exception"

            try:
                dk_props = future_dk.result()
                if len(dk_props) == 0: errors["draftkings"] = "Empty response"
            except Exception as e:
                logger.error(f"DraftKings scraper failed: {e}")
                dk_props, errors["draftkings"] = [], "Exception"

            try:
                pin_props = future_pin.result()
                if len(pin_props) == 0: errors["pinnacle"] = "Empty response"
            except Exception as e:
                logger.error(f"Pinnacle scraper failed: {e}")
                pin_props, errors["pinnacle"] = [], "Exception"

        # If every source failed, skip the state update entirely so the UI
        # keeps the last good snapshot (avoids clearing screens on a bad cycle).
        if not pp_lines and not fd_props and not dk_props and not pin_props:
            logger.warning("Pipeline: all scrapers returned 0 — preserving previous state.")
            with _lock:
                _state["scrape_errors"] = errors
            return

        # PrizePicks is the source of truth for what we match against. If PP
        # alone fails (transient rate-limit / connection rejection), we must
        # NOT wipe the previous match graph — there's nothing to match against
        # without PP. Preserve the last good state and try again next cycle.
        if not pp_lines:
            logger.warning(
                "Pipeline: PrizePicks returned 0 lines (other books OK) — "
                "preserving previous state to avoid wiping the match graph."
            )
            with _lock:
                _state["scrape_errors"] = errors
            return

        serialized_pp = []
        for l in pp_lines:
            lg = _intern(l.league)
            st = _intern(l.stat_type)
            if l.side == "both":
                common = {
                    "league": lg,
                    "player_name": l.player_name,
                    "stat_type": st,
                    "line_score": l.line_score,
                    "start_time": l.start_time,
                }
                serialized_pp.append({**common, "side": "over"})
                serialized_pp.append({**common, "side": "under"})
            else:
                serialized_pp.append({
                    "league": lg,
                    "player_name": l.player_name,
                    "stat_type": st,
                    "line_score": l.line_score,
                    "side": _intern(l.side),
                    "start_time": l.start_time,
                })

        from engine.devig import devig_power, devig_single_sided, prob_to_american

        def _serialize_book(props):
            out = []
            _OVER = sys.intern("over")
            _UNDER = sys.intern("under")
            for p in props:
                true_over, true_under = None, None
                if p.both_sided and p.over_odds is not None and p.under_odds is not None:
                    true_over, true_under = devig_power(p.over_odds, p.under_odds)
                else:
                    if p.over_odds is not None:
                        true_over = devig_single_sided(p.over_odds)
                    if p.under_odds is not None:
                        true_under = devig_single_sided(p.under_odds)
                lg = _intern(p.league)
                st = _intern(p.prop_type)
                start = getattr(p, "start_time", None)
                if p.over_odds is not None:
                    out.append({
                        "league": lg, "player_name": p.player_name, "stat_type": st,
                        "line_score": p.line, "side": _OVER, "line_odds": p.over_odds,
                        "true_odds": prob_to_american(true_over) if true_over else None,
                        "start_time": start,
                    })
                if p.under_odds is not None:
                    out.append({
                        "league": lg, "player_name": p.player_name, "stat_type": st,
                        "line_score": p.line, "side": _UNDER, "line_odds": p.under_odds,
                        "true_odds": prob_to_american(true_under) if true_under else None,
                        "start_time": start,
                    })
            return out

        serialized_fd = _serialize_book(fd_props)
        serialized_dk = _serialize_book(dk_props)
        serialized_pin = _serialize_book(pin_props)

        logger.info("Pipeline: matching %d PP lines vs %d FD, %d DK, %d Pinnacle props...", len(pp_lines), len(fd_props), len(dk_props), len(pin_props))
        matches = match_props(fd_props, dk_props, pp_lines, pin_props)

        # Compute each book's median overround from its own both-sided lines
        fd_margin = _compute_book_overround(fd_props)
        dk_margin = _compute_book_overround(dk_props)
        pin_margin = _compute_book_overround(pin_props)
        logger.info("Book overrounds: FD=%.2f%% DK=%.2f%% PIN=%.2f%%",
                     fd_margin * 100, dk_margin * 100, pin_margin * 100)

        from engine.devig import prob_to_american
        from engine.ev_calculator import BetResult
        from engine.consensus import compute_true_probability, books_from_match_for_side

        with _lock:
            min_ev = _state["min_ev_pct"]
        bets: list[BetResult] = []
        bet_book_odds: dict[str, dict] = {}  # bet_id -> {fd_odds, dk_odds, pin_odds}
        serialized_matches = []
        # Observatory feed. Every priced side with raw consensus prob >= 0.30
        # is logged regardless of EV filter. Using `bets` (the EV-filtered
        # list) as the feed biased the calibrator toward markets where the
        # consensus agreed with our model — a closed feedback loop that
        # systematically under-represented sides where the book was sharp.
        # Especially visible on low-volume leagues (NHL) where most matches
        # ended up below the EV bar and never reached the observatory.
        # The 0.30 floor is a sanity gate: extreme tails (<0.30 raw) get
        # logged on the complement side anyway since that side is >=0.70.
        # OBS_MIN_PROB lives in engine.observatory so the gate has a single
        # source of truth — `_observe` reads it via `should_log_observation`.
        from engine.observatory import (
            build_observation_entry as _build_obs,
            should_log_observation as _should_obs,
        )
        obs_entries: list[dict] = []

        def _devigged_book_probs(_m, _side: str) -> dict:
            """Snapshot each book's devigged probability for `_side`. Mirrors
            `_per_book_probs` defined further down the loop, but available
            outside the per-match closure so observatory rows can attach a
            books dict for every priced side, not just the +EV ones."""
            from engine.devig import devig_power as _dp, devig_single_sided_scaled as _dss
            out: dict[str, float] = {}
            for name, prefix in (("fanduel", "fd"), ("draftkings", "dk"), ("pinnacle", "pin")):
                bk = getattr(_m, f"{prefix}_{_side}_equiv", None)
                if bk is None:
                    continue
                prob = None
                is_exact = bk.line == _m.pp.line_score
                if is_exact and bk.both_sided and bk.over_odds is not None and bk.under_odds is not None:
                    t_o, t_u = _dp(bk.over_odds, bk.under_odds)
                    prob = t_o if _side == "over" else t_u
                elif _side == "over" and bk.over_odds is not None:
                    prob = _dss(bk.over_odds)
                elif _side == "under" and bk.under_odds is not None:
                    prob = _dss(bk.under_odds)
                if prob is not None and 0.0 < prob < 1.0:
                    out[name] = round(float(prob), 4)
            return out

        def _observe(_m, _side: str, raw_prob: float, mw):
            """Capture an observatory entry for the given side if the raw
            consensus prob clears the threshold. Threshold + entry shape
            are owned by engine.observatory so this closure stays thin —
            a single bug-fix surface for "what does a logged observation
            look like?"."""
            if not _should_obs(raw_prob):
                return
            entry = _build_obs(
                player_name=_m.pp.player_name,
                league=_m.pp.league,
                prop=_m.pp.stat_type,
                line=_m.pp.line_score,
                side=_side,
                raw_prob=raw_prob,
                market_width=mw,
                team=getattr(_m.pp, "team", "") or "",
                start_time=_m.pp.start_time,
                books_probs=_devigged_book_probs(_m, _side),
            )
            if entry is not None:
                obs_entries.append(entry)

        for m in matches:
            # The matcher already populates per-side equivalents. A match
            # is kept when any book has a usable line for at least one
            # side — exact match or, for whole-number PP lines, the
            # half-step alt that's mathematically equivalent under
            # push-on-tie semantics (over PP-3 == over book-3.5).
            any_over = m.fd_over_equiv or m.dk_over_equiv or m.pin_over_equiv
            any_under = m.fd_under_equiv or m.dk_under_equiv or m.pin_under_equiv
            if not any_over and not any_under:
                continue

            pp_side = getattr(m.pp, "side", "both")

            def _book_for_side(side: str, book_attr_prefix: str):
                return getattr(m, f"{book_attr_prefix}_{side}_equiv")

            def _line_for_side(side: str, book_attr_prefix: str):
                bk = _book_for_side(side, book_attr_prefix)
                return bk.line if bk is not None else None

            def _base_for_side(side: str) -> dict:
                # The displayed line is always PP's; per-book line fields
                # show the actual book line consulted for that side, which
                # makes half-step usage visible in the diagnostic UI.
                return {
                    "player_name": m.pp.player_name,
                    "league":      m.pp.league,
                    "stat_type":   m.pp.stat_type,
                    "pp_line":     m.pp.line_score,
                    "fd_line":     _line_for_side(side, "fd"),
                    "dk_line":     _line_for_side(side, "dk"),
                    "pin_line":    _line_for_side(side, "pin"),
                    "start_time":  m.pp.start_time,
                }

            def _per_book_probs(side: str) -> dict:
                """Snapshot each book's devigged probability for `side` so the
                sharpness fitter (engine/sharpness_calibration.py) can later
                compare each book's price to the eventual closing line.
                Returns e.g. {"fanduel": 0.62, "draftkings": 0.61}."""
                from engine.devig import devig_power as _dp, devig_single_sided_scaled as _dss
                out: dict[str, float] = {}
                for name, prefix in (("fanduel", "fd"), ("draftkings", "dk"), ("pinnacle", "pin")):
                    bk = _book_for_side(side, prefix)
                    if bk is None:
                        continue
                    prob = None
                    is_exact = bk.line == m.pp.line_score
                    if is_exact and bk.both_sided and bk.over_odds is not None and bk.under_odds is not None:
                        t_o, t_u = _dp(bk.over_odds, bk.under_odds)
                        prob = t_o if side == "over" else t_u
                    elif side == "over" and bk.over_odds is not None:
                        prob = _dss(bk.over_odds)
                    elif side == "under" and bk.under_odds is not None:
                        prob = _dss(bk.under_odds)
                    if prob is not None and 0.0 < prob < 1.0:
                        out[name] = round(float(prob), 4)
                return out

            def _book_display_odds(side: str, prefix: str, margin: float):
                bk = _book_for_side(side, prefix)
                if bk is None:
                    return None
                # On a half-step equivalent we only have one side's price;
                # _display_odds already devigs/re-vigs from the available
                # side, so this still produces a sensible display number.
                return _display_odds(bk, side, margin)

            def get_combined_true_odds(side):
                """Compute the bet-decision probability for one side.

                Returns (best_odds, final_true_prob, true_american_odds,
                market_width, sharp_missing).

                SHARP ANCHOR mode (config.USE_SHARP_ANCHOR): the decision
                probability is the devigged two-sided Pinnacle price and
                nothing else. When Pinnacle doesn't price the market,
                sharp_missing=True and the blend is used for DISPLAY only —
                ev_calculator marks such legs halted so the auto-backtest
                can never log them.

                Legacy mode: worst-case blended consensus (min across devig
                methods and books), later calibrated in BetResult."""
                match_books = books_from_match_for_side(m, side)
                if not match_books:
                    return None, None, None, None, True
                consensus_prob, worst_case_prob, meta = compute_true_probability(
                    match_books, side, league=m.pp.league, prop=m.pp.stat_type,
                )

                if consensus_prob is None:
                    return None, None, None, None, True

                sharp_missing = False
                if cfg.USE_SHARP_ANCHOR:
                    from engine.sharp_anchor import pinnacle_fair_from_books
                    pin_fair = pinnacle_fair_from_books(match_books, side)
                    if pin_fair is not None:
                        worst_case_prob = pin_fair
                    else:
                        sharp_missing = True

                # Find the best odds for display (includes derived complement odds)
                odds_list = [
                    o for o in [
                        _book_display_odds(side, "fd",  fd_margin),
                        _book_display_odds(side, "dk",  dk_margin),
                        _book_display_odds(side, "pin", pin_margin),
                    ] if o is not None
                ]
                best_odds = max(odds_list) if odds_list else None

                # Decision probability: Pinnacle fair in sharp mode (or the
                # worst-case blend for display when sharp_missing / legacy).
                final_true_prob = worst_case_prob
                market_width = meta.get("market_width") if isinstance(meta, dict) else None
                return (
                    best_odds,
                    final_true_prob,
                    prob_to_american(final_true_prob) if final_true_prob else None,
                    market_width,
                    sharp_missing,
                )

            def _first_book_for_side(side: str):
                """Pick the first book that prices the given side. Used
                only to fill BetResult.over_odds/under_odds/both_sided
                with sensible representative values."""
                for prefix in ("pin", "fd", "dk"):
                    bk = _book_for_side(side, prefix)
                    if bk is not None:
                        return bk
                return None

            # Process Over side
            if pp_side in ("both", "over") and any_over:
                best, prob, true, mw, sharp_miss = get_combined_true_odds("over")
                # Log to observatory whenever a probability was successfully
                # computed — even when `best` is None (no displayable odds
                # for the UI). Calibration cares about the consensus prob,
                # not about whether a book chose to display this side.
                _observe(m, "over", prob, mw)
                if best is not None:
                    base_over = _base_for_side("over")
                    fd_o  = _book_display_odds("over", "fd",  fd_margin)
                    dk_o  = _book_display_odds("over", "dk",  dk_margin)
                    pin_o = _book_display_odds("over", "pin", pin_margin)
                    serialized_matches.append({
                        **base_over,
                        "side": "over",
                        "best_odds": best,
                        "fd_odds":  fd_o,
                        "dk_odds":  dk_o,
                        "pin_odds": pin_o,
                        "true_odds": true,
                    })

                    # Also create +EV bet if applicable
                    first_bk = _first_book_for_side("over")
                    if prob is not None and first_bk:
                        bet_id = f"{m.pp.player_id}_{m.pp.stat_type}_over"
                        res = BetResult(
                            bet_id=bet_id,
                            player_name=m.pp.player_name,
                            league=m.pp.league,
                            prop_type=m.pp.stat_type,
                            pp_line=m.pp.line_score,
                            fd_line=base_over["fd_line"] or base_over["dk_line"] or base_over["pin_line"],
                            side="over",
                            true_prob=prob,
                            over_odds=first_bk.over_odds,
                            under_odds=first_bk.under_odds,
                            both_sided=first_bk.both_sided,
                            pp_player_id=m.pp.player_id,
                            market_width=mw,
                            team=getattr(m.pp, "team", "") or "",
                            sharp_missing=sharp_miss,
                        )
                        if res.individual_ev_pct >= min_ev:
                            bets.append(res)
                            bet_book_odds[bet_id] = {
                                "fd_odds":    fd_o,
                                "dk_odds":    dk_o,
                                "pin_odds":   pin_o,
                                "start_time": base_over.get("start_time", ""),
                                "books_probs": _per_book_probs("over"),
                            }

            # Process Under side
            if pp_side in ("both", "under") and any_under:
                best, prob, true, mw, sharp_miss = get_combined_true_odds("under")
                _observe(m, "under", prob, mw)
                if best is not None:
                    base_under = _base_for_side("under")
                    fd_u  = _book_display_odds("under", "fd",  fd_margin)
                    dk_u  = _book_display_odds("under", "dk",  dk_margin)
                    pin_u = _book_display_odds("under", "pin", pin_margin)
                    serialized_matches.append({
                        **base_under,
                        "side": "under",
                        "best_odds": best,
                        "fd_odds":  fd_u,
                        "dk_odds":  dk_u,
                        "pin_odds": pin_u,
                        "true_odds": true,
                    })

                    # Also create +EV bet if applicable
                    first_bk = _first_book_for_side("under")
                    if prob is not None and first_bk:
                        bet_id = f"{m.pp.player_id}_{m.pp.stat_type}_under"
                        res = BetResult(
                            bet_id=bet_id,
                            player_name=m.pp.player_name,
                            league=m.pp.league,
                            prop_type=m.pp.stat_type,
                            pp_line=m.pp.line_score,
                            fd_line=base_under["fd_line"] or base_under["dk_line"] or base_under["pin_line"],
                            side="under",
                            true_prob=prob,
                            over_odds=first_bk.over_odds,
                            under_odds=first_bk.under_odds,
                            both_sided=first_bk.both_sided,
                            pp_player_id=m.pp.player_id,
                            market_width=mw,
                            team=getattr(m.pp, "team", "") or "",
                            sharp_missing=sharp_miss,
                        )
                        if res.individual_ev_pct >= min_ev:
                            bets.append(res)
                            bet_book_odds[bet_id] = {
                                "fd_odds":    fd_u,
                                "dk_odds":    dk_u,
                                "pin_odds":   pin_u,
                                "start_time": base_under.get("start_time", ""),
                                "books_probs": _per_book_probs("under"),
                            }

        # Per-league observability: log how many priced sides each league
        # contributed to obs_entries this cycle, and what fraction were
        # >= the observatory threshold vs below it. Surfaces the
        # difference between "the scrapers found no NHL games" (counts
        # are 0) and "we matched NHL props but they all came back below
        # 0.30" (matched > 0, obs == 0). Without this, the only way to
        # diagnose a missing-league report was to scan the entire scrape
        # log by hand.
        try:
            from collections import Counter
            obs_counts = Counter()
            for e in obs_entries:
                lg = (e.get("league") or "UNKNOWN").upper()
                obs_counts[lg] += 1
            match_counts = Counter((getattr(m.pp, "league", "") or "UNKNOWN").upper() for m in matches)
            league_summary = ", ".join(
                f"{lg}: matched={match_counts.get(lg, 0)} obs={obs_counts.get(lg, 0)}"
                for lg in sorted(set(match_counts) | set(obs_counts))
            )
            logger.info("Pipeline per-league: %s", league_summary)
        except Exception as exc:
            logger.warning("Pipeline per-league summary failed: %s", exc)

        # Deduplicate bets based on bet_id, keeping the one with highest EV
        unique_bets = {}
        for b in bets:
            if b.bet_id not in unique_bets or b.individual_ev_pct > unique_bets[b.bet_id].individual_ev_pct:
                unique_bets[b.bet_id] = b
        bets = list(unique_bets.values())

        # Sort by individual EV% descending
        bets.sort(key=lambda b: b.individual_ev_pct, reverse=True)

        # Per-book devigged prob attachment moved upstream: the observatory
        # now feeds from `obs_entries` (built directly in the match loop),
        # which already carries each side's `books_probs` dict. The
        # sidecar map this scope used to maintain is no longer read.

        serialized_bets = []
        for b in bets:
            d = b.to_dict()
            extras = bet_book_odds.get(b.bet_id, {})
            d["fd_odds_book"] = extras.get("fd_odds")
            d["dk_odds_book"] = extras.get("dk_odds")
            d["pin_odds_book"] = extras.get("pin_odds")
            start_time = extras.get("start_time", "")
            d["start_time"] = start_time
            # Precompute the backtest-dedup key so the client can join
            # in_backtest flags locally against /api/backtest/keys, removing
            # per-request copies and a Supabase round-trip off the hot path.
            player_key, time_key = make_bet_key(d.get("player_name", ""), start_time)
            d["bet_key"] = f"{player_key}|{time_key}"
            serialized_bets.append(d)

        # Free intermediate mapping before we hand off to Supabase + background
        # workers. On a full scrape this dict contains one entry per bet and
        # holds references the GC would otherwise retain until cycle end.
        bet_book_odds.clear()
        del bet_book_odds

        # Precompute what CLV actually needs from `matches` so we can drop the
        # heavy MatchedProp list (and its transitive references to every raw
        # FanDuel/DK/Pinnacle prop) before launching the background threads.
        try:
            clv_current_probs = _clv_tracker._build_current_probs(matches)
        except Exception as clv_pre_exc:
            logger.warning("CLV precompute error: %s", clv_pre_exc)
            clv_current_probs = {}

        with _lock:
            _state["bets"]         = serialized_bets
            _state["bet_map"]      = {b.bet_id: b for b in bets}
            _state["matches"]      = serialized_matches
            _state["pp_lines"]     = serialized_pp
            _state["fd_lines"]     = serialized_fd
            _state["dk_lines"]     = serialized_dk
            _state["pin_lines"]    = serialized_pin
            _state["last_refresh"] = datetime.now()
            _state["next_refresh"] = datetime.now() + timedelta(minutes=_state["interval_min"])
            _state["scrape_errors"] = errors
            interval_min_snapshot = _state["interval_min"]
            last_refresh_snapshot = _state["last_refresh"]

        # Rebuild the pre-serialized payload cache so subsequent GETs avoid
        # per-request json.dumps. Done outside the state lock because json
        # encoding is CPU-bound and doesn't touch _state.
        _refresh_payload_cache(
            serialized_bets,
            serialized_matches,
            serialized_pp,
            serialized_fd,
            serialized_dk,
            serialized_pin,
            last_refresh_snapshot,
            interval_min_snapshot,
        )
        # Books bytes are now in the payload cache — the Python lists in
        # _state are redundant (never read elsewhere). Releasing them here
        # halves the resident memory footprint of book datasets, which on a
        # full scrape is ~3-5MB per book.
        with _lock:
            _state["fd_lines"]  = []
            _state["dk_lines"]  = []
            _state["pin_lines"] = []
        logger.info("Pipeline complete: %d +EV bets found.", len(bets))

        # Log memory trajectory so we can see whether each refresh leaks or
        # plateaus on the 512 MB tier. Cheap (psutil only).
        try:
            _msnap = _memory_snapshot()
            _pdf = _msnap.get("pandas_dataframes") or {}
            logger.info(
                "Memory post-pipeline: rss=%.1fMB peak=%.1fMB threads=%s cache=%.1fKB pandas=%dx%.2fMB gc_objs=%s",
                _msnap.get("rss_mb", 0.0),
                _msnap.get("peak_rss_mb", 0.0),
                _msnap.get("thread_count", "?"),
                _msnap.get("payload_cache_total_kb", 0.0),
                _pdf.get("count", 0),
                _pdf.get("total_mb", 0.0),
                _msnap.get("gc_objects", "?"),
            )
        except Exception as _mexc:
            logger.debug("Memory snapshot post-pipeline failed: %s", _mexc)

        # ── Supabase Sync: Persist the new state for instant load on restart ──
        # sync_state_to_supabase auto-gzips payloads over 256KB, so the books
        # (fd/dk/pin_lines, 2-5MB raw) compress to ~300-500KB — well under any
        # PostgREST request cap.
        def _sync_all():
            sync_state_to_supabase("bets", serialized_bets)
            sync_state_to_supabase("matches", serialized_matches)
            sync_state_to_supabase("pp_lines", serialized_pp)
            sync_state_to_supabase("fd_lines", serialized_fd)
            sync_state_to_supabase("dk_lines", serialized_dk)
            sync_state_to_supabase("pin_lines", serialized_pin)
            if _state["last_refresh"]:
                sync_state_to_supabase("last_refresh", _state["last_refresh"].isoformat())

        if cfg.DISABLE_PERSISTENCE:
            logger.info(
                "persistence      DISABLE_PERSISTENCE=true — skipping sync_state_to_supabase "
                "(comparison-server mode; primary server's seed stays untouched)"
            )
        else:
            threading.Thread(target=_sync_all, daemon=True).start()

        # Auto-backtest logging for opted-in users — each user's chosen
        # slip_type / n_legs (from the Slip Builder) drives what gets logged.
        def _auto_log_bg(bets=serialized_bets):
            try:
                from engine.database import get_db
                db = get_db()
                if not db:
                    return
                # LOCAL_AUTO_BACKTEST_USER_IDS override: comma-separated user
                # IDs that get auto-backtested by *this* server regardless
                # of their auto_backtest flag. Used for localhost-exclusive
                # logger mode — pair it with flipping auto_backtest=false on
                # those users in Supabase so the production Render instance
                # stops logging slips for them. Other users keep their
                # normal flag-driven behavior (Render still serves them).
                _override_csv = os.getenv("LOCAL_AUTO_BACKTEST_USER_IDS", "").strip()
                _override_uids = [u.strip() for u in _override_csv.split(",") if u.strip()]
                if _override_uids:
                    # Union: flag-on users PLUS the override list. Dedup
                    # via dict keyed on user_id below.
                    flag_res = (
                        db.table("user_config")
                        .select("user_id, auto_slip_type, auto_slip_legs, auto_slip_min_prob")
                        .eq("auto_backtest", True)
                        .execute()
                    )
                    over_res = (
                        db.table("user_config")
                        .select("user_id, auto_slip_type, auto_slip_legs, auto_slip_min_prob")
                        .in_("user_id", _override_uids)
                        .execute()
                    )
                    merged: dict = {}
                    for r in (flag_res.data or []) + (over_res.data or []):
                        uid = r.get("user_id")
                        if uid:
                            merged[uid] = r
                    users_res = type("R", (), {"data": list(merged.values())})()
                    logger.info(
                        "auto_backtest    LOCAL_AUTO_BACKTEST_USER_IDS active — "
                        "running for %d users (%d flag-on + %d override = %d unique)",
                        len(merged), len(flag_res.data or []),
                        len(over_res.data or []), len(merged),
                    )
                else:
                    users_res = (
                        db.table("user_config")
                        .select("user_id, auto_slip_type, auto_slip_legs, auto_slip_min_prob")
                        .eq("auto_backtest", True)
                        .execute()
                    )
                from engine.constants import BREAK_EVEN
                for row in (users_res.data or []):
                    uid = row.get("user_id")
                    if not uid:
                        continue

                    slip_type = row.get("auto_slip_type") or "Power"
                    try:
                        n_legs = int(row.get("auto_slip_legs") or 6)
                    except (TypeError, ValueError):
                        n_legs = 6
                    n_legs = max(2, min(6, n_legs))

                    raw_min = row.get("auto_slip_min_prob")
                    try:
                        user_min = float(raw_min) if raw_min is not None else None
                    except (TypeError, ValueError):
                        user_min = None

                    if cfg.USE_SHARP_ANCHOR:
                        # SHARP ANCHOR mode — the validated rule, nothing else:
                        #   * Default shape: 3-leg Power. Feasibility-
                        #     constrained backtest: 44 placeable slips
                        #     (1.52/day, +25.0u, +56.8%/slip) vs 29 for
                        #     4-Flex and 12 for strict 6-Flex — 3 legs
                        #     co-available is the easiest fill, so 3-Power
                        #     converts sharp legs into the most placed bets.
                        #     Users who explicitly configured a validated
                        #     Flex shape (4/5/6) keep their choice.
                        #   * Floor = max(user's min, SHARP_MIN_PROB=0.56).
                        #   * Legs without a two-sided Pinnacle price never
                        #     enter the pool (sharp_missing -> halted).
                        # Tier routing and shade filters belong to the old
                        # model and are intentionally bypassed.
                        from engine.sharp_anchor import (
                            slip_shape_allowed, DEFAULT_SLIP_TYPE, DEFAULT_SLIP_SIZE,
                        )
                        if not slip_shape_allowed(slip_type, n_legs):
                            slip_type = DEFAULT_SLIP_TYPE
                            n_legs = DEFAULT_SLIP_SIZE
                        min_prob = max(user_min or 0.0, cfg.SHARP_MIN_PROB)
                        pool = [
                            b for b in bets
                            if float(b.get("true_prob") or 0.0) >= min_prob
                            and not b.get("sharp_missing")
                            and not b.get("calibration_halted")
                        ]
                        logger.info(
                            "auto_backtest    user=%s SHARP_ANCHOR pool=%d "
                            "(floor=%.3f, %s-%d)",
                            uid, len(pool), min_prob, slip_type, n_legs,
                        )
                        from engine.backtest import BacktestLogger
                        bl = BacktestLogger(user_id=uid, db_client=db)
                        # Flex keeps its thin-slate size fallback (6->5->4);
                        # Power-3 is already the minimum fill.
                        if slip_type.lower() == "flex":
                            for try_n in range(n_legs, 3, -1):
                                if bl.try_log_slip(pool[:40], slip_type=slip_type, n_legs=try_n):
                                    break
                        else:
                            bl.try_log_slip(pool[:40], slip_type=slip_type, n_legs=n_legs)
                        continue

                    # ── Legacy path (USE_SHARP_ANCHOR=false) ────────────────
                    # Per-leg min: tier-aware effective_min_prob respects
                    # the user's explicit override; otherwise routes to the
                    # tier floor that's eligible for this (slip_type, n_legs).
                    # See engine/tier.py for the routing table. The override
                    # is still capped from below by the slip's break-even —
                    # `effective_min_prob` handles the floor.
                    from engine.tier import (
                        effective_min_prob, filter_legs_for_slip, tier_summary,
                    )
                    min_prob = effective_min_prob(user_min, slip_type, n_legs)

                    pool = [b for b in bets if float(b.get("true_prob") or 0.0) >= min_prob]

                    from engine.backtest import BacktestLogger
                    bl = BacktestLogger(user_id=uid, db_client=db)

                    # RWBC circuit-breaker filter — when USE_RWBC is on,
                    # legs whose calibration cell has w_cell < threshold
                    # are flagged with `calibration_halted=True` by
                    # engine/ev_calculator.py. Skip them before the tier
                    # filter so a halted leg can never sneak in via a
                    # Tier-A probability assignment.
                    n_before = len(pool)
                    pool = [b for b in pool if not b.get("calibration_halted")]
                    n_skipped = n_before - len(pool)
                    if n_skipped > 0:
                        logger.info(
                            "auto_backtest    user=%s skipped_due_to_circuit_breaker=%d remaining_pool=%d",
                            uid, n_skipped, len(pool),
                        )

                    # Phase 1A tier routing. Tier A enters any slip; Tier B
                    # only 5/6-pick; Tier C never auto-logs. Filter against
                    # the requested (slip_type, n_legs) so a 3-Power request
                    # only sees Tier A legs.
                    pre_tier = len(pool)
                    pool = filter_legs_for_slip(pool, slip_type, n_legs)

                    # Maybe Cool Fix C1: anti-public-side filter (OFF unless
                    # USE_SHADE_FILTER=true). Drops legs in (league, side,
                    # shade_bucket) cells flagged as historically anti-skill
                    # at high shade. Refit by Phase 3 strategy logger.
                    if cfg.USE_SHADE_FILTER:
                        from engine.shade_signal import is_anti_public
                        pre_shade = len(pool)
                        pool = [b for b in pool if not is_anti_public(b)]
                        n_dropped = pre_shade - len(pool)
                        if n_dropped > 0:
                            logger.info(
                                "auto_backtest    user=%s shade_filter "
                                "dropped=%d remaining=%d",
                                uid, n_dropped, len(pool),
                            )
                    if pre_tier != len(pool):
                        counts = tier_summary(pool)
                        logger.info(
                            "auto_backtest    user=%s tier_filter slip=%s-%d "
                            "kept=%d (A=%d B=%d C=%d REJECT=%d) dropped=%d",
                            uid, slip_type, n_legs, len(pool),
                            counts["A"], counts["B"], counts["C"], counts["REJECT"],
                            pre_tier - len(pool),
                        )

                    # Pass the top ~40 candidates from the filtered pool.
                    bl.try_log_slip(pool[:40], slip_type=slip_type, n_legs=n_legs)
            except Exception as e:
                logger.error("Auto-backtest background worker error: %s", e)

        if cfg.DISABLE_AUTO_BACKTEST:
            logger.info(
                "auto_backtest    DISABLE_AUTO_BACKTEST=true — skipping slip logging "
                "(this server is in display-only comparison mode)"
            )
        else:
            threading.Thread(target=_auto_log_bg, daemon=True).start()

        # ── Market Observatory: log lines for global calibration ──
        # Source: `obs_entries`, which is every priced side from the match
        # loop with raw consensus prob >= 0.30. This is deliberately upstream
        # of the EV filter and the bet-id dedup that shapes `serialized_bets`
        # — both of those bias the calibration sample toward markets the
        # current model already agrees with. Logging from `obs_entries`
        # restores full coverage across leagues (the NHL gap was a
        # downstream symptom of this filter) and gives the calibrator the
        # representative sample it needs.
        def _log_observatory_bg(entries=obs_entries):
            try:
                from engine.writer import writer as _writer
                # All writes from the pipeline observatory feed flow through
                # a single audited surface (see engine/writer.py). The
                # underlying client is still the service-role one, but the
                # surface is the seam where a future two-pod split can route
                # writes to a separate process without touching this file.
                obs_db = _writer("observatory.log")
                if obs_db is None:
                    return
                rows_to_upsert = []
                for e in entries:
                    raw_tp_val = e.get("raw_true_prob")
                    if raw_tp_val is None:
                        continue
                    raw_tp = float(raw_tp_val)
                    tp_val = e.get("true_prob")
                    tp = float(tp_val) if tp_val is not None else raw_tp
                    mw_val = e.get("market_width")
                    mw = float(mw_val) if mw_val is not None else None
                    player = e.get("player_name", "")
                    league = e.get("league", "")
                    prop   = e.get("prop_type", "")
                    line   = e.get("pp_line", "")
                    side   = e.get("side", "")
                    start  = e.get("start_time", "")
                    market_key = f"{player}|{league}|{prop}|{line}|{side}|{start}"
                    # Note: `result` is deliberately omitted. The DB default
                    # is 'pending' on insert; on conflict the migration_009
                    # trigger preserves the existing value. Including it
                    # here would reset already-graded rows to 'pending' on
                    # every re-scrape if the trigger isn't deployed yet.
                    row = {
                        "market_key":     market_key,
                        "player":         player,
                        "league":         league,
                        "prop":           prop,
                        "line":           float(line) if line != "" else 0,
                        "side":           side,
                        "true_prob":      round(tp, 4),
                        "raw_true_prob":  round(raw_tp, 4),
                        "game_start":     start if start else None,
                    }
                    if mw is not None:
                        row["market_width"] = round(mw, 4)
                    team_val = (e.get("team") or "").strip()
                    if team_val:
                        row["team"] = team_val
                    bp = e.get("books_probs")
                    if bp:
                        row["books"] = bp
                    rows_to_upsert.append(row)
                if rows_to_upsert:
                    # Try the full row first. If a column is missing on the
                    # deployed schema (pre-migration_003 lacks `books`,
                    # pre-migration_006 lacks `raw_true_prob` /
                    # `market_width`), strip it and retry — keeps logging
                    # working before the operator runs the new migration.
                    _OPTIONAL_COLS = ("books", "raw_true_prob", "market_width", "team")
                    def _strip(rows, col):
                        for r in rows:
                            r.pop(col, None)
                    # ignore_duplicates=False so the conflict path fires an
                    # UPDATE. The market_observatory_upsert_guard trigger
                    # (migration_009) preserves first_seen_at + resolution
                    # state and bumps last_seen_at to NOW(), so the sandbox
                    # can later reconstruct which legs were live at each
                    # historical scrape moment.
                    try:
                        obs_db.table("market_observatory").upsert(
                            rows_to_upsert,
                            on_conflict="market_key",
                            ignore_duplicates=False
                        ).execute()
                        logger.info("Observatory: logged %d observations", len(rows_to_upsert))
                    except Exception as upsert_exc:
                        last_exc = upsert_exc
                        succeeded = False
                        # Strip optional columns one at a time, retrying after each.
                        for col in _OPTIONAL_COLS:
                            if not any(col in r for r in rows_to_upsert):
                                continue
                            _strip(rows_to_upsert, col)
                            try:
                                obs_db.table("market_observatory").upsert(
                                    rows_to_upsert,
                                    on_conflict="market_key",
                                    ignore_duplicates=False
                                ).execute()
                                logger.info(
                                    "Observatory: logged %d observations (stripped '%s' — apply pending migration to enable)",
                                    len(rows_to_upsert), col,
                                )
                                succeeded = True
                                break
                            except Exception as retry_exc:
                                last_exc = retry_exc
                                continue
                        if not succeeded:
                            logger.error("Observatory logging error: %s", last_exc)
            except Exception as e:
                logger.error("Observatory logging error: %s", e)

        threading.Thread(target=_log_observatory_bg, daemon=True).start()

        # ── CLV Tracker: update closing lines for pending bets non-blocking ──
        # Pass the precomputed probs dict only — not the full matches list —
        # so matches can be freed as soon as this scope exits.
        #
        # Phase 1A audit PR-3a: in addition to the legacy legs-driven path
        # (which writes closing_prob only for the ~6.5k logged legs out of
        # 47k observatory rows), we now also run a standalone observatory
        # capture pass. That path writes closing_prob to ALL pending obs
        # rows within a 4h pre-game window, raising the calibrator's CLV
        # signal coverage from 4.7% toward the 60-80% range and unlocking
        # the dynamic CLV-weight estimator that's currently bound by
        # CLV_FIT_MIN_OBS=200.
        # Build the obs-shaped key dict from the same source — the
        # observatory join keys on (player, prop, side, line) without
        # game_start (because the obs table is the source of truth for
        # game_start), so we rekey here.
        def _obs_keyed_probs(probs: dict) -> dict:
            out: dict[tuple[str, str, str, float], float] = {}
            for (player, prop, side, line), v in probs.items():
                # Source dict uses (player_lower, prop_lower, side, line).
                # Observatory match expects the same shape — no rekey needed.
                out[(player, prop, side, float(line))] = float(v)
            return out

        def _update_clv_bg(current_probs=clv_current_probs):
            try:
                updated_legs = _clv_tracker.update_closing_lines_from_probs(current_probs)
                finalized = _clv_tracker.finalize_missed()
                updated_obs = _clv_tracker.update_observatory_closing_lines(
                    _obs_keyed_probs(current_probs),
                )
                if updated_legs or updated_obs or finalized:
                    logger.info(
                        "CLVTracker: legs=%d obs=%d finalized=%d (background)",
                        updated_legs, updated_obs, finalized,
                    )
            except Exception as clv_exc:
                logger.warning("CLVTracker background error: %s", clv_exc)

        threading.Thread(target=_update_clv_bg, daemon=True).start()

        # ── Results checker: back-fill any pending rows non-blocking ──
        def _check_results_bg():
            try:
                updated = _results_checker.check_pending_results()
                if updated:
                    logger.info("ResultsChecker: %d rows updated in background", updated)
            except Exception as rc_exc:
                logger.warning("ResultsChecker background error: %s", rc_exc)
            # Also resolve observatory rows (reuses ESPN cache from above)
            try:
                obs_updated = _results_checker.check_observatory_results()
                if obs_updated:
                    logger.info("Observatory: %d observations resolved in background", obs_updated)
            except Exception as obs_exc:
                logger.warning("Observatory resolution error: %s", obs_exc)

        threading.Thread(target=_check_results_bg, daemon=True).start()

    except Exception as e:
        logger.exception("Pipeline error: %s", e)
        errors["pipeline"] = str(e)
        with _lock:
            _state["scrape_errors"] = errors


def _reschedule(interval_min: int):
    """Reschedule the auto-refresh job with a new interval.

    Only touches the auto_refresh job; leaves other jobs (e.g. daily_calibration)
    intact so a user config change doesn't silently cancel the calibration cron.
    """
    try:
        scheduler.remove_job("auto_refresh")
    except JobLookupError:
        pass
    scheduler.add_job(
        run_pipeline,
        trigger="interval",
        minutes=interval_min,
        id="auto_refresh",
        next_run_time=datetime.now() + timedelta(minutes=interval_min),
    )
    with _lock:
        _state["next_refresh"] = datetime.now() + timedelta(minutes=interval_min)


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------

@app.on_event("startup")
def startup():
    # ── Seed state from Supabase SYNCHRONOUSLY first ──
    # This must happen before the pipeline starts so the fresh scrape can't
    # be overwritten by a late-arriving stale seed. It also guarantees any
    # request that hits /api/bets, /api/bootstrap etc. during warm-up gets
    # the most recent persisted snapshot instead of empty arrays.
    _seed_state_from_db_sync()

    # Seed the payload cache from whatever we just loaded so warm-start GETs
    # don't fall through to the cold-start empty shape. Safe to call even if
    # some datasets are empty — empty arrays serialize fine.
    try:
        _rebuild_cache_from_state()
        # Books are authoritative as pre-serialized bytes now. Drop the
        # duplicate Python lists to cut ~10MB of RSS before the pipeline runs.
        with _lock:
            _state["fd_lines"]  = []
            _state["dk_lines"]  = []
            _state["pin_lines"] = []
    except Exception as exc:
        logger.warning("Initial cache seed failed: %s", exc)

    scheduler.start()
    _reschedule(_state["interval_min"])
    logger.info("Scheduler started. Auto-refresh every %d min.", _state["interval_min"])

    # Warm the RWBC cell cache from calibration_cells so calibrate() works
    # for the auto-backtester before the first hourly refit completes.
    # Safe to call even if migration_010 hasn't been applied yet — the
    # loader logs and returns 0.
    try:
        from engine import rwbc_calibration as _rwbc_boot
        from engine.database import get_db as _get_db_boot
        n_cells = _rwbc_boot.load_cell_cache_from_db(_get_db_boot())
        logger.info("Startup: RWBC cell cache warmed with %d cells.", n_cells)
    except Exception as exc:
        logger.warning("Startup: RWBC cache warm failed (continuing): %s", exc)

    # ── Startup recovery: finalize any missed CLV rows from when the app was down ──
    def _startup_clv_recovery():
        try:
            finalized = _clv_tracker.finalize_missed()
            if finalized:
                logger.info("Startup CLV recovery: finalized %d missed rows", finalized)
        except Exception as exc:
            logger.warning("Startup CLV recovery error: %s", exc)

    threading.Thread(target=_startup_clv_recovery, daemon=True).start()

    # ── RWBC loader + Brier-decomp helpers (used by the hourly refit) ──
    def _load_observations_for_rwbc(db):
        """Pull settled observatory rows, group by (league, prop, side),
        compute global hit rate, and return everything the RWBC fit
        + history-recording path needs in one dict.

        Recency-weighting: we use a flat weight=1.0 per observation here
        because the existing isotonic refit's recency loader (60-day
        half-life) feeds the persistence layer that's already aged the
        sample; RWBC fitting from market_observatory directly means we
        get the full population. If you want recency weighting in RWBC,
        compute weight = exp(-Δdays/H) here. Keeping it flat for v1 so
        the empirical match with probes 06-07 is exact.
        """
        if db is None:
            return None
        try:
            from engine import rwbc_calibration as _rwbc
            from engine.isotonic_calibration import (
                calibrate as _apply_iso, load_isotonic_calibration as _load_iso,
            )
        except Exception as exc:
            logger.error("RWBC.load: import failure %s", exc)
            return None

        # Page through settled observations. We deliberately cap at
        # MAX_ROWS to avoid an unbounded scan as history grows; the
        # 60-day half-life already saturates well before this cap.
        MAX_ROWS = 80_000
        PAGE = 1000
        rows: list[dict] = []
        offset = 0
        try:
            while offset < MAX_ROWS:
                res = (
                    db.table("market_observatory")
                      .select("league, prop, side, true_prob, result, game_start, resolved_at")
                      .neq("result", "pending")
                      .in_("league", ["NBA", "WNBA", "MLB", "NHL"])
                      .order("resolved_at", desc=True)
                      .range(offset, offset + PAGE - 1)
                      .execute()
                )
                chunk = res.data or []
                if not chunk:
                    break
                rows.extend(chunk)
                if len(chunk) < PAGE:
                    break
                offset += PAGE
        except Exception as exc:
            logger.error("RWBC.load: query failure %s", exc)
            return None

        # Convert outcome strings to binary y; drop pushes/dnp.
        def _y(r):
            s = str(r or "").lower()
            if s in ("hit", "won", "win"):  return 1.0
            if s in ("miss", "lost", "loss"): return 0.0
            return None

        settled = []
        for r in rows:
            y = _y(r.get("result"))
            tp = r.get("true_prob")
            if y is None or tp is None:
                continue
            try:
                tp_f = float(tp)
            except (TypeError, ValueError):
                continue
            side = str(r.get("side") or "").strip().lower()
            if side not in ("over", "under"):
                continue
            settled.append({
                "league": r.get("league"),
                "prop":   r.get("prop"),
                "side":   side,
                "pred":   tp_f,
                "y":      y,
                "weight": 1.0,
                "prob_bucket": _rwbc.bucket_of(tp_f),
            })

        if not settled:
            return None

        global_hit_rate = sum(o["y"] for o in settled) / len(settled)
        by_cell: dict[tuple[str, str, str], list[dict]] = {}
        for o in settled:
            by_cell.setdefault((o["league"], o["prop"], o["side"]), []).append(o)

        # Brier comparison: current isotonic vs RWBC, in-sample on the
        # same population. RWBC is computed AFTER fitting (we'll do that
        # in the caller), so here we only stage the current-isotonic
        # Brier — it's free since the curves are already loaded.
        try:
            curves = _load_iso()
        except Exception:
            curves = None

        brier_current = None
        if curves:
            sq_err = 0.0
            for o in settled:
                p_iso = _apply_iso(curves, o["league"], o["prop"], o["side"], o["pred"])
                sq_err += (p_iso - o["y"]) ** 2
            brier_current = sq_err / len(settled)

        return {
            "by_cell":         by_cell,
            "global_hit_rate": global_hit_rate,
            "n_settled":       len(settled),
            "brier_current":   brier_current,
            "brier_rwbc":      None,   # filled by caller post-fit
            "settled":         settled,  # for post-fit Brier recompute
        }

    # ── Hourly calibration + correlation refit ──
    # Updated from daily → hourly: calibration multipliers and leg-pair
    # correlations both benefit from faster feedback as the backtest log
    # grows. Each run is a single aggregation query on resolved observations
    # (no scraping), so the load is negligible.
    def _run_periodic_models():
        # Don't load full table dumps into RAM while a scrape is allocating
        # raw line lists — the overlap blew through the 512 MB tier.
        # Defer to 60 s from now and let the next interval handle it.
        with _lock:
            scraping = bool(_state.get("is_scraping"))
        if scraping:
            logger.info("Hourly refit: deferring — scrape in progress")
            try:
                scheduler.add_job(
                    _run_periodic_models,
                    trigger="date",
                    run_date=datetime.now() + timedelta(seconds=60),
                    id=f"periodic_models_deferred_{int(time.time())}",
                )
            except Exception as exc:
                logger.warning("Hourly refit: deferral scheduling failed: %s", exc)
            return

        try:
            from engine.isotonic_calibration import update_isotonic_calibration
            from engine.ev_calculator import reload_calibration
            curves = update_isotonic_calibration()
            if curves:
                reload_calibration()
                logger.info(
                    "Hourly refit: isotonic curves reloaded — %d leagues, %d (league,prop) buckets",
                    len(curves.get("leagues") or {}), len(curves.get("props") or {}),
                )
            else:
                logger.info("Hourly refit: calibration — no mature data yet")
        except Exception as exc:
            logger.error("Hourly refit: calibration error: %s", exc)

        # ── RWBC refit ──────────────────────────────────────────────────
        # Always run, regardless of USE_RWBC flag — that way calibration_cells
        # stays warm and the Observatory monitoring panels have data even
        # when the live calibrator is still isotonic. The flag only gates
        # which calibrator engine/ev_calculator.py consults at scoring time.
        try:
            from engine import rwbc_calibration
            from engine.database import get_db as _get_db
            db = _get_db()
            obs = _load_observations_for_rwbc(db)
            if obs and obs.get("by_cell"):
                fits = rwbc_calibration.fit_all_cells(
                    obs["by_cell"], obs["global_hit_rate"],
                )

                # Compute RWBC Brier on the same population so the trend
                # panel can plot side-by-side current vs RWBC.
                if fits and obs.get("settled"):
                    sq_err = 0.0
                    for o in obs["settled"]:
                        fit = fits.get((o["league"], o["prop"], o["side"]))
                        if fit is None or fit["w_cell"] < rwbc_calibration.W_CELL_HALT_THRESHOLD:
                            # Halted cell — fall back to raw model prob,
                            # matching the auto-backtester's display path.
                            p_rwbc = o["pred"]
                        else:
                            p_rwbc = (fit["w_cell"] * o["pred"]
                                      + (1 - fit["w_cell"]) * fit["p_post"])
                        sq_err += (p_rwbc - o["y"]) ** 2
                    brier_rwbc = sq_err / len(obs["settled"])
                else:
                    brier_rwbc = None

                pub, skip = rwbc_calibration.publish_if_delta_n_exceeded(db, fits)
                # Prefer the DB-persisted cells (gives us recency-weighted
                # historical state across restarts), but fall back to the
                # in-memory fits if the DB load returns 0 — happens when
                # migration_010 hasn't landed yet or the table is empty
                # on first boot. Either way, calibrate() and the Observatory
                # panels see real data immediately.
                n_loaded = rwbc_calibration.load_cell_cache_from_db(db)
                if n_loaded == 0 and fits:
                    rwbc_calibration.set_cell_cache_from_fits(fits)
                rwbc_calibration.record_history(
                    db,
                    scope="global",
                    brier_current=obs.get("brier_current"),
                    brier_rwbc=brier_rwbc,
                    n_settled=obs.get("n_settled", 0),
                    publish_skipped=(pub == 0 and skip > 0),
                )
                logger.info(
                    "Hourly refit: RWBC — %d cells fit, %d published, %d gated; "
                    "Brier current=%.5f rwbc=%.5f",
                    len(fits), pub, skip,
                    obs.get("brier_current") or 0.0, brier_rwbc or 0.0,
                )
            else:
                logger.info("Hourly refit: RWBC — no observations available yet")
        except Exception as exc:
            logger.error("Hourly refit: RWBC error: %s", exc)

        try:
            from engine.sharpness_calibration import update_sharpness_weights
            from engine.consensus import reload_sharpness
            sharp = update_sharpness_weights()
            if sharp:
                n_books = reload_sharpness()
                logger.info("Hourly refit: sharpness weights refit for %d books", n_books)
            else:
                logger.info(
                    "Hourly refit: sharpness — no per-book CLV data yet "
                    "(apply migration_003.sql once observations accumulate)",
                )
        except Exception as exc:
            logger.error("Hourly refit: sharpness error: %s", exc)

        try:
            from engine.correlation import update_correlation_map, reload_correlation, MIN_PAIR_OBS
            corr = update_correlation_map()
            if corr:
                n_trusted = reload_correlation()
                logger.info(
                    "Hourly refit: %d correlation buckets total, %d above %d-pair threshold",
                    len(corr.get("buckets", {})), n_trusted, MIN_PAIR_OBS,
                )
            else:
                logger.info("Hourly refit: correlation — no data yet")
        except Exception as exc:
            logger.error("Hourly refit: correlation error: %s", exc)

        # Brier monitor runs *after* the calibration + sharpness refits so
        # the snapshot reflects the post-refit state. WARNs if the
        # calibrated probability is a worse predictor than the raw
        # consensus over the rolling window — that's the operational
        # signal that the calibrator regressed and needs attention.
        try:
            from engine.calibration_monitor import update_brier_monitor
            snap = update_brier_monitor()
            if snap.get("status") == "regression":
                logger.warning(
                    "Hourly refit: Brier monitor flagged a regression — "
                    "raw=%s calibrated=%s n=%s",
                    snap.get("brier_raw"), snap.get("brier_calibrated"),
                    snap.get("n_obs"),
                )
        except Exception as exc:
            logger.error("Hourly refit: brier monitor error: %s", exc)

    scheduler.add_job(
        _run_periodic_models,
        trigger="interval",
        hours=1,
        id="periodic_models",
        next_run_time=datetime.now() + timedelta(minutes=1),
        replace_existing=True,
    )
    # Also run once on startup (off-thread so we don't block boot).
    threading.Thread(target=_run_periodic_models, daemon=True).start()

    # Run pipeline immediately on startup so data is ready
    threading.Thread(target=run_pipeline, daemon=True).start()


# Any cached snapshot older than this is considered stale and ignored on
# startup (and proactively purged from Supabase). 
# Set to 1440 (24h) so users can instantly load the UI with yesterday's lines 
# while the background scrape runs, rather than facing a 60s loading screen.
_SEED_MAX_AGE_MIN = 1440


def _parse_updated_at(s: str | None):
    if not s:
        return None
    try:
        # Supabase returns UTC ISO with trailing '+00:00' or 'Z'
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _seed_state_from_db_sync():
    """
    Load the last persisted snapshot from Supabase into in-memory state.

    Only seeds keys that are still empty AND whose cached value is newer
    than _SEED_MAX_AGE_MIN. Stale rows are deleted from app_state_cache so
    they can't resurface on the next restart.
    """
    from datetime import timezone
    from engine.database import get_db
    from engine.persistence import load_multiple_states_from_supabase
    
    logger.info("Startup: Seeding state from Supabase cache...")
    keys = ["bets", "matches", "pp_lines", "fd_lines", "dk_lines", "pin_lines", "last_refresh"]
    now = datetime.now(timezone.utc)
    db = get_db()

    try:
        states = load_multiple_states_from_supabase(keys)
    except Exception as exc:
        logger.warning(f"Seed: failed to load states: {exc}")
        states = {}

    purged = []
    
    for k in keys:
        if k not in states:
            # Maybe log or just continue
            continue
            
        data, updated_at = states[k]
        if data is None:
            continue

        ts = _parse_updated_at(updated_at)
        age_min = None
        if ts is not None:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_min = (now - ts).total_seconds() / 60.0

        # Stale (or un-timestamped) → skip and delete so we never see it again.
        if ts is None or age_min > _SEED_MAX_AGE_MIN:
            purged.append((k, age_min))
            if db:
                try:
                    db.table("app_state_cache").delete().eq("key", k).execute()
                except Exception as exc:
                    logger.warning("Seed: failed to purge stale '%s': %s", k, exc)
            continue

        with _lock:
            current = _state.get(k)
            if k == "last_refresh":
                if current is not None:
                    continue
                try:
                    _state[k] = datetime.fromisoformat(data)
                except Exception:
                    _state[k] = None
            else:
                if current:  # non-empty list already — keep it
                    continue
                _state[k] = data

    if purged:
        logger.info(
            "Startup: purged %d stale cache key(s) (> %dmin old): %s",
            len(purged), _SEED_MAX_AGE_MIN,
            ", ".join(f"{k}({'?' if a is None else f'{a:.0f}m'})" for k, a in purged),
        )
    logger.info("Startup: Seeding complete.")


@app.on_event("shutdown")
def shutdown():
    scheduler.shutdown(wait=False)


# ---------------------------------------------------------------------------
# Static files + root
# ---------------------------------------------------------------------------

import pathlib
STATIC_DIR = pathlib.Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/health")
@app.head("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def root():
    from engine.database import SUPABASE_URL, SUPABASE_ANON_KEY
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    # Inject runtime config so the frontend doesn't need to fetch /api/ui-config
    config_script = (
        f'<script>window.__COREPROP_CONFIG='
        f'{{"supabase_url":"{SUPABASE_URL}","supabase_anon_key":"{SUPABASE_ANON_KEY}"}}'
        f'</script>'
    )
    html = html.replace("</head>", config_script + "\n</head>", 1)
    from fastapi.responses import HTMLResponse
    return HTMLResponse(content=html)


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/api/auth/me")
def get_auth_me(user: dict = Depends(get_current_user)):
    """Return current verified user metadata."""
    return user


@app.get("/api/auth/check-username")
def check_username(username: str):
    """Check if a username is already taken. Public endpoint."""
    import requests as _req
    from engine.database import SUPABASE_URL, SUPABASE_KEY
    if not username or len(username) < 2 or len(username) > 20:
        raise HTTPException(status_code=400, detail="Username must be 2-20 characters.")
    if not username.replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Username may only contain letters, numbers, and underscores.")

    target = username.lower()
    # Page through the admin API. Default page size is 50; without paging,
    # users beyond page 1 silently pass as "available". We cap at 20 pages
    # (1000 users) — well above current scale, with a hard exit so a future
    # 10k-user database can't pin this endpoint.
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    PER_PAGE = 200
    for page in range(1, 21):
        try:
            r = _req.get(
                f"{SUPABASE_URL}/auth/v1/admin/users",
                params={"page": page, "per_page": PER_PAGE},
                headers=headers,
                timeout=5,
            )
        except Exception as exc:
            logger.warning("check_username request failed: %s", exc)
            raise HTTPException(status_code=500, detail="Could not verify username.")
        if r.status_code != 200:
            logger.warning("check_username admin API returned %s: %s", r.status_code, r.text[:200])
            raise HTTPException(status_code=500, detail="Could not verify username.")
        users = r.json().get("users", []) or []
        for u in users:
            meta = u.get("user_metadata") or {}
            if (meta.get("username", "") or "").lower() == target:
                return {"available": False}
        if len(users) < PER_PAGE:
            break
    return {"available": True}


def _last_refresh_iso():
    """Return last_refresh as ISO string or None (callers already hold no lock)."""
    ts = _state.get("last_refresh")
    if isinstance(ts, datetime):
        return ts.isoformat()
    if isinstance(ts, str):
        return ts
    return None


def _get_user_config(user: Optional[dict]) -> dict:
    import config as cfg
    base = {
        "min_ev_pct": -10.0,
        "active_leagues": dict(cfg.ACTIVE_LEAGUES),
        "refresh_interval_min": 5,
        "auto_backtest": False,
        "auto_slip_type": "Power",
        "auto_slip_legs": 6,
        "auto_slip_min_prob": None,
    }
    if not user:
        return base

    try:
        from engine.database import get_user_db
        db = get_user_db(user["jwt"])
        if db:
            res = db.table("user_config").select("*").eq("user_id", user["id"]).execute()
            if res.data:
                row = res.data[0]
                base["min_ev_pct"] = float(row.get("min_ev_pct", -10.0))
                if row.get("active_leagues"):
                    # Merge so newly-added leagues (e.g. WNBA) inherit the
                    # config default for users who saved settings before the
                    # league existed.
                    merged = dict(base["active_leagues"])
                    merged.update(row["active_leagues"])
                    base["active_leagues"] = merged
                base["refresh_interval_min"] = int(row.get("refresh_interval_min", 5))
                base["auto_backtest"] = bool(row.get("auto_backtest", False))
                # auto_slip_* may be missing on rows from before migration_004.
                if row.get("auto_slip_type"):
                    base["auto_slip_type"] = str(row["auto_slip_type"])
                if row.get("auto_slip_legs") is not None:
                    base["auto_slip_legs"] = int(row["auto_slip_legs"])
                if row.get("auto_slip_min_prob") is not None:
                    try:
                        base["auto_slip_min_prob"] = float(row["auto_slip_min_prob"])
                    except (TypeError, ValueError):
                        pass
    except Exception as e:
        logger.warning(f"Failed to fetch user config for {user['id']}: {e}")

    return base

@app.get("/api/bets")
def get_bets(request: Request):
    """Serve the pre-serialized bets payload. Per-user filtering (min_ev_pct,
    active_leagues, in_backtest) now happens client-side via `/api/backtest/keys`
    — this removes the per-request dict-copy (~one full payload of allocations)
    and the Supabase round-trip that used to block the hot path."""
    return _cached_response("bets", request)


@app.get("/api/ui-config")
def get_ui_config():
    from engine.database import SUPABASE_URL, SUPABASE_ANON_KEY
    return {
        "supabase_url": SUPABASE_URL,
        "supabase_anon_key": SUPABASE_ANON_KEY
    }


@app.get("/api/matched")
def get_matched(request: Request):
    return _cached_response("matches", request)


@app.get("/api/bootstrap/core")
def get_bootstrap_core(request: Request, user: Optional[dict] = Depends(get_current_user_optional)):
    """Critical-path payload for first paint: bets + meta only.

    Intentionally does NOT include matches/pp/fd/dk/pin — those load lazily
    when their tab is activated. This trims the initial payload by ~80% and
    removes 5 heavy allocations from the common request path on the 512MB tier.

    Gated behind an active subscription ONLY when BILLING_ENFORCE=true; when
    enforcement is off (the default) _user_has_access() returns True for
    everyone and this is a no-op, so the hot path is unchanged until billing
    is switched on.
    """
    if not _user_has_access(user):
        raise HTTPException(status_code=402, detail="Subscription required.")
    return _cached_response("core", request)


@app.get("/api/backtest/keys")
def get_backtest_keys(user: dict = Depends(get_current_user)):
    """Lightweight endpoint: returns the set of `bet_key` strings already
    logged by this user, so the client can join in_backtest flags locally.

    Replaces the full per-request dict-copy + Supabase round-trip that used
    to live inside /api/bets and /api/bootstrap."""
    from engine.backtest import BacktestLogger
    logger_inst = BacktestLogger(user["id"], user["jwt"])
    used = logger_inst._load_used_keys_from_db()
    # used is a set of (normalized_player, yyyy-mm-dd) tuples — flatten to
    # the same "player|date" string we stamp on each bet during pipeline.
    keys = [f"{p}|{d}" for (p, d) in used]
    return {"keys": keys}


@app.get("/api/bootstrap")
def get_bootstrap_legacy(request: Request):
    """Back-compat shim: old clients (or stale cached HTML) may still call
    this. Redirect to the lean core endpoint — the extra datasets load lazily
    on tab activation now."""
    return _cached_response("core", request)


@app.get("/api/status")
def get_status():
    with _lock:
        return {
            "is_scraping":   _state["is_scraping"],
            "last_refresh":  _state["last_refresh"].isoformat() if _state["last_refresh"] else None,
            "next_refresh":  _state["next_refresh"].isoformat() if _state["next_refresh"] else None,
            "scrape_errors": _state["scrape_errors"],
            "interval_min":  _state["interval_min"],
            "total_bets":    len(_state["bets"]),
        }


class SlipRequest(BaseModel):
    bet_ids: list[str]
    bankroll: float = 100.0


@app.post("/api/slip")
def build_slip(req: SlipRequest, user: Optional[dict] = Depends(get_current_user_optional)):
    if not req.bet_ids:
        raise HTTPException(status_code=400, detail="No bet IDs provided.")
    if len(req.bet_ids) < 2 or len(req.bet_ids) > 6:
        raise HTTPException(status_code=400, detail="Slip must have 2-6 picks.")

    with _lock:
        bet_map = _state["bet_map"]

    selected: list[BetResult] = []
    missing = []
    for bid in req.bet_ids:
        bet = bet_map.get(bid)
        if bet is None:
            missing.append(bid)
        else:
            selected.append(bet)

    if missing:
        raise HTTPException(status_code=404, detail=f"Bet IDs not found: {missing}")

    result = calculate_slip(selected, req.bankroll)
    return result


#  Candidate pool cap for /api/slip/auto. Limits combinatorial blow-up of the
#  subset search: Σ C(12, k) for k=2..6 is 2,497 subsets — each scored via the
#  exact independence-based EV formulas (microseconds each) before the single
#  winning subset gets the full correlation-aware Monte Carlo.
_AUTO_SLIP_MAX_CANDIDATES = 12


@app.post("/api/slip/auto")
def auto_build_slip(req: SlipRequest, user: Optional[dict] = Depends(get_current_user_optional)):
    """Pick the best 2–6-leg slip from a candidate pool.

    Unlike the prior implementation (which only tried EV-sorted prefixes),
    this enumerates *every* subset of size K ∈ [2, 6] from the deduplicated
    candidate pool and scores each under the exact independence formulas —
    significantly more accurate for Flex slips where a low-probability leg
    can hurt despite having positive individual EV. The winner is then
    re-scored with correlation-aware Monte Carlo so same-game stacks aren't
    penalised for picking up latent positive correlation.
    """
    if not req.bet_ids:
        raise HTTPException(status_code=400, detail="No bet IDs provided.")
    if len(req.bet_ids) < 2:
        raise HTTPException(status_code=400, detail="Must provide at least 2 bets.")

    with _lock:
        bet_map = _state["bet_map"]

    # Dedupe by (player, game_day). Input order is preserved — the client sorts
    # by EV before sending, so the cap keeps the most promising candidates.
    candidates: list[BetResult] = []
    seen: set[tuple[str, str]] = set()
    for bid in req.bet_ids:
        bet = bet_map.get(bid)
        if bet is None:
            continue
        key = make_bet_key(bet.player_name, bet.start_time)
        if key in seen:
            continue
        candidates.append(bet)
        seen.add(key)
        if len(candidates) >= _AUTO_SLIP_MAX_CANDIDATES:
            break

    if len(candidates) < 2:
        raise HTTPException(status_code=400, detail="Not enough unique bets after dedup.")

    from itertools import combinations
    from engine.ev_calculator import power_slip_ev, flex_slip_ev

    best_ev = -float("inf")
    best_subset: list[BetResult] = []
    max_k = min(6, len(candidates))
    for k in range(2, max_k + 1):
        for combo in combinations(candidates, k):
            probs = [b.true_prob for b in combo]
            p_ev = power_slip_ev(probs)
            f_ev = flex_slip_ev(probs)
            evs = [ev for ev in (p_ev, f_ev) if ev is not None]
            if not evs:
                continue
            ev = max(evs)
            if ev > best_ev + 1e-9:
                best_ev = ev
                best_subset = list(combo)

    if not best_subset:
        raise HTTPException(status_code=400, detail="Could not calculate any valid slip.")

    # Final evaluation with correlation-aware Monte Carlo for the returned metrics.
    final = calculate_slip(best_subset, req.bankroll)
    return {
        **final,
        "optimal_k":       len(best_subset),
        "optimal_bet_ids": [b.bet_id for b in best_subset],
    }


# Sandbox endpoints, SandboxRequest, stat-types cache, simulation cache,
# and per-config invalidation all moved to web/routers/sandbox.py during
# the Phase-1 router split. The router is mounted at the bottom of this
# file via `app.include_router(...)`.




class ConfigUpdate(BaseModel):
    interval_min:    Optional[int]   = None
    min_ev_pct:      Optional[float] = None
    active_leagues:  Optional[dict]  = None


@app.get("/api/config")
def get_config(user: dict = Depends(get_current_user)):
    cfg = _get_user_config(user)
    return {
        "interval_min":    cfg["refresh_interval_min"],
        "min_ev_pct":      cfg["min_ev_pct"],
        "active_leagues":  cfg["active_leagues"],
        "auto_backtest":   cfg.get("auto_backtest", False),
        "auto_slip_type":     cfg.get("auto_slip_type", "Power"),
        "auto_slip_legs":     cfg.get("auto_slip_legs", 6),
        "auto_slip_min_prob": cfg.get("auto_slip_min_prob"),
    }


@app.post("/api/config")
def update_config(update: ConfigUpdate, user: dict = Depends(get_current_user)):
    with _lock:
        if update.interval_min is not None:
            if update.interval_min < 1:
                raise HTTPException(status_code=400, detail="interval_min must be >= 1")
            _state["interval_min"] = update.interval_min
            _reschedule(update.interval_min)

        if update.min_ev_pct is not None:
            _state["min_ev_pct"] = update.min_ev_pct

        if update.active_leagues is not None:
            _state["active_leagues"].update(update.active_leagues)

    return {"status": "config updated"}


class AutoBacktestUpdate(BaseModel):
    auto_backtest: bool

@app.post("/api/user/auto-backtest")
def update_auto_backtest(update: AutoBacktestUpdate, user: dict = Depends(get_current_user)):
    from engine.database import get_user_db
    db = get_user_db(user["jwt"])
    if not db:
        raise HTTPException(status_code=500, detail="Database not reachable.")

    # Upsert the user_config row. (Requires an ON CONFLICT clause in raw sql, but we'll try a basic upsert via PostgREST)
    try:
        db.table("user_config").upsert({
            "user_id": user["id"],
            "auto_backtest": update.auto_backtest,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).execute()
        return {"status": "success", "auto_backtest": update.auto_backtest}
    except Exception as e:
        logger.error(f"Failed to update auto_backtest for user {user['id']}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update configuration.")


class SlipPrefsUpdate(BaseModel):
    auto_slip_type: str
    auto_slip_legs: int
    auto_slip_min_prob: Optional[float] = None


@app.post("/api/user/slip-prefs")
def update_slip_prefs(update: SlipPrefsUpdate, user: dict = Depends(get_current_user)):
    """Persist the user's preferred slip type (Power/Flex), leg count, and the
    per-leg minimum true probability. These drive Auto-Backtest's slip
    construction on every refresh."""
    if update.auto_slip_type not in ("Power", "Flex"):
        raise HTTPException(status_code=400, detail="auto_slip_type must be 'Power' or 'Flex'")
    if not (2 <= update.auto_slip_legs <= 6):
        raise HTTPException(status_code=400, detail="auto_slip_legs must be between 2 and 6")
    if update.auto_slip_type == "Flex" and update.auto_slip_legs < 3:
        raise HTTPException(status_code=400, detail="Flex slips require at least 3 legs.")
    if update.auto_slip_min_prob is not None and not (0.0 < update.auto_slip_min_prob < 1.0):
        raise HTTPException(status_code=400, detail="auto_slip_min_prob must be between 0 and 1 (exclusive).")

    from engine.database import get_user_db
    db = get_user_db(user["jwt"])
    if not db:
        raise HTTPException(status_code=500, detail="Database not reachable.")

    payload = {
        "user_id": user["id"],
        "auto_slip_type": update.auto_slip_type,
        "auto_slip_legs": update.auto_slip_legs,
        "auto_slip_min_prob": update.auto_slip_min_prob,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        db.table("user_config").upsert(payload).execute()
        return {
            "status": "success",
            "auto_slip_type": update.auto_slip_type,
            "auto_slip_legs": update.auto_slip_legs,
            "auto_slip_min_prob": update.auto_slip_min_prob,
        }
    except Exception as e:
        logger.error(f"Failed to update slip prefs for user {user['id']}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update slip preferences.")

# ---------------------------------------------------------------------------
# PrizePicks-only endpoints
# ---------------------------------------------------------------------------

@app.get("/api/prizepicks")
def get_prizepicks(request: Request):
    return _cached_response("pp_lines", request)


@app.post("/api/prizepicks/refresh")
def refresh_prizepicks():
    with _lock:
        if _state["is_scraping_pp"]:
            raise HTTPException(status_code=409, detail="PrizePicks scrape already in progress.")
    threading.Thread(target=_run_pp_scrape, daemon=True).start()
    return {"status": "prizepicks refresh started"}


def _run_pp_scrape():
    with _lock:
        if _state["is_scraping_pp"]:
            return
        _state["is_scraping_pp"] = True

    try:
        with _lock:
            leagues = dict(_state["active_leagues"])

        logger.info("PrizePicks-only scrape starting...")
        pp_lines = scrape_prizepicks(active_leagues=leagues)
        if len(pp_lines) == 0:
            logger.warning("PrizePicks returned 0 lines — keeping previous serialized state")
            return
        serialized = []
        for l in pp_lines:
            if l.side == "both":
                common = {
                    "league": l.league,
                    "player_name": l.player_name,
                    "stat_type": l.stat_type,
                    "line_score": l.line_score,
                    "start_time": l.start_time,
                }
                serialized.append({**common, "side": "over"})
                serialized.append({**common, "side": "under"})
            else:
                serialized.append({
                    "league": l.league,
                    "player_name": l.player_name,
                    "stat_type": l.stat_type,
                    "line_score": l.line_score,
                    "side": l.side,
                    "start_time": l.start_time,
                })
        with _lock:
            _state["last_refresh"] = datetime.now()
            last_ref = _state["last_refresh"]
        _update_one_payload("pp_lines", serialized, last_ref)
        sync_state_to_supabase("pp_lines", serialized)
        with _lock:
            _state["pp_lines"] = []
        logger.info("PrizePicks-only scrape complete: %d lines.", len(serialized))
    except Exception as e:
        logger.exception("PrizePicks scrape error: %s", e)
    finally:
        with _lock:
            _state["is_scraping_pp"] = False

# ---------------------------------------------------------------------------
# FanDuel-only endpoints
# ---------------------------------------------------------------------------

@app.get("/api/fanduel")
def get_fanduel(request: Request):
    return _cached_response("fd_lines", request)


@app.post("/api/fanduel/refresh")
def refresh_fanduel():
    with _lock:
        if _state["is_scraping_fd"]:
            raise HTTPException(status_code=409, detail="FanDuel scrape already in progress.")
    threading.Thread(target=_run_fd_scrape, daemon=True).start()
    return {"status": "fanduel refresh started"}


def _run_fd_scrape():
    with _lock:
        if _state["is_scraping_fd"]:
            return
        _state["is_scraping_fd"] = True

    try:
        with _lock:
            leagues = dict(_state["active_leagues"])

        logger.info("FanDuel scrape starting...")
        fd_props = scrape_fanduel(active_leagues=leagues)
        if len(fd_props) == 0:
            logger.warning("FanDuel returned 0 props — keeping previous serialized state")
            return
        from engine.devig import devig_power, devig_single_sided, prob_to_american
        serialized = []
        for p in fd_props:
            true_over, true_under = None, None
            if p.both_sided and p.over_odds is not None and p.under_odds is not None:
                true_over, true_under = devig_power(p.over_odds, p.under_odds)
            else:
                if p.over_odds is not None:
                    true_over = devig_single_sided(p.over_odds)
                if p.under_odds is not None:
                    true_under = devig_single_sided(p.under_odds)

            if p.over_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "over",
                    "line_odds": p.over_odds,
                    "true_odds": prob_to_american(true_over) if true_over else None,
                    "start_time": getattr(p, "start_time", None),
                })
            if p.under_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "under",
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": getattr(p, "start_time", None),
                })
        with _lock:
            _state["last_refresh"] = datetime.now()
            last_ref = _state["last_refresh"]
        # Only swap the fd_lines cache slot — the other five (bets, matches,
        # pp_lines, dk_lines, pin_lines, core) are untouched, so re-encoding
        # them would just churn memory.
        _update_one_payload("fd_lines", serialized, last_ref)
        # Persist so a cold start can serve data before the next scrape.
        # sync_state_to_supabase auto-gzips payloads over 256KB.
        threading.Thread(target=sync_state_to_supabase, args=("fd_lines", serialized), daemon=True).start()
        # Drop the Python list: the bytes cache is now authoritative for GETs,
        # and _state[fd_lines] isn't read anywhere else.
        with _lock:
            _state["fd_lines"] = []
        logger.info("FanDuel scrape complete: %d lines.", len(serialized))
    except Exception as e:
        logger.exception("FanDuel scrape error: %s", e)
    finally:
        with _lock:
            _state["is_scraping_fd"] = False


# ---------------------------------------------------------------------------
# DraftKings-only endpoints
# ---------------------------------------------------------------------------

@app.get("/api/draftkings")
def get_draftkings(request: Request):
    return _cached_response("dk_lines", request)


@app.post("/api/draftkings/refresh")
def refresh_draftkings():
    with _lock:
        if _state["is_scraping_dk"]:
            raise HTTPException(status_code=409, detail="DraftKings scrape already in progress.")
    threading.Thread(target=_run_dk_scrape, daemon=True).start()
    return {"status": "draftkings refresh started"}


def _run_dk_scrape():
    with _lock:
        if _state["is_scraping_dk"]:
            return
        _state["is_scraping_dk"] = True

    try:
        with _lock:
            leagues = dict(_state["active_leagues"])

        logger.info("DraftKings scrape starting...")
        dk_props = scrape_draftkings(active_leagues=leagues)
        if len(dk_props) == 0:
            logger.warning("DraftKings returned 0 props — keeping previous serialized state")
            return
        from engine.devig import devig_power, devig_single_sided, prob_to_american
        serialized = []
        for p in dk_props:
            true_over, true_under = None, None
            if p.both_sided and p.over_odds is not None and p.under_odds is not None:
                true_over, true_under = devig_power(p.over_odds, p.under_odds)
            else:
                if p.over_odds is not None:
                    true_over = devig_single_sided(p.over_odds)
                if p.under_odds is not None:
                    true_under = devig_single_sided(p.under_odds)

            if p.over_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "over",
                    "line_odds": p.over_odds,
                    "true_odds": prob_to_american(true_over) if true_over else None,
                    "start_time": getattr(p, "start_time", None),
                })
            if p.under_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "under",
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": getattr(p, "start_time", None),
                })
        with _lock:
            _state["last_refresh"] = datetime.now()
            last_ref = _state["last_refresh"]
        _update_one_payload("dk_lines", serialized, last_ref)
        threading.Thread(target=sync_state_to_supabase, args=("dk_lines", serialized), daemon=True).start()
        with _lock:
            _state["dk_lines"] = []
        logger.info("DraftKings scrape complete: %d lines.", len(serialized))
    except Exception as e:
        logger.exception("DraftKings scrape error: %s", e)
    finally:
        with _lock:
            _state["is_scraping_dk"] = False


# ---------------------------------------------------------------------------
# Pinnacle-only endpoints
# ---------------------------------------------------------------------------

@app.get("/api/pinnacle")
def get_pinnacle(request: Request):
    return _cached_response("pin_lines", request)


@app.post("/api/pinnacle/refresh")
def refresh_pinnacle():
    with _lock:
        if _state["is_scraping_pin"]:
            raise HTTPException(status_code=409, detail="Pinnacle scrape already in progress.")
    threading.Thread(target=_run_pin_scrape, daemon=True).start()
    return {"status": "pinnacle refresh started"}


def _run_pin_scrape():
    with _lock:
        if _state["is_scraping_pin"]:
            return
        _state["is_scraping_pin"] = True

    try:
        with _lock:
            leagues = dict(_state["active_leagues"])

        logger.info("Pinnacle scrape starting...")
        pin_props = scrape_pinnacle(active_leagues=leagues)
        if len(pin_props) == 0:
            logger.warning("Pinnacle returned 0 props — keeping previous serialized state")
            return
        from engine.devig import devig_power, devig_single_sided, prob_to_american
        serialized = []
        for p in pin_props:
            true_over, true_under = None, None
            if p.both_sided and p.over_odds is not None and p.under_odds is not None:
                true_over, true_under = devig_power(p.over_odds, p.under_odds)
            else:
                if p.over_odds is not None:
                    true_over = devig_single_sided(p.over_odds)
                if p.under_odds is not None:
                    true_under = devig_single_sided(p.under_odds)

            if p.over_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "over",
                    "line_odds": p.over_odds,
                    "true_odds": prob_to_american(true_over) if true_over else None,
                    "start_time": getattr(p, "start_time", None),
                })
            if p.under_odds is not None:
                serialized.append({
                    "league": p.league,
                    "player_name": p.player_name,
                    "stat_type": p.prop_type,
                    "line_score": p.line,
                    "side": "under",
                    "line_odds": p.under_odds,
                    "true_odds": prob_to_american(true_under) if true_under else None,
                    "start_time": getattr(p, "start_time", None),
                })
        with _lock:
            _state["last_refresh"] = datetime.now()
            last_ref = _state["last_refresh"]
        _update_one_payload("pin_lines", serialized, last_ref)
        threading.Thread(target=sync_state_to_supabase, args=("pin_lines", serialized), daemon=True).start()
        with _lock:
            _state["pin_lines"] = []
        logger.info("Pinnacle scrape complete: %d lines.", len(serialized))
    except Exception as e:
        logger.exception("Pinnacle scrape error: %s", e)
    finally:
        with _lock:
            _state["is_scraping_pin"] = False


# ---------------------------------------------------------------------------
# Calibration metrics endpoint
# ---------------------------------------------------------------------------

@app.get("/api/calibration")
def get_calibration(user: dict = Depends(get_current_user)):
    """Return Brier Score, Log-Loss, and calibration buckets from resolved backtest data."""
    from engine.calibration import evaluate_calibration
    return evaluate_calibration(user_jwt=user["jwt"])


@app.get("/api/analytics")
def get_analytics(user: dict = Depends(get_current_user)):
    """
    Richer analytics payload: calibration + per-league / per-prop performance,
    cumulative P&L timeline, and slip outcome mix.

    Per-user TTL cache (30s) because the frontend re-hits this on every tab
    activation and status-poll refresh, but the underlying backtest state
    rarely changes between those calls. Invalidated on add/delete slip.
    """
    uid = user["id"]
    now = time.monotonic()
    with _analytics_cache_lock:
        cached = _analytics_cache.get(uid)
        if cached and (now - cached[0]) < _ANALYTICS_TTL_SEC:
            return cached[1]

    from engine.calibration import evaluate_analytics
    data = evaluate_analytics(user_jwt=user["jwt"])
    with _analytics_cache_lock:
        _analytics_cache[uid] = (now, data)
    return data


# ---------------------------------------------------------------------------
# PrizePicks 1-click slip endpoints
# ---------------------------------------------------------------------------

class PendingSlipRequest(BaseModel):
    legs: list[dict]   # [{player, league, prop, line, side}, ...]
    slip_type: str = "Power"
    n_legs: int = 6


@app.post("/api/pending-slip")
def set_pending_slip(req: PendingSlipRequest, user: dict = Depends(get_current_user)):
    """Store a slip for the PP browser extension to pick up. TTL: 5 min."""
    uid = user["id"]
    expires_at = time.monotonic() + _PENDING_SLIP_TTL_SEC
    payload = {
        "user_id": uid,
        "slip_type": req.slip_type,
        "n_legs": req.n_legs,
        "legs": req.legs,
    }
    with _pending_slips_lock:
        _pending_slips[uid] = (payload, expires_at)
    return {"ok": True}


@app.get("/api/pending-slip")
def get_pending_slip():
    """
    Return the most recent non-expired pending slip.
    No auth — called by the Chrome extension from app.prizepicks.com context.
    Returns {} when nothing is pending.
    """
    now = time.monotonic()
    with _pending_slips_lock:
        # Evict expired entries
        expired = [k for k, (_, exp) in _pending_slips.items() if exp < now]
        for k in expired:
            del _pending_slips[k]
        if not _pending_slips:
            return {}
        # Return the most recently stored slip
        latest = max(_pending_slips.items(), key=lambda kv: kv[1][1])
        slip_data, _ = latest[1]
        return slip_data


@app.delete("/api/pending-slip")
def clear_pending_slip():
    """Extension calls this after successfully building the slip on PP."""
    with _pending_slips_lock:
        _pending_slips.clear()
    return {"ok": True}


class PPAvailabilityRequest(BaseModel):
    legs: list[dict]


@app.post("/api/check-pp-availability")
def check_pp_availability(req: PPAvailabilityRequest):
    """
    Given a list of leg dicts [{player, prop, line, side}], check whether
    each leg is currently available on PrizePicks (matches live pp_lines).
    Returns {available: bool, legs: [{...leg, available: bool, matched_line: float|null}]}.
    """
    from rapidfuzz import fuzz

    legs = req.legs
    if not legs:
        return {"available": False, "legs": []}

    with _lock:
        pp_lines = list(_state.get("pp_lines") or [])

    results = []
    all_available = True

    for leg in legs:
        player = (leg.get("player") or "").strip()
        prop   = (leg.get("prop") or "").strip().lower()
        line   = float(leg.get("line") or 0)
        side   = (leg.get("side") or "").strip().lower()

        best_score = 0
        matched_line = None

        for pp in pp_lines:
            pp_player = (pp.get("player_name") or "").strip()
            pp_stat   = (pp.get("stat_type") or "").strip().lower()
            pp_line   = float(pp.get("line_score") or 0)
            pp_side   = (pp.get("side") or "").strip().lower()

            # Stat type must match (case-insensitive)
            if pp_stat != prop:
                continue
            # PP lines always carry side="both" (PP doesn't restrict the
            # user's pick at scrape time). Accept any leg side against a
            # "both" PP line; otherwise insist on an exact side match.
            # Without this, every leg the user logged ("over"/"under")
            # was rejected against the "both" PP catalogue and the
            # "Place on PrizePicks" button stayed permanently disabled.
            if pp_side and pp_side != "both" and pp_side != side:
                continue
            # Line must be exact
            if abs(pp_line - line) > 0.01:
                continue

            score = fuzz.ratio(player.lower(), pp_player.lower())
            if score > best_score:
                best_score = score
                matched_line = pp_line

        available = best_score >= 85
        if not available:
            all_available = False

        results.append({**leg, "available": available, "matched_line": matched_line, "match_score": best_score})

    return {"available": all_available, "legs": results}


# ---------------------------------------------------------------------------
# Backtest endpoints
# ---------------------------------------------------------------------------


@app.get("/api/backtest/slips")
def get_backtest_slips(user: dict = Depends(get_current_user)):
    """Return the last 100 logged slips from Supabase."""
    from engine.database import get_user_db

    db = get_user_db(user["jwt"])
    all_slips = []

    if not db:
        return {"slips": [], "total": 0}

    try:
        # 1. Fetch the latest 300 slips (5 pages × 60 slips/page on the UI)
        slips_res = db.table("slips").select("*").order("timestamp", desc=True).limit(300).execute()
        slip_data = slips_res.data
        if not slip_data:
            return {"slips": [], "total": 0}

        sids = [s["id"] for s in slip_data]
        # 2. Fetch all legs for these slips
        legs_res = db.table("legs").select("*").in_("slip_id", sids).execute()
        legs_by_slip = {}
        for l in legs_res.data:
            sid = l["slip_id"]
            if sid not in legs_by_slip:
                legs_by_slip[sid] = []
            legs_by_slip[sid].append(l)
        
        for s in slip_data:
            s["slip_id"] = s["id"]
            s["legs"] = sorted(legs_by_slip.get(s["id"], []), key=lambda x: x["leg_num"])
        all_slips = slip_data
    except Exception as db_err:
        logger.error("Backtest API: Supabase fetch failed: %s", db_err)
        return {"slips": [], "total": 0}

    # Compute payout per slip (Common Logic)
    from engine.constants import POWER_PAYOUTS, FLEX_PAYOUTS

    for slip in all_slips:
        legs = slip.get("legs", [])
        n_legs = int(slip.get("n_legs") or len(legs))
        slip_type = (slip.get("slip_type") or "").lower()
        results = [l.get("result", "pending") for l in legs]

        completed = all(r in ("hit", "miss", "push", "dnp") for r in results)
        slip["completed"] = completed

        if completed:
            effective_results = [r for r in results if r not in ("push", "dnp")]
            n_eff = len(effective_results)
            hits_eff = sum(1 for r in effective_results if r == "hit")

            if n_eff < 2:
                payout = 1.0 if (n_eff == 0 or (n_eff == 1 and hits_eff == 1)) else 0
            elif slip_type == "power":
                payout = POWER_PAYOUTS.get(n_eff, 0) if hits_eff == n_eff else 0
            else:  # flex
                if n_eff == 2:
                    payout = POWER_PAYOUTS.get(2, 0) if hits_eff == 2 else 0
                else:
                    payout = FLEX_PAYOUTS.get(n_eff, {}).get(hits_eff, 0)

            slip["payout"] = payout
            slip["hits"] = hits_eff
        else:
            slip["payout"] = None
            slip["hits"] = None

    return {"slips": all_slips, "total": len(all_slips)}


class BacktestAddSlipRequest(BaseModel):
    bet_ids: list[str]
    # The slip type the user picked in the +EV slip builder. Stored on the
    # slip row so the backtest reflects user intent (Power / Flex). Falls
    # back to "Manual" for older clients that don't send it.
    slip_type: str = "Manual"


@app.post("/api/admin/trigger-auto-backtest")
def admin_trigger_auto_backtest():
    """LOCAL-TEST-ONLY endpoint. Fires the auto-backtest worker against
    the bets currently in _state, bypassing the run_pipeline early-returns
    that suppress it when the PrizePicks scrape fails (e.g. on a home IP
    where PP returns "Empty response" via Cloudflare).

    Pairs with USE_RWBC=true + LOCAL_AUTO_BACKTEST_USER_IDS=<uid> to make
    localhost the exclusive logger for a specific user while the cached
    bet pool refreshes on each successful scrape cycle.

    Gated by env: returns 404 unless ENABLE_ADMIN_TRIGGERS=true. NEVER
    enable this in production — it's an unauthenticated slip writer.
    """
    if os.getenv("ENABLE_ADMIN_TRIGGERS", "false").lower() not in ("true", "1", "yes"):
        raise HTTPException(status_code=404, detail="Not Found")

    with _lock:
        bets_payload = list(_state.get("bets") or [])
    if not bets_payload:
        return {"ok": False, "reason": "no bets in state cache yet"}

    from engine.database import get_db
    db = get_db()
    if db is None:
        raise HTTPException(status_code=500, detail="DB unavailable")

    # The state's serialized bets carry true_prob from whoever last
    # serialized them (probably an isotonic-calibrated Render snapshot).
    # Re-score live through the active calibrator path so the admin
    # trigger reflects what THIS server's calibrator says now. Without
    # this re-score, RWBC's halts wouldn't be visible on cached bets.
    from engine import rwbc_calibration as _rwbc
    use_rwbc = os.getenv("USE_RWBC", "false").lower() in ("true", "1", "yes")
    rescored = []
    n_rewritten = 0
    n_halted_rescore = 0
    for b in bets_payload:
        nb = dict(b)
        raw = b.get("raw_true_prob") or b.get("true_prob")
        if raw is None:
            rescored.append(nb)
            continue
        if use_rwbc:
            p_cal = _rwbc.calibrate(float(raw), b.get("league"), b.get("prop_type"), b.get("side"))
            if p_cal is None:
                nb["calibration_halted"] = True
                # Display value falls back to raw model output so this bet
                # still appears in display contexts; auto-backtester
                # filters on calibration_halted.
                nb["true_prob"] = max(0.001, min(0.999, float(raw)))
                n_halted_rescore += 1
            else:
                nb["calibration_halted"] = False
                nb["true_prob"] = p_cal
                n_rewritten += 1
        elif cfg.USE_RAW_CONSENSUS_ONLY:
            # Raw consensus mode — just use raw_true_prob as true_prob,
            # bypassing any historical-correction layer. No halts.
            nb["calibration_halted"] = False
            nb["true_prob"] = max(0.001, min(0.999, float(raw)))
            n_rewritten += 1
        rescored.append(nb)
    bets_payload = rescored

    # Mirror the user-selection logic from _auto_log_bg, including the
    # LOCAL_AUTO_BACKTEST_USER_IDS override.
    _override_csv = os.getenv("LOCAL_AUTO_BACKTEST_USER_IDS", "").strip()
    _override_uids = [u.strip() for u in _override_csv.split(",") if u.strip()]
    if _override_uids:
        flag_res = (
            db.table("user_config")
              .select("user_id, auto_slip_type, auto_slip_legs, auto_slip_min_prob")
              .eq("auto_backtest", True).execute()
        )
        over_res = (
            db.table("user_config")
              .select("user_id, auto_slip_type, auto_slip_legs, auto_slip_min_prob")
              .in_("user_id", _override_uids).execute()
        )
        users = {r["user_id"]: r for r in (flag_res.data or []) + (over_res.data or []) if r.get("user_id")}
        users = list(users.values())
    else:
        users = (db.table("user_config")
                   .select("user_id, auto_slip_type, auto_slip_legs, auto_slip_min_prob")
                   .eq("auto_backtest", True).execute()).data or []

    from engine.constants import BREAK_EVEN
    from engine.backtest import BacktestLogger
    MAX_PER_USER = 20
    results = []
    for row in users:
        uid = row["user_id"]
        slip_type = row.get("auto_slip_type") or "Power"
        try:
            n_legs = int(row.get("auto_slip_legs") or 6)
        except (TypeError, ValueError):
            n_legs = 6
        n_legs = max(2, min(6, n_legs))
        raw_min = row.get("auto_slip_min_prob")
        try:
            min_prob = float(raw_min) if raw_min is not None else None
        except (TypeError, ValueError):
            min_prob = None
        if min_prob is None:
            min_prob = BREAK_EVEN.get((str(n_legs), slip_type.lower()), 0.5407)

        pool = [b for b in bets_payload if float(b.get("true_prob") or 0.0) >= min_prob]
        n_before = len(pool)
        pool = [b for b in pool if not b.get("calibration_halted")]
        n_skipped = n_before - len(pool)

        bl = BacktestLogger(user_id=uid, db_client=db)
        n_logged = 0
        for _ in range(MAX_PER_USER):
            r = bl.try_log_slip(pool[:40], slip_type=slip_type, n_legs=n_legs)
            if r is None:
                break
            n_logged += 1

        results.append({
            "user_id": uid,
            "slip_type": slip_type,
            "n_legs": n_legs,
            "min_prob": min_prob,
            "pool_before_halt": n_before,
            "skipped_circuit_breaker": n_skipped,
            "pool_after_halt": len(pool),
            "slips_logged": n_logged,
        })
        logger.info(
            "auto_backtest    user=%s admin_trigger slips_logged=%d skipped_due_to_circuit_breaker=%d pool=%d",
            uid, n_logged, n_skipped, len(pool),
        )

    # Diagnostic histogram so the operator can see WHERE the re-scored bets
    # landed in probability space — explains zero-pool results when every
    # bet got shrunk below the user threshold.
    rescored_bin = {"<0.50": 0, "0.50-0.55": 0, "0.55-0.60": 0,
                    "0.60-0.65": 0, "0.65-0.70": 0, ">=0.70": 0,
                    "halted": 0, "no_raw": 0}
    for b in bets_payload:
        if b.get("calibration_halted"):
            rescored_bin["halted"] += 1; continue
        p = b.get("true_prob")
        if p is None:
            rescored_bin["no_raw"] += 1; continue
        p = float(p)
        if   p < 0.50: rescored_bin["<0.50"]    += 1
        elif p < 0.55: rescored_bin["0.50-0.55"] += 1
        elif p < 0.60: rescored_bin["0.55-0.60"] += 1
        elif p < 0.65: rescored_bin["0.60-0.65"] += 1
        elif p < 0.70: rescored_bin["0.65-0.70"] += 1
        else:          rescored_bin[">=0.70"]   += 1

    return {
        "ok": True,
        "bets_in_state": len(bets_payload),
        "use_rwbc": use_rwbc,
        "rescore": {
            "n_recalibrated":   n_rewritten,
            "n_halted_by_RWBC": n_halted_rescore,
            "histogram":        rescored_bin,
        },
        "users": results,
    }


@app.post("/api/backtest/add-slip")
def add_slip_to_backtest(req: BacktestAddSlipRequest, user: dict = Depends(get_current_user)):
    """
    Manually log a slip from the currently selected +EV bets.
    bet_ids must refer to bets currently in _state['bet_map'].
    """
    if not req.bet_ids or len(req.bet_ids) < 2 or len(req.bet_ids) > 6:
        raise HTTPException(status_code=400, detail="Slip must have 2-6 legs.")

    # Normalise the requested slip type. Anything we don't recognise is
    # stored verbatim (e.g. "Manual") so we never reject on this field.
    req_slip_type = (req.slip_type or "Manual").strip().title()
    if req_slip_type not in ("Power", "Flex", "Manual"):
        req_slip_type = "Manual"

    with _lock:
        bet_map      = _state["bet_map"]
        serialized   = _state["bets"]

    # Build a lookup of full serialized dicts (includes start_time)
    ser_map = {d["bet_id"]: d for d in serialized}

    backtest_bets = []
    missing = []
    for bid in req.bet_ids:
        d = ser_map.get(bid)
        b = bet_map.get(bid)
        if d is None or b is None:
            missing.append(bid)
            continue
        backtest_bets.append({
            "player_name":       d.get("player_name", ""),
            "league":            d.get("league", ""),
            "prop_type":         d.get("prop_type", ""),
            "pp_line":           d.get("pp_line"),
            "side":              d.get("side", "over"),
            "true_prob":         d.get("true_prob"),
            "individual_ev_pct": d.get("individual_ev_pct"),
            "start_time":        d.get("start_time", ""),
        })

    if missing:
        raise HTTPException(status_code=404, detail=f"Bet IDs not found: {missing}")

    # Within-slip dedup: a single slip cannot contain two legs on the
    # same (player, start_time) — e.g. selecting both "Player A points
    # over" and "Player A assists over" for the same game.
    seen_keys: set[tuple[str, str]] = set()
    dup_within: list[str] = []
    for b in backtest_bets:
        k = make_bet_key(b.get("player_name", ""), b.get("start_time", ""))
        if k in seen_keys:
            dup_within.append(b.get("player_name", "") or "?")
        seen_keys.add(k)
    if dup_within:
        raise HTTPException(
            status_code=400,
            detail=(
                "A slip cannot contain multiple legs on the same player+game: "
                + ", ".join(dup_within)
            ),
        )

    from engine.backtest import BacktestLogger
    _logger = BacktestLogger(user["id"], user["jwt"])

    # Cross-slip dedup: any (player, start_time) already used in another
    # slip within the last 48h blocks this whole slip. Same player in a
    # different game (different start_time) is fine.
    
    used_keys = _logger._load_used_keys_from_db()
    conflicts = _logger.find_conflicting_legs(backtest_bets, used_keys=used_keys)
    if conflicts:
        detail_parts = [
            f"{c.get('player_name', '')} @ {c.get('start_time', '')}"
            for c in conflicts
        ]
        raise HTTPException(
            status_code=409,
            detail=(
                f"{len(conflicts)} leg(s) already used in another slip "
                f"within the last 48 hours: " + ", ".join(detail_parts)
            ),
        )

    # Force the slip through — bypass the "enough bets" / EV gate by
    # calling try_log_slip with only these bets (already in correct format).
    new_slip = _logger.try_log_slip(backtest_bets, slip_type=req_slip_type)
    if new_slip is None:
        # try_log_slip may reject due to EV or already-used bets;
        # for manual adds we force-log it anyway.
        #
        # Re-check for player conflicts one more time: if another process
        # inserted a conflicting leg between our top-of-endpoint check and
        # now, we want to surface that rather than silently force-insert.
        late_used = _logger._load_used_keys_from_db()
        late_conflicts = _logger.find_conflicting_legs(backtest_bets, used_keys=late_used)
        if late_conflicts:
            detail_parts = [
                f"{c.get('player_name', '')} @ {c.get('start_time', '')}"
                for c in late_conflicts
            ]
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{len(late_conflicts)} leg(s) already used in another slip "
                    f"within the last 48 hours: " + ", ".join(detail_parts)
                ),
            )

        import uuid
        from datetime import datetime as _dt
        from engine.database import get_db

        true_probs = [float(b.get("true_prob") or 0) for b in backtest_bets]
        k = len(backtest_bets)
        # Compute the better of Power/Flex for the projected EV, but always
        # tag the row as "Manual" so the backtest view reflects user intent.
        try:
            import numpy as _np
            from engine.ev_calculator import (
                power_slip_ev, flex_slip_ev,
                power_slip_ev_corr, flex_slip_ev_corr,
            )
            from engine.correlation import build_correlation_matrix, legs_metadata_from_bets
            corr = build_correlation_matrix(legs_metadata_from_bets(backtest_bets))
            if k >= 2 and not _np.allclose(corr, _np.eye(k), atol=1e-8):
                power_ev = power_slip_ev_corr(true_probs, corr)
                flex_ev  = flex_slip_ev_corr(true_probs,  corr)
            else:
                power_ev = power_slip_ev(true_probs)
                flex_ev  = flex_slip_ev(true_probs)
            candidates = [ev for ev in (power_ev, flex_ev) if ev is not None]
            best_ev = max(candidates) if candidates else 0.0
        except Exception:
            best_ev = 0.0
        # Tag the slip with the user's requested type so the backtest view
        # reflects intent (Power / Flex) rather than a generic "Manual".
        best_type = req_slip_type

        slip_id   = str(uuid.uuid4())[:8].upper()
        timestamp = _dt.now().isoformat(timespec="seconds")
        proj_ev   = round(best_ev, 4)

        rows = []
        for i, b in enumerate(backtest_bets, start=1):
            true_p = round(float(b.get("true_prob") or 0), 4)
            rows.append({
                "leg_num":          i,
                "player":           b.get("player_name", ""),
                "league":           b.get("league", ""),
                "prop":             b.get("prop_type", ""),
                "line":             b.get("pp_line", ""),
                "side":             b.get("side", ""),
                "true_prob":        true_p,
                "ind_ev_pct":       round(float(b.get("individual_ev_pct") or 0), 4),
                "game_start":       b.get("start_time", ""),
                "closing_prob":     true_p,
                "clv_pct":          0.0,
                "result":           "pending",
                "stat_actual":      None,
            })

        # Write to Supabase
        db_client = get_db()
        if not db_client:
            raise HTTPException(status_code=500, detail="No database connection available.")

        try:
            # 1. Insert slip header
            db_client.table("slips").insert({
                "id":               slip_id,
                "timestamp":        timestamp,
                "slip_type":        best_type,
                "n_legs":           k,
                "proj_slip_ev_pct": proj_ev
            }).execute()
            # 2. Insert legs
            from engine.backtest import make_leg_fingerprint
            db_legs = []
            for r in rows:
                db_legs.append({
                    "slip_id":      slip_id,
                    "leg_num":      r["leg_num"],
                    "player":       r["player"],
                    "league":       r["league"],
                    "prop":         r["prop"],
                    "line":         r["line"],
                    "side":         r["side"],
                    "true_prob":    r["true_prob"],
                    "ind_ev_pct":   r["ind_ev_pct"],
                    "game_start":   r["game_start"] if r["game_start"] else None,
                    "closing_prob": r["closing_prob"],
                    "clv_pct":      r["clv_pct"],
                    "result":       r["result"],
                    "stat_actual":  r["stat_actual"],
                    # See engine/backtest.py — same partial-unique-index
                    # backstop. The manual add-slip path historically had
                    # no dedup at all; the DB-level constraint now blocks
                    # both auto and manual paths uniformly.
                    "dedup_key":    make_leg_fingerprint(
                        r["player"], r["prop"], r["line"], r["side"], r["game_start"] or "",
                    ),
                })
            try:
                db_client.table("legs").insert(db_legs).execute()
            except Exception as legs_exc:
                err_msg = str(legs_exc)
                # Pre-migration_008 schema: drop dedup_key and retry once.
                if "dedup_key" in err_msg.lower():
                    for dl in db_legs:
                        dl.pop("dedup_key", None)
                    db_client.table("legs").insert(db_legs).execute()
                # Unique-violation: another slip (auto or manual) already
                # contains a leg with this canonical fingerprint. Roll
                # back the slip header and surface a clear 409 so the UI
                # can tell the user they already have this leg.
                elif "23505" in err_msg or "duplicate key" in err_msg.lower():
                    try:
                        db_client.table("slips").delete().eq("id", slip_id).execute()
                    except Exception as cleanup_exc:
                        logger.warning(
                            "Backtest: failed to roll back orphan manual slip %s after "
                            "duplicate-leg rejection: %s", slip_id, cleanup_exc,
                        )
                    raise HTTPException(
                        status_code=409,
                        detail="One or more legs in this slip duplicate a leg already in your backtest.",
                    )
                else:
                    raise
            logger.info("Backtest: manually added slip %s to Supabase", slip_id)
        except HTTPException:
            raise
        except Exception as db_exc:
            logger.error("Backtest: manual slip write failed: %s", db_exc)
            raise HTTPException(status_code=500, detail=f"Database write failed: {db_exc}")

        # No in-memory "used" set to update — BacktestLogger reads fresh
        # conflicts from Supabase on every call via _load_used_keys_from_db(),
        # so the newly-inserted legs are picked up automatically on the next
        # dedup check.

        new_slip = {
            "slip_id":          slip_id,
            "timestamp":        timestamp,
            "slip_type":        best_type,
            "n_legs":           k,
            "proj_slip_ev_pct": proj_ev,
            "legs": [
                {
                    "player":     r["player"],
                    "league":     r["league"],
                    "prop":       r["prop"],
                    "line":       r["line"],
                    "side":       r["side"],
                    "true_prob":  r["true_prob"],
                    "ind_ev_pct": r["ind_ev_pct"],
                    "game_start": r["game_start"],
                    "closing_prob": r["closing_prob"],
                    "clv_pct":      r["clv_pct"],
                }
                for r in rows
            ],
        }

    with _lock:
        _state["latest_slip"] = new_slip

    _invalidate_analytics_cache(user["id"])
    logger.info("Manual backtest slip logged: %s (%d legs)", new_slip["slip_id"], new_slip["n_legs"])
    return {"slip": new_slip}


@app.delete("/api/backtest/slip/{slip_id}")
def delete_backtest_slip(slip_id: str, user: dict = Depends(get_current_user)):
    """Delete a slip and its legs from the user's backtest history."""
    from engine.database import get_user_db
    db = get_user_db(user["jwt"])
    if not db:
        raise HTTPException(status_code=500, detail="No database connection.")

    try:
        # Verify the slip belongs to this user
        check = db.table("slips").select("id").eq("id", slip_id).execute()
        if not check.data:
            raise HTTPException(status_code=404, detail="Slip not found.")

        # Delete legs first (foreign key), then slip header
        db.table("legs").delete().eq("slip_id", slip_id).execute()
        db.table("slips").delete().eq("id", slip_id).execute()

        _invalidate_analytics_cache(user["id"])
        logger.info("Backtest: deleted slip %s for user %s", slip_id, user["id"])
        return {"status": "deleted", "slip_id": slip_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Backtest: delete slip failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Delete failed: {e}")


# ── Admin / diagnostics moved to web/routers/admin.py ─────────────────────


# ── Market Observatory Endpoints ───────────────────────────────────────────

# Per-league row caps. The earlier endpoint took the 100 newest resolved +
# 100 newest pending GLOBALLY — which meant a high-volume league (NBA)
# would crowd out every NHL row in-season and every NHL row would be
# absent off-season too, since NHL never makes the global top-N by
# created_at. The UI's Market Feed (Verified Lines) table then showed
# "no NHL" not because the observatory wasn't logging NHL — it WAS — but
# because the API capped the response before NHL could surface.
#
# Per-league fetch: each (league × result-bucket) gets its own cap, so
# low-volume leagues are guaranteed visibility. Total ceiling is large
# enough to be useful for the UI but bounded so a single endpoint hit
# can't OOM the 512 MB tier.
_OBS_PER_LEAGUE_PER_BUCKET_LIMIT = 200          # per league × {resolved, pending}
_OBS_RESPONSE_HARD_CAP           = 3000         # overall safety net

_OBS_TRACKED_LEAGUES = ("NBA", "WNBA", "MLB", "NHL", "NCAAB")


def _fetch_observatory_for_league(db, league: str, *, resolved: bool,
                                  limit: int) -> list[dict]:
    """One DB call: latest `limit` rows of (league, resolved-or-pending),
    ordered most-recent first. Pulled into a helper so the per-league
    loop is uniform across resolved/pending and easier to test."""
    q = db.table("market_observatory").select("*").eq("league", league)
    if resolved:
        q = q.neq("result", "pending")
    else:
        q = q.eq("result", "pending")
    res = q.order("created_at", desc=True).limit(limit).execute()
    return res.data or []


@app.get("/api/observatory")
def get_observatory_data(league: Optional[str] = None):
    """Returns the latest observations from the market_observatory table.

    Per-league round-robin: each tracked league gets up to
    `_OBS_PER_LEAGUE_PER_BUCKET_LIMIT` resolved AND that many pending
    rows. Low-volume leagues (NHL off-season, WNBA off-season) thus
    stay visible in the UI even when high-volume leagues dominate the
    global recency ordering.

    `?league=NHL` requests a single league at the full per-bucket cap —
    useful for the UI to deep-dive one league without re-pulling the
    others.
    """
    try:
        from engine.database import get_db
        db = get_db()
        if not db:
            return []

        from engine.constants import is_excluded_league
        if league and is_excluded_league(league):
            # Excluded leagues (e.g. soccer) must never return rows even
            # if an explicit query param requests them.
            return []
        leagues = [league.upper()] if league else list(_OBS_TRACKED_LEAGUES)
        per_bucket = _OBS_PER_LEAGUE_PER_BUCKET_LIMIT

        combined: list[dict] = []
        for lg in leagues:
            try:
                resolved = _fetch_observatory_for_league(db, lg, resolved=True,  limit=per_bucket)
                pending  = _fetch_observatory_for_league(db, lg, resolved=False, limit=per_bucket)
            except Exception as exc:
                logger.warning("Observatory: fetch failed for %s: %s", lg, exc)
                continue
            combined.extend(resolved)
            combined.extend(pending)
            if len(combined) >= _OBS_RESPONSE_HARD_CAP:
                # Already over the cap from one verbose league; stop
                # before doing more I/O we'll just throw away.
                break

        # Newest first across the union so the UI's "latest" ordering is
        # preserved. Truncate to the hard cap *after* the round-robin so
        # the cap doesn't bias against the last-iterated leagues.
        combined.sort(key=lambda r: r.get("created_at") or "", reverse=True)
        if len(combined) > _OBS_RESPONSE_HARD_CAP:
            combined = combined[:_OBS_RESPONSE_HARD_CAP]
        return combined
    except Exception as e:
        logger.error("API: observatory fetch error: %s", e)
        return []


@app.get("/api/observatory/coverage")
def get_observatory_coverage():
    """Per-league row counts for the observatory. Surfaces a simple
    diagnostic the UI can use to tell the user "NHL has 0 rows logged"
    vs "NHL has 47 rows logged but the API only returns 200 newest" —
    the second was the long-standing source of the "no NHL" complaint.

    Counts are CHEAP — Postgres handles `count='exact'` on a few
    thousand rows in milliseconds — and run once per request. Cached
    upstream by the dashboard tab so we're not hitting this every paint.

    Returns:
        {
          "by_league": [
            {"league": "NBA", "total": 12345, "resolved": 9000,
             "pending": 3345, "newest_at": "2026-05-14T01:00:00Z"},
            ...
          ],
          "total": 23456,
        }
    """
    try:
        from engine.database import get_db
        db = get_db()
        if not db:
            return {"by_league": [], "total": 0}

        by_league: list[dict] = []
        grand_total = 0
        for lg in _OBS_TRACKED_LEAGUES:
            try:
                # Three head-counts per league: total, resolved-only,
                # pending-only. Each runs as a separate Postgres COUNT
                # with the planner choosing an index — sub-ms on the
                # current row volume.
                total_res = (db.table("market_observatory")
                             .select("id", count="exact")
                             .eq("league", lg)
                             .limit(1)
                             .execute())
                total = total_res.count or 0
                pending_res = (db.table("market_observatory")
                               .select("id", count="exact")
                               .eq("league", lg)
                               .eq("result", "pending")
                               .limit(1)
                               .execute())
                pending = pending_res.count or 0
                resolved = max(0, total - pending)

                newest_at = None
                if total > 0:
                    newest_res = (db.table("market_observatory")
                                  .select("created_at")
                                  .eq("league", lg)
                                  .order("created_at", desc=True)
                                  .limit(1)
                                  .execute())
                    if newest_res.data:
                        newest_at = newest_res.data[0].get("created_at")

                by_league.append({
                    "league":    lg,
                    "total":     total,
                    "resolved":  resolved,
                    "pending":   pending,
                    "newest_at": newest_at,
                })
                grand_total += total
            except Exception as exc:
                logger.warning("Observatory coverage: %s failed: %s", lg, exc)
                by_league.append({
                    "league": lg, "total": 0, "resolved": 0,
                    "pending": 0, "newest_at": None,
                    "error": str(exc)[:120],
                })
        return {"by_league": by_league, "total": grand_total}
    except Exception as e:
        logger.error("API: observatory coverage error: %s", e)
        return {"by_league": [], "total": 0}

@app.get("/api/observatory/multipliers")
def get_calibration_map_api():
    """Per-(league, prop, side) calibration summary for the Observatory tab.

    For every fitted prop curve, reports the calibrated probability at the
    DISPLAY_ANCHOR (currently 0.60) — i.e. exactly what `calibrate()` would
    return for raw_prob = anchor. This is the calibration we *actually*
    apply on the bet path, surfaced per (league, prop, side) so over and
    under correction asymmetries are visible.

    Shape:

        {
          "anchor": 0.60,
          "fitted_at": "...",
          "rows": [
            {"league": "NBA", "prop": "Points",
             "over":  {"q": 0.573, "delta_pp": -2.7, "n_eff": 120.0, "calibrated": true},
             "under": {"q": 0.612, "delta_pp":  1.2, "n_eff":  95.0, "calibrated": true}}
          ]
        }

    A side reports `calibrated: false` when its prop curve is too thin to
    surface (the apply path then falls through to global, which is still
    reflected by `q`).
    """
    try:
        from engine.isotonic_calibration import (
            load_isotonic_calibration, _interp, _shrink, DISPLAY_ANCHOR,
        )
        curves = load_isotonic_calibration()
        global_level = curves.get("global")
        q_global = _interp(global_level["curve"], DISPLAY_ANCHOR) if global_level else DISPLAY_ANCHOR

        # Bucket prop curves into rows keyed by (league, prop), with over/under
        # as siblings. Mirrors the heatmap's pairing so the two panels read
        # the same way.
        rows: dict[tuple[str, str], dict] = {}
        for key, level in (curves.get("props") or {}).items():
            if not level:
                continue
            parts = key.split("|")
            if len(parts) != 3:
                continue
            league, prop, side = parts
            if side not in ("over", "under"):
                continue
            q = _shrink(q_global, level, DISPLAY_ANCHOR)
            rows.setdefault((league, prop), {"league": league, "prop": prop,
                                             "over": None, "under": None})
            rows[(league, prop)][side] = {
                "q":          round(q, 4),
                "delta_pp":   round((q - DISPLAY_ANCHOR) * 100, 2),
                "n_eff":      round(float(level.get("n_eff") or 0.0), 2),
                "calibrated": True,
            }

        out_rows = sorted(
            rows.values(),
            key=lambda r: (r["league"], r["prop"]),
        )
        return {
            "anchor":    DISPLAY_ANCHOR,
            "fitted_at": curves.get("fitted_at"),
            "rows":      out_rows,
        }
    except Exception as e:
        logger.error("API: calibration fetch error: %s", e)
        return {"anchor": 0.60, "fitted_at": None, "rows": []}


@app.get("/api/observatory/rwbc")
def get_rwbc_observatory():
    """Per-cell RWBC state for the Observatory monitoring panels.

    Returns one row per (league, prop, side) cell currently in the
    process-local cell cache, plus a small `summary` dict for the
    headline numbers above the chart. Used by:
      - Panel 2 (cell trust heatmap)  → rows[]
      - Panel 1 (calibration curves)  → derived from rows[] + observatory hits

    Mirrors what's persisted in calibration_cells; for fresh boots the
    cache may be empty until the first refit completes. In that case
    `rows` is [] and the panels render a "warming up" placeholder.
    """
    try:
        from engine import rwbc_calibration as _rwbc
        cells = _rwbc.all_cells()
        summary = _rwbc.cache_stats()
        rows = [{
            "league":            c.league,
            "prop":              c.prop,
            "side":              c.side,
            "w_cell":            round(c.w_cell, 4),
            "p_post":            round(c.p_post, 4),
            "n_eff":             round(c.n_eff, 1),
            "resolution":        round(c.resolution, 6),
            "reliability_error": round(c.reliability_error, 6),
            "mean_pred":         round(c.mean_pred, 4),
            "mean_obs":          round(c.mean_obs, 4),
            "halted":            c.w_cell < _rwbc.W_CELL_HALT_THRESHOLD,
            "last_fit_at":       c.last_fit_at.isoformat() if c.last_fit_at else None,
            "last_publish_at":   c.last_publish_at.isoformat() if c.last_publish_at else None,
        } for c in cells]
        return {
            "summary": summary,
            "rows": rows,
        }
    except Exception as exc:
        logger.error("API: RWBC observatory fetch error: %s", exc)
        return {"summary": {}, "rows": []}


@app.get("/api/observatory/rwbc/trend")
def get_rwbc_brier_trend(days: int = 30):
    """Brier-trend sparkline for Observatory Panel 3.

    Pulls the last `days` of calibration_history rows for the 'global'
    scope, returns aligned current-vs-RWBC Brier series. The plot is
    a small sparkline; ~30 points (one per refit, ~hourly) is plenty.
    """
    try:
        from engine.database import get_db as _get_db
        db = _get_db()
        if db is None:
            return {"series": []}
        # Bound the response — at hourly refits, 30 days is ~720 rows.
        limit = max(50, min(days * 24 + 50, 1200))
        res = (
            db.table("calibration_history")
              .select("fit_at, brier_current, brier_rwbc, n_settled, publish_skipped")
              .eq("scope", "global")
              .order("fit_at", desc=True)
              .limit(limit)
              .execute()
        )
        rows = list(reversed(res.data or []))  # chronological for plot
        return {"series": rows}
    except Exception as exc:
        logger.error("API: RWBC trend fetch error: %s", exc)
        return {"series": []}


@app.get("/api/calibration/brier_monitor")
def get_calibration_brier_monitor_api():
    """Rolling Brier-score monitor history.

    Each entry compares the calibrated probability vs the raw consensus
    probability over the last 7 days of resolved observations. A
    `status="regression"` entry means the calibrator made things worse
    on that snapshot — investigate before acting on +EV signals.

    Returns:
        {
          "history": [snapshot, ...],          # most recent last
          "latest":  snapshot | None,
        }
    """
    try:
        from engine.calibration_monitor import load_brier_history
        history = load_brier_history()
        return {
            "history": history,
            "latest":  history[-1] if history else None,
        }
    except Exception as e:
        logger.error("API: brier monitor fetch error: %s", e)
        return {"history": [], "latest": None}


@app.get("/api/calibration/heatmap")
def get_calibration_heatmap_api():
    """Per-(league, prop) actual hit rate sliced by 5% expected-probability
    bands. Sourced from the incremental calibration sufficient statistics so
    no extra database round-trip is needed."""
    try:
        from engine.isotonic_calibration import export_heatmap
        return export_heatmap()
    except Exception as e:
        logger.error("API: calibration heatmap fetch error: %s", e)
        return {"buckets": [], "rows": []}


@app.get("/api/calibration/curves")
def get_calibration_curves_api():
    """Full hierarchical calibration state for diagnostics / debugging.

    Returns the global, per-league, and per-(league, prop) curves with their
    effective sample sizes, plus the empirical book sharpness weights when
    available. Intended for power users; not currently surfaced in the UI.
    """
    try:
        from engine.isotonic_calibration import load_isotonic_calibration
        from engine.sharpness_calibration import load_sharpness_weights
        return {
            "isotonic":  load_isotonic_calibration(),
            "sharpness": load_sharpness_weights(),
        }
    except Exception as e:
        logger.error("API: calibration curves fetch error: %s", e)
        return {"isotonic": {}, "sharpness": {}}


# ===========================================================================
# Stripe billing
# ===========================================================================
# Subscription billing: a single "CoreProp All Access" product with monthly
# and yearly recurring prices, each with a card-required N-day free trial.
# Access to the logged-in app is gated behind an active trial/subscription
# ONLY when BILLING_ENFORCE=true — so the site keeps working until billing
# is fully configured and explicitly switched on.
import os as _os

STRIPE_SECRET_KEY      = _os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET  = _os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PUBLISHABLE_KEY = _os.getenv("STRIPE_PUBLISHABLE_KEY", "")
STRIPE_PRICE_MONTHLY   = _os.getenv("STRIPE_PRICE_MONTHLY", "")
STRIPE_PRICE_YEARLY    = _os.getenv("STRIPE_PRICE_YEARLY", "")
BILLING_TRIAL_DAYS     = int(_os.getenv("BILLING_TRIAL_DAYS", "7"))
BILLING_ENFORCE        = _os.getenv("BILLING_ENFORCE", "false").lower() == "true"
PUBLIC_BASE_URL        = _os.getenv("PUBLIC_BASE_URL", "https://coreprop.onrender.com").rstrip("/")

# Statuses Stripe reports that we treat as "access granted".
_ACTIVE_SUB_STATUSES = {"trialing", "active", "past_due"}


def _billing_configured() -> bool:
    """True when we have enough Stripe config to actually run checkout."""
    return bool(STRIPE_SECRET_KEY and STRIPE_PRICE_MONTHLY and STRIPE_PRICE_YEARLY)


def _stripe():
    """Lazy-import + key the Stripe SDK. Raises 503 if billing isn't set up."""
    if not _billing_configured():
        raise HTTPException(status_code=503, detail="Billing is not configured.")
    try:
        import stripe
    except ImportError:
        raise HTTPException(status_code=503, detail="Stripe library not installed.")
    stripe.api_key = STRIPE_SECRET_KEY
    return stripe


def _price_id_for_plan(plan: str) -> Optional[str]:
    return {"monthly": STRIPE_PRICE_MONTHLY, "yearly": STRIPE_PRICE_YEARLY}.get(plan)


def _plan_for_price_id(price_id: str) -> Optional[str]:
    if price_id == STRIPE_PRICE_MONTHLY:
        return "monthly"
    if price_id == STRIPE_PRICE_YEARLY:
        return "yearly"
    return None


def _read_user_billing(user: dict) -> dict:
    """Return {stripe_customer_id, subscription_status, subscription_plan,
    current_period_end} for the user from user_config (via service DB so it
    works even if RLS columns aren't selectable for the anon role)."""
    out = {"stripe_customer_id": None, "subscription_status": None,
           "subscription_plan": None, "current_period_end": None}
    try:
        from engine.database import get_db
        db = get_db()
        if db:
            res = db.table("user_config").select(
                "stripe_customer_id, subscription_status, subscription_plan, current_period_end"
            ).eq("user_id", user["id"]).execute()
            if res.data:
                out.update({k: res.data[0].get(k) for k in out})
    except Exception as e:
        logger.warning("billing: read user billing failed for %s: %s", user.get("id"), e)
    return out


def _upsert_user_billing(user_id: str, fields: dict):
    """Write billing fields onto the user_config row (service DB, bypasses RLS)."""
    from engine.database import get_db
    db = get_db()
    if not db:
        raise RuntimeError("No DB connection for billing upsert.")
    payload = {"user_id": user_id, **fields,
               "updated_at": datetime.now(timezone.utc).isoformat()}
    db.table("user_config").upsert(payload, on_conflict="user_id").execute()


def _find_user_id_by_customer(customer_id: str) -> Optional[str]:
    from engine.database import get_db
    db = get_db()
    if not db:
        return None
    res = db.table("user_config").select("user_id").eq(
        "stripe_customer_id", customer_id).limit(1).execute()
    return res.data[0]["user_id"] if res.data else None


def _get_or_create_customer(user: dict) -> str:
    """Return the user's Stripe customer id, creating + persisting it once."""
    existing = _read_user_billing(user).get("stripe_customer_id")
    if existing:
        return existing
    stripe = _stripe()
    cust = stripe.Customer.create(
        email=user.get("email") or None,
        metadata={"supabase_user_id": user["id"]},
    )
    _upsert_user_billing(user["id"], {"stripe_customer_id": cust.id})
    return cust.id


def _user_has_access(user: Optional[dict]) -> bool:
    """Access policy. When enforcement is off (or billing unconfigured), always
    grant. When on, require an active/trialing/past_due subscription, OR a
    canceled sub still inside its paid period."""
    if not BILLING_ENFORCE or not _billing_configured():
        return True
    if not user:
        return False
    b = _read_user_billing(user)
    status = (b.get("subscription_status") or "").lower()
    if status in _ACTIVE_SUB_STATUSES:
        return True
    # Canceled but still within the paid period → keep access until it ends.
    cpe = b.get("current_period_end")
    if status == "canceled" and cpe:
        try:
            return datetime.fromisoformat(str(cpe).replace("Z", "+00:00")) > datetime.now(timezone.utc)
        except Exception:
            return False
    return False


@app.get("/api/billing/config")
def billing_config():
    """Public billing config the frontend needs to render the pricing CTA."""
    return {
        "enabled":        _billing_configured(),
        "enforce":        BILLING_ENFORCE,
        "publishable_key": STRIPE_PUBLISHABLE_KEY,
        "price_monthly":  STRIPE_PRICE_MONTHLY,
        "price_yearly":   STRIPE_PRICE_YEARLY,
        "trial_days":     BILLING_TRIAL_DAYS,
    }


@app.get("/api/billing/status")
def billing_status(user: dict = Depends(get_current_user)):
    """The signed-in user's subscription state + whether the app is unlocked."""
    b = _read_user_billing(user)
    return {
        "status":             b.get("subscription_status"),
        "plan":               b.get("subscription_plan"),
        "current_period_end": b.get("current_period_end"),
        "active":             _user_has_access(user),
        "enforce":            BILLING_ENFORCE,
        "configured":         _billing_configured(),
    }


class CheckoutRequest(BaseModel):
    plan: str = "monthly"   # "monthly" | "yearly"


@app.post("/api/billing/checkout")
def billing_checkout(req: CheckoutRequest, user: dict = Depends(get_current_user)):
    """Create a Stripe Checkout Session for a card-required trial subscription
    and return its hosted URL for the client to redirect to."""
    stripe = _stripe()
    price_id = _price_id_for_plan(req.plan)
    if not price_id:
        raise HTTPException(status_code=400, detail="Unknown plan.")

    customer_id = _get_or_create_customer(user)
    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            customer=customer_id,
            line_items=[{"price": price_id, "quantity": 1}],
            subscription_data={
                "trial_period_days": BILLING_TRIAL_DAYS,
                "metadata": {"supabase_user_id": user["id"], "plan": req.plan},
            },
            # Card required up front so the trial auto-converts to paid.
            payment_method_collection="always",
            allow_promotion_codes=True,
            success_url=f"{PUBLIC_BASE_URL}/?checkout=success",
            cancel_url=f"{PUBLIC_BASE_URL}/?checkout=cancelled",
            metadata={"supabase_user_id": user["id"], "plan": req.plan},
        )
        return {"url": session.url, "id": session.id}
    except Exception as e:
        logger.error("billing: checkout create failed for %s: %s", user["id"], e)
        raise HTTPException(status_code=502, detail=f"Stripe checkout failed: {e}")


@app.post("/api/billing/portal")
def billing_portal(user: dict = Depends(get_current_user)):
    """Open the Stripe-hosted Customer Portal so the user can update payment
    method, switch plans, or cancel."""
    stripe = _stripe()
    customer_id = _read_user_billing(user).get("stripe_customer_id")
    if not customer_id:
        raise HTTPException(status_code=400, detail="No billing account yet.")
    try:
        sess = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=f"{PUBLIC_BASE_URL}/?tab=pricing",
        )
        return {"url": sess.url}
    except Exception as e:
        logger.error("billing: portal create failed for %s: %s", user["id"], e)
        raise HTTPException(status_code=502, detail=f"Stripe portal failed: {e}")


def _sync_subscription_to_db(sub: dict):
    """Persist a Stripe Subscription object onto the owning user's row."""
    customer_id = sub.get("customer")
    if not customer_id:
        return
    user_id = _find_user_id_by_customer(customer_id)
    if not user_id:
        # Fall back to the metadata stamp we set at checkout.
        user_id = (sub.get("metadata") or {}).get("supabase_user_id")
    if not user_id:
        logger.warning("billing: no user for customer %s; skipping sync", customer_id)
        return

    status = sub.get("status")
    # Plan label from the first line item's price id.
    plan = None
    try:
        items = (sub.get("items") or {}).get("data") or []
        if items:
            plan = _plan_for_price_id(items[0]["price"]["id"])
    except Exception:
        pass
    cpe = sub.get("current_period_end")
    cpe_iso = (datetime.fromtimestamp(cpe, tz=timezone.utc).isoformat()
               if isinstance(cpe, (int, float)) else None)

    fields = {
        "stripe_customer_id":  customer_id,
        "subscription_status": status,
    }
    if plan:
        fields["subscription_plan"] = plan
    if cpe_iso:
        fields["current_period_end"] = cpe_iso
    _upsert_user_billing(user_id, fields)
    logger.info("billing: synced sub for user %s → %s (%s)", user_id, status, plan)


@app.post("/api/billing/webhook")
async def billing_webhook(request: Request):
    """Stripe webhook. Verifies the signature, then keeps subscription_status
    in sync on subscription create/update/delete and checkout completion."""
    if not _billing_configured():
        raise HTTPException(status_code=503, detail="Billing not configured.")
    stripe = _stripe()
    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")

    try:
        if STRIPE_WEBHOOK_SECRET:
            event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
        else:
            # No signing secret set yet — parse without verification (dev only).
            import json as _json
            event = _json.loads(payload.decode("utf-8"))
            logger.warning("billing: webhook signature NOT verified (no STRIPE_WEBHOOK_SECRET)")
    except Exception as e:
        logger.error("billing: webhook signature verification failed: %s", e)
        raise HTTPException(status_code=400, detail="Invalid signature.")

    etype = event.get("type", "")
    obj = (event.get("data") or {}).get("object") or {}

    try:
        if etype in ("customer.subscription.created",
                     "customer.subscription.updated",
                     "customer.subscription.deleted"):
            _sync_subscription_to_db(obj)
        elif etype == "checkout.session.completed":
            # Pull the full subscription so we record trial status + period end.
            sub_id = obj.get("subscription")
            if sub_id:
                sub = stripe.Subscription.retrieve(sub_id)
                _sync_subscription_to_db(sub)
        elif etype == "invoice.payment_failed":
            sub_id = obj.get("subscription")
            if sub_id:
                sub = stripe.Subscription.retrieve(sub_id)
                _sync_subscription_to_db(sub)
    except Exception as e:
        logger.error("billing: webhook handler error for %s: %s", etype, e)
        # 200 anyway so Stripe doesn't hammer retries on a transient DB blip;
        # the next subscription.updated event will re-sync.
        return {"received": True, "handled": False}

    return {"received": True}





# ---------------------------------------------------------------------------
# Routers extracted during the Phase-1 split (see web/routers/__init__.py for
# migration status). Each router is self-contained: it imports shared state
# from web/state.py only, never from this module. Mounted here so the
# OpenAPI schema stays unified and the lifespan / scheduler stays one place.
# ---------------------------------------------------------------------------
from web.routers import sandbox as _r_sandbox
from web.routers import admin as _r_admin

app.include_router(_r_sandbox.router)
app.include_router(_r_admin.router)
