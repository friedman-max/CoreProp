"""Sandbox endpoints: historical-strategy replay over market_observatory.

Three endpoints:

  GET  /api/sandbox/stat-types  — distinct (league, stat_type) pairs
  POST /api/sandbox/run         — replay a strategy config; result cached
  POST /api/sandbox/optimize    — threshold sweep, returns best ROI

Why this is its own router
--------------------------
- Self-contained data flow (talks to market_observatory, not to the
  scrape pipeline state). Lifting it doesn't touch the scraping locks.
- Recently churned (preload, idempotency, calibration parity), so the
  diff is fresh in head and easy to verify against the legacy callsites.
- The per-config result cache is the largest sandbox-specific concern;
  keeping it next to the endpoints that consume it makes the cache
  policy reviewable in one place.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from threading import Lock
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from engine.persistence import load_state_from_supabase, sync_state_to_supabase
from web.auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sandbox", tags=["sandbox"])


# ── Request schema ─────────────────────────────────────────────────────

class SandboxRequest(BaseModel):
    leagues:        list[str] = []
    min_prob:       float     = 0.5408
    slip_size:      int       = 6
    slip_type:      str       = "flex"
    bet_size:       float     = 1.0
    use_kelly:      bool      = False
    included_props: list[str] = []
    # Defaults reproduce v1 behavior except slip_strategy, which defaults
    # to "live_replay" so the headline numbers reflect what the live
    # system would actually have done. Per-game-cap is gone (replaced by
    # the PrizePicks ≥2-distinct-teams rule, enforced inside the tester).
    slip_strategy:  str       = "live_replay"
    start_date:     Optional[str] = None      # YYYY-MM-DD
    end_date:       Optional[str] = None
    bootstrap:      bool      = True


# ── Stat-types cache (module-local) ────────────────────────────────────
# The (league, prop) set changes slowly — only when scrapers start
# ingesting a new league or prop type, measured in days. Caching the
# result for 5 minutes avoids 22+ paginated round-trips on every load.

_stat_types_cache: dict[str, list] = {}
_stat_types_cache_lock = Lock()
_stat_types_cache_ts: float = 0.0
_STAT_TYPES_TTL_SEC = 300


@router.get("/stat-types")
def list_sandbox_stat_types(user: dict = Depends(get_current_user)):
    """Return distinct (league, stat_type) pairs from market_observatory.

    Parallel pagination (8 concurrent page fetches) with `result != pending`
    so the rows we walk are the same superset the simulation sees. Without
    parallelism the 22+ sequential round-trips timed out the client.
    """
    global _stat_types_cache, _stat_types_cache_ts
    from engine.database import get_db

    now = time.monotonic()
    with _stat_types_cache_lock:
        if _stat_types_cache and (now - _stat_types_cache_ts) < _STAT_TYPES_TTL_SEC:
            return _stat_types_cache

    db = get_db()
    if not db:
        raise HTTPException(status_code=503, detail="Database not connected")

    PAGE = 1000
    CONCURRENCY = 8
    MAX_OFFSET = 200_000

    def _fetch_page(offset: int) -> tuple[list, bool]:
        try:
            res = (
                db.table("market_observatory")
                .select("league,prop")
                .order("market_key", desc=False)
                .range(offset, offset + PAGE - 1)
                .execute()
            )
            rows = res.data or []
            return rows, len(rows) >= PAGE
        except Exception as exc:
            logger.warning("stat-types: page fetch failed at offset %d: %s", offset, exc)
            return [], False

    grouped: dict[str, set] = {}
    offset = 0
    done = False
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        while not done and offset <= MAX_OFFSET:
            batch_offsets = [offset + i * PAGE for i in range(CONCURRENCY)
                             if offset + i * PAGE <= MAX_OFFSET]
            futs = {pool.submit(_fetch_page, o): o for o in batch_offsets}
            results: dict[int, tuple[list, bool]] = {}
            for fut in as_completed(futs):
                o = futs[fut]
                try:
                    results[o] = fut.result()
                except Exception as exc:
                    logger.warning("stat-types: future failed at %d: %s", o, exc)
                    results[o] = ([], False)
                    done = True
            for o in sorted(results):
                rows, full = results[o]
                for r in rows:
                    lg = (r.get("league") or "").strip() or "Unknown"
                    prop = (r.get("prop") or "").strip()
                    if not prop:
                        continue
                    grouped.setdefault(lg, set()).add(prop)
                if not full:
                    done = True
            offset += CONCURRENCY * PAGE

    out = {lg: sorted(props) for lg, props in sorted(grouped.items())}
    with _stat_types_cache_lock:
        _stat_types_cache = out
        _stat_types_cache_ts = time.monotonic()
    return out


# ── Config & cache helpers ─────────────────────────────────────────────

def _config_from_req(req: SandboxRequest):
    from engine.strategy_tester import StrategyConfig
    return StrategyConfig(
        leagues=req.leagues,
        min_prob=req.min_prob,
        slip_size=req.slip_size,
        slip_type=req.slip_type,
        bet_size=req.bet_size,
        use_kelly=req.use_kelly,
        included_props=req.included_props,
        slip_strategy=req.slip_strategy,
        start_date=req.start_date,
        end_date=req.end_date,
        bootstrap=req.bootstrap,
    )


_SANDBOX_CACHE_TTL_SEC = 30 * 60
_SANDBOX_CACHE_PREFIX = "sandbox.result."


def _sandbox_cache_key(req: SandboxRequest) -> str:
    """Stable hash of every field that affects simulation output, plus a
    fingerprint of the current calibration. Including `fitted_at` means
    an hourly refit naturally invalidates every cached sandbox result —
    we never serve numbers computed under stale curves."""
    try:
        from engine.isotonic_calibration import load_isotonic_calibration
        cal_fitted_at = load_isotonic_calibration().get("fitted_at") or ""
    except Exception:
        cal_fitted_at = ""
    payload = {
        "leagues":        sorted(req.leagues or []),
        "min_prob":       round(float(req.min_prob), 6),
        "slip_size":      int(req.slip_size),
        "slip_type":      str(req.slip_type),
        "bet_size":       round(float(req.bet_size), 6),
        "use_kelly":      bool(req.use_kelly),
        "included_props": sorted(req.included_props or []),
        "slip_strategy":  str(req.slip_strategy),
        "start_date":     req.start_date,
        "end_date":       req.end_date,
        "bootstrap":      bool(req.bootstrap),
        "cal_fitted_at":  cal_fitted_at,
    }
    blob = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:24]


def _sandbox_cache_get(key: str) -> Optional[dict]:
    try:
        payload, ts = load_state_from_supabase(_SANDBOX_CACHE_PREFIX + key)
        if not isinstance(payload, dict) or not ts:
            return None
        cached_at = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if cached_at.tzinfo is None:
            cached_at = cached_at.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - cached_at).total_seconds()
        if age > _SANDBOX_CACHE_TTL_SEC:
            return None
        return payload
    except Exception as exc:
        logger.debug("Sandbox cache get failed for %s: %s", key, exc)
        return None


def _sandbox_cache_put(key: str, payload: dict) -> None:
    try:
        sync_state_to_supabase(_SANDBOX_CACHE_PREFIX + key, payload)
    except Exception as exc:
        logger.debug("Sandbox cache put failed for %s: %s", key, exc)


# ── Simulation endpoints ───────────────────────────────────────────────

@router.post("/run")
def run_sandbox_simulation(req: SandboxRequest, user: dict = Depends(get_current_user)):
    """Replay a strategy configuration over the resolved observatory and
    return the full result payload (summary, equity curve, slips,
    breakdowns, funnel). Per-config-hash cache with calibration-aware
    invalidation absorbs back-to-back identical requests."""
    key = _sandbox_cache_key(req)
    cached = _sandbox_cache_get(key)
    if cached is not None:
        cached.setdefault("_meta", {})["from_cache"] = True
        return cached

    from engine.strategy_tester import StrategyTester
    tester = StrategyTester()
    result = tester.run_simulation(_config_from_req(req))
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    _sandbox_cache_put(key, result)
    return result


@router.post("/optimize")
def optimize_sandbox_threshold(req: SandboxRequest, user: dict = Depends(get_current_user)):
    """Sweep min_prob thresholds and return the ROI-maximizing point.
    The sweep itself isn't cached — individual iterations hit /run's
    cache, so the next sweep is cheap automatically."""
    from engine.strategy_tester import StrategyTester
    tester = StrategyTester()
    result = tester.optimize_threshold(_config_from_req(req))
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result
