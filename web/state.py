"""Shared in-process state for the FastAPI app.

Why this lives in its own module
--------------------------------
`web/app.py` historically held everything: state dicts, payload cache,
analytics cache, scheduler, helpers, every endpoint. As the file pushed
past 3,000 lines, adding a router meant scanning the whole thing for
"where does this state live, what's already using it, and what lock
guards it."

This module centralizes the mutable state so:

  * New routers can `from web.state import _state, _lock, _payload_cache`
    instead of importing from `web/app.py` (which would create a circular
    import once the routers are mounted at app-construction time).
  * The mutation contracts are documented in one place rather than
    scattered next to whatever endpoint happened to need them.
  * Tests that need to stub state can patch one module, not many.

Mutation rules
--------------
- `_state` is a plain dict shared across threads. All mutations and
  multi-field reads MUST happen inside `with _lock:`. Single-field reads
  are safe without the lock for primitive values, but any reader that
  combines multiple fields (`bets` + `last_refresh`, say) must hold the
  lock to avoid seeing a torn write.
- `_payload_cache` mutations go through `_payload_lock`, which is a
  bare `Lock` (not reentrant). Hold one lock at a time — never nest.
- `_pending_slips` and `_analytics_cache` each have their own lock for
  the same reason; the access patterns are independent and we don't
  want a slow analytics build to block a pending-slip write.

Reassignment caveat
-------------------
Other modules import the *names* `_state`, `_payload_cache`, etc. as
bindings. Rebinding (`_state = {...}`) in any module would break the
sharing. Only mutate the existing dicts in place.
"""
from __future__ import annotations

import threading

import config as cfg


# ── Locks (declared before the dicts they guard so `with _lock` reads
#    intuitively when scanning the file top-to-bottom) ──────────────────

_lock = threading.RLock()
_payload_lock = threading.Lock()
_pending_slips_lock = threading.Lock()
_analytics_cache_lock = threading.Lock()


# ── Pipeline state ─────────────────────────────────────────────────────

_state: dict = {
    "bets":          [],        # list[dict] — serialized BetResult
    "bet_map":       {},        # bet_id -> BetResult (for slip calc)
    "matches":       [],        # list[dict] — unfiltered combined lines
    "pp_player_images": {},     # pp_player_id -> PP image_url (landing minigame)
    "pp_lines":      [],        # list[dict] — raw PrizePicks lines
    "fd_lines":      [],        # list[dict] — raw FanDuel lines
    "dk_lines":      [],        # list[dict] — raw DraftKings lines
    "pin_lines":     [],        # list[dict] — raw Pinnacle lines
    "last_refresh":  None,      # datetime | None
    "next_refresh":  None,      # datetime | None
    "is_scraping":   False,
    "is_scraping_pp": False,
    "is_scraping_fd": False,
    "is_scraping_dk": False,
    "is_scraping_pin": False,
    "scrape_errors": {},        # league -> error str | None
    # Honours REFRESH_INTERVAL_MINUTES. This was hardcoded to 5, and because
    # nothing read the env var, every restart silently reset the cadence to 5
    # minutes no matter what .env said — which is what kept PrizePicks
    # rate-limiting this host (4h of "PrizePicks returned 0 lines" while a
    # cycle took 20+ min to finish, so the scrapers effectively never stopped
    # hammering). POST /api/config still overrides it at runtime; that override
    # is still not persisted, so a restart returns to this configured value
    # rather than to a magic number.
    "interval_min":  cfg.REFRESH_INTERVAL_MINUTES,
    "min_ev_pct":    -10.0,         # fallback default
    "active_leagues": dict(cfg.ACTIVE_LEAGUES),
}


# ── Pre-serialized response cache ──────────────────────────────────────
# Populated at the end of each scrape cycle so GET endpoints can return
# bytes directly without a per-request json.dumps (which on the 512 MB
# tier was the largest transient allocation). `etag` is a weak hash of
# last_refresh so clients can 304-short-circuit unchanged polls.

_payload_cache: dict = {
    "bets":      None,   # bytes | None
    "matches":   None,
    "pp_lines":  None,
    "fd_lines":  None,
    "dk_lines":  None,
    "pin_lines": None,
    "core":      None,   # bootstrap/core — bets + meta
    "etag":      None,   # str | None
}


# ── Pending-slip store ─────────────────────────────────────────────────
# token -> (slip_data, expires_at_monotonic). Written by POST
# /api/pending-slip (authenticated), read by GET /api/pending-slip from
# the browser extension at app.prizepicks.com — no auth required.

_pending_slips: dict = {}
# 15 minutes, not 5. The real flow between POST and the extension's GET can
# include signing in to PrizePicks, a location check, and a DataDome challenge,
# on top of a free-tier cold start on our side. A 5-minute window expired
# mid-login and the user got "no pending slip" with no way to tell why.
_PENDING_SLIP_TTL_SEC = 900.0


# ── Per-user analytics cache ───────────────────────────────────────────
# /api/analytics is the slowest endpoint (3 legs scans + 1 slips scan).
# The data only changes when a slip is added/resolved/deleted, so a
# short TTL makes repeat tab visits essentially free.

_analytics_cache: dict = {}       # user_id -> (monotonic_ts, data_dict)
_ANALYTICS_TTL_SEC = 300.0


def invalidate_analytics_cache(user_id: str | None = None) -> None:
    """Drop a specific user's cached analytics (after add/delete slip),
    or everyone's (pass None) if global state changed."""
    with _analytics_cache_lock:
        if user_id is None:
            _analytics_cache.clear()
        else:
            _analytics_cache.pop(user_id, None)
