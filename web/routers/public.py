"""Unauthenticated endpoints the marketing landing page reads.

The landing page used to hardcode its numbers ("2,847 bets scanned today",
"4,200+ sharps", "58.4% backtested hit rate", "refreshed every 30 seconds").
None of them came from anywhere — they were invented, and two were provably
wrong: the scheduler's real cadence is `_state["interval_min"]` (5 min by
default, user-configurable), not 30 seconds, and the league list advertised NFL,
which `config.ACTIVE_LEAGUES` has never contained.

`/api/public/coverage` exists so the page can state coverage facts it reads
from the running process. Anything the server can't answer, the page omits
rather than guesses — see `useCoverage()` in web/static/components.jsx, shared
by both landing.jsx and pricing.jsx.

Everything here is deliberately public and carries no user data: it is the
same class of information as `/api/status`, which is already unauthenticated.
"""
from __future__ import annotations

from fastapi import APIRouter

import config as cfg
from web.state import _lock, _state

router = APIRouter(prefix="/api/public", tags=["public"])

# The books the pipeline actually scrapes for pricing, in the order the +EV
# board shows them. PrizePicks is listed separately because it is the source of
# the LINES being evaluated, not one of the books being devigged against.
# Novig is conditional on cfg.NOVIG_ENABLED.
_PRICING_BOOKS = ("FanDuel", "DraftKings", "Pinnacle")

# Novig is a peer-to-peer exchange, not a sportsbook, so a flat "4 books" count
# is imprecise once it's in the list. `price_sources` is the honest noun for the
# mixed set and is what the marketing copy counts.
_NOVIG = "Novig"


@router.get("/coverage")
def get_coverage():
    """Coverage facts for the marketing pages: which books, which leagues, how
    often the board refreshes, and the trial length. No user data, no auth."""
    books = list(_PRICING_BOOKS)
    if cfg.NOVIG_ENABLED:
        books.append(_NOVIG)

    # Single-field read, but still under the lock — `interval_min` is mutated by
    # POST /api/config while the pipeline may be rebinding neighbours.
    with _lock:
        refresh_minutes = _state["interval_min"]

    # Lazy import: web/app.py imports this module, so a top-level import would
    # cycle. Same pattern as web/routers/admin.py's `_memory_snapshot` import.
    #
    # The trial length is served rather than written into the copy because
    # BILLING_TRIAL_DAYS is env-configurable and the pricing page stated "7" in
    # six places. Setting BILLING_TRIAL_DAYS=14 would have left every one of
    # them advertising a shorter trial than Stripe actually grants.
    from web.app import BILLING_TRIAL_DAYS

    return {
        "prop_source":     "PrizePicks",
        "books":           books,
        # The noun the copy should use for len(books). "4 books" is wrong once
        # the exchange is in the list; "4 price sources" is right either way.
        "books_noun":      "price sources" if _NOVIG in books else "sportsbooks",
        "leagues":         [lg for lg, on in cfg.ACTIVE_LEAGUES.items() if on],
        "refresh_minutes": refresh_minutes,
        "trial_days":      BILLING_TRIAL_DAYS,
    }
