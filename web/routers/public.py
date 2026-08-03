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


@router.get("/coverage")
def get_coverage():
    """Coverage facts for the landing page: which books, which leagues, and how
    often the board refreshes. No user data, no auth."""
    books = list(_PRICING_BOOKS)
    if cfg.NOVIG_ENABLED:
        books.append("Novig")

    # Single-field read, but still under the lock — `interval_min` is mutated by
    # POST /api/config while the pipeline may be rebinding neighbours.
    with _lock:
        refresh_minutes = _state["interval_min"]

    return {
        "prop_source":     "PrizePicks",
        "books":           books,
        "leagues":         [lg for lg, on in cfg.ACTIVE_LEAGUES.items() if on],
        "refresh_minutes": refresh_minutes,
    }
