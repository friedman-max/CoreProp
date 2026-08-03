"""FastAPI routers, one module per logical group of endpoints.

Each router exposes a single `router` attribute which `web/app.py`
mounts via `app.include_router(...)`. Routers must not import each other
— they share state through `web/state.py` only. This keeps the
include-order in `web/app.py` flexible and prevents import cycles.

Extracted:
  - admin.py       (GET /api/admin/memory; POST /api/admin/refit-calibration)
  - public.py      (GET /api/public/coverage — unauthenticated landing-page facts)

Still in web/app.py — same pattern applies, lift when touched:
  - bets, matched, bootstrap, status
  - slip builder
  - books (prizepicks, fanduel, draftkings, pinnacle)
  - backtest
  - pending-slip, check-pp-availability
  - user config / slip prefs / auto-backtest
  - auth (me, check-username)
"""
