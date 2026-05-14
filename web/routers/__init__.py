"""FastAPI routers, one module per logical group of endpoints.

Each router exposes a single `router` attribute which `web/app.py`
mounts via `app.include_router(...)`. Routers must not import each other
— they share state through `web/state.py` only. This keeps the
include-order in `web/app.py` flexible and prevents import cycles.

Migration status (2026-05-14):

  Extracted:
    - sandbox.py     (POST /api/sandbox/run, /optimize; GET /stat-types)
    - admin.py       (GET /api/admin/memory; POST /api/admin/refit-calibration)

  Still in web/app.py — same pattern applies, lift when touched:
    - bets, matched, bootstrap, status
    - slip builder
    - books (prizepicks, fanduel, draftkings, pinnacle)
    - calibration, analytics, observatory
    - backtest
    - pending-slip, check-pp-availability
    - user config / slip prefs / auto-backtest
    - auth (me, check-username)
"""
