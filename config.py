"""
App-wide configuration. Edit defaults here or override via .env file.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Scraping
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"  # headed by default for Cloudflare bypass
REFRESH_INTERVAL_MINUTES = int(os.getenv("REFRESH_INTERVAL_MINUTES", "15"))

# EV filtering
MIN_INDIVIDUAL_EV_PCT = float(os.getenv("MIN_INDIVIDUAL_EV_PCT", "0.01"))  # 1%

# Leagues to scrape (set specific sports to false to exclude)
SCRAPE_ALL_LEAGUES = os.getenv("SCRAPE_ALL_LEAGUES", "false").lower() == "true"
ACTIVE_LEAGUES = {
    "NBA":   os.getenv("LEAGUE_NBA", "true").lower()   == "true",
    "WNBA":  os.getenv("LEAGUE_WNBA", "true").lower()  == "true",
    "MLB":   os.getenv("LEAGUE_MLB", "true").lower()   == "true",
    "NHL":   os.getenv("LEAGUE_NHL", "true").lower()   == "true",
    "NCAAB": os.getenv("LEAGUE_NCAAB", "true").lower() == "true",
    "SOCCER": os.getenv("LEAGUE_SOCCER", "true").lower() == "true",
}

# FanDuel URLs per league
FANDUEL_URLS = {
    "NBA":   "https://sportsbook.fanduel.com/navigation/nba",
    "WNBA":  "https://sportsbook.fanduel.com/navigation/wnba",
    "MLB":   "https://sportsbook.fanduel.com/navigation/mlb",
    "NHL":   "https://sportsbook.fanduel.com/navigation/nhl",
    "NCAAB": "https://sportsbook.fanduel.com/navigation/ncaab",
}

# PrizePicks API league IDs
PRIZEPICKS_LEAGUE_IDS = {
    "NBA":   7,
    "WNBA":  3,
    "MLB":   2,
    "NHL":   8,
    "NCAAB": 20,   # PrizePicks calls this "CBB" (ID=20); 189 is a defunct alias that returns 0
    # PrizePicks "WORLD CUP" league. The legacy "SOCCER" league (id=82) is now
    # dormant (0 projections off-season); 241 is where the live 2026 World Cup
    # player props are published. Re-point here if PP rotates the active soccer
    # league (e.g. EPL=14, EUROCUP=287) — verify with /leagues before editing.
    "SOCCER": 241,
}

# Fuzzy match threshold (0-100)
FUZZY_THRESHOLD = 91

# Single-sided vig assumption
SINGLE_SIDE_VIG = 0.070

# Server
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8000"))

# ---------------------------------------------------------------------------
# RWBC (Reliability-Weighted Bayesian Calibration) — feature flag.
#
# When true, engine/ev_calculator.py routes the calibration step through
# engine/rwbc_calibration.py instead of the hierarchical isotonic curve.
# RWBC is per-(league, prop, side) cell with a hard circuit breaker
# (w_cell < 0.20 → cell is untradeable, auto-backtester skips). When false
# the existing isotonic code path is preserved verbatim.
#
# Refit cadence + recency half-life are unchanged: RWBC consumes the same
# observation stream the isotonic refit already loads, so the existing
# hourly scheduler in web/app.py drives both code paths.
# ---------------------------------------------------------------------------
# Default flipped to true after the Phase 1A audit. RWBC's 30 hard-halted
# cells (NHL/NBA-UNDER/WNBA-UNDER) match FINDINGS.md's "destructive cells"
# exactly. Leaving it off would re-introduce the auto-backtest's logged-leg
# pollution from those cells. Set USE_RWBC=false to revert to isotonic-only.
USE_RWBC = os.getenv("USE_RWBC", "true").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# USE_RAW_CONSENSUS_ONLY — diagnostic mode. When true, engine/ev_calculator.py
# bypasses *both* isotonic AND RWBC calibrators and sets
#     true_prob = raw_true_prob
# exactly — the vig-stripped, book-sharpness-weighted consensus from
# engine.consensus.compute_true_probability(), with no model layer on top.
#
# Useful for A/B comparison against the calibrated paths: run two servers
# side-by-side on different ports, one with USE_RWBC=true and one with
# USE_RAW_CONSENSUS_ONLY=true, and diff the +EV tables.
#
# Takes precedence over USE_RWBC if both are set.
# ---------------------------------------------------------------------------
USE_RAW_CONSENSUS_ONLY = os.getenv("USE_RAW_CONSENSUS_ONLY", "false").lower() in ("true", "1", "yes")

# Operational toggles for running a "display-only" comparison server that
# shouldn't write to shared state. When both set, the server still scrapes
# and renders +EV but skips auto-backtest logging and skips persisting its
# scrape snapshot back to Supabase (so it doesn't clobber the seed used
# by the primary server on restart).
DISABLE_AUTO_BACKTEST = os.getenv("DISABLE_AUTO_BACKTEST", "false").lower() in ("true", "1", "yes")
DISABLE_PERSISTENCE   = os.getenv("DISABLE_PERSISTENCE",   "false").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# Maybe Cool Fix experimental toggles. Each is OFF by default; the Phase 3
# strategy comparison logger runs the same model with these flipped ON to
# measure the experimental delta against the Holy Fix baseline.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# SHARP ANCHOR — the OddsJam method. When true, the decision path is ONLY:
#   fair = devig(Pinnacle two-sided market); tradeable iff fair >= SHARP_MIN_PROB.
# All calibrators (isotonic / RWBC / beta), sharpness weights, worst-case
# minimums, and tier routing are BYPASSED for decisions. No Pinnacle price
# means the leg is display-only and never auto-logged.
# Validated on 40k settled rows: pin>=0.56 hits 60.5% (UNDER 65.6%) vs the
# 54.2% 6-Flex break-even; pin>=0.54 hits 53.7% (below BE) — the floor is
# load-bearing, never lower it for volume.
# ---------------------------------------------------------------------------
USE_SHARP_ANCHOR = os.getenv("USE_SHARP_ANCHOR", "true").lower() in ("true", "1", "yes")
SHARP_MIN_PROB = float(os.getenv("SHARP_MIN_PROB", "0.56"))

# Fair-value source for the sharp engine. Backtested (29 days, placeable
# 3-Power sim, real outcomes):
#   "pinnacle"   pin devig only.            7.1 legs/d, 60.5% hit, +25.0u  (default)
#   "hybrid"     pin first; rows Pinnacle doesn't price fall back to the
#                worst-case-across-books fair, UNDER side only.
#                11.2 legs/d, 59.5% hit, +11.0u  (more volume, diluted ROI)
#   "worst_case" most conservative book + worst-case devig for everything.
#                With under-gate off this is the any-side rule: 29.9 legs/d
#                but 55.1% hit ~= exactly break-even -> realized -14.0u.
#                Kept for experimentation; not a recommended default.
# Default switched to "hybrid" (owner decision 2026-06-10): Pinnacle stays
# the primary anchor; rows Pinnacle doesn't price fall back to the
# worst-case cross-book floor, UNDER side only. Backtest: 11.2 legs/d at
# 59.5% hit vs 7.1/d at 60.5% for pure pinnacle. Set ANCHOR_MODE=pinnacle
# to revert to the strictest mode.
ANCHOR_MODE = os.getenv("ANCHOR_MODE", "hybrid").lower()
# Gate the worst-case fallback to UNDERs (PP shades OVERs; UNDER-only is what
# kept the fallback +EV in the backtest: 59.8% vs 55.1% any-side).
WORST_CASE_UNDER_ONLY = os.getenv("WORST_CASE_UNDER_ONLY", "true").lower() in ("true", "1", "yes")

# C1: anti-public-side filter using the data/anti_public_cells.json deny-list.
USE_SHADE_FILTER = os.getenv("USE_SHADE_FILTER", "false").lower() in ("true", "1", "yes")
# C2: Beta calibration with shade conditioning, replacing isotonic on top of RWBC.
USE_BETA_CAL = os.getenv("USE_BETA_CAL", "false").lower() in ("true", "1", "yes")
# C3: Portfolio Kelly across same-day slips (joint optimization).
USE_PORTFOLIO_KELLY = os.getenv("USE_PORTFOLIO_KELLY", "false").lower() in ("true", "1", "yes")
