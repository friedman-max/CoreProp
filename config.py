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
# C1: anti-public-side filter using the data/anti_public_cells.json deny-list.
USE_SHADE_FILTER = os.getenv("USE_SHADE_FILTER", "false").lower() in ("true", "1", "yes")
# C2: Beta calibration with shade conditioning, replacing isotonic on top of RWBC.
USE_BETA_CAL = os.getenv("USE_BETA_CAL", "false").lower() in ("true", "1", "yes")
# C3: Portfolio Kelly across same-day slips (joint optimization).
USE_PORTFOLIO_KELLY = os.getenv("USE_PORTFOLIO_KELLY", "false").lower() in ("true", "1", "yes")
