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
    # Soccer is DEFERRED in simplify-v1: its props are single-sided ("N+"
    # milestone ladders) with no two-sided market, so conservative devig
    # can't be trusted yet (this was the product's central failure mode).
    # The scraper/parser/scoring code stays in the tree, dormant — set
    # LEAGUE_SOCCER=true once a trustworthy single-sided decision path
    # exists (v2). Defaulting OFF here is the whole "defer soccer" switch.
    "SOCCER": os.getenv("LEAGUE_SOCCER", "false").lower() == "true",
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

# ---------------------------------------------------------------------------
# Decision knobs. The engine computes each leg's HONEST no-vig probability —
# engine.consensus consensus_prob, the vig-stripped market consensus across
# books — and shows every leg whose chosen side is at least a coin flip,
# ranked by probability. (We do NOT gate on the pessimistic worst_case min:
# applied per-side it pushes BOTH sides of a balanced market below 50%, so
# near-50/50 PrizePicks lines would never surface — see git history.)
# ---------------------------------------------------------------------------
# Server-side floor for which legs are SENT to the +EV tab. 0.50 = the favored
# side of every matched line, so the client slider's 50%+ range actually has
# data and "best bets above 50%" appear (sorted best-first).
MIN_DISPLAY_PROB = float(os.getenv("MIN_DISPLAY_PROB", "0.50"))

# DEFAULT per-leg floor for AUTO-BACKTEST when a user hasn't set their own
# threshold (auto_slip_min_prob). Kept above the break-even so auto-logged
# slips aren't -EV coin flips — distinct from MIN_DISPLAY_PROB, which only
# controls what's shown.
DEFAULT_LEG_THRESHOLD = float(os.getenv("DEFAULT_LEG_THRESHOLD", "0.55"))

# ---------------------------------------------------------------------------
# FINDINGS.md prescriptions (analysis/FINDINGS.md, 38,788-row selection-bias
# study). Three results-validated corrections, each env-killable:
#
#   A. CELL_DROPS  — (league, side) cells where selection was proven worse
#      than random (NBA OVER -7pp, NHL OVER -26pp, NHL UNDER -19pp, WNBA
#      OVER -4pp). Never auto-logged.
#   B. SIDE_BIAS   — constant additive correction per (league, side). PP
#      shades lines toward OVERs and the devig doesn't undo it; UNDERs beat
#      their predicted prob by 5-8pp in every league, OVERs undershoot.
#      Applied to the DECISION prob only (BetResult.true_prob); the raw
#      consensus is preserved in raw_true_prob so CLV and the observatory
#      training data stay uncorrected (one ruler). Refit cadence: monthly,
#      from settled observatory rows.
#   C. AUTO_SLIP_MIN_PROB_FLOOR — hard floor under every user's auto-slip
#      threshold. FINDINGS: at the old default (0.5407) every slip size was
#      EV-negative; the model only has real edge in its top slice.
# ---------------------------------------------------------------------------
SIDE_BIAS_ENABLED = os.getenv("SIDE_BIAS_ENABLED", "true").lower() in ("true", "1", "yes")
SIDE_BIAS = {
    ("MLB",  "under"): +0.054,
    ("NBA",  "under"): +0.075,
    ("NHL",  "under"): +0.061,
    ("WNBA", "under"): +0.083,
    ("MLB",  "over"):   0.000,
    ("NBA",  "over"):  -0.031,
    ("NHL",  "over"):  -0.019,
    ("WNBA", "over"):  -0.005,
}

CELL_DROPS_ENABLED = os.getenv("CELL_DROPS_ENABLED", "true").lower() in ("true", "1", "yes")
CELL_DROPS = {
    ("NBA",  "over"),
    ("NHL",  "over"),
    ("NHL",  "under"),
    ("WNBA", "over"),
}

AUTO_SLIP_MIN_PROB_FLOOR = float(os.getenv("AUTO_SLIP_MIN_PROB_FLOOR", "0.65"))

# Juice guardrail for single-sided / milestone lines (NHL goalscorer, NBA
# double-double / first-basket alts, half-step alts). A book posting an
# extreme price like -10000 isn't telling us the true probability — it's
# protecting itself — so we refuse to derive a fair from it. If the only
# available single-sided American price is more negative than this cutoff,
# the book contributes nothing and the leg is dropped.
MAX_SINGLE_SIDED_JUICE = float(os.getenv("MAX_SINGLE_SIDED_JUICE", "-1000"))

# Hard credibility cap on any single-sided devigged probability — no one-way
# market with no two-sided market behind it deserves more trust than this.
SINGLE_SIDED_PROB_CAP = float(os.getenv("SINGLE_SIDED_PROB_CAP", "0.90"))

# Novig (no-vig peer-to-peer exchange) scraper. Public odds API, no auth.
NOVIG_ENABLED = os.getenv("NOVIG_ENABLED", "true").lower() in ("true", "1", "yes")
# Drop a Novig market when its bid/ask spread (the amount its two ask prices
# sum over 1.0) exceeds this — thin exchange liquidity = untrustworthy price.
NOVIG_MAX_SPREAD = float(os.getenv("NOVIG_MAX_SPREAD", "0.12"))

# Server
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8000"))

# Operational toggles for running a "display-only" comparison server that
# shouldn't write to shared state. When both set, the server still scrapes
# and renders +EV but skips auto-backtest logging and skips persisting its
# scrape snapshot back to Supabase (so it doesn't clobber the seed used
# by the primary server on restart).
DISABLE_AUTO_BACKTEST = os.getenv("DISABLE_AUTO_BACKTEST", "false").lower() in ("true", "1", "yes")
DISABLE_PERSISTENCE   = os.getenv("DISABLE_PERSISTENCE",   "false").lower() in ("true", "1", "yes")

# API-Football (api-sports.io) key. ESPN stays the primary soccer scorer;
# this is consulted ONLY as a fallback for stats ESPN's free World Cup feed
# doesn't expose — tackles and shots-assisted (key passes). Empty = fallback
# disabled (those two props stay pending). Free tier: 100 req/day.
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "").strip()
