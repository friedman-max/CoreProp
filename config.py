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
# Only corrections whose SIGN replicated on out-of-sample data are applied.
# Out-of-sample check 2026-07-02 (10,976 settled rows from Jun29-Jul2 games,
# fully disjoint from the May FINDINGS fit):
#   MLB under: May +0.054, fresh +0.020  -> sign replicated; shrunk to the
#              conservative fresh estimate.
#   WNBA under: May +0.083, fresh -0.022 -> SIGN REVERSED. Zeroed.
#   WNBA over:  May -0.005, fresh +0.078 -> SIGN REVERSED. Zeroed.
#   NBA/NHL: off-season — no validation data possible until the seasons
#            resume; May-fit values zeroed rather than trusted blind. The
#            CELL_DROPS rails below still cover the catastrophic cells.
# Missing keys mean 0.0 (no correction). Refit via
# analysis/12_side_bias_refit.py — apply a cell only when its sign is stable
# across two disjoint time windows.
SIDE_BIAS = {
    ("MLB", "under"): +0.025,
}

# Default OFF: with NBA/NHL off-season, the only live cell this drops is
# WNBA-over, and excluding it (plus the 0.65 floor) starved the auto-log pool
# so nothing reached the backtest tab. Re-enable per-cell via CELL_DROPS_ENABLED=true
# once there's enough resolved WNBA volume to re-confirm the FINDINGS signal.
CELL_DROPS_ENABLED = os.getenv("CELL_DROPS_ENABLED", "false").lower() in ("true", "1", "yes")
CELL_DROPS = {
    ("NBA",  "over"),
    ("NHL",  "over"),
    ("NHL",  "under"),
    ("WNBA", "over"),
}

AUTO_SLIP_MIN_PROB_FLOOR = float(os.getenv("AUTO_SLIP_MIN_PROB_FLOOR", "0.65"))

# ---------------------------------------------------------------------------
# Isotonic recalibration map (engine.calibration_map). FINDINGS.md §2: the
# raw consensus has a SLOPE error (alpha ~= 0.49 — 2x overconfident, inverted
# on some cells) that SIDE_BIAS, a constant additive shift, cannot fix. The
# isotonic map bends raw -> realized per (league, side), correcting slope AND
# offset in one pass, so calibration extends below the 0.65 gate instead of
# relying on the threshold alone.
#
# Default OFF, per CALIBRATION_RUNBOOK discipline: a calibrator is a ruler you
# must validate before you gate on it. Enable ONLY after inspecting
# analysis/13_calibration_map_report.py on a settled window and confirming the
# per-cell reliability curves are monotone-sane and the trusted cells replicate
# out-of-sample — the same bar SIDE_BIAS clears. Fit on raw_true_prob (one
# ruler); when enabled and a cell has a TRUSTED fit the map REPLACES that
# cell's SIDE_BIAS (the curve already carries the offset — stacking would
# double-correct); cells without a trusted fit fall back to SIDE_BIAS.
CALIBRATION_MAP_ENABLED = os.getenv("CALIBRATION_MAP_ENABLED", "false").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# Per-book consensus weights (research-driven, 2026-07). The decision prob is
# a weighted mean of the per-book devigged probs. Player-prop research
# converges — from independent sources (Pikkit, Unabated, Sharp App's
# Proptimizer, Outlier) — on FanDuel being the SHARPEST US player-prop
# market-maker, and on Pinnacle being NOT the sharp anchor for props that it
# is for main markets: Pinnacle outsources its prop lines (to Swish
# Analytics), runs low limits, and charges 3-5% prop vig. CoreProp previously
# used a PLAIN UNWEIGHTED mean, which diluted FanDuel's signal with Pinnacle's
# weaker prop line. These are EDUCATED-GUESS starting weights (the "FanDuel
# 100 / others 25" recipe those tools publish, with DraftKings and the Novig
# exchange bumped for genuine US prop liquidity / no-vig pricing). VALIDATE
# against per-book CLV once settled data accrues (analysis probe), then refit.
# Kill switch: CONSENSUS_WEIGHTS_ENABLED=false reverts to the plain mean.
#
# DEFAULT FLIPPED TO false (odds-audit, 2026-07-09): the 0.65 auto-slip floor,
# the SIDE_BIAS table, and the CELL_DROPS were ALL fit against the plain
# UNWEIGHTED consensus (the May FINDINGS study). The FanDuel-heavy weighting
# was added in July on top of that calibration WITHOUT re-fitting those anchors,
# so production was gating/bias-correcting on a different ruler than the one
# every threshold was validated on — a "stale ruler" that contributes to
# realized < predicted. Reverting to the unweighted mean restores the ruler the
# thresholds were earned on (invents no new numbers). Re-enable the weighting
# ONLY after re-deriving the threshold + SIDE_BIAS on the weighted prob across
# two disjoint settled windows (analysis/12_side_bias_refit.py), the same
# sign-stability bar SIDE_BIAS already requires.
CONSENSUS_WEIGHTS_ENABLED = os.getenv("CONSENSUS_WEIGHTS_ENABLED", "false").lower() in ("true", "1", "yes")
CONSENSUS_BOOK_WEIGHTS = {
    "fanduel":    1.00,   # sharpest US player-prop maker (highest prop volume)
    "draftkings": 0.50,   # major US prop market, high volume
    "novig":      0.35,   # p2p exchange: no-vig by construction, but thin
    "pinnacle":   0.25,   # outsourced props, low limits, 3-5% vig — weakest here
}
CONSENSUS_DEFAULT_WEIGHT = float(os.getenv("CONSENSUS_DEFAULT_WEIGHT", "0.25"))

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

# Landing minigame: serve PrizePicks player headshots through
# GET /api/public/player-image. Off = the route 404s and the frontend falls
# back to its no-photo card. A kill switch rather than a feature flag: PP could
# start blocking the server-side fetch (or object to the proxying), and turning
# it off must not require a code change.
HEADSHOTS_ENABLED = os.getenv("HEADSHOTS_ENABLED", "true").lower() in ("true", "1", "yes")

# ── Web Push (iOS 16.4+ PWA notifications) ──────────────────────────────────
# VAPID keypair for Web Push. The whole feature is a NO-OP unless BOTH keys are
# set (see engine/push.is_configured), so shipping it changes nothing in
# production until you configure it — same env-gating discipline as SIDE_BIAS /
# billing. The public key is safe to expose (browsers need it to subscribe); the
# private key signs push messages and must stay server-side. Generate a keypair
# (base64url forms — verified to load in pywebpush and as the browser's
# applicationServerKey):
#   pip install py-vapid
#   python -c "import base64;from py_vapid import Vapid01;\
#     from cryptography.hazmat.primitives import serialization as s;\
#     v=Vapid01();v.generate_keys();b=lambda x:base64.urlsafe_b64encode(x).rstrip(b'=').decode();\
#     print('VAPID_PUBLIC_KEY =',b(v.public_key.public_bytes(s.Encoding.X962,s.PublicFormat.UncompressedPoint)));\
#     print('VAPID_PRIVATE_KEY =',b(v.private_key.private_numbers().private_value.to_bytes(32,'big')))"
# then set VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY / VAPID_SUBJECT in Render.
VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "")
VAPID_SUBJECT = os.getenv("VAPID_SUBJECT", "mailto:notifications@coreprop.app")

# ── APNs (native iOS app push) ──────────────────────────────────────────────
# Apple Push Notification service for the native iOS app (ios/). Web Push
# (VAPID above) only reaches the browser/PWA, so the native app needs APNs.
# The whole feature is a NO-OP unless ALL of APNS_AUTH_KEY / APNS_KEY_ID /
# APNS_TEAM_ID / APNS_BUNDLE_ID are set (see engine/push.apns_is_configured),
# so shipping it changes nothing until you configure it — same env-gating
# discipline as VAPID / SIDE_BIAS / billing.
#
# Token-based auth (no certs): create an APNs Auth Key (.p8) in the Apple
# Developer portal → Keys → enable "Apple Push Notifications service". You get a
# 10-char Key ID and the .p8 file; your Team ID is in the membership page.
#   APNS_AUTH_KEY  = the full contents of the .p8 (a PEM "-----BEGIN PRIVATE
#                    KEY----- …"). Multi-line: set via a file/secret. Stays
#                    server-side — it signs the APNs JWT (ES256).
#   APNS_KEY_ID    = the .p8 Key ID.
#   APNS_TEAM_ID   = your Apple Developer Team ID.
#   APNS_BUNDLE_ID = the app bundle id / APNs topic (default me.coreprop.app).
# The send host per device follows the token's stored `environment`
# ('production' → api.push.apple.com, 'sandbox' → api.sandbox.push.apple.com).
APNS_AUTH_KEY = os.getenv("APNS_AUTH_KEY", "")
APNS_KEY_ID = os.getenv("APNS_KEY_ID", "")
APNS_TEAM_ID = os.getenv("APNS_TEAM_ID", "")
APNS_BUNDLE_ID = os.getenv("APNS_BUNDLE_ID", "me.coreprop.app")

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
