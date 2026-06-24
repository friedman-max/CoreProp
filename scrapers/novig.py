"""
Novig scraper — Novig is a no-vig peer-to-peer betting EXCHANGE
(novig.com). Unlike the sportsbooks, its odds come from an order book, but
the consumer odds API is fully public (no login): the web app reads it
anonymously, so we hit the same endpoint with browser impersonation.

Data path (reverse-engineered from the logged-out web app, 2026-06):
  POST https://api.novig.us/v1/graphql   (Hasura; no auth)
  query market(where: league=<L>, status=OPEN, outcomes.available not null)
    -> per market: type (stat), strike (line), player{full_name, team},
       event{scheduled_start}, outcomes[{type: Over/Under, available}]

`available` is the best ASK probability on each side (price to take it). The
two asks sum to slightly >1 — that overround IS the bid/ask spread. We:
  - require both sides priced,
  - drop markets whose spread is too wide (illiquid / untrustworthy),
  - hand the two asks to the engine as a normal two-sided book (American
    odds); engine.consensus then devigs the spread away like any other book.

Only the stat types we can map to PrizePicks labels are emitted; futures /
awards (MVP, Cy Young, etc.) are skipped automatically.
"""
import logging
from typing import Optional

from curl_cffi import requests

from config import ACTIVE_LEAGUES, NOVIG_ENABLED, NOVIG_MAX_SPREAD
from engine.matcher import FanDuelProp
from engine.devig import prob_to_american

logger = logging.getLogger(__name__)

NOVIG_GRAPHQL = "https://api.novig.us/v1/graphql"

# Our league name -> Novig's `league` string. MLB is confirmed live; the
# others are best-effort (out of season now) and harmlessly return nothing
# if the string doesn't match.
NOVIG_LEAGUE = {
    "MLB":   "MLB",
    "NBA":   "NBA",
    "WNBA":  "WNBA",
    "NHL":   "NHL",
    "NCAAB": "NCAAB",
}

# Basketball (NBA + WNBA share identical stat types on both Novig and
# PrizePicks). Only the countable, genuinely TWO-SIDED props are mapped —
# Novig also lists single-sided milestone markets (DOUBLE_DOUBLE,
# TRIPLE_DOUBLE, FIRST_BASKET) and futures (MVP_WINNER), which are deliberately
# left out: their phantom +EV is exactly the failure mode this tool avoids, and
# FIRST_BASKET in particular sometimes prices both sides (~0.085/0.981), so it
# would slip past the spread guard if it were ever mapped.
_BASKETBALL_PROP_MAP = {
    "POINTS":                   "Points",
    "REBOUNDS":                 "Rebounds",
    "ASSISTS":                  "Assists",
    "THREE_POINTERS_MADE":      "3-PT Made",
    "POINTS_REBOUNDS_ASSISTS":  "Pts+Rebs+Asts",
    "POINTS_REBOUNDS":          "Pts+Rebs",
    # Forward-looking: not seen live yet, but Novig's naming pattern makes these
    # the expected keys; harmless if absent (only mapped types are emitted).
    "POINTS_ASSISTS":           "Pts+Asts",
    "REBOUNDS_ASSISTS":         "Rebs+Asts",
}

# Novig market `type` -> PrizePicks stat_type label (must match exactly so the
# matcher can join Novig lines to PP lines). Only mapped types are emitted, so
# futures/awards are dropped. BATTING_* are hitter stats; plain pitcher-walk
# "WALKS" is intentionally omitted to avoid colliding with batter "Walks".
NOVIG_PROP_MAP = {
    "MLB": {
        "HITS":                "Hits",
        "HOME_RUNS":           "Home Runs",
        "RBIS":                "RBIs",
        "TOTAL_BASES":         "Total Bases",
        "RUNS":                "Runs",
        "STOLEN_BASES":        "Stolen Bases",
        "SINGLES":             "Singles",
        "DOUBLES":             "Doubles",
        "TRIPLES":             "Triples",
        "BATTING_WALKS":       "Walks",
        "BATTING_STRIKEOUTS":  "Hitter Strikeouts",
        "PITCHER_STRIKEOUTS":  "Pitcher Strikeouts",
        "HITS_RUNS_RBIS":      "Hits+Runs+RBIs",
        "HITS_ALLOWED":        "Hits Allowed",
        "PITCHER_OUTS":        "Pitching Outs",
        "EARNED_RUNS":         "Earned Runs Allowed",
    },
    # WNBA confirmed live (873 open markets, 2026-06-24). NBA shares the same
    # map and is wired for when its season returns (no open Novig NBA markets now).
    "WNBA": _BASKETBALL_PROP_MAP,
    "NBA":  _BASKETBALL_PROP_MAP,
}

_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://novig.com",
    "Referer": "https://novig.com/",
}

# One compact query covers every open market for a league in a single request.
_QUERY = """query CorePropMarkets {
  market(where: {league: {_eq: "%s"}, status: {_eq: "OPEN"}, outcomes: {available: {_is_null: false}}}, limit: 8000) {
    type
    strike
    player { full_name player_competitors { competitor { short_name } } }
    event { scheduled_start }
    outcomes { type available }
  }
}"""


def _to_float(v) -> Optional[float]:
    try:
        f = float(v)
        return f if 0.0 < f < 1.0 else None
    except (TypeError, ValueError):
        return None


def _market_to_prop(m: dict, league: str, prop_map: dict) -> Optional[FanDuelProp]:
    player = m.get("player")
    if not player or not player.get("full_name"):
        return None
    stat = prop_map.get(m.get("type"))
    if not stat:
        return None
    try:
        line = float(m.get("strike"))
    except (TypeError, ValueError):
        return None

    over_ask = under_ask = None
    for o in (m.get("outcomes") or []):
        side = (o.get("type") or "").lower()
        if side == "over":
            over_ask = _to_float(o.get("available"))
        elif side == "under":
            under_ask = _to_float(o.get("available"))
    if over_ask is None or under_ask is None:
        return None

    # The two asks sum to >1; the excess is the bid/ask spread. Drop the
    # market when that spread is too wide to trust (thin liquidity).
    spread = over_ask + under_ask - 1.0
    if spread < 0 or spread > NOVIG_MAX_SPREAD:
        return None

    start = ""
    ev = m.get("event")
    if isinstance(ev, dict):
        start = ev.get("scheduled_start") or ""

    return FanDuelProp(
        league=league,
        player_name=player["full_name"].strip(),
        prop_type=stat,
        line=line,
        over_odds=int(prob_to_american(over_ask)),
        under_odds=int(prob_to_american(under_ask)),
        both_sided=True,
        start_time=start,
    )


def _scrape_league(session: requests.Session, our_league: str) -> list[FanDuelProp]:
    novig_league = NOVIG_LEAGUE.get(our_league)
    prop_map = NOVIG_PROP_MAP.get(our_league)
    if not novig_league or not prop_map:
        return []
    try:
        r = session.post(
            NOVIG_GRAPHQL,
            headers=_HEADERS,
            json={"operationName": "CorePropMarkets", "query": _QUERY % novig_league},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        logger.error("Novig [%s] error: %s", our_league, exc)
        return []
    if data.get("errors"):
        logger.error("Novig [%s] graphql errors: %s", our_league, data["errors"][:2])
        return []

    markets = (data.get("data") or {}).get("market") or []
    out: list[FanDuelProp] = []
    for m in markets:
        prop = _market_to_prop(m, our_league, prop_map)
        if prop is not None:
            out.append(prop)
    logger.info("Novig [%s]: %d player props (from %d markets)", our_league, len(out), len(markets))
    return out


def scrape_novig(active_leagues: dict | None = None) -> list[FanDuelProp]:
    """Fetch Novig player-prop lines for the active, supported leagues."""
    if not NOVIG_ENABLED:
        return []
    target = active_leagues if active_leagues is not None else ACTIVE_LEAGUES
    leagues = [lg for lg, on in target.items() if on and lg in NOVIG_LEAGUE and lg in NOVIG_PROP_MAP]
    if not leagues:
        return []

    all_props: list[FanDuelProp] = []
    with requests.Session(impersonate="chrome") as session:
        for lg in leagues:
            all_props.extend(_scrape_league(session, lg))
    if not all_props:
        logger.warning("Novig: 0 props across leagues %s", leagues)
    return all_props
