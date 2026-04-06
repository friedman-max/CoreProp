"""
PrizePicks scraper using their public (undocumented) API.
No authentication required.

Strategy: fetch ALL projections in one paginated call (no league_id filter).
PP returns everything in a single page of ~16k rows, so we get all leagues
in 1 HTTP request instead of 4 separate calls with 40-second sleeps between
them. League filtering is done client-side using the `included` league map.
"""
import logging
import time
import uuid
from typing import Optional

from curl_cffi import requests

from config import PRIZEPICKS_LEAGUE_IDS, ACTIVE_LEAGUES
from engine.matcher import PrizePickLine

logger = logging.getLogger(__name__)

PP_BASE = "https://partner-api.prizepicks.com/projections"

# Invert: league_id (int) → league_name so we can filter included projections
_ID_TO_LEAGUE: dict[str, str] = {str(v): k for k, v in PRIZEPICKS_LEAGUE_IDS.items()}


def _request_with_retry(
    session: requests.Session, method: str, url: str, **kwargs
) -> requests.Response:
    """Make an HTTP request with retries on 429/403."""
    max_retries = 3
    base_delay = 10
    for attempt in range(max_retries):
        try:
            resp = session.request(method, url, **kwargs)
            if resp.status_code in (429, 403):
                delay = base_delay * (3 ** attempt)
                logger.warning(
                    "PrizePicks %d – retrying in %d s…", resp.status_code, delay
                )
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        except Exception as exc:
            if attempt == max_retries - 1:
                raise
            time.sleep(base_delay)
    raise Exception("Max retries reached")


def scrape_prizepicks(active_leagues: dict | None = None) -> list[PrizePickLine]:
    """
    Fetch all PrizePicks projections in a single paginated request, then
    filter client-side to the active leagues.

    This avoids the old approach of one request-per-league with 40-second
    inter-league sleeps (required by PP's 2-req/60s WAF), which caused the
    total scrape to exceed the 60-second pipeline interval.
    """
    target = active_leagues if active_leagues is not None else ACTIVE_LEAGUES
    # Build the set of PP league IDs we actually want
    wanted_ids: set[str] = {
        str(PRIZEPICKS_LEAGUE_IDS[name])
        for name, active in target.items()
        if active and name in PRIZEPICKS_LEAGUE_IDS
    }

    if not wanted_ids:
        logger.warning("PrizePicks: no active leagues configured, skipping scrape.")
        return []

    device_id = str(uuid.uuid4())
    headers = {
        "Accept": "application/json",
        "Referer": "https://app.prizepicks.com/",
        "x-device-id": device_id,
    }

    all_lines: list[PrizePickLine] = []

    with requests.Session(impersonate="safari17_2_ios") as session:
        page = 1
        while True:
            try:
                resp = _request_with_retry(
                    session,
                    "GET",
                    PP_BASE,
                    params={"per_page": 250, "page": page},
                    headers=headers,
                    timeout=30,
                )
            except Exception as exc:
                logger.error("PrizePicks HTTP error page %d: %s", page, exc)
                break

            data = resp.json()
            projections = data.get("data", [])
            included = data.get("included", [])

            # Build player_id → name and league_id → league_name lookups
            player_map: dict[str, str] = {}
            for item in included:
                itype = item.get("type")
                if itype == "new_player":
                    pid = item.get("id", "")
                    name = item.get("attributes", {}).get("display_name", "")
                    if pid and name:
                        player_map[pid] = name

            for proj in projections:
                if proj.get("type") != "projection":
                    continue

                # ── League filter ─────────────────────────────────────────
                league_rel = (
                    proj.get("relationships", {})
                    .get("league", {})
                    .get("data", {})
                )
                proj_league_id = str(league_rel.get("id", ""))
                if proj_league_id not in wanted_ids:
                    continue
                league_name = _ID_TO_LEAGUE.get(proj_league_id, proj_league_id)

                # ── Attributes ───────────────────────────────────────────
                attrs = proj.get("attributes", {})
                stat_type = attrs.get("stat_type", "")
                line_score_raw = attrs.get("line_score")
                odds_type = attrs.get("odds_type", "standard")
                start_time = attrs.get("start_time", "")

                # Only keep standard lines (filter out demons/goblins)
                if odds_type != "standard":
                    continue

                # ── Player name ──────────────────────────────────────────
                rel = proj.get("relationships", {})
                player_rel = rel.get("new_player", {}).get("data", {})
                player_id = player_rel.get("id", proj.get("id", ""))
                player_name = player_map.get(player_id, attrs.get("description", ""))

                if not player_name or not stat_type or line_score_raw is None:
                    continue

                try:
                    line_score = float(line_score_raw)
                except (ValueError, TypeError):
                    continue

                if line_score % 1 == 0:
                    # Whole number → split into restrictive Over/Under lines
                    all_lines.append(
                        PrizePickLine(
                            league=league_name,
                            player_name=player_name,
                            stat_type=stat_type,
                            line_score=line_score + 0.5,
                            player_id=player_id,
                            start_time=start_time or "",
                            side="over",
                        )
                    )
                    all_lines.append(
                        PrizePickLine(
                            league=league_name,
                            player_name=player_name,
                            stat_type=stat_type,
                            line_score=line_score - 0.5,
                            player_id=player_id,
                            start_time=start_time or "",
                            side="under",
                        )
                    )
                else:
                    all_lines.append(
                        PrizePickLine(
                            league=league_name,
                            player_name=player_name,
                            stat_type=stat_type,
                            line_score=line_score,
                            player_id=player_id,
                            start_time=start_time or "",
                            side="both",
                        )
                    )

            # ── Pagination ───────────────────────────────────────────────
            meta = data.get("meta", {})
            total_pages = meta.get("last_page") or meta.get("total_pages") or 1
            if page >= total_pages or not projections:
                break
            page += 1
            time.sleep(2.0)  # Small pause between pages (pagination, not rate-limiting)

    # Log per-league counts
    from collections import Counter
    counts = Counter(ln.league for ln in all_lines)
    for league_name, cnt in counts.items():
        logger.info("PrizePicks [%s]: %d lines fetched", league_name, cnt)
    if not all_lines:
        logger.warning("PrizePicks: 0 lines fetched across all leagues.")

    return all_lines
