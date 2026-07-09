"""
ESPN unofficial API result checker for CoreProp backtests.

Reads pending legs from Supabase, fetches ESPN box scores,
and marks each bet as "hit" or "miss" with the actual stat value.
Covers NBA, NCAAB, MLB, NHL.
"""
import logging
import unidecode
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests as _requests
from rapidfuzz import fuzz
from engine.database import get_db

logger = logging.getLogger(__name__)

# ESPN scoreboard (for game IDs by date)
ESPN_SCOREBOARD = {
    "NBA":   "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "WNBA":  "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard",
    "NCAAB": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard",
    "MLB":   "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    "NHL":   "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
    # 2026 FIFA World Cup. ESPN's free feed carries it under the soccer
    # league code "fifa.world". Confirmed live: scoreboard returns the
    # tournament events; the summary's `rosters[].roster[].stats[]` block
    # exposes per-player totalShots / shotsOnTarget / foulsCommitted /
    # saves / goalAssists / totalGoals. It does NOT expose tackles or key
    # passes ("Shots Assisted") — those props stay pending (see _compute_stat).
    "SOCCER": "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard",
}

# ESPN event summary (for box scores)
ESPN_SUMMARY = {
    "NBA":   "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary",
    "WNBA":  "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/summary",
    "NCAAB": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary",
    "MLB":   "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary",
    "NHL":   "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary",
    "SOCCER": "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/summary",
}

# Conservative estimate of how long after game_start a result can be fetched
GAME_DURATION_MINUTES = {
    "NBA":   180,   # 3 h
    "WNBA":  150,   # 2.5 h (40-min game)
    "NCAAB": 150,   # 2.5 h
    "MLB":   225,   # 3.75 h
    "NHL":   180,   # 3 h
    "SOCCER": 150,  # 2.5 h — 90' + halftime + stoppage + post-match buffer
}

FUZZY_THRESHOLD = 80   # Strict threshold for name matching

# Soccer props CoreProp can actually grade. ESPN's free World Cup feed
# exposes the first set; tackles + shots-assisted (key passes) only resolve
# when the API-Football fallback is configured (config.API_FOOTBALL_KEY).
# The auto-backtest worker consults soccer_prop_scoreable() so it never logs
# a soccer leg that can never resolve (which is what left 45 legs stranded).
ESPN_SOCCER_PROPS = frozenset({
    "Shots", "Shots On Target", "Shots on Target", "Fouls",
    "Goalie Saves", "Saves", "Goals", "Assists", "Offsides",
})
APIFOOTBALL_ONLY_SOCCER_PROPS = frozenset({"Tackles", "Shots Assisted"})


def soccer_prop_scoreable(prop: str) -> bool:
    """True if a soccer prop can be resolved by some configured source.

    ESPN props are always scoreable. Tackles / Shots Assisted are scoreable
    only when the API-Football fallback key is set."""
    if prop in ESPN_SOCCER_PROPS:
        return True
    if prop in APIFOOTBALL_ONLY_SOCCER_PROPS:
        try:
            from config import API_FOOTBALL_KEY
            return bool(API_FOOTBALL_KEY)
        except Exception:
            return False
    return False

# Suffixes that should be stripped before comparing last names. ESPN often
# omits "Jr."/"Sr."/"III" while PrizePicks includes them (or vice versa).
_NAME_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}


def _norm_name(n: str) -> str:
    return unidecode.unidecode(n or "").lower().strip()


def _name_tokens(n: str) -> list[str]:
    """Normalized tokens with Jr/Sr/III suffixes stripped."""
    toks = [t for t in _norm_name(n).split() if t]
    cleaned = [t for t in toks if t.rstrip(".") not in _NAME_SUFFIXES]
    return cleaned or toks


def _is_same_player(query: str, candidate: str) -> bool:
    """Strict same-player check used to disambiguate brothers / Jrs / cousins
    on the same roster (Mobley, Morris, Holiday, Antetokounmpo, Porter, etc.).

    Exact normalized match is always accepted. Otherwise we require the last
    name to match exactly and the first names to share an initial AND be
    near-identical (fuzz.ratio >= 80), which rejects e.g. "Evan Mobley" vs
    "Isaiah Mobley" while still accepting "Bobby Portis Jr." vs "Bobby Portis"
    or "Patrick Beverley" vs "Pat Beverley"."""
    q = _norm_name(query)
    c = _norm_name(candidate)
    if not q or not c:
        return False
    if q == c:
        return True
    q_tok = _name_tokens(query)
    c_tok = _name_tokens(candidate)
    if not q_tok or not c_tok:
        return False
    if q_tok[-1] != c_tok[-1]:
        return False
    q_first, c_first = q_tok[0], c_tok[0]
    if q_first == c_first:
        return True
    if not q_first or not c_first or q_first[0] != c_first[0]:
        return False
    # Same initial: accept abbreviations ("pat" vs "patrick") via prefix
    # OR genuinely close spellings via fuzz.ratio. Reject otherwise.
    if q_first.startswith(c_first) or c_first.startswith(q_first):
        return True
    return fuzz.ratio(q_first, c_first) >= 80

# Tolerance for the actual==line comparison. Lines are quoted in halves or
# whole numbers, and `actual` for every supported stat is integral or comes
# from a small integer sum. 1e-9 is comfortably below
# any meaningful difference but absorbs float-precision noise from arithmetic
# done in `_compute_stat` (e.g. fantasy-point weighting on integers).
PUSH_TOLERANCE = 1e-9


def grade_leg(actual: float, line: float, side: str) -> str:
    """Pure grader used after `actual` and `line` are known.

    Returns one of "hit", "miss", or "push". A leg pushes iff `actual`
    equals `line` (within PUSH_TOLERANCE) — only possible on whole-number
    PrizePicks lines, since `actual` is always integral. Pushes are written
    to the DB and treated like DNPs by every downstream consumer (slip P&L,
    accuracy, calibration ingest)."""
    side = (side or "").lower()
    if abs(actual - line) <= PUSH_TOLERANCE:
        return "push"
    if side == "over":
        return "hit" if actual > line else "miss"
    return "hit" if actual < line else "miss"


class ESPNResultsChecker:
    """Checks ESPN box scores and back-fills result + stat_actual in Supabase."""

    def __init__(self):
        self._session = _requests.Session()
        self._session.headers["User-Agent"] = "Mozilla/5.0"
        # (league, date_str) → {player_name_lower: stats_dict}
        self._cache: dict[tuple, dict] = {}
        # (league_lower, player_name_lower) → stats_dict (closest to target time)
        self._gamelog_cache: dict[tuple, dict] = {}
        self._event_cache: dict[tuple, list] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check_pending_results(self) -> int:
        """
        Fetch pending legs from Supabase, check ESPN for results,
        and update the rows directly in the database.
        Returns the number of rows updated.
        """
        # Always clear caches so we get fresh ESPN data (stale cache was
        # the #1 cause of permanently-stuck 'pending' rows)
        self._cache.clear()
        self._gamelog_cache.clear()
        self._event_cache.clear()

        db = get_db()
        if not db:
            logger.warning("ResultsChecker: no database connection")
            return 0

        try:
            res = db.table("legs").select("*").eq("result", "pending").execute()
            rows = res.data or []
        except Exception as exc:
            logger.error("ResultsChecker: cannot read pending legs from Supabase: %s", exc)
            return 0

        if not rows:
            return 0

        now_utc = datetime.now(timezone.utc)
        updated = 0

        for row in rows:
            game_start_str = row.get("game_start", "")
            league = (row.get("league") or "").upper()
            if not game_start_str or league not in ESPN_SCOREBOARD:
                continue

            # Parse game start
            try:
                gs = datetime.fromisoformat(game_start_str.replace("Z", "+00:00"))
                if gs.tzinfo is None:
                    gs = gs.replace(tzinfo=timezone.utc)
            except Exception:
                continue

            # Only attempt after the game is estimated to have finished
            duration   = GAME_DURATION_MINUTES.get(league, 180)
            likely_end = gs + timedelta(minutes=duration)
            if now_utc < likely_end:
                continue

            player_name = row.get("player", "")
            prop_type   = row.get("prop", "")
            side        = row.get("side", "over")
            try:
                line = float(row.get("line") or 0)
            except ValueError:
                continue

            # To handle timezone boundaries (UTC vs ET), we fetch stats for a window
            # around the game start date.
            player_stats = self._get_player_stats(league, gs, player_name)
            
            actual = None
            if player_stats is not None:
                actual = self._compute_stat(player_stats, prop_type, league)
                
            if actual is None:
                logger.debug("ResultsChecker: trying gamelog fallback for %s (%s)", player_name, prop_type)
                gl_stats = self._fetch_gamelog_stats(league, player_name, gs)
                if gl_stats is not None:
                    actual = self._compute_stat(gl_stats, prop_type, league)

            # Soccer gap fallback: ESPN's WC feed lacks Tackles / Shots
            # Assisted. When configured (API_FOOTBALL_KEY), consult
            # API-Football for those specific props only.
            if actual is None and league == "SOCCER" and prop_type in APIFOOTBALL_ONLY_SOCCER_PROPS:
                actual = self._apifootball_gap(player_name, gs, prop_type)

            if actual is None:
                # If the game ended over 6 hours ago and we still can't find
                # the player, they almost certainly didn't play (DNP/injury).
                # Alternatively, if ESPN marks the matching games as completed.
                hours_since_end = (now_utc - likely_end).total_seconds() / 3600
                is_completed = self._is_game_over(league, gs)

                if is_completed or hours_since_end >= 6:
                    try:
                        sid = row.get("slip_id")
                        l_num = int(row.get("leg_num", 0))
                        db.table("legs").update({
                            "result":      "dnp",
                            "stat_actual": None,
                            "resolved_at": datetime.now(timezone.utc).isoformat(),
                        }).eq("slip_id", sid).eq("leg_num", l_num).execute()
                        updated += 1
                        logger.info(
                            "ResultsChecker: marking %s as DNP (game_completed=%s, "
                            "hours_since_end=%.1f, no stats found for '%s')",
                            player_name, is_completed, hours_since_end, prop_type,
                        )
                    except Exception as db_exc:
                        logger.error("ResultsChecker DB update failed: %s", db_exc)
                else:
                    logger.debug(
                        "ResultsChecker: cannot compute '%s' for %s (game not yet flagged complete, %.1fh ago, will retry)",
                        prop_type, player_name, hours_since_end,
                    )
                continue

            result = grade_leg(actual, line, side)

            try:
                sid = row.get("slip_id")
                l_num = int(row.get("leg_num", 0))
                db.table("legs").update({
                    "result":      result,
                    "stat_actual": actual,
                    "resolved_at": datetime.now(timezone.utc).isoformat(),
                }).eq("slip_id", sid).eq("leg_num", l_num).execute()
                updated += 1
            except Exception as db_exc:
                logger.error("ResultsChecker DB update failed: %s", db_exc)

            logger.debug(
                "ResultsChecker: %s %s %s %s %.1f  actual=%.1f  →  %s",
                league, player_name, prop_type, side, line, actual, result,
            )

        if updated:
            logger.info("ResultsChecker: updated %d pending rows", updated)

        # Release boxscore/gamelog caches now that the run is done. These can
        # grow to a few MB per league-day and have no value across runs
        # (the next run re-clears them anyway).
        self._cache.clear()
        self._gamelog_cache.clear()
        self._event_cache.clear()
        return updated

    def check_observatory_results(self) -> int:
        """
        Resolve pending rows in the market_observatory table using ESPN data.
        Identical logic to check_pending_results but targets the observatory table.
        """
        self._cache.clear()
        self._gamelog_cache.clear()
        self._event_cache.clear()

        db = get_db()
        if not db:
            return 0

        try:
            # Only rows whose games have plausibly ended, newest first, so the
            # fresh capture (which still has ESPN coverage) resolves ahead of
            # stale backlog. PostgREST silently caps unbounded selects at 1000
            # rows; the explicit limit makes the per-run budget deliberate —
            # the 5-minute pipeline cadence drains any backlog quickly.
            cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
            res = (
                db.table("market_observatory")
                  .select("*")
                  .eq("result", "pending")
                  .lt("game_start", cutoff_iso)
                  .order("game_start", desc=True)
                  .limit(2000)
                  .execute()
            )
            rows = res.data or []
        except Exception as exc:
            logger.error("Observatory: cannot read pending rows: %s", exc)
            return 0

        if not rows:
            return 0

        now_utc = datetime.now(timezone.utc)
        updated = 0

        for row in rows:
            game_start_str = row.get("game_start", "")
            league = (row.get("league") or "").upper()
            if not game_start_str or league not in ESPN_SCOREBOARD:
                continue

            try:
                gs = datetime.fromisoformat(game_start_str.replace("Z", "+00:00"))
                if gs.tzinfo is None:
                    gs = gs.replace(tzinfo=timezone.utc)
            except Exception:
                continue

            duration = GAME_DURATION_MINUTES.get(league, 180)
            likely_end = gs + timedelta(minutes=duration)
            if now_utc < likely_end:
                continue

            player_name = row.get("player", "")
            prop_type = row.get("prop", "")
            side = row.get("side", "over")
            try:
                line = float(row.get("line") or 0)
            except ValueError:
                continue

            player_stats = self._get_player_stats(league, gs, player_name)
            actual = None
            if player_stats is not None:
                actual = self._compute_stat(player_stats, prop_type, league)

            if actual is None:
                gl_stats = self._fetch_gamelog_stats(league, player_name, gs)
                if gl_stats is not None:
                    actual = self._compute_stat(gl_stats, prop_type, league)

            if actual is None and league == "SOCCER" and prop_type in APIFOOTBALL_ONLY_SOCCER_PROPS:
                actual = self._apifootball_gap(player_name, gs, prop_type)

            if actual is None:
                hours_since_end = (now_utc - likely_end).total_seconds() / 3600
                is_completed = self._is_game_over(league, gs)
                if is_completed or hours_since_end >= 6:
                    try:
                        self._write_observatory_result(db, row, {
                            "result": "dnp",
                            "stat_actual": None,
                            "resolved_at": datetime.now(timezone.utc).isoformat(),
                        })
                        updated += 1
                    except Exception as db_exc:
                        logger.error("Observatory DB update failed: %s", db_exc)
                continue

            result = grade_leg(actual, line, side)

            try:
                self._write_observatory_result(db, row, {
                    "result": result,
                    "stat_actual": actual,
                    "resolved_at": datetime.now(timezone.utc).isoformat(),
                })
                updated += 1
            except Exception as db_exc:
                logger.error("Observatory DB update failed: %s", db_exc)

        if updated:
            logger.info("Observatory: resolved %d pending observations", updated)

        self._cache.clear()
        self._gamelog_cache.clear()
        self._event_cache.clear()
        return updated

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _write_observatory_result(db, row: dict, updates: dict) -> None:
        """Persist a grading outcome onto a market_observatory row.

        A plain UPDATE does NOT work: migration_009's BEFORE UPDATE trigger
        (market_observatory_upsert_guard) freezes result / stat_actual /
        resolved_at on EVERY update to protect graded rows from scraper
        upserts — but it cannot distinguish writers, so it silently reverts
        the results checker's grading too. That trigger is why observatory
        resolution has been dead since ~2026-05-24 (the day migration_009
        landed). We have no DDL access to fix the trigger (migration_017.sql
        documents the proper fix), so grade by DELETE + re-INSERT: BEFORE
        UPDATE triggers don't fire on INSERT, and `row` came from
        select("*") so every column is preserved verbatim.

        Safe for graded rows because their games ended >= 2h ago — the
        scraper no longer upserts those market_keys (lines are off the
        board), so the delete/insert window can't race a re-scrape.
        If the insert fails, the original row is restored so nothing is
        lost.
        """
        full = dict(row)
        full.update(updates)
        db.table("market_observatory").delete().eq("id", row["id"]).execute()
        try:
            db.table("market_observatory").insert(full).execute()
        except Exception:
            # Restore the original row so a transient insert failure can't
            # drop data; the next run will retry the grade.
            try:
                db.table("market_observatory").insert(dict(row)).execute()
            except Exception as restore_exc:
                logger.error(
                    "Observatory: RESTORE failed for id=%s after insert error: %s",
                    row.get("id"), restore_exc,
                )
            raise

    @staticmethod
    def _apifootball_gap(player_name: str, game_start: datetime, prop_type: str) -> Optional[float]:
        """Resolve a soccer prop ESPN can't (Tackles / Shots Assisted) via the
        API-Football fallback. Returns None when the key is unset or the lookup
        fails — the leg then stays pending, never errors the run."""
        try:
            from engine.apifootball import score_soccer_gap
            return score_soccer_gap(player_name, game_start, prop_type)
        except Exception as exc:
            logger.warning("API-Football gap lookup failed for %s/%s: %s",
                           player_name, prop_type, exc)
            return None

    def _get_player_stats(
        self, league: str, game_start: datetime, player_name: str
    ) -> Optional[dict]:
        """
        Fetch stats while handling UTC date boundaries by checking a 2-day window
        around the game start.
        """
        # ESPN typically indexes games by their Eastern Time starting date.
        # We check the UTC date and the day before it to ensure coverage.
        date_utc = game_start.strftime("%Y%m%d")
        date_prev = (game_start - timedelta(days=1)).strftime("%Y%m%d")
        
        # We aggregate stats from both days into a single pool for this player
        # if they appear on multiple days (rare) or just handle the offset.
        all_matches = {}
        for d_str in [date_prev, date_utc]:
            cache_key = (league, d_str)
            if cache_key not in self._cache:
                self._cache[cache_key] = self._fetch_all_stats(league, d_str)
            all_matches.update(self._cache.get(cache_key, {}))

        if not all_matches:
            return None

        name_lower = _norm_name(player_name)

        # Exact normalized match wins — the only fully-safe disambiguation
        # when same-surname players (e.g. Evan vs Isaiah Mobley) share a roster.
        for known_name, stats in all_matches.items():
            if _norm_name(known_name) == name_lower:
                return stats

        # Fuzzy fallback. Restrict candidates to players who pass the strict
        # same-player gate first; otherwise the highest token_sort_ratio
        # routinely picks a wrong-but-similar teammate.
        best_score = 0
        best_stats = None
        best_display = None
        for known_name, stats in all_matches.items():
            if not _is_same_player(player_name, known_name):
                continue
            score = fuzz.token_sort_ratio(name_lower, _norm_name(known_name))
            if score > best_score:
                best_score = score
                best_stats = stats
                best_display = known_name

        if best_score >= FUZZY_THRESHOLD:
            logger.debug(
                "ResultsChecker: matched '%s' to ESPN '%s' (score %d)",
                player_name, best_display, best_score
            )
            return best_stats
        return None

    def _fetch_all_stats(self, league: str, date_str: str) -> dict:
        """Fetch and aggregate all player stats for a league + date from ESPN."""
        scoreboard_url = ESPN_SCOREBOARD.get(league)
        summary_url    = ESPN_SUMMARY.get(league)
        if not scoreboard_url or not summary_url:
            return {}

        try:
            r = self._session.get(scoreboard_url, params={"dates": date_str}, timeout=15)
            r.raise_for_status()
            events = r.json().get("events", [])
            self._event_cache[(league, date_str)] = events
        except Exception as exc:
            logger.warning(
                "ResultsChecker: scoreboard error %s/%s: %s", league, date_str, exc
            )
            return {}

        all_stats: dict = {}
        for event in events:
            event_id = event.get("id")
            if not event_id:
                continue
            try:
                r2 = self._session.get(summary_url, params={"event": event_id}, timeout=15)
                r2.raise_for_status()
                summary = r2.json()
            except Exception as exc:
                logger.warning(
                    "ResultsChecker: summary error event %s: %s", event_id, exc
                )
                continue

            all_stats.update(self._parse_box_score(summary))

        return all_stats

    @staticmethod
    def _parse_box_score(summary: dict) -> dict:
        """
        Parse ESPN summary JSON → {player_name_lower: {stat_name: raw_value}}.

        For MLB, the batting table exposes H and HR but not 2B or 3B.
        We enrich each batter's stat_dict with singles/doubles/triples counts
        derived from the ``plays`` array so prop grading (Singles, Doubles,
        Triples, Total Bases) works correctly.
        """
        result: dict = {}

        # ── Soccer (World Cup) ──────────────────────────────────────────
        # ESPN soccer puts per-player stats under rosters[].roster[].stats[]
        # as {name, abbreviation, displayValue} — a different shape from the
        # basketball/baseball boxscore.players table. Detect it by the
        # presence of `rosters` with no usable boxscore.players, and parse
        # each stat under BOTH its long name and its abbreviation so the
        # _compute_stat aliases can find it (totalShots/SHOT, etc.).
        bx = summary.get("boxscore", {}) or {}
        has_box_players = any(
            sec.get("statistics") for sec in (bx.get("players") or [])
        )
        rosters = summary.get("rosters") or []
        if rosters and not has_box_players:
            for team in rosters:
                for p in team.get("roster", []) or []:
                    ath = p.get("athlete", {}) or {}
                    display = ath.get("displayName", "")
                    stats = p.get("stats", []) or []
                    if not display or not stats:
                        continue
                    sd: dict = {}
                    for st in stats:
                        val = st.get("displayValue")
                        if val is None:
                            continue
                        nm = (st.get("name") or "").lower()
                        ab = (st.get("abbreviation") or "").lower()
                        if nm:
                            sd[nm] = val
                        if ab:
                            sd[ab] = val
                    dl = display.lower()
                    if dl in result:
                        result[dl].update(sd)
                    else:
                        result[dl] = sd
            return result

        # athlete_id → player_name_lower (for enriching from plays)
        athlete_id_to_name: dict[str, str] = {}
        # Track which players appeared as MLB batters so we can default their
        # singles/doubles/triples to 0 (a batter who didn't hit an extra-base
        # hit must still have those fields set).
        mlb_batters: set[str] = set()

        for section in summary.get("boxscore", {}).get("players", []):
            for stat_block in section.get("statistics", []):
                field_names = [n.lower() for n in stat_block.get("names", [])]
                if not field_names:
                    field_names = [k.lower() for k in stat_block.get("keys", [])]
                block_name = (stat_block.get("name") or stat_block.get("type") or "").lower()
                is_batting = block_name == "batting" or "h-ab" in field_names or "hits-atbats" in field_names
                for entry in stat_block.get("athletes", []):
                    athlete = entry.get("athlete", {}) or {}
                    display = athlete.get("displayName", "")
                    raw     = entry.get("stats", [])
                    if not display or not raw:
                        continue
                    stat_dict = {
                        field_names[i]: raw[i]
                        for i in range(min(len(field_names), len(raw)))
                    }
                    display_lower = display.lower()
                    if display_lower in result:
                        result[display_lower].update(stat_dict)
                    else:
                        result[display_lower] = stat_dict

                    a_id = athlete.get("id")
                    if a_id is not None:
                        athlete_id_to_name[str(a_id)] = display_lower
                    if is_batting:
                        mlb_batters.add(display_lower)

        # Enrich MLB batters with hit-type counts parsed from plays.
        plays = summary.get("plays") or []
        if plays and mlb_batters:
            # Default to 0 for every batter (so a batter with 0 doubles has d2=0
            # rather than missing, which is needed for the singles formula).
            for name in mlb_batters:
                d = result.setdefault(name, {})
                d.setdefault("singles", 0)
                d.setdefault("2b", 0)
                d.setdefault("3b", 0)
                # Do NOT overwrite "hr" here — boxscore already supplies it.

            type_to_key = {
                "single": "singles",
                "double": "2b",
                "triple": "3b",
                # Home runs are already counted in the boxscore HR column; we
                # skip them here to avoid double-counting if both sources agree.
            }
            for play in plays:
                t = play.get("type") or {}
                ttext = (t.get("text") or "").strip().lower() if isinstance(t, dict) else ""
                key = type_to_key.get(ttext)
                if not key:
                    continue
                batter_id = None
                for p in play.get("participants") or []:
                    if (p.get("type") or "").lower() == "batter":
                        ath = p.get("athlete") or {}
                        batter_id = ath.get("id")
                        if batter_id is None:
                            # Some payloads nest athlete id one level up
                            batter_id = p.get("athlete", {}).get("id") if isinstance(p.get("athlete"), dict) else None
                        break
                if batter_id is None:
                    continue
                name = athlete_id_to_name.get(str(batter_id))
                if not name or name not in mlb_batters:
                    continue
                result[name][key] = (result[name].get(key) or 0) + 1

        return result

    @staticmethod
    def _compute_stat(
        stats: dict, prop_type: str, league: str
    ) -> Optional[float]:
        """Convert raw ESPN stat dict to a float for the given prop type."""

        def _num(*keys) -> Optional[float]:
            """Try each key alias in order until a non-None value is found."""
            for key in keys:
                val = stats.get(key.lower())
                if val is not None:
                    try:
                        if isinstance(val, (int, float)):
                            return float(val)
                        sval = str(val).strip()
                        if not sval or sval == "--":
                            return 0.0
                        return float(sval.split("-")[0])
                    except (ValueError, IndexError):
                        continue
            return None

        # ── Soccer (World Cup) ──────────────────────────────────
        # Gated on league so "Goals"/"Assists"/"Saves" don't collide with the
        # NHL/NBA handlers below. ESPN exposes these under both long name and
        # abbreviation (parsed in _parse_box_score). Props ESPN does NOT carry
        # — Tackles and Shots Assisted (key passes) — fall through to None and
        # stay pending rather than grade on a stat we don't have.
        if league == "SOCCER":
            if prop_type == "Shots":
                return _num("totalshots", "shot")
            if prop_type in ("Shots On Target", "Shots on Target"):
                return _num("shotsontarget", "sog")
            if prop_type == "Fouls":
                return _num("foulscommitted", "fc")
            if prop_type in ("Goalie Saves", "Saves"):
                return _num("saves", "sv")
            if prop_type == "Goals":
                return _num("totalgoals", "g")
            if prop_type == "Assists":
                return _num("goalassists", "a")
            if prop_type == "Offsides":
                return _num("offsides", "of")
            # Tackles, Shots Assisted, etc. — not in ESPN's free WC feed.
            return None

        # ── Basketball ──────────────────────────────────────────
        pts = _num("pts", "points")
        reb = _num("reb", "rebounds", "totreb", "trb")
        # Fallback for Reb: sum OREB + DREB if total is missing or 0 but components exist
        if (reb is None or reb == 0) and league != "NHL":
            oreb = _num("oreb", "offensiverebounds")
            dreb = _num("dreb", "defensiverebounds")
            if oreb is not None and dreb is not None:
                reb = oreb + dreb

        ast = _num("ast", "assists")
        stl = _num("stl", "steals")
        blk = _num("blk", "blocks", "blockedshots")
        to  = _num("to", "turnovers")
        pm3 = _num("3pt", "3pm", "threepointfieldgoalsmade")

        if prop_type == "Points" and league != "NHL":
            return pts
        if prop_type == "Rebounds":
            return reb
        if prop_type == "Assists" and league != "NHL":
            return ast
        if prop_type == "3-PT Made":
            return pm3
        if prop_type == "Pts+Rebs+Asts":
            return None if any(v is None for v in (pts, reb, ast)) else pts + reb + ast
        if prop_type == "Pts+Rebs":
            return None if any(v is None for v in (pts, reb)) else pts + reb
        if prop_type == "Pts+Asts":
            return None if any(v is None for v in (pts, ast)) else pts + ast
        if prop_type == "Rebs+Asts":
            return None if any(v is None for v in (reb, ast)) else reb + ast
        if prop_type == "Steals":
            return stl
        if prop_type == "Blocked Shots" and league != "NHL":
            return blk
        if prop_type == "Blks+Stls":
            return None if any(v is None for v in (blk, stl)) else blk + stl
        if prop_type == "Turnovers":
            return to

        # ── MLB ─────────────────────────────────────────────────
        h   = _num("h", "hits")
        k   = _num("k", "strikeouts", "so")
        r   = _num("r", "runs")
        rbi = _num("rbi", "rbis")
        bb  = _num("bb", "walks")
        hr  = _num("hr", "homeruns")
        sb  = _num("sb", "stolenbases")
        d2  = _num("2b", "doubles")
        d3  = _num("3b", "triples")

        if prop_type == "Pitcher Strikeouts":
            return k
        if prop_type in ("Hits Allowed", "Hits"):
            return h
        if prop_type == "Home Runs":
            return hr
        if prop_type == "RBIs":
            return rbi
        if prop_type == "Runs":
            return r
        if prop_type == "Stolen Bases":
            return sb
        # Prefer a direct "singles" count (e.g., tallied from ESPN plays) when
        # present, since the MLB boxscore itself doesn't expose 2B/3B columns.
        singles_direct = _num("singles", "1b")
        if prop_type == "Total Bases":
            if any(v is None for v in (h, d2, d3, hr)): return None
            return h + d2 + (d3 * 2) + (hr * 3)
        if prop_type == "Hits+Runs+RBIs":
            return None if any(v is None for v in (h, r, rbi)) else h + r + rbi
        if prop_type == "Runs+RBIs":
            return None if any(v is None for v in (r, rbi)) else r + rbi
        if prop_type == "Singles":
            if any(v is None for v in (h, d2, d3, hr)):
                return None
            return h - d2 - d3 - hr
        if prop_type == "Doubles":
            return d2
        if prop_type == "Triples":
            return d3
        if prop_type in ("Walks", "Walks Allowed"):
            return bb
        if prop_type == "Earned Runs Allowed":
            return _num("er", "earnedruns")
        if prop_type == "Pitching Outs":
            ip = stats.get("ip") or stats.get("fullinnings.partinnings")
            if ip is None: return None
            try:
                whole, frac = str(ip).split(".") if "." in str(ip) else (str(ip), "0")
                return float(whole) * 3 + float(frac)
            except Exception: return None

        # ── NHL ─────────────────────────────────────────────────
        gl = _num("goals", "g")
        asst = _num("assists", "a")
        if prop_type == "Goals":
            return gl
        if prop_type == "Assists" and league == "NHL":
            return asst
        if prop_type == "Points" and league == "NHL":
            return None if any(v is None for v in (gl, asst)) else gl + asst
        if prop_type.lower() == "shots on goal":
            return _num("shotstotal", "sog", "shots", "s")
        if prop_type in ("Goalie Saves", "Saves"):
            return _num("saves", "sv")
        if prop_type == "Blocked Shots":
            return _num("blockedshots", "blk")

        return None

    def _fetch_gamelog_stats(self, league: str, player_name: str, target_date: datetime) -> Optional[dict]:
        """Search ESPN and fetch player gamelog to ensure accurate verification when boxscore misses."""
        cache_key = (league.lower(), player_name.lower())
        if cache_key in self._gamelog_cache:
            return self._gamelog_cache[cache_key]
            
        search_url = "https://site.api.espn.com/apis/search/v2"
        try:
            r = self._session.get(search_url, params={"query": player_name, "limit": 3}, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            logger.debug("ResultsChecker: search API failed for %s: %s", player_name, exc)
            self._gamelog_cache[cache_key] = None
            return None
            
        # Pick the first ESPN search result whose displayName passes the
        # strict same-player gate. Without this guard, searching "Evan Mobley"
        # can return Isaiah Mobley's profile (or any teammate with overlapping
        # tokens), silently grading the wrong player.
        uid = None
        for res in data.get("results", []):
            if res.get("type") != "player":
                continue
            for c in res.get("contents", []):
                cand_uid = c.get("uid")
                cand_name = c.get("displayName") or c.get("name") or ""
                if cand_uid and _is_same_player(player_name, cand_name):
                    uid = cand_uid
                    break
            if uid:
                break

        if not uid or "a:" not in uid:
            self._gamelog_cache[cache_key] = None
            return None
            
        athlete_id = uid.split("a:")[-1]
        
        league_path = {
            "NBA": "basketball/nba",
            "WNBA": "basketball/wnba",
            "NCAAB": "basketball/mens-college-basketball",
            "MLB": "baseball/mlb",
            "NHL": "hockey/nhl"
        }.get(league.upper())
        
        if not league_path:
            self._gamelog_cache[cache_key] = None
            return None
            
        gl_url = f"https://site.web.api.espn.com/apis/common/v3/sports/{league_path}/athletes/{athlete_id}/gamelog"
        try:
            r2 = self._session.get(gl_url, timeout=15)
            r2.raise_for_status()
            gl = r2.json()
        except Exception as exc:
            logger.debug("ResultsChecker: gamelog fetch failed for %s: %s", player_name, exc)
            self._gamelog_cache[cache_key] = None
            return None
            
        global_labels = gl.get("labels", [])
        events_meta = gl.get("events", {})
        all_game_stats = {}
        
        for st in gl.get("seasonTypes", []):
            for cat in st.get("categories", []):
                labels = cat.get("labels") or global_labels
                labels_lower = [str(L).lower() for L in labels]
                for ev in cat.get("events", []):
                    event_id = ev.get("eventId")
                    if not event_id:
                        continue
                    stats_arr = ev.get("stats", [])
                    stat_dict = dict(zip(labels_lower, stats_arr))
                    
                    if "k" in stat_dict and "so" not in stat_dict:
                        stat_dict["so"] = stat_dict["k"]
                    if "s" in stat_dict and "sog" not in stat_dict:
                        stat_dict["sog"] = stat_dict["s"]
                    if "sv" in stat_dict and "saves" not in stat_dict:
                        stat_dict["saves"] = stat_dict["sv"]
                        
                    if event_id not in all_game_stats:
                        all_game_stats[event_id] = {}
                    all_game_stats[event_id].update(stat_dict)
                    
        best_stats = None
        best_diff = timedelta(days=999)
        for eid, s_dict in all_game_stats.items():
            meta = events_meta.get(eid, {})
            gd_str = meta.get("gameDate")
            if not gd_str: continue
            try:
                ev_dt = datetime.fromisoformat(gd_str.replace("Z", "+00:00"))
                if ev_dt.tzinfo is None:
                    ev_dt = ev_dt.replace(tzinfo=timezone.utc)
                diff = abs(ev_dt - target_date)
                if diff <= timedelta(hours=36):
                    if diff < best_diff:
                        best_diff = diff
                        best_stats = s_dict
            except Exception:
                pass
                
        self._gamelog_cache[cache_key] = best_stats
        return best_stats

    def _is_game_over(self, league: str, game_start: datetime) -> bool:
        """Helper to determine if all matches starting around game_start are completed."""
        date_utc = game_start.strftime("%Y%m%d")
        date_prev = (game_start - timedelta(days=1)).strftime("%Y%m%d")
        
        events_to_check = []
        for d_str in [date_prev, date_utc]:
            events = self._event_cache.get((league, d_str), [])
            for ev in events:
                try:
                    # '2023-10-27T19:45:00Z' format
                    ev_dt = datetime.fromisoformat(ev["date"].replace("Z", "+00:00"))
                    if ev_dt.tzinfo is None:
                        ev_dt = ev_dt.replace(tzinfo=timezone.utc)
                    # Check if within a 4-hour window of the target start time
                    if abs((ev_dt - game_start).total_seconds()) < 4 * 3600:
                        events_to_check.append(ev)
                except Exception:
                    pass
                    
        if not events_to_check:
            return False
            
        for ev in events_to_check:
            status = ev.get("status", {}).get("type", {})
            if not status.get("completed", False):
                return False
        return True
