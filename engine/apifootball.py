"""
API-Football (api-sports.io) fallback scorer — soccer ONLY, gaps ONLY.

ESPN stays the primary soccer scorer (engine/results_checker). This module
is consulted exclusively for the two stats ESPN's free World Cup feed does
not expose:

    Tackles        -> statistics.tackles.total
    Shots Assisted -> statistics.passes.key   (key passes / chances created)

Activation is entirely key-gated: with no API_FOOTBALL_KEY set, every call
returns None and the affected legs simply stay pending — exactly today's
behavior, no new failure mode. Free tier is 100 req/day, so calls are
cached per (date) and per (fixture) to stay well under the cap during the
World Cup.

Everything is wrapped so a network hiccup, schema drift, or quota error
can only ever yield None (leg stays pending), never an exception that
breaks the results-checker run.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

import requests
import unidecode
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

_BASE = "https://v3.football.api-sports.io"
# API-Football league id for the FIFA World Cup.
_WORLD_CUP_LEAGUE_ID = 1

# PrizePicks prop -> (statistics group, field) in the /fixtures/players payload.
_PROP_MAP = {
    "Tackles":        ("tackles", "total"),
    "Shots Assisted": ("passes", "key"),
}

_NAME_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}


def _norm(n: str) -> str:
    return unidecode.unidecode(n or "").lower().strip()


def _same_player(a: str, b: str) -> bool:
    na, nb = _norm(a), _norm(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    ta = [t for t in na.split() if t.rstrip(".") not in _NAME_SUFFIXES]
    tb = [t for t in nb.split() if t.rstrip(".") not in _NAME_SUFFIXES]
    if ta and tb and ta[-1] == tb[-1]:
        # same surname + close-enough first token
        return ta[0][:1] == tb[0][:1] and fuzz.ratio(ta[0], tb[0]) >= 70
    return fuzz.token_sort_ratio(na, nb) >= 88


class APIFootballClient:
    """Lazily-constructed, cached client. Safe to instantiate even with no
    key — it just returns None for everything."""

    def __init__(self, api_key: Optional[str] = None):
        if api_key is None:
            try:
                from config import API_FOOTBALL_KEY
                api_key = API_FOOTBALL_KEY
            except Exception:
                api_key = ""
        self.api_key = (api_key or "").strip()
        self._session = requests.Session()
        if self.api_key:
            self._session.headers["x-apisports-key"] = self.api_key
        # date_str -> [fixture dicts]; fixture_id -> {player_lower: stats[0]}
        self._fixtures_cache: dict[str, list] = {}
        self._players_cache: dict[int, dict] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def _get(self, path: str, params: dict) -> Optional[dict]:
        try:
            r = self._session.get(f"{_BASE}{path}", params=params, timeout=15)
            if r.status_code != 200:
                logger.warning("API-Football %s -> HTTP %s", path, r.status_code)
                return None
            return r.json()
        except Exception as exc:
            logger.warning("API-Football %s failed: %s", path, exc)
            return None

    def _fixtures_for_date(self, date_str: str, season: int) -> list:
        if date_str in self._fixtures_cache:
            return self._fixtures_cache[date_str]
        data = self._get("/fixtures", {
            "league": _WORLD_CUP_LEAGUE_ID, "season": season, "date": date_str,
        })
        fixtures = (data or {}).get("response", []) or []
        self._fixtures_cache[date_str] = fixtures
        return fixtures

    def _players_for_fixture(self, fixture_id: int) -> dict:
        if fixture_id in self._players_cache:
            return self._players_cache[fixture_id]
        data = self._get("/fixtures/players", {"fixture": fixture_id})
        out: dict[str, dict] = {}
        for team in (data or {}).get("response", []) or []:
            for p in team.get("players", []) or []:
                name = (p.get("player") or {}).get("name") or ""
                stats = p.get("statistics") or []
                if name and stats:
                    out[_norm(name)] = stats[0]
        self._players_cache[fixture_id] = out
        return out

    def get_player_stat(
        self, player_name: str, game_start: datetime, prop: str,
    ) -> Optional[float]:
        """Return the API-Football value for a soccer prop, or None.

        None whenever: no key, prop unsupported, fixture/player not found,
        or any error — so the caller leaves the leg pending."""
        if not self.enabled:
            return None
        group_field = _PROP_MAP.get(prop)
        if group_field is None:
            return None
        group, field = group_field

        # World Cup matches can cross the UTC date boundary; check the game's
        # date and the day before, like the ESPN path.
        season = game_start.year
        dates = [
            (game_start - timedelta(days=1)).strftime("%Y-%m-%d"),
            game_start.strftime("%Y-%m-%d"),
        ]
        for d in dates:
            for fx in self._fixtures_for_date(d, season):
                # Only spend a /players call on fixtures near this kickoff.
                fdate = ((fx.get("fixture") or {}).get("date") or "")
                try:
                    fdt = datetime.fromisoformat(fdate.replace("Z", "+00:00"))
                    if abs((fdt - game_start).total_seconds()) > 6 * 3600:
                        continue
                except Exception:
                    pass
                fid = (fx.get("fixture") or {}).get("id")
                if fid is None:
                    continue
                players = self._players_for_fixture(int(fid))
                stats = players.get(_norm(player_name))
                if stats is None:
                    for known, s in players.items():
                        if _same_player(player_name, known):
                            stats = s
                            break
                if stats is None:
                    continue
                val = (stats.get(group) or {}).get(field)
                if val is None:
                    return 0.0   # player appeared but stat is 0/blank
                try:
                    return float(val)
                except (TypeError, ValueError):
                    return None
        return None


# Process-wide singleton; rebuilt only if the key changes.
_client: Optional[APIFootballClient] = None


def get_client() -> APIFootballClient:
    global _client
    try:
        from config import API_FOOTBALL_KEY
        key = API_FOOTBALL_KEY
    except Exception:
        key = ""
    if _client is None or _client.api_key != (key or "").strip():
        _client = APIFootballClient(key)
    return _client


def score_soccer_gap(player_name: str, game_start: datetime, prop: str) -> Optional[float]:
    """Module-level convenience used by results_checker. None when disabled."""
    return get_client().get_player_stat(player_name, game_start, prop)
