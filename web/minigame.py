"""Landing-page minigame: a deterministic "beat the market" daily pick.

The landing page shows three PrizePicks-style player cards a day. The visitor
guesses More or Less; the reveal shows which side the devigged multi-book
consensus favors, with the per-book quotes as the receipt.

Design constraints that shaped this module
------------------------------------------
* **The answer must not leak.** GET /api/public/daily-pick carries no
  probabilities, no favored side, no odds — a visitor reading the Network tab
  before picking learns nothing. The full blob (including the answer) lives
  server-side; the reveal endpoint serves it per pick id after the fact.
* **The receipt must be re-derivable.** The reveal's p_more/p_less/vig_pct are
  computed FROM EXACTLY the books array it returns (mean of per-book implied
  probabilities per side, vig stripped by normalization). A reader with a
  calculator can check the math — the same "state no number you can't source"
  doctrine as web/routers/public.py.
* **One frozen selection per day, no scheduler.** Selection is computed lazily
  on the first request after the 8am America/New_York boundary and frozen in
  app_state_cache (+ an in-process cache). Deterministic given the same match
  snapshot: same day -> same picks, so a Render restart mid-day recomputes
  approximately the same thing even if Supabase is unreachable.
* **Never invent data.** When fewer than 3 pairs qualify the fallback widens
  the probability band, then backfills from the most recent previous day's
  frozen selection. If everything is exhausted the day simply has fewer picks.

Blob lifecycle: provisional -> earned
-------------------------------------
Every cached blob carries ``status``:

* ``"earned"``   — final for the day. Served verbatim to every later request.
* ``"provisional"`` — servable but not final (a backfill-only or floor-tier
  board built before the day's real slate posted).

The rule a visitor feels: **reloading the page must give the identical three
cards.** v1.1 (282ff3f) stopped freezing un-earned boards, which fixed stale
morning cards but reintroduced churn — a not-yet-earned board was recomputed
from scratch on every request, and the candidate pool moves every 5-minute
scrape, so the three players changed per reload. The fix is to cache
provisional blobs too, and allow exactly one *upgrade*: a cached provisional is
replaced only by a board that earned the freeze (or, past the settle deadline,
by whatever the day managed, provided it is not SHORTER than what is cached —
the settle arm is quantity-blind, and a board that shrinks is not an upgrade).
Net effect is at most one visible board change per day and it is always an
improvement. Upgrade attempts are throttled to ``_UPGRADE_MIN_INTERVAL_SEC``
because the underlying scrape only moves every 5 minutes — without that, every
landing-page hit would rebuild the candidate graph and re-read the backfill
window.

The board a visitor already started on is never taken away from them: the
replaced blob is kept in ``_superseded`` for the rest of the day, ``find_pick``
grades its ids, and ``GET /api/public/daily-pick?have=<ids>`` keeps serving it
to the client that holds them (otherwise the upgrade would drop their plays and
silently hand them three fresh free picks).

Provisional blobs mirror to app_state_cache exactly like earned ones. That is
what makes the game load instantly after a Render wake: the process comes back
with no match snapshot and a ~90s boot scrape ahead of it, and the mirror is
the only thing standing between the visitor and an empty hero. They are NOT
backfill sources, though — see _backfill.
"""
from __future__ import annotations

import hashlib
import logging
import random
import threading
import time
from datetime import date, datetime, timedelta, timezone
from statistics import fmean
from zoneinfo import ZoneInfo

import config as cfg
from engine.devig import american_to_implied
# Imported as module-level names (not called through the module) so tests can
# monkeypatch web.minigame.load_state_from_supabase without touching the real
# persistence layer.
from engine.persistence import load_state_from_supabase, sync_state_to_supabase
from web.state import _lock, _state

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")
_EPOCH = date(1970, 1, 1)
# The day flips at 8am ET, not midnight: late games finish ~1am ET and West
# Coast visitors are still up — a midnight boundary would swap the board out
# from under both. 8am ET is after every league's last game settles.
_DAY_BOUNDARY_HOUR = 8

_STATE_KEY_PREFIX = "landing_minigame_day_"

# Favored-side probability band. Below 0.55 the market is a coin flip and the
# reveal is unsatisfying ("50.8% said you were wrong" teaches nothing); above
# 0.60 the favorite is guessable from the line alone and the game is trivial.
_BAND_LO = 0.55
_BAND_HI = 0.60
# Last-resort floor when the band is empty AND there is no frozen history to
# backfill from (launch day, or a thin overnight slate before the morning
# boards post). Below ~0.52 the reveal genuinely teaches nothing — the two
# bars read as the coin flip the game pretends to be — so an emptier board is
# more honest than a flatter one.
_BAND_FLOOR = 0.52

# The over-row and under-row consensus probabilities come from independent
# devigs and can disagree slightly (different books priced each side). Beyond
# this the reveal would not read as a true complement, so the pair is skipped.
_COMPLEMENT_TOL = 0.02

_TOP_N = 8       # trending pool the daily shuffle draws from
_NUM_PICKS = 3
# How far back the backfill will look for a previous frozen day. Render free
# tier sleeps and loses the in-process cache, and a broken scrape can leave a
# day unfrozen — but a two-week-old card is stale enough to be worse than a
# short board.
_BACKFILL_LOOKBACK_DAYS = 14

# Hours after the 8am boundary during which a sub-standard board is served but
# NOT frozen. The boundary is deliberately set before most of the day's lines
# post, so the 8am snapshot is the *worst* one all day: observed live on
# 2026-08-06, the 8:04am recompute found nothing in band and backfilled all
# three slots from the previous day. Freezing that would have locked yesterday's
# cards in until the next 8am. Instead the board stays provisional until it can
# be built from today's own in-band candidates; past this deadline whatever is
# best-available freezes, so the board stops churning for the bulk of the day.
_SETTLE_HOURS = 4

# Serialized match rows carry one display-odds column per book (see
# web/app.py::_base_for_side and the fd_odds/dk_odds/... fields appended to
# each side's row). These are the same numbers the +EV table shows. Each odds
# column is paired with a *_posted flag: display odds can be DERIVED from the
# opposite side (web/app.py::_display_odds re-vigs the complement), and the
# receipt must never show a quote the book didn't post — see _build_candidates.
_BOOK_COLUMNS = (
    ("fd_odds", "fd_posted", "FanDuel"),
    ("dk_odds", "dk_posted", "DraftKings"),
    ("pin_odds", "pin_posted", "Pinnacle"),
    ("nv_odds", "nv_posted", "Novig"),
)

# Blob status values. A blob without the key predates the field; those were
# only ever mirrored when they earned the freeze, so absence means "earned".
_STATUS_EARNED = "earned"
_STATUS_PROVISIONAL = "provisional"

# Minimum spacing between upgrade attempts on a provisional board. The scrape
# behind the candidate pool only moves every REFRESH_INTERVAL_MINUTES (5 by
# default), so retrying faster than this can only burn CPU and Supabase reads
# on a landing page that may be hit many times a second.
_UPGRADE_MIN_INTERVAL_SEC = 60.0

# Cached blobs this process has already seen: day_index -> blob. Single
# gunicorn worker (--workers 1), so this is the fast path and Supabase is only
# the restart-survival mirror. Holds provisional blobs as well as earned ones —
# a provisional entry is what makes reloads stable (see module docstring).
_frozen_cache: dict[int, dict] = {}
# Wall-clock time of the last upgrade attempt per day. Keyed off the caller's
# `now` rather than time.monotonic() so the throttle is testable and so it
# matches the clock the settle deadline uses.
_last_upgrade_attempt: dict[int, datetime] = {}
# PAST days that came back empty from the mirror: day_index -> monotonic
# deadline. Without it, _backfill re-issues up to _BACKFILL_LOOKBACK_DAYS
# Supabase selects on every request that lands while the board is still empty.
# Today's key is deliberately NOT negative-cached: it is the one that can go
# from absent to present while the process is running.
#
# The entries EXPIRE rather than being remembered for the life of the process.
# load_state_from_supabase collapses three different outcomes into (None, None)
# — row absent, query raised, and client not built yet (get_db() is None under
# DISABLE_PERSISTENCE or before startup has configured it). A permanent miss
# would therefore let one transient blip during a Render wake poison all 14
# lookback days for the whole process lifetime, which is exactly the cold-start
# window the mirror exists to cover. A TTL keeps essentially all of the saving
# (a request storm inside one window still costs one select per day) while
# staying self-healing, and it also means a backfill script writing historical
# days is picked up without a restart.
_MISSING_TTL_SEC = 300.0
_missing_cache: dict[int, float] = {}
# Boards this process served today and then replaced (the one allowed
# provisional -> earned upgrade, or a concurrent-compute loser). A visitor is
# still holding their ids: the reveal must grade them, and /daily-pick?have=
# keeps serving them the board they are mid-game on rather than resetting their
# play meter. Only today's are kept, at most _SUPERSEDED_MAX of them.
_SUPERSEDED_MAX = 3
_superseded: dict[int, list[dict]] = {}
_frozen_lock = threading.RLock()


# ---------------------------------------------------------------------------
# Day bucketing
# ---------------------------------------------------------------------------

def day_index(now: datetime | None = None) -> int:
    """Days since the Unix epoch, with the day boundary at 8am ET.

    Implemented by shifting the ET clock back 8 hours and taking the civil
    date: 7:59am ET still belongs to yesterday's board, 8:01am ET starts
    today's. zoneinfo handles DST — the boundary is always 8am *local* ET.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    shifted = now.astimezone(_ET) - timedelta(hours=_DAY_BOUNDARY_HOUR)
    return (shifted.date() - _EPOCH).days


def _hours_since_boundary(now: datetime) -> float:
    """Hours elapsed since this board's 8am ET boundary. Computed off the ET
    civil clock (not a fixed 24h grid) so it stays correct across DST."""
    et = now.astimezone(_ET)
    boundary = et.replace(hour=_DAY_BOUNDARY_HOUR, minute=0, second=0, microsecond=0)
    if et < boundary:
        # Before 8am: this board began at 8am *yesterday*.
        boundary -= timedelta(days=1)
    return (et - boundary).total_seconds() / 3600.0


def _state_key(idx: int) -> str:
    return f"{_STATE_KEY_PREFIX}{idx}"


# ---------------------------------------------------------------------------
# Pair building
# ---------------------------------------------------------------------------

def _pick_id(player: str, league: str, prop: str, line: float, game_start: str) -> str:
    """12-char stable hash of the market pair. Stable across processes and
    across days (a re-offered market keeps its id), unlike hash() which is
    salted per interpreter."""
    raw = f"{player}|{league}|{prop}|{line}|{game_start}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _to_utc_iso(start_time: str) -> str:
    """Normalize PP's ISO start_time (usually ET-offset) to UTC ISO8601.
    Unparsable values pass through verbatim — better an odd timestamp on a
    card than a dropped pick."""
    if not start_time:
        return ""
    try:
        dt = datetime.fromisoformat(str(start_time).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except (ValueError, TypeError):
        return str(start_time)


def _parse_game_start(iso: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _valid_american(x) -> bool:
    """American odds are always <= -100 or >= +100. Anything inside that hole
    (including 0) is a malformed quote and would blow up implied-probability
    math, so the book is skipped rather than 'repaired'."""
    try:
        return abs(float(x)) >= 100.0
    except (ValueError, TypeError):
        return False


def devig_books(books: list[dict]) -> tuple[float, float, float] | None:
    """Two-sided devig of the exact quotes a reveal returns.

    Mean of per-book implied probabilities on each side, then normalization to
    strip the vig. Returns (p_more, p_less, vig_pct) unrounded, or None when a
    side has no quotes. This is THE function the reveal uses — the on-screen
    receipt must reproduce from the books array alone, so nothing else (no
    consensus weighting, no corrections) may enter here.
    """
    more = [american_to_implied(b["american"]) for b in books
            if b.get("side") == "more" and _valid_american(b.get("american"))]
    less = [american_to_implied(b["american"]) for b in books
            if b.get("side") == "less" and _valid_american(b.get("american"))]
    if not more or not less:
        return None
    avg_more = fmean(more)
    avg_less = fmean(less)
    total = avg_more + avg_less
    if total <= 0:
        return None
    return avg_more / total, avg_less / total, (total - 1.0) * 100.0


def _build_candidates(match_rows: list[dict], image_map: dict[str, str]) -> list[dict]:
    """Pair over/under match rows into minigame candidates.

    Applies the unconditional filters (standard odds only, at least one book
    pricing BOTH sides, complement sanity). The tunable filters — probability
    band and future-game preference — are applied by the selector so the
    fallback chain can relax them without rebuilding pairs.
    """
    # Pair key: same market, both sides. start_time is part of the key so a
    # rescheduled game doesn't pair yesterday's over with today's under.
    pairs: dict[tuple, dict[str, dict]] = {}
    for row in match_rows:
        side = row.get("side")
        if side not in ("over", "under"):
            continue
        key = (
            row.get("player_name"), row.get("league"), row.get("stat_type"),
            row.get("pp_line"), row.get("start_time"),
        )
        pairs.setdefault(key, {})[side] = row

    candidates: list[dict] = []
    for (player, league, prop, line, start_time), sides in pairs.items():
        over_row = sides.get("over")
        under_row = sides.get("under")
        if over_row is None or under_row is None:
            continue

        # A goblin square has no Less button — the card layout requires a
        # two-button market, so this is a hard exclusion, not a preference.
        if over_row.get("odds_type") != "standard" or under_row.get("odds_type") != "standard":
            continue

        # Books array: only books quoting BOTH sides. A one-sided book can't
        # participate in a two-sided devig, and showing its quote on the
        # receipt without using it would make the math non-reproducible.
        books: list[dict] = []
        for col, posted_col, book_name in _BOOK_COLUMNS:
            over_am = over_row.get(col)
            under_am = under_row.get(col)
            if not _valid_american(over_am) or not _valid_american(under_am):
                continue
            # The display odds existing on both rows is NOT evidence the book
            # quoted both sides: _display_odds fills a missing side by
            # re-vigging the complement, and the matcher reuses the exact-line
            # prop for both equivalents even when only one side is priced. A
            # derived quote on the receipt would show a price the book never
            # posted and feed the devig the book's own mirrored complement
            # (circular), so require the per-side *_posted flags web/app.py
            # stamps at serialization time. Rows from before the flag existed
            # default to True and self-heal on the next pipeline cycle.
            if not over_row.get(posted_col, True) or not under_row.get(posted_col, True):
                continue
            # Round to int: American odds are integers everywhere users see
            # them, and the devig below runs on these exact stored values so
            # rounding here keeps the receipt consistent.
            books.append({"book": book_name, "side": "more", "american": int(round(float(over_am)))})
            books.append({"book": book_name, "side": "less", "american": int(round(float(under_am)))})
        if not books:
            continue

        devigged = devig_books(books)
        if devigged is None:
            continue
        p_more, p_less, vig_pct = devigged

        # Complement sanity on the pipeline's own consensus numbers: the two
        # sides were devigged independently (possibly from different books),
        # and if they don't roughly sum to 1 the reveal would contradict the
        # +EV board. |sum - 1| <= 0.02 keeps the story coherent.
        t_over = over_row.get("true_prob")
        t_under = under_row.get("true_prob")
        if t_over is None or t_under is None:
            continue
        if abs(float(t_over) + float(t_under) - 1.0) > _COMPLEMENT_TOL:
            continue

        game_start = _to_utc_iso(start_time or "")
        candidates.append({
            "id": _pick_id(player, league, prop, line, game_start),
            "player": player,
            "team": over_row.get("team") or "",
            "opponent": over_row.get("opponent") or "",
            "position": over_row.get("position") or "",
            "league": league,
            "prop": prop,
            "line": line,
            "game_start": game_start,
            "trending_count": int(over_row.get("trending_count") or 0),
            # Raw PP URL; served only through the /api/public/player-image
            # proxy so the page never hotlinks PrizePicks directly.
            "image_source_url": image_map.get(over_row.get("pp_player_id") or "") or None,
            "favored": "more" if p_more >= p_less else "less",
            "p_more": round(p_more, 4),
            "p_less": round(p_less, 4),
            "vig_pct": round(vig_pct, 2),
            "books": books,
        })
    return candidates


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------

def _in_band(c: dict, lo: float, hi: float | None) -> bool:
    fav_p = max(c["p_more"], c["p_less"])
    if fav_p < lo:
        return False
    return hi is None or fav_p <= hi


def _is_future(c: dict, now_utc: datetime) -> bool:
    dt = _parse_game_start(c.get("game_start") or "")
    return dt is not None and dt > now_utc


def _pool(candidates: list[dict], lo: float, hi: float | None, now_utc: datetime) -> list[dict]:
    """Band-qualified candidates, preferring not-yet-started games: if enough
    future games qualify to fill the board, started games are excluded; if
    not, they're allowed back in ("in the future at freeze time if possible")."""
    qualified = [c for c in candidates if _in_band(c, lo, hi)]
    future = [c for c in qualified if _is_future(c, now_utc)]
    return future if len(future) >= _NUM_PICKS else qualified


def _select_picks(candidates: list[dict], idx: int, now_utc: datetime) -> list[dict]:
    """Rank by trending, take the top 8, deterministic day-seeded shuffle,
    keep 3. The shuffle exists so the board isn't just "the three most
    popular props" every single day; seeding by day_index keeps the same
    day's recompute identical (Render restarts mid-day)."""
    pool = _pool(candidates, _BAND_LO, _BAND_HI, now_utc)
    if len(pool) < _NUM_PICKS:
        # Fallback 1: drop the upper bound. A 63% favorite makes for an easier
        # card, but an easier card beats an empty one.
        pool = _pool(candidates, _BAND_LO, None, now_utc)

    # Tie-break by id so equal trending counts still order deterministically.
    ranked = sorted(pool, key=lambda c: (-c["trending_count"], c["id"]))[:_TOP_N]
    rng = random.Random(idx)
    rng.shuffle(ranked)

    # One card per player. Trending concentrates on stars, so without this the
    # board can read "A'ja Wilson, A'ja Wilson, Caitlin Clark" — contract-legal
    # but it looks like a bug to a visitor. Greedy in shuffle order keeps the
    # day-seeded determinism; if the pool has fewer than 3 distinct players,
    # duplicates fill the tail because a repeat card beats an empty slot.
    picks: list[dict] = []
    seen_players: set[str] = set()
    for c in ranked:
        if len(picks) >= _NUM_PICKS:
            break
        if c.get("player") not in seen_players:
            picks.append(c)
            seen_players.add(c.get("player"))
    for c in ranked:
        if len(picks) >= _NUM_PICKS:
            break
        if c not in picks:
            picks.append(c)
    return picks


def _load_frozen(idx: int, *, negative_cache: bool = False) -> dict | None:
    """Cached blob for a day: in-process first, then the Supabase mirror.

    `negative_cache=True` is for PAST days only (backfill, reveal lookback):
    a miss is remembered for _MISSING_TTL_SEC so the next request doesn't
    re-issue the same select. Never pass it for today — today's key is exactly
    the one that can go from absent to present while the process is running.
    """
    mono = time.monotonic()
    with _frozen_lock:
        blob = _frozen_cache.get(idx)
        deadline = _missing_cache.get(idx) if negative_cache else None
    if blob is not None:
        return blob
    if deadline is not None and mono < deadline:
        return None
    value, _ts = load_state_from_supabase(_state_key(idx))
    if isinstance(value, dict) and isinstance(value.get("picks"), list):
        with _frozen_lock:
            _frozen_cache[idx] = value
            _missing_cache.pop(idx, None)
        return value
    if negative_cache:
        with _frozen_lock:
            _missing_cache[idx] = mono + _MISSING_TTL_SEC
            # Bounded: only today +/- the lookback window is ever queried, so
            # yesterday's expired entries are dead weight.
            for old in [k for k, dl in _missing_cache.items()
                        if k < idx - _BACKFILL_LOOKBACK_DAYS and dl <= mono]:
                _missing_cache.pop(old, None)
    return None


def _backfill(picks: list[dict], idx: int) -> list[dict]:
    """Fill remaining slots from the most recent previous frozen day(s).

    Product decision: yesterday's real card beats an empty slot. Walks
    backwards a bounded number of days; skips markets already on the board.
    Never fabricates anything — if history is exhausted the board is short.
    """
    have = {p["id"] for p in picks}
    # Same one-card-per-player rule as _select_picks: don't backfill a second
    # card for a player already on today's board.
    have_players = {p.get("player") for p in picks}
    for back in range(1, _BACKFILL_LOOKBACK_DAYS + 1):
        if len(picks) >= _NUM_PICKS:
            break
        prev = _load_frozen(idx - back, negative_cache=True)
        # EARNED days only. A provisional board is usually *itself* mostly
        # backfill, and provisional blobs now mirror to app_state_cache
        # (see _adopt), so accepting one as a source would let a card hop
        # forward a day at a time forever — the chain, not the card, would be
        # what _BACKFILL_LOOKBACK_DAYS bounds. Blobs written before the field
        # existed default to earned: those were only ever mirrored when they
        # earned the freeze.
        if not prev or prev.get("status", _STATUS_EARNED) != _STATUS_EARNED:
            continue
        for p in prev.get("picks", []):
            if len(picks) >= _NUM_PICKS:
                break
            if (isinstance(p, dict) and p.get("id") and p["id"] not in have
                    and p.get("player") not in have_players):
                picks.append(p)
                have.add(p["id"])
                have_players.add(p.get("player"))
    return picks


def _compute_blob(idx: int, now_utc: datetime) -> tuple[dict, bool]:
    """Build the day's blob from the live match snapshot.

    Returns (blob, freezable). Not freezable when the snapshot was empty —
    that's the Render-just-woke-up window before the boot scrape lands, and
    freezing whatever backfill produced would lock a stale board in for the
    whole day when 2 minutes later real data exists.
    """
    with _lock:
        match_rows = _state.get("matches") or []
        image_map = _state.get("pp_player_images") or {}

    candidates = _build_candidates(match_rows, image_map)
    picks = _select_picks(candidates, idx, now_utc)
    picks = _backfill(picks, idx)

    # Last-resort tier, deliberately AFTER backfill: yesterday's in-band card
    # teaches better than today's marginal one, so history wins when it
    # exists. This tier only fires when both the band and the archive came up
    # short — the alternative is the landing hero rendering without its game.
    if len(picks) < _NUM_PICKS:
        have = {p["id"] for p in picks}
        have_players = {p.get("player") for p in picks}
        floor_pool = sorted(
            (c for c in candidates
             if _in_band(c, _BAND_FLOOR, None)
             and c["id"] not in have and c.get("player") not in have_players),
            # Strongest lesson first, id tie-break for recompute determinism.
            key=lambda c: (-max(c["p_more"], c["p_less"]), c["id"]),
        )
        picks.extend(floor_pool[:_NUM_PICKS - len(picks)])

    # A board only earns the freeze when it is a full slate built from TODAY's
    # own in-band candidates. `bool(match_rows)` is not enough: a successful
    # scrape that yields nothing in band still backfills a full board out of
    # yesterday's blob, and freezing that locks stale cards in for 24h (the
    # 8am boundary lands before most lines post, so this is the common case,
    # not the edge case). Anything weaker is served provisionally and
    # recomputed on the next request until the day's real slate posts.
    fresh_ids = {c["id"] for c in candidates}
    earned = (
        len(picks) == _NUM_PICKS
        and all(p["id"] in fresh_ids for p in picks)
        and all(_in_band(p, _BAND_LO, None) for p in picks)
    )
    # Escape hatch: on a genuinely thin day the bar above may never be met, and
    # a board that reshuffles every 5 minutes is worse than a settled marginal
    # one. Past the deadline, take what we have.
    settled = _hours_since_boundary(now_utc) >= _SETTLE_HOURS
    freezable = bool(picks) and (earned or (settled and bool(match_rows)))

    return _blob(idx, now_utc, picks, freezable), freezable


def _blob(idx: int, now_utc: datetime, picks: list[dict], freezable: bool) -> dict:
    return {
        "day_index": idx,
        "frozen_at": now_utc.isoformat().replace("+00:00", "Z"),
        # Stamped into the blob (not just returned) so the status survives the
        # app_state_cache mirror: a restart must be able to tell a settled
        # board from one that is still waiting for the day's real slate.
        "status": _STATUS_EARNED if freezable else _STATUS_PROVISIONAL,
        "picks": picks,
    }


def _claim_compute_slot(idx: int, now_utc: datetime) -> bool:
    """Record that a candidate rebuild is happening now; return whether it may.

    Rate-limits the *attempt*, not the adoption: a provisional board is only
    worth recomputing as often as the scrape behind it moves (5 min), so on a
    landing page that may be hit many times a second every extra rebuild is
    pure waste.

    The FIRST attempt of a day is always granted — making the very first
    visitor wait out a throttle window would leave the hero's game empty for
    exactly the reason the caller is trying to avoid. Every attempt after it is
    throttled on the stamp, INCLUDING the ones that produced no board at all.
    That last part used to be an `enforce=cached is not None` flag, which never
    bound: an empty board is deliberately not adopted, so `cached` stayed None
    and every subsequent request rebuilt the whole candidate graph
    (O(len(_state["matches"])) with the GIL held, on the one endpoint every
    landing hit and every bot calls). The stamp is the throttle; whether the
    attempt happened to yield something is not the throttle's business.

    A backwards clock step (NTP correction, or a test replaying an earlier
    instant) yields a negative delta and is treated as "go ahead": one extra
    compute is cheap, whereas treating it as throttled could wedge the day.
    """
    with _frozen_lock:
        last = _last_upgrade_attempt.get(idx)
        granted = True
        if last is not None:
            elapsed = (now_utc - last).total_seconds()
            if 0.0 <= elapsed < _UPGRADE_MIN_INTERVAL_SEC:
                granted = False
        if granted:
            _last_upgrade_attempt[idx] = now_utc
            # Only today's entry is ever consulted; drop the rest.
            for old in [k for k in _last_upgrade_attempt if k < idx]:
                _last_upgrade_attempt.pop(old, None)
        return granted


def _adopt(idx: int, blob: dict, previous: dict | None) -> dict:
    """Install a blob as the day's board and mirror it to app_state_cache.

    Mirrors PROVISIONAL blobs too, which earned boards used to be alone in
    doing. That mirror is the instant-load path: a Render wake starts with an
    empty match snapshot and a ~90s boot scrape ahead of it, and without a
    mirrored board the landing hero renders its game empty for that whole
    window. A provisional blob on disk is strictly better than nothing, and it
    is still allowed to upgrade once the real slate posts.
    """
    with _frozen_lock:
        existing = _frozen_cache.get(idx)
        # Another thread installed a board while we were computing (the compute
        # deliberately runs without _frozen_lock — see the caller). Defer to it
        # unconditionally, not just when it earned the freeze: on the
        # cold-start path N concurrent requests each build their own board, and
        # last-writer-wins there means visitor A's already-served three cards
        # are replaced by visitor B's, which is the churn this module exists to
        # eliminate. First board of the day wins; a legitimate upgrade still
        # goes through because there `existing is previous` (the caller read
        # the very object it is now replacing).
        if existing is not None and existing is not previous:
            return existing
        _frozen_cache[idx] = blob
        _missing_cache.pop(idx, None)
        if existing is not None:
            # Keep the board we just replaced resolvable. A visitor mid-play is
            # holding ids from it: /daily-pick?have= keeps serving them their
            # board, and find_pick grades those ids instead of 404ing the
            # reveal of a pick that was on screen a second ago.
            prior = _superseded.setdefault(idx, [])
            prior.append(existing)
            del prior[:-_SUPERSEDED_MAX]
        # Bound the in-process caches: only today + the backfill window are
        # ever read again, and superseded boards only matter for today.
        for old in [k for k in _frozen_cache if k < idx - _BACKFILL_LOOKBACK_DAYS]:
            _frozen_cache.pop(old, None)
        for old in [k for k in _superseded if k < idx]:
            _superseded.pop(old, None)
    # Fire-and-forget mirror. sync_state_to_supabase swallows and logs
    # failures; in-process-only is acceptable (a Render sleep loses it, and the
    # recompute is deterministic given the same snapshot).
    #
    # DISABLE_PERSISTENCE is honoured for the same reason web/app.py::_sync_all
    # honours it: a local/comparison instance shares Supabase with production,
    # and app_state_cache is keyed by day alone, so an ungated write here lets
    # a dev box overwrite the board prod is serving. That was survivable while
    # only EARNED boards mirrored (rare, and the recompute is deterministic);
    # now that provisional boards mirror too it would fire on essentially every
    # local boot. Reads stay ungated on purpose — pulling prod's board into a
    # local preview is exactly what you want.
    if not cfg.DISABLE_PERSISTENCE:
        threading.Thread(
            target=sync_state_to_supabase,
            args=(_state_key(idx), blob),
            daemon=True,
        ).start()
    return blob


def get_or_freeze_today(now: datetime | None = None) -> tuple[int, dict]:
    """The one entry point the routers use.

    Returns (day_index, blob). The first caller of the day computes; everyone
    after is served the cached copy (in-process, or the app_state_cache mirror
    after a restart) so a reload always shows the identical three cards.

    A cached blob is replaced at most once, and only upwards: a provisional
    board yields to one that earned the freeze (or, past the settle deadline,
    to whatever the day managed to build). An earned board is never recomputed.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    idx = day_index(now)

    cached = _load_frozen(idx)
    if cached is not None and cached.get("status", _STATUS_EARNED) == _STATUS_EARNED:
        return idx, cached
    # Provisional (or no board yet): a better board may exist, but finding out
    # costs a full candidate rebuild, so only look occasionally. Throttled even
    # with nothing to serve — an empty day must not mean a rebuild per request.
    if not _claim_compute_slot(idx, now):
        return idx, cached if cached is not None else _blob(idx, now, [], False)

    # Compute WITHOUT holding _frozen_lock: the compute path takes
    # web.state._lock for its snapshot, and the codebase rule is one lock at a
    # time, never nested.
    blob, freezable = _compute_blob(idx, now)

    if not blob["picks"]:
        # An empty board is never cached — that would pin the emptiness for the
        # day — and never replaces a board we already have (the scrape can fail
        # after a good board was built; yesterday's cards beat no cards).
        #
        # market_observatory is NOT a fallback source here, despite covering
        # every priced market. Its `books` jsonb holds per-book DEVIGGED
        # PROBABILITIES for one side ({"fanduel": 0.61}) — see
        # web/app.py::_per_book_probs and the migration_003/016 column
        # comments — not the per-book American quotes a reveal receipt is
        # built from. Reconstructing odds from a devigged probability would
        # print a price no book posted (the exact failure the *_posted gate
        # above exists to prevent) and the stripped vig makes vig_pct
        # unrecoverable, so the receipt could not be re-derived from what the
        # page shows. The mirror in _adopt is the real cold-start answer.
        return idx, cached if cached is not None else blob
    if cached is not None:
        if not freezable:
            # Still provisional. Serving the NEW board here is what made the
            # three players change on every reload; the cached one wins until
            # an upgrade is genuinely available.
            return idx, cached
        if len(blob["picks"]) < len(cached["picks"]):
            # Freezable, but SHORTER. The settle-deadline arm of `freezable` is
            # quantity-blind (`bool(picks)`), so an evening recompute that finds
            # one card left — the others tipped off and were pulled — would
            # replace a full board and then pin the shrunken one for the rest of
            # the day. The visitor would lose two of their three free picks
            # mid-session (the client derives its play count from the board it
            # is handed). An upgrade is only an upgrade if it goes up.
            return idx, cached
    return idx, _adopt(idx, blob, cached)


def board_for_ids(idx: int, blob: dict, ids: list[str]) -> dict:
    """The board a visitor is mid-game on, given the pick ids they hold.

    The day allows exactly one board change (provisional -> earned). For a
    visitor who has already played, that change is not an improvement: their
    stored plays are keyed on ids the new board doesn't have, so the client
    filters them all out, the meter resets to "Play 1 of 3 free" and they are
    handed three fresh picks they didn't ask for. Serving the superseded board
    to callers who prove they hold it keeps the session they started; everyone
    else gets the upgraded board.

    Unknown/forged ids simply don't match anything and fall through to `blob` —
    only boards this process actually served are candidates.
    """
    if not ids:
        return blob
    wanted = set(ids)
    if wanted <= {p.get("id") for p in blob.get("picks", [])}:
        return blob
    with _frozen_lock:
        prior = list(_superseded.get(idx, ()))
    for old in reversed(prior):    # most recently replaced first
        if wanted <= {p.get("id") for p in old.get("picks", [])}:
            return old
    return blob


def find_pick(pick_id: str, now: datetime | None = None) -> dict | None:
    """Locate a pick by id: today's blob, then today's superseded boards, then
    the recent frozen days.

    A pure READ, deliberately. It used to call get_or_freeze_today, which is
    the call that performs the provisional -> earned upgrade — so the reveal
    request could trigger the very swap that invalidated its own pick id, and
    the visitor got a 404 on the card they were looking at. Grading must never
    move the board.

    The fallbacks exist because a visitor's board can outlive the blob that
    served it: a tab left open across the 8am ET boundary is holding
    yesterday's ids, and the Render-wake window can serve a backfill-only
    board that is replaced minutes later once the boot scrape lands. Grading an
    old-board pick is exact — the full pick dicts (favored, p_more/p_less,
    books) are stored per board — so falling back beats 404ing the visitor into
    a dead retry loop. A genuinely unknown id still misses everything and the
    caller 404s.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    idx = day_index(now)
    blob = _load_frozen(idx) or {}
    for p in blob.get("picks", []):
        if isinstance(p, dict) and p.get("id") == pick_id:
            return p
    with _frozen_lock:
        prior = list(_superseded.get(idx, ()))
    for old in reversed(prior):
        for p in old.get("picks", []):
            if isinstance(p, dict) and p.get("id") == pick_id:
                return p
    for back in range(1, _BACKFILL_LOOKBACK_DAYS + 1):
        prev = _load_frozen(idx - back, negative_cache=True)
        if not prev:
            continue
        for p in prev.get("picks", []):
            if isinstance(p, dict) and p.get("id") == pick_id:
                return p
    return None


def reset_for_tests() -> None:
    """Drop every in-process cache (blobs, negative lookups, upgrade throttle).
    Tests only."""
    with _frozen_lock:
        _frozen_cache.clear()
        _missing_cache.clear()
        _last_upgrade_attempt.clear()
        _superseded.clear()
