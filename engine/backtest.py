"""
Backtest and slip logger for CoreProp.

Automatically documents the best +EV slip combinations as they appear
throughout the day. Logs to Supabase with one row per leg.

Dedup is layered:
  • (player, sports_day) — a player in a given game appears in at most
    one slip across the rolling 48 h window. `sports_day` is the UTC
    date of (game_start − 12 h), so a late ET game spilling past
    midnight UTC still buckets with the rest of the night's slate.
  • (player, prop, line, side, sports_day) — an exact leg never appears
    in two slips. Catches edge cases where the player-key dedup might
    miss (e.g. a defensive belt-and-suspenders).
  • Per-user threading.Lock — serializes concurrent log attempts so two
    refreshes can't both read the same "used keys" snapshot before either
    commits its slip.

Within-slip caps:
  • At most 3 legs share a single (league, game_date) bucket — prevents
    runaway same-game stacks where correlation makes the joint EV worse
    than the marginal EVs imply.
"""
import logging
import threading
import uuid
from datetime import date, datetime, timezone, timedelta
from typing import Optional
import unidecode

from engine.constants import BREAK_EVEN, OPTIMAL_BREAK_EVEN
from engine.ev_calculator import power_slip_ev, flex_slip_ev
from engine.database import get_db, get_user_db

logger = logging.getLogger(__name__)

# Hard floor: legs with individual EV below this are never included
MIN_LEG_EV_PCT = -0.01   # -1%

# How far back we scan for duplicate slips and used-player keys.
# 48 h covers every intra-day restart plus the midnight rollover.
_DEDUP_WINDOW_HOURS = 48

# PrizePicks rule: every slip must contain picks from at least two
# distinct teams. Two teams playing each other count as two — the rule
# is purely about player team membership, not whether the picks are on
# the same game. Enforced at slip-construction time below; without team
# data on a leg (NULL `team`), we fall back to player_id for the
# distinctness check so the rule still has a sane meaning on legacy
# data, even though it's structurally weaker (any pair of distinct
# players passes).
TWO_TEAMS_REQUIRED = 2

# Per-user lock registry. Each user gets one lock; concurrent calls to
# try_log_slip for the same user serialize so the dedup snapshot a thread
# reads is never invalidated by a peer commit mid-flight.
_user_locks: dict[str, threading.Lock] = {}
_user_locks_registry_lock = threading.Lock()

def _get_user_lock(user_id: str) -> threading.Lock:
    with _user_locks_registry_lock:
        lk = _user_locks.get(user_id)
        if lk is None:
            lk = threading.Lock()
            _user_locks[user_id] = lk
        return lk


# Punctuation that is intra-word (initials, apostrophes) is stripped; those
# that mark word boundaries (hyphens, underscores, commas) become spaces.
_PUNCT_STRIP = str.maketrans({c: "" for c in ".'`"})
_PUNCT_SPACE = str.maketrans({c: " " for c in "-_,"})

def _normalize(s: str) -> str:
    """Standardize strings for identity comparison.

    Applies: unidecode → lowercase → strip intra-word punctuation
    (`.`, `'`, `` ` ``) → replace boundary punctuation (`-`, `_`, `,`) with
    spaces → merge consecutive single-letter tokens (so "R J Barrett"
    collapses to "rj barrett") → collapse whitespace → strip.

    This folds scraper-level spelling drift like "R.J. Barrett" vs
    "RJ Barrett" vs "R J Barrett", "D'Angelo" vs "DAngelo", "Jean-Luc"
    vs "Jean Luc" to the same key so cross-slip dedup on
    (player, game_start) actually holds.
    """
    if not s:
        return ""
    s = unidecode.unidecode(s).lower()
    s = s.translate(_PUNCT_STRIP)
    s = s.translate(_PUNCT_SPACE)
    tokens = s.split()

    # Merge runs of single-character tokens into one: [r, j, barrett]
    # → [rj, barrett]. Only runs of length ≥ 2 are merged, so a stray
    # single-letter first name like "A Davis" still becomes "a davis".
    merged: list[str] = []
    i = 0
    while i < len(tokens):
        if len(tokens[i]) == 1 and i + 1 < len(tokens) and len(tokens[i + 1]) == 1:
            run = [tokens[i]]
            j = i + 1
            while j < len(tokens) and len(tokens[j]) == 1:
                run.append(tokens[j])
                j += 1
            merged.append("".join(run))
            i = j
        else:
            merged.append(tokens[i])
            i += 1
    return " ".join(merged)

def _normalize_line(line) -> str:
    """Normalize a line value for fingerprinting. Handles strings and floats
    consistently — '0.5' and 0.5 collapse to the same key."""
    try:
        return f"{float(line):.4f}"
    except (TypeError, ValueError):
        return str(line or "").strip()


def make_leg_key(player: str, prop: str, line, side: str, start_time: str) -> tuple:
    """Exact-leg fingerprint: same player + prop + line + side in the same
    game produces the same key. Used to enforce that no identical leg
    appears in two different slips."""
    p_key, t_key = make_bet_key(player, start_time)
    prop_key = (prop or "").strip().lower()
    side_key = (side or "").strip().lower()
    return (p_key, prop_key, _normalize_line(line), side_key, t_key)


def make_leg_fingerprint(player: str, prop: str, line, side: str, start_time: str) -> str:
    """String form of make_leg_key, suitable for the `legs.dedup_key`
    column. The DB has a partial UNIQUE INDEX on (user_id, dedup_key)
    so a second insert with the same fingerprint is rejected at write
    time (migration_008). This is the bulletproof backstop to
    application-level dedup, which is race-prone across multiple
    Render workers."""
    p_key, prop_key, line_key, side_key, t_key = make_leg_key(
        player, prop, line, side, start_time
    )
    return f"{p_key}|{prop_key}|{line_key}|{side_key}|{t_key}"


def make_game_key(league: str, start_time: str) -> tuple[str, str]:
    """Best-effort 'same game' bucket. Two legs sharing this key are
    presumed to be from the same game (or at least same league + same UTC
    date), so the within-slip cap can prevent over-stacking."""
    _, t_key = make_bet_key("", start_time)
    return ((league or "").strip().upper() or "UNKNOWN", t_key)


def make_bet_key(player: str, start_time: str) -> tuple[str, str]:
    """Build a unique signature for a player in a specific game.

    The time component is a "sports day" — the UTC timestamp shifted
    back 12 h before extracting the date, so a single game can't straddle
    the day boundary. Without the shift, a 10 pm ET tip-off (≈02:00 UTC
    next day) buckets to a different date than a 7 pm ET tip-off the
    same night, and the SAME game can land in two buckets if scrapers
    store its `game_start` inconsistently (naive ET parsed as UTC, vs.
    offset-aware ISO). 12 h puts the new midnight at 12:00 UTC ≈ 07:00
    ET, before any North-American game starts — so every same-night
    slate collapses to one bucket regardless of TZ-parsing drift.
    """
    time_key = "no_time"
    if start_time:
        try:
            clean_ts = start_time.replace(" ", "T")
            if clean_ts.endswith("Z"):
                clean_ts = clean_ts[:-1] + "+00:00"
            dt = datetime.fromisoformat(clean_ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            time_key = (dt - timedelta(hours=12)).strftime("%Y-%m-%d")
        except Exception as e:
            logger.warning("Backtest: _make_key failed to parse '%s': %s", start_time, e)
            time_key = start_time[:10] if start_time else "no_time"

    return (_normalize(player), time_key)


# Optional leg columns that may be absent on an un-migrated deploy. Stripped
# from the payload ONLY when the DB error actually names the column — never
# preemptively (stripping dedup_key blindly on any error was what let a
# retried batch bypass the (user_id, dedup_key) unique index).
_OPTIONAL_LEG_COLS = ("dedup_key", "team", "raw_true_prob", "closing_books", "books")


def _is_duplicate_err(msg: str) -> bool:
    """Postgres unique_violation (SQLSTATE 23505) surfaced through PostgREST."""
    m = (msg or "").lower()
    return "23505" in m or "duplicate key" in m


def insert_legs_idempotent(db, slip_id: str, db_legs: list[dict]) -> None:
    """Insert a slip's legs exactly once, tolerant of lost-response retries.

    The failure this exists to prevent: a PostgREST batch insert is atomic on
    the server, but the CLIENT can't distinguish a genuine failure from a
    LOST RESPONSE — a read timeout / connection reset / 5xx from the
    Cloudflare-PgBouncer edge AFTER Postgres already committed the rows. The
    previous code retried the same batch on any non-23505 error, so a lost
    response double-inserted every leg and produced a 12-leg slip from a
    6-leg config (and the retry stripped dedup_key first, so the unique index
    couldn't catch it either).

    Because `slip_id` is freshly generated for this write and belongs to no
    other slip, ANY legs already present under it can only be a partial write
    from THIS call. So the safe, idempotent rule is: on any error, re-read
    which leg_nums already landed under this slip_id and insert only the ones
    still missing. Repeat until nothing is missing.

    Column-compat: only strip an optional column when the error names it
    (un-migrated schema), and re-derive the missing set each pass so a genuine
    duplicate-key rejection from a DIFFERENT slip still propagates.
    """
    def _existing_leg_nums() -> set:
        try:
            got = (
                db.table("legs").select("leg_num").eq("slip_id", slip_id).execute().data
            ) or []
            return {r.get("leg_num") for r in got}
        except Exception:
            # If we can't read back, assume nothing landed and let the insert
            # (and its own error handling) decide — never silently drop legs.
            return set()

    pending = list(db_legs)
    last_exc = None
    for _ in range(len(_OPTIONAL_LEG_COLS) + 2):
        # Drop any leg that already committed under this slip_id (partial
        # write from a prior attempt in this same call).
        already = _existing_leg_nums()
        if already:
            pending = [dl for dl in pending if dl.get("leg_num") not in already]
        if not pending:
            return  # every leg is present exactly once
        try:
            db.table("legs").insert(pending).execute()
            return
        except Exception as exc:
            last_exc = exc
            msg = str(exc)
            # A duplicate-key error here is meaningful: after removing this
            # slip's own already-landed legs, a 23505 means the leg collides
            # with a DIFFERENT slip (the cross-slip dedup index). Propagate so
            # the caller rolls back the header and abandons the slip.
            if _is_duplicate_err(msg):
                raise
            # Un-migrated column? Strip the one the error names and retry.
            stripped = False
            for col in _OPTIONAL_LEG_COLS:
                if col in msg.lower() and any(col in dl for dl in pending):
                    for dl in pending:
                        dl.pop(col, None)
                    stripped = True
                    break
            if stripped:
                continue
            # Otherwise it's a lost-response / transient error: loop, re-read
            # what landed, and insert only the remainder (idempotent).
    if last_exc is not None:
        raise last_exc


class BacktestLogger:
    """
    Builds and logs the best available +EV slips to Supabase.

    Dedup invariant (enforced end-to-end):
      Once a (player, game_start) pair appears in any slip within the last
      _DEDUP_WINDOW_HOURS, that pair cannot appear in any future slip.

    Selection logic:
      - Reads used keys directly from Supabase for the specific user
      - Applies hard floor: individual_ev_pct >= -1%
      - Picks top 6 legs greedily
      - Writes to Supabase using the user's RLS-scoped JWT
    """

    def __init__(self, user_id: str, jwt: Optional[str] = None, db_client=None):
        self.user_id = user_id
        self.jwt = jwt
        self.db_client = db_client

    def _fetch_recent_slips_with_legs(self) -> list[dict]:
        """
        Return all slips for this user from the last _DEDUP_WINDOW_HOURS with their legs
        attached.
        """
        db = self.db_client or get_user_db(self.jwt)
        if not db:
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(hours=_DEDUP_WINDOW_HOURS)
        cutoff_iso = cutoff.isoformat()

        try:
            slips_res = (
                db.table("slips")
                .select("id, timestamp")
                .gte("timestamp", cutoff_iso)
                .eq("user_id", self.user_id)
                .order("timestamp", desc=True)
                .limit(500)
                .execute()
            )
            slips = slips_res.data or []
            if not slips:
                return []

            sids = [s["id"] for s in slips]
            legs_res = (
                db.table("legs")
                .select("slip_id, player, game_start, prop, line, side")
                .in_("slip_id", sids)
                .execute()
            )
            legs_by_slip: dict[str, list[dict]] = {}
            for leg in (legs_res.data or []):
                legs_by_slip.setdefault(leg["slip_id"], []).append(leg)

            out = []
            for s in slips:
                out.append({
                    "id": s["id"],
                    "timestamp": s.get("timestamp", ""),
                    "legs": legs_by_slip.get(s["id"], []),
                })
            return out
        except Exception as exc:
            logger.error("Backtest: could not fetch recent slips from Supabase: %s", exc)
            return []

    def _load_used_keys_from_db(self) -> set[tuple[str, str]]:
        """Backwards-compatible: return the set of (player, game_date)
        pair keys used in the dedup window. Callers that need the
        exact-leg set should use _load_dedup_sets() instead."""
        pair_keys, _ = self._load_dedup_sets()
        return pair_keys

    def _load_dedup_sets(self) -> tuple[set[tuple], set[tuple]]:
        """Return (pair_keys, leg_keys) used within the dedup window.
          pair_keys: {(player, game_date)} — ensures a player isn't bet
                     twice in the same game across slips.
          leg_keys:  {(player, prop, line, side, game_date)} — ensures the
                     exact same leg never repeats."""
        pair_keys: set[tuple] = set()
        leg_keys: set[tuple] = set()
        for slip in self._fetch_recent_slips_with_legs():
            for leg in slip["legs"]:
                player = leg.get("player", "") or ""
                start  = leg.get("game_start", "") or ""
                pair_keys.add(make_bet_key(player, start))
                leg_keys.add(make_leg_key(
                    player,
                    leg.get("prop", "") or "",
                    leg.get("line", "") or "",
                    leg.get("side", "") or "",
                    start,
                ))
        return pair_keys, leg_keys

    def find_conflicting_legs(self, bets: list[dict], used_keys: set) -> list[dict]:
        if not bets:
            return []
        conflicts = []
        for bet in bets:
            key = make_bet_key(
                bet.get("player_name", "") or "",
                bet.get("start_time", "") or "",
            )
            if key in used_keys:
                conflicts.append(bet)
        return conflicts

    def try_log_slip(self, bets: list[dict], slip_type: str = "Power", n_legs: int = 6) -> Optional[dict]:
        """Build and log the best available auto-slip with unique players.

        Serialized per-user: the body runs under a per-user lock so
        concurrent refreshes can't both pass the dedup gate with the
        same candidates. Without the lock, two threads could each read
        an empty used-keys set, both pick Jake McCarthy, and both write
        slips containing him."""
        with _get_user_lock(self.user_id):
            return self._try_log_slip_locked(bets, slip_type, n_legs)

    def _try_log_slip_locked(self, bets: list[dict], slip_type: str, n_legs: int) -> Optional[dict]:
        used_pair_keys, used_leg_keys = self._load_dedup_sets()

        def _ev(b: dict) -> float:
            return float(b.get("individual_ev_pct") or 0.0)

        valid = [b for b in bets if _ev(b) >= MIN_LEG_EV_PCT]
        pool = sorted(valid, key=_ev, reverse=True)

        # Two-pass selection so the PrizePicks ≥2-distinct-teams rule is
        # enforced as a hard validity constraint rather than a per-leg
        # filter. First pass takes the top legs by EV ignoring teams;
        # second pass swaps the lowest-EV "redundant" legs out for the
        # highest-EV legs from a different team if the first pass produced
        # a single-team slip.

        best_legs: list[dict] = []
        seen_pair: set[tuple] = set()
        seen_leg:  set[tuple] = set()
        team_of:   dict[int, str] = {}   # idx in best_legs → team key

        def _team_key(b: dict) -> str:
            """Distinct-team identifier. Prefer PP team abbreviation; fall
            back to the player_id so legacy bets without team data still
            give a sensible (though weaker) distinctness signal."""
            t = (b.get("team") or "").strip().upper()
            if t:
                return t
            return f"player:{(b.get('pp_player_id') or b.get('player_name') or '').strip().lower()}"

        for bet in pool:
            player = bet.get("player_name", "") or ""
            start  = bet.get("start_time", "") or ""
            p_key = make_bet_key(player, start)
            l_key = make_leg_key(
                player,
                bet.get("prop_type", "") or "",
                bet.get("pp_line", "") or "",
                bet.get("side", "") or "",
                start,
            )
            if p_key in used_pair_keys: continue
            if l_key in used_leg_keys:  continue
            if p_key in seen_pair: continue
            if l_key in seen_leg:  continue
            best_legs.append(bet)
            seen_pair.add(p_key)
            seen_leg.add(l_key)
            team_of[len(best_legs) - 1] = _team_key(bet)
            if len(best_legs) == n_legs:
                break

        if len(best_legs) < n_legs:
            return None

        # Two-team enforcement. If all selected legs share a single team,
        # try to swap a leg out for the highest-EV remaining leg from a
        # different team. Iterate from the lowest-EV slot upward so we
        # surrender the least edge per swap.
        distinct_teams = set(team_of.values())
        if len(distinct_teams) < TWO_TEAMS_REQUIRED:
            chosen_keys: set[tuple] = {
                make_bet_key(b.get("player_name", "") or "", b.get("start_time", "") or "")
                for b in best_legs
            }
            chosen_legs: set[tuple] = {
                make_leg_key(
                    b.get("player_name", "") or "",
                    b.get("prop_type", "") or "",
                    b.get("pp_line", "") or "",
                    b.get("side", "") or "",
                    b.get("start_time", "") or "",
                )
                for b in best_legs
            }
            single_team = next(iter(distinct_teams))
            # Find the best different-team replacement that's not already
            # selected and not blocked by cross-slip dedup.
            replacement = None
            for cand in pool:
                if _team_key(cand) == single_team:
                    continue
                cp = make_bet_key(cand.get("player_name", "") or "", cand.get("start_time", "") or "")
                cl = make_leg_key(
                    cand.get("player_name", "") or "",
                    cand.get("prop_type", "") or "",
                    cand.get("pp_line", "") or "",
                    cand.get("side", "") or "",
                    cand.get("start_time", "") or "",
                )
                if cp in used_pair_keys: continue
                if cl in used_leg_keys:  continue
                if cp in chosen_keys:    continue
                if cl in chosen_legs:    continue
                replacement = cand
                break
            if replacement is None:
                # No different-team candidate available — slip can't be
                # PP-legal, so abandon it.
                logger.info("Backtest: skipping slip — no second team available in candidate pool")
                return None
            # Drop the lowest-EV existing leg (last index after the
            # EV-descending pass) and append the replacement.
            best_legs.pop()
            best_legs.append(replacement)

        true_probs = [float(b.get("true_prob") or 0.0) for b in best_legs]
        avg_prob = sum(true_probs) / n_legs
        be_key = (str(n_legs), slip_type.lower())
        # Fall back to the optimal per-leg break-even (~0.5408) for any combo
        # not in the table (e.g. ("2","flex")). Using raw .get() -> None here
        # meant such slips were SILENTLY dropped (never logged) rather than
        # gated on a sensible threshold.
        slip_be = BREAK_EVEN.get(be_key, OPTIMAL_BREAK_EVEN)

        if avg_prob < slip_be:
            return None

        # Use the correct EV function for the slip type. A 2-leg Flex is
        # identical to a 2-leg Power (FLEX_PAYOUTS has no n=2 tier, so
        # flex_slip_ev would return None and silently drop the slip) — this
        # mirrors the "Flex 2-leg = Power 2-leg" rule used in the payout math
        # (web/app.py get_backtest_slips, frontend btSlipEvPct).
        if slip_type.lower() == "power" or len(true_probs) == 2:
            best_ev = power_slip_ev(true_probs)
        else:
            best_ev = flex_slip_ev(true_probs)

        if best_ev is None or best_ev <= 0:
            return None

        # ── Pre-insert invariant check ─────────────────────────────────
        # Belt-and-suspenders: verify the slip we're about to write has
        # no within-slip pair_key duplicate AND none of its pair_keys
        # already exist in a recently-committed slip. The per-user lock
        # serializes try_log_slip, but the dedup set read above happened
        # at the top of the method — if a peer process bypassed the
        # lock somehow (e.g. logged via a different code path), we'd
        # catch it here rather than create a duplicate.
        final_pair_keys: list[tuple] = []
        final_leg_keys:  list[tuple] = []
        for bet in best_legs:
            p_name = bet.get("player_name", "") or ""
            start  = bet.get("start_time", "") or ""
            final_pair_keys.append(make_bet_key(p_name, start))
            final_leg_keys.append(make_leg_key(
                p_name,
                bet.get("prop_type", "") or "",
                bet.get("pp_line", "") or "",
                bet.get("side", "") or "",
                start,
            ))
        if len(set(final_pair_keys)) != len(final_pair_keys):
            logger.error(
                "Backtest: within-slip pair_key duplicate detected just "
                "before insert — aborting slip for user %s. keys=%s",
                self.user_id, final_pair_keys,
            )
            return None
        # Re-read the dedup set inside the lock — catches the rare case
        # of a concurrent commit between the top-of-method read and now.
        verify_pair, verify_leg = self._load_dedup_sets()
        if any(k in verify_pair for k in final_pair_keys):
            logger.error(
                "Backtest: cross-slip pair_key collision detected just "
                "before insert — aborting slip for user %s. keys=%s",
                self.user_id, final_pair_keys,
            )
            return None
        if any(k in verify_leg for k in final_leg_keys):
            logger.error(
                "Backtest: cross-slip leg_key collision detected just "
                "before insert — aborting slip for user %s.",
                self.user_id,
            )
            return None

        slip_id = str(uuid.uuid4())[:8].upper()
        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        proj_ev = round(best_ev, 4)

        rows = []
        for i, bet in enumerate(best_legs, start=1):
            true_p = round(float(bet.get("true_prob") or 0), 4)
            # raw_true_prob is the pre-calibration consensus probability —
            # required by the calibration refit so it trains on raw → outcome
            # rather than on calibrated → outcome (the feedback-loop bug
            # documented in migration_006). Falls back to true_prob for
            # safety in case a caller forgets to populate it.
            raw_p_val = bet.get("raw_true_prob")
            raw_p = round(float(raw_p_val), 4) if raw_p_val is not None else true_p
            rows.append({
                "slip_id":          slip_id,
                "user_id":          self.user_id,
                "timestamp":        timestamp,
                "slip_type":        slip_type,
                "n_legs":           n_legs,
                "proj_slip_ev_pct": proj_ev,
                "leg_num":          i,
                "player":           bet.get("player_name", ""),
                "league":           bet.get("league", ""),
                "prop":             bet.get("prop_type", ""),
                "line":             bet.get("pp_line", ""),
                "side":             bet.get("side", ""),
                "true_prob":        true_p,
                "raw_true_prob":    raw_p,
                "ind_ev_pct":       round(_ev(bet), 4),
                "game_start":       bet.get("start_time", ""),
                "closing_prob":     true_p,
                "clv_pct":          0.0,
                "result":           "pending",
                "stat_actual":      "",
                "team":             (bet.get("team") or "").strip(),
            })

        # Write to Supabase using provided client or user-scoped DB
        db = self.db_client or get_user_db(self.jwt)
        if not db:
            logger.error("Backtest: no database connection, cannot log slip %s", slip_id)
            return None

        try:
            # 1. Insert slip header
            db.table("slips").insert({
                "id":               slip_id,
                "user_id":          self.user_id,
                "timestamp":        timestamp,
                "slip_type":        slip_type,
                "n_legs":           n_legs,
                "proj_slip_ev_pct": proj_ev
            }).execute()
            # 2. Insert legs
            db_legs = []
            for r in rows:
                db_legs.append({
                    "slip_id":       slip_id,
                    "user_id":       self.user_id,
                    "leg_num":       r["leg_num"],
                    "player":        r["player"],
                    "league":        r["league"],
                    "prop":          r["prop"],
                    "line":          r["line"],
                    "side":          r["side"],
                    "true_prob":     r["true_prob"],
                    "raw_true_prob": r["raw_true_prob"],
                    "ind_ev_pct":    r["ind_ev_pct"],
                    "game_start":    r["game_start"] if r["game_start"] else None,
                    "closing_prob":  r["closing_prob"],
                    "clv_pct":       r["clv_pct"],
                    "result":        r["result"],
                    "stat_actual":   None if r["stat_actual"] == "" else r["stat_actual"],
                    "team":          r["team"] or None,
                    # Canonical fingerprint — backed by the partial
                    # UNIQUE INDEX on (user_id, dedup_key) added in
                    # migration_008. Second insertion of the same
                    # (player, prop, line, side, game-day) for this
                    # user is rejected by Postgres regardless of which
                    # worker/process attempts it.
                    "dedup_key":     make_leg_fingerprint(
                        r["player"], r["prop"], r["line"], r["side"], r["game_start"] or "",
                    ),
                })
            # Insert legs idempotently: never double-inserts on a lost-response
            # retry (the 12-leg-slip bug), keeps dedup_key so the unique index
            # stays effective, and strips an optional column only when the DB
            # error names it. A genuine cross-slip duplicate (23505 after this
            # slip's own legs are excluded) propagates to the rollback below.
            try:
                insert_legs_idempotent(db, slip_id, db_legs)
            except Exception as legs_exc:
                err_msg = str(legs_exc)
                if _is_duplicate_err(err_msg):
                    # Another slip already contains one of these legs (the
                    # cross-slip dedup index fired). Roll back the header and
                    # abandon — auto-logger retries next cycle with the
                    # duplicate excluded from the pool.
                    try:
                        db.table("slips").delete().eq("id", slip_id).execute()
                    except Exception as cleanup_exc:
                        logger.warning(
                            "Backtest: failed to roll back orphan slip header %s after duplicate-leg "
                            "rejection: %s", slip_id, cleanup_exc,
                        )
                    logger.info(
                        "Backtest: skipped Auto-Slip %s — duplicate leg(s) already in DB for user %s "
                        "(unique-index rejected the insert: %s)", slip_id, self.user_id, err_msg[:200],
                    )
                    return None
                raise
            logger.info("Backtest: logged Auto-Slip %s (%d-leg EV=%.2f%%) for user %s", slip_id, n_legs, best_ev * 100, self.user_id)
        except Exception as db_exc:
            logger.error("Backtest: Supabase write failed for slip %s: %s", slip_id, db_exc)
            # The slip header is written before the legs (two separate inserts —
            # PostgREST has no cross-table transaction). If anything after the
            # header fails (a non-duplicate legs-insert error that exhausted the
            # optional-column retries, an RLS/constraint rejection on `legs`, a
            # transient network error), the header is already committed. Delete
            # it so we never leave an orphaned "N-leg" slip with zero legs
            # showing up in the backtest. Deleting a not-yet-inserted id is a
            # harmless no-op if the failure was the header insert itself.
            try:
                db.table("slips").delete().eq("id", slip_id).execute()
            except Exception as cleanup_exc:
                logger.warning(
                    "Backtest: failed to roll back orphan slip header %s after write "
                    "error: %s", slip_id, cleanup_exc,
                )
            return None

        return {
            "slip_id":          slip_id,
            "timestamp":        timestamp,
            "slip_type":        slip_type,
            "n_legs":           n_legs,
            "proj_slip_ev_pct": proj_ev,
            "legs": [
                {
                    "player":     r["player"],
                    "league":     r["league"],
                    "prop":       r["prop"],
                    "line":       r["line"],
                    "side":       r["side"],
                    "true_prob":  r["true_prob"],
                    "ind_ev_pct": r["ind_ev_pct"],
                    "game_start": r["game_start"],
                }
                for r in rows
            ],
        }
