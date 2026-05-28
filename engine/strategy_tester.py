"""
Strategy Sandbox: historical-performance evaluator for tracked props.

Pipeline (per `run_simulation` call):
  1. Pull resolved observatory rows (paged) including raw_true_prob, side,
     market_width.
  2. Apply the *current* isotonic calibration to raw_true_prob so the
     reported numbers reflect what today's pipeline would have produced
     on past raw markets — not whatever calibration vintage was logged
     at the time.
  3. Apply user filters (date range, leagues, prop includes/excludes,
     min_prob on the calibrated value).
  4. Group rows into slates (one per calendar day).
  5. Build slips per the chosen `slip_strategy`:
       top_prob       — top-N by calibrated_prob (legacy default)
       top_ev         — top-N by individual EV%
       top_ev_capped  — top-N by EV%, with a per-(league, game_date) cap
       live_replay    — mirrors engine/backtest.BacktestLogger:
                        EV-sorted, per-game cap, player-game dedup
  6. Simulate each slip with full PrizePicks push/dnp semantics: pushes
     and DNPs reduce the effective slip size and refund proportionally,
     matching the live API at web/app.py:2446-2458.
  7. Bootstrap (N=BOOTSTRAP_RESAMPLES) the slip set to produce 95% CIs
     on aggregate ROI, win-rate, max-drawdown.
  8. Build per-(league, prop, hits) breakdowns with bootstrap CI on each
     row; flag rows with n_slips < MIN_BUCKET_SLIPS.
  9. Emit a `funnel` diagnostic that reports row counts at every filter
     step plus push/dnp leg counts in formed slips, so the user can
     see exactly how many observations survived to inform the report.

The legacy `optimize_threshold` function is preserved but rewired to use
the calibration-on-the-fly path so its output is consistent with the
main report.
"""
import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Iterable, Tuple
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

from engine.database import get_db
from engine.constants import (
    POWER_PAYOUTS, FLEX_PAYOUTS, OPTIMAL_IMPLIED_DECIMAL, BREAK_EVEN,
    EXCLUDED_LEAGUES,
)
from engine.isotonic_calibration import load_isotonic_calibration, calibrate
from engine.backtest import (
    make_bet_key, make_leg_key,
    MIN_LEG_EV_PCT as _LIVE_MIN_LEG_EV_PCT,
    _DEDUP_WINDOW_HOURS as _LIVE_DEDUP_WINDOW_HOURS,
)
from engine.ev_calculator import power_slip_ev, flex_slip_ev
from engine.dedup import drop_duplicate_slips

logger = logging.getLogger(__name__)


# ── Constants ────────────────────────────────────────────────────────────

# PrizePicks rule: every slip must contain picks from at least two
# distinct teams. Players from opposing teams in the same game count as
# two. Enforced in `_enforce_two_teams()` below; supersedes the v2
# per-game cap that used to live here.
TWO_TEAMS_REQUIRED = 2

# Distinctness fallback: when a leg has no team value (legacy data
# pre-migration_007), use this prefix on player as the team identifier
# so the rule still has a sane meaning rather than silently passing
# every slip.
_FALLBACK_TEAM_PREFIX = "player:"

# Bootstrap sample count. 500 is enough for stable 95% CIs on n>=50 slips
# while keeping the simulation under ~1s for typical filter sets.
BOOTSTRAP_RESAMPLES = 500

# Minimum slips a per-(league, prop) breakdown bucket needs to qualify
# for a non-flagged row. Below this threshold the breakdown row is
# returned with `is_thin: True` so the UI can grey it out.
MIN_BUCKET_SLIPS = 20

# Bootstrap RNG seed. Deterministic so re-runs produce identical CIs;
# operational sandboxing wants reproducibility, not novel randomness.
_BOOTSTRAP_SEED = 0xC0DE


# ── Config ───────────────────────────────────────────────────────────────

@dataclass
class StrategyConfig:
    leagues: List[str] = field(default_factory=list)
    min_prob: float = 0.5408                    # break-even default
    slip_size: int = 6                          # 2..6
    slip_type: str = "flex"                     # "power" | "flex"
    bankroll: float = 100.0
    bet_size: float = 1.0
    excluded_props: List[str] = field(default_factory=list)
    included_props: List[str] = field(default_factory=list)
    use_calibration: bool = True
    use_kelly: bool = False

    # New (sandbox v2) fields:
    slip_strategy: str = "live_replay"          # see module docstring
    start_date: Optional[str] = None            # YYYY-MM-DD inclusive
    end_date:   Optional[str] = None            # YYYY-MM-DD inclusive
    bootstrap:  bool = True                     # toggle CI computation


# ── Data fetch ───────────────────────────────────────────────────────────

class StrategyTester:
    def __init__(self):
        self.db = get_db()
        # Load the live calibration curves once per simulation. The same
        # curves apply to every row; loading them per-row would re-read
        # the JSON ~10K times.
        self._curves: dict = load_isotonic_calibration()

    def reload_calibration(self) -> None:
        """Re-read isotonic_calibration.json. Call this between simulations
        if a refit ran in between (e.g. from /api/calibration/refit)."""
        self._curves = load_isotonic_calibration()

    def _fetch_resolved_observatory(self, leagues: List[str]) -> pd.DataFrame:
        """Page through every resolved market_observatory row matching the
        league filter. Pulls raw_true_prob and side too — raw_true_prob
        is the pre-calibration consensus probability we re-calibrate
        with the current curves, and `side` is needed for the per-(L,
        prop, side) bucket lookup."""
        from engine.constants import is_excluded_league
        # Apply the league exclusion list before the query so even raw API
        # callers that pass `leagues=["SOCCER"]` can't dredge up legacy
        # observatory rows. When the caller passes no leagues at all (=
        # "all leagues"), we still filter post-fetch below; the in_()
        # clause only narrows positively.
        leagues = [lg for lg in (leagues or []) if not is_excluded_league(lg)]
        # Three column tiers, tried in order on the first page. The fallback
        # is graceful per-migration: if migration_007 (team) isn't applied
        # we still get raw_true_prob/market_width from migration_006; if
        # neither is applied we drop to the v1 set. Switching tiers only
        # happens at offset 0 — once a tier succeeds, subsequent pages
        # use the same column list.
        # `line` is included so the per-leg dedup key (`make_leg_key`)
        # is actually keyed on line, not collapsed to "". Without it,
        # two rows that legitimately differ only by line (line moved
        # mid-day) appear as the same leg_key — which is the wrong
        # direction of strictness for leg-level dedup, even though
        # pair_key would still catch them as same-player-same-game.
        # v4 adds first_seen_at / last_seen_at (migration_009) so the
        # tick-based replay can reconstruct the live pool at every
        # historical scrape moment. Pre-migration deploys fall through
        # to v3 and the replay treats those rows as single-instant
        # observations at game_start.
        cols_v4 = (
            "result, prop, line, true_prob, raw_true_prob, side, "
            "game_start, league, player, market_width, team, "
            "first_seen_at, last_seen_at"
        )
        cols_v3 = (
            "result, prop, line, true_prob, raw_true_prob, side, "
            "game_start, league, player, market_width, team"
        )
        cols_v2 = (
            "result, prop, line, true_prob, raw_true_prob, side, "
            "game_start, league, player, market_width"
        )
        cols_v1 = "result, prop, line, true_prob, side, game_start, league, player"
        tier_order = [cols_v4, cols_v3, cols_v2, cols_v1]

        PAGE = 1000
        # Number of concurrent page fetches. PostgREST is happy with a few
        # parallel requests; supabase-py uses a synchronous httpx client
        # so threads each hold their own connection. 8 keeps the
        # round-trip latency dominated rather than the per-page time.
        CONCURRENCY = 8

        def _fetch_page(cols: str, offset: int) -> tuple[list, bool]:
            """Returns (rows, was_full_page). was_full_page tells the
            caller whether to keep paging.

            Pages are explicitly ordered so the same query returns the
            same rows in the same order every time. Without an order(),
            PostgREST is free to return rows in whatever sequence the
            planner chooses, which makes downstream tie-breaking and
            re-runs nondeterministic."""
            q = (self.db.table("market_observatory")
                     .select(cols)
                     .neq("result", "pending")
                     .order("game_start", desc=False)
                     .order("market_key", desc=False))
            if leagues:
                q = q.in_("league", leagues)
            res = q.range(offset, offset + PAGE - 1).execute()
            rows = res.data or []
            return rows, len(rows) >= PAGE

        # ── Phase 1: column-tier negotiation on the first page ──────────
        active_cols: str | None = None
        head_rows: list = []
        head_full = False
        for cols in tier_order:
            try:
                head_rows, head_full = _fetch_page(cols, 0)
                active_cols = cols
                break
            except Exception as exc:
                logger.info(
                    "Sandbox: falling back to lower observatory column tier "
                    "(missing migration?): %s", exc,
                )
                continue

        if active_cols is None:
            logger.warning("Sandbox: every observatory column tier failed.")
            return pd.DataFrame()

        all_rows: list[dict] = list(head_rows)
        if not head_full:
            return pd.DataFrame(all_rows)

        # ── Phase 2: parallel paged fetches for the rest ────────────────
        # Single ThreadPoolExecutor across all batches (creating one per
        # batch added 50-100ms of pool-spin-up overhead per batch). Each
        # batch dispatches CONCURRENCY pages, waits for all to complete
        # in offset order, and stops as soon as it sees a short page.
        offset = PAGE
        MAX_OFFSET = 500_000
        done = False
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            while not done and offset <= MAX_OFFSET:
                batch_offsets = [offset + i * PAGE for i in range(CONCURRENCY)
                                 if offset + i * PAGE <= MAX_OFFSET]
                futs = {pool.submit(_fetch_page, active_cols, o): o
                        for o in batch_offsets}
                # Collect results so the final list reads back the same as
                # the sequential implementation would have produced
                # (downstream stable-sort tie-breaking depends on order).
                results: dict[int, tuple[list, bool]] = {}
                for fut in as_completed(futs):
                    o = futs[fut]
                    try:
                        results[o] = fut.result()
                    except Exception as exc:
                        logger.warning(
                            "Sandbox: observatory page fetch failed at offset %d: %s",
                            o, exc,
                        )
                        results[o] = ([], False)
                        done = True
                for o in sorted(results):
                    rows, full = results[o]
                    all_rows.extend(rows)
                    if not full:
                        done = True
                offset += CONCURRENCY * PAGE
        df = pd.DataFrame(all_rows)
        if not df.empty and "league" in df.columns:
            # Belt-and-suspenders: drop excluded leagues post-fetch too,
            # since `leagues=[]` (= all leagues) bypasses the in_() guard.
            df = df[~df["league"].fillna("").str.upper().isin(EXCLUDED_LEAGUES)]
        return df


    # ── Calibration ──────────────────────────────────────────────────────

    def _apply_current_calibration(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply today's isotonic calibration to each row's raw_true_prob.

        Falls back to the stored true_prob when raw_true_prob is missing
        (legacy rows pre-migration_006), so historical data still
        contributes — just without the loop-bug correction.

        Vectorized: extracts league/prop/side/base as numpy arrays once,
        then iterates with a plain Python loop. Avoids `df.apply(axis=1)`
        which builds a Series per row (~10x slower on 22k rows).
        """
        if df.empty:
            df = df.copy()
            df["calibrated_prob"] = []
            return df

        # Prefer raw_true_prob; fall back to true_prob.
        if "raw_true_prob" in df.columns:
            base_series = df["raw_true_prob"].combine_first(df["true_prob"])
        else:
            base_series = df["true_prob"]
        base_arr   = pd.to_numeric(base_series, errors="coerce").to_numpy()
        league_arr = df["league"].fillna("").to_numpy()
        prop_arr   = df["prop"].fillna("").to_numpy()
        if "side" in df.columns:
            side_arr = df["side"].fillna("").to_numpy()
        else:
            side_arr = np.full(len(df), "", dtype=object)

        curves = self._curves
        # Bind locally for the hot loop — attribute lookup avoidance.
        _calibrate = calibrate

        n = len(df)
        out = np.full(n, np.nan, dtype=float)
        for i in range(n):
            v = base_arr[i]
            # NaN check + bounds check in one — np.isfinite covers both.
            if not (np.isfinite(v) and 0.0 < v < 1.0):
                continue
            out[i] = _calibrate(curves, league_arr[i], prop_arr[i], side_arr[i], float(v))

        df = df.copy()
        df["calibrated_prob"] = out
        return df


    # ── Slip construction strategies ────────────────────────────────────

    @staticmethod
    def _ev_pct(p: float) -> float:
        """Individual EV% at the optimal Power-6 implied decimal. Same
        formula BetResult uses — kept here so we don't import the whole
        ev_calculator just for one constant multiply."""
        return p * OPTIMAL_IMPLIED_DECIMAL - 1.0

    @staticmethod
    def _team_key(row) -> str:
        """PrizePicks distinct-team identifier. Prefer the team
        abbreviation captured in the observatory (added by
        migration_007); fall back to the player name so slips on legacy
        data still get sane (if structurally weaker) distinctness
        checks. Two players from opposing teams in the same game count
        as two distinct teams under PP rules — this is naturally
        satisfied because each player has a distinct `team`."""
        if hasattr(row, "get"):
            t = (row.get("team") or "")
        else:
            t = ""
        t = t.strip().upper() if isinstance(t, str) else ""
        if t:
            return t
        # Fallback: use the player as the team identity.
        player = (row.get("player") if hasattr(row, "get") else "") or ""
        return _FALLBACK_TEAM_PREFIX + str(player).strip().lower()

    def _select_ranked(self, slate: pd.DataFrame, slip_size: int,
                       *, score_col: str, dedup: bool,
                       used_pair: Optional[set] = None,
                       used_leg:  Optional[set] = None) -> List[List[dict]]:
        """Greedy slip builder used by all sandbox strategies.

        `score_col` selects the ranking signal ('calibrated_prob' or
        '_ev'). `dedup` toggles cross-slip player-game / leg dedup
        (live_replay turns it on; top_prob/top_ev leave it off for
        backward compat with v2). Two-team enforcement is always on —
        PrizePicks rejects single-team slips, so the sandbox can't
        report ROI on slips that wouldn't actually be bettable.

        Returns a list of slips, each slip being a list of row-dicts.
        Switched from per-row `DataFrame.loc[idx]` to plain Python
        lists/dicts after profiling showed `.loc` was the dominant
        cost (~10s on a 234-slip simulation; ~1s after this rewrite).
        Callers consume the dicts directly (no DataFrame APIs).

        Slips that can't satisfy the two-team rule are dropped, not
        downgraded to single-team — this matches the live system's
        behavior and keeps the simulated slip set apples-to-apples
        with what would actually have been logged."""
        # Stable sort + deterministic tie-breaker. Pandas' default kind
        # is quicksort (NOT stable), so two legs with the same score
        # could come out in different relative order between runs. The
        # tie-breaker uses player+prop+side as a string fallback so the
        # final pick set is identical for identical inputs.
        slate = slate.assign(
            _tie=(slate.get("player", "").astype(str)
                  + "|" + slate.get("prop", "").astype(str)
                  + "|" + slate.get("side", "").astype(str))
        )
        sorted_df = slate.sort_values(
            [score_col, "_tie"],
            ascending=[False, True],
            kind="stable",
        )
        # to_dict("records") is one O(n) conversion; subsequent access is
        # O(1) per field versus pandas' O(log n) plus per-call overhead.
        rows = sorted_df.to_dict("records")
        n = len(rows)
        if n < slip_size:
            return []

        # Pre-compute the dedup keys + team keys once per row. Avoids
        # rebuilding them inside the inner loop (the hot path), and
        # lets the two-team swap iterate without a second DataFrame
        # traversal.
        team_keys: list[str] = [self._team_key(r) for r in rows]
        if dedup:
            p_keys: list[tuple] = [None] * n
            l_keys: list[tuple] = [None] * n
            for i, r in enumerate(rows):
                player = r.get("player", "") or ""
                start  = r.get("game_start", "") or ""
                p_keys[i] = make_bet_key(player, start)
                l_keys[i] = make_leg_key(
                    player,
                    r.get("prop", "") or "",
                    r.get("line", "") if "line" in r else "",
                    r.get("side", "") or "",
                    start,
                )
        else:
            p_keys = l_keys = None

        slips: list[list[dict]] = []
        used = [False] * n
        # Caller-provided dedup sets persist across slates so a single
        # leg/player-game can never appear in two slips across the whole
        # simulation, not just within one slate. Default to empty sets
        # when no caller passes them (e.g., legacy call sites).
        if used_pair is None:
            used_pair = set()
        if used_leg is None:
            used_leg = set()

        while True:
            picks: list[int] = []
            seen_pair: set = set()
            seen_leg:  set = set()
            for i in range(n):
                if used[i]:
                    continue
                if dedup:
                    pk, lk = p_keys[i], l_keys[i]
                    if pk in used_pair: continue
                    if lk in used_leg:  continue
                    if pk in seen_pair: continue
                    if lk in seen_leg:  continue
                    seen_pair.add(pk); seen_leg.add(lk)
                picks.append(i)
                if len(picks) == slip_size:
                    break

            if len(picks) < slip_size:
                break

            # Two-team enforcement (PrizePicks rule). Swap the lowest-
            # ranked pick for the highest-ranked unused leg from a
            # different team if the EV-greedy first pass produced a
            # single-team slip. List index lookup is O(1) per row, so
            # the linear scan stays cheap.
            team_set = {team_keys[i] for i in picks}
            if len(team_set) < TWO_TEAMS_REQUIRED:
                single_team = next(iter(team_set))
                pick_set = set(picks)
                # Within-slip dedup state for the legs we'd keep (all
                # but the last pick, which is the swap victim). The
                # replacement must not collide with these either —
                # otherwise the swap can put e.g. the same player back
                # into the slip under a different team string.
                keep_idxs = picks[:-1]
                kept_pair: set = {p_keys[k] for k in keep_idxs} if dedup else set()
                kept_leg:  set = {l_keys[k] for k in keep_idxs} if dedup else set()
                replacement = -1
                for j in range(n):
                    if used[j] or j in pick_set:
                        continue
                    if team_keys[j] == single_team:
                        continue
                    if dedup:
                        if p_keys[j] in used_pair: continue
                        if l_keys[j] in used_leg:  continue
                        if p_keys[j] in kept_pair: continue
                        if l_keys[j] in kept_leg:  continue
                    replacement = j
                    break
                if replacement < 0:
                    break
                picks = picks[:-1] + [replacement]

            slips.append([rows[i] for i in picks])
            for i in picks:
                used[i] = True
            if dedup:
                for i in picks:
                    used_pair.add(p_keys[i])
                    used_leg.add(l_keys[i])
        return slips

    def _build_slips(
        self, slate: pd.DataFrame, slip_size: int, strategy: str,
        *, used_pair: Optional[set] = None, used_leg: Optional[set] = None,
    ) -> List[List[dict]]:
        """Dispatch to the chosen slip-construction strategy. Per-game
        cap is intentionally absent — superseded by the PrizePicks
        two-team requirement enforced inside `_select_ranked`."""
        # Pre-compute EV column on the underlying ndarray (avoids the
        # per-row `.apply` overhead the v2 code path was paying).
        if "_ev" not in slate.columns:
            slate = slate.copy()
            slate["_ev"] = slate["calibrated_prob"].to_numpy() * OPTIMAL_IMPLIED_DECIMAL - 1.0

        # All strategies now enforce cross-slip dedup. The legacy
        # `dedup=False` paths for top_prob/top_ev were intentionally
        # permissive for v2 backward compat, but they let the same leg
        # (e.g., "Breanna Stewart over 21.5 points" in the same game)
        # land in multiple slips — surprising to users reading the slip
        # log, and indefensible when the user asked "no prop in two
        # slips." Forcing dedup on universally fixes that without
        # changing what callers pass.
        if strategy == "top_prob":
            return self._select_ranked(slate, slip_size, score_col="calibrated_prob",
                                       dedup=True, used_pair=used_pair, used_leg=used_leg)
        if strategy == "top_ev":
            return self._select_ranked(slate, slip_size, score_col="_ev",
                                       dedup=True, used_pair=used_pair, used_leg=used_leg)
        if strategy == "live_replay":
            return self._select_ranked(slate, slip_size, score_col="_ev",
                                       dedup=True, used_pair=used_pair, used_leg=used_leg)
        logger.warning("Sandbox: unknown slip_strategy %r — using live_replay", strategy)
        return self._select_ranked(slate, slip_size, score_col="_ev",
                                   dedup=True, used_pair=used_pair, used_leg=used_leg)


    # ── Live auto-builder replay ────────────────────────────────────────

    def _replay_live_auto_builder(
        self, df: pd.DataFrame, config: StrategyConfig,
    ) -> tuple[list[dict], dict]:
        """Tick-based replay of engine/backtest.BacktestLogger.try_log_slip.

        At every distinct scrape moment (first_seen_at rounded down to the
        minute) the sandbox asks: "which slips could the strategy have
        produced given exactly the legs that were live on the board at
        that moment, plus the 48 h dedup state from the slips it would
        already have logged earlier in this replay?"

        Unlike the live auto-builder — which places a single best slip per
        scrape because you can only stake one bet at a time — the sandbox
        is an analytics surface and reports the *complete* opportunity
        set: every disjoint valid slip the live pool supported at the
        tick, partitioned greedily in EV-descending order (no leg reused
        across slips). This is what makes the equity curve, breakdowns,
        and bootstrap CIs reflect the strategy's full historical edge
        rather than a thin one-slip-per-tick sample.

        This is the path the user reaches via `slip_strategy='live_replay'`.
        Each individual slip mirrors the production
        `BacktestLogger._try_log_slip_locked` gates exactly: EV-floor
        filter, EV-descending greedy pick, cross-slip (player, sports_day)
        + exact-leg dedup, two-distinct-teams swap, BREAK_EVEN avg-prob
        gate, slip-EV > 0 gate.

        The user's `config.min_prob` filter is applied upstream in
        `run_simulation` (the live builder doesn't have a prob floor, but
        the sandbox exposes one so the user can test "what if I only took
        legs above X%"). Everything else is identical to live."""
        if df.empty:
            return [], {}

        df = df.copy()
        # Coerce availability bounds; legacy rows pre-migration_009 lack
        # these columns and collapse to a single-instant observation at
        # game_start (best-available proxy — we genuinely don't know how
        # long pre-migration legs sat on the board).
        for col in ("first_seen_at", "last_seen_at"):
            if col not in df.columns:
                df[col] = pd.NaT
        df["first_seen_dt"] = pd.to_datetime(
            df["first_seen_at"], errors="coerce", utc=True,
        )
        df["last_seen_dt"] = pd.to_datetime(
            df["last_seen_at"], errors="coerce", utc=True,
        )
        df["first_seen_dt"] = df["first_seen_dt"].fillna(df["game_start_dt"])
        df["last_seen_dt"]  = df["last_seen_dt"].fillna(df["first_seen_dt"])
        df = df[df["first_seen_dt"].notna() & df["last_seen_dt"].notna()]
        if df.empty:
            return [], {}

        # Per-leg EV column (matches `_select_ranked`'s `_ev`).
        df["_ev"] = df["calibrated_prob"].to_numpy() * OPTIMAL_IMPLIED_DECIMAL - 1.0
        # Live builder's hard EV floor. Below this, the production logger
        # refuses to include the leg at all — replay must mirror.
        df = df[df["_ev"] >= _LIVE_MIN_LEG_EV_PCT]
        if df.empty:
            return [], {}

        # Tick set: minute-floor of first_seen_at. Postgres NOW() is
        # statement-time so a single batch insert lands every row at the
        # same microsecond; the minute floor folds neighbouring batches
        # from the same scrape cycle so we don't double-attempt slips.
        df["_tick"] = df["first_seen_dt"].dt.floor("min")
        ticks = np.sort(df["_tick"].unique())

        # Pre-compute everything the inner loop needs as plain Python so
        # the per-tick mask + scan stays in numpy / list-comprehension
        # territory (DataFrame.loc is ~10x slower per row).
        # Compare against the FLOORED first_seen / last_seen so a row
        # logged at 12:00:23 is correctly "live" at its own floored tick
        # (12:00:00) — the raw comparison missed every row whose seconds
        # were >0 and left every tick with an empty live pool.
        df["_first_floor"] = df["first_seen_dt"].dt.floor("min")
        df["_last_floor"]  = df["last_seen_dt"].dt.floor("min")
        first_arr = df["_first_floor"].to_numpy()
        last_arr  = df["_last_floor"].to_numpy()
        rows_all  = df.to_dict("records")
        n_rows = len(rows_all)

        p_keys_all: list[tuple] = [None] * n_rows
        l_keys_all: list[tuple] = [None] * n_rows
        team_all:   list[str]   = [""] * n_rows
        for i, r in enumerate(rows_all):
            player = r.get("player", "") or ""
            start  = r.get("game_start", "") or ""
            p_keys_all[i] = make_bet_key(player, start)
            l_keys_all[i] = make_leg_key(
                player, r.get("prop", "") or "",
                r.get("line", "") if "line" in r else "",
                r.get("side", "") or "", start,
            )
            team_all[i] = self._team_key(r)

        be_key = (str(config.slip_size), config.slip_type.lower())
        slip_be = BREAK_EVEN.get(be_key)
        is_power = config.slip_type.lower() == "power"
        slip_ev_fn = power_slip_ev if is_power else flex_slip_ev
        dedup_window = pd.Timedelta(hours=_LIVE_DEDUP_WINDOW_HOURS)

        sim_slips: list[dict] = []
        # Rolling 48h log of placed slips for dedup decay. Each entry:
        # (tick_timestamp, [(p_key, l_key) per leg]).
        placed: list[tuple] = []

        funnel = {
            "distinct_scrape_ticks": int(len(ticks)),
            "ticks_with_slip":       0,
            "ticks_without_pool":    0,
            "ticks_no_valid_slip":   0,
        }

        for tick in ticks:
            # Decay dedup window relative to *this* tick — matches live,
            # which queries `timestamp > now() - 48h` at each attempt.
            cutoff = tick - dedup_window
            placed = [p for p in placed if p[0] > cutoff]
            used_pair: set = set()
            used_leg:  set = set()
            for _ts, leg_keys in placed:
                for pk, lk in leg_keys:
                    used_pair.add(pk)
                    used_leg.add(lk)

            mask = (first_arr <= tick) & (last_arr >= tick)
            idxs_arr = np.where(mask)[0]
            if len(idxs_arr) < config.slip_size:
                funnel["ticks_without_pool"] += 1
                continue

            # EV-descending sort with deterministic tie-break (same shape
            # as `_select_ranked` so identical inputs yield identical
            # outputs across runs).
            idxs = sorted(
                idxs_arr.tolist(),
                key=lambda i: (
                    -float(rows_all[i].get("_ev") or 0.0),
                    f'{rows_all[i].get("player","")}|'
                    f'{rows_all[i].get("prop","")}|'
                    f'{rows_all[i].get("side","")}',
                ),
            )

            # Form EVERY disjoint valid slip available at this tick, not
            # just the single best one. The live auto-builder logs one
            # slip per scrape (you can only place one bet at a time), but
            # the sandbox is an *analytics* surface: the user wants to see
            # the full opportunity set the strategy exposed over the
            # window. So we keep greedily partitioning the live pool —
            # highest-EV legs first — into non-overlapping slips until no
            # further full, valid slip can be built. Each leg is consumed
            # by at most one slip per tick (the "no prop in two slips"
            # contract), tracked in the tick-local sets below; cross-tick
            # dedup still flows through `placed` / `used_pair` / `used_leg`.
            # Reduce the live pool to legs that survive cross-tick dedup
            # and (player, leg) de-duplication, preserving EV-descending
            # order. These are the candidates to partition into slips.
            avail: list[int] = []
            seen_pair: set = set()
            seen_leg:  set = set()
            for i in idxs:
                pk, lk = p_keys_all[i], l_keys_all[i]
                if pk in used_pair or lk in used_leg:   # cross-tick dedup
                    continue
                if pk in seen_pair or lk in seen_leg:   # intra-pool dedup
                    continue
                avail.append(i)
                seen_pair.add(pk); seen_leg.add(lk)

            # Partition the pool into as MANY disjoint valid slips as it
            # supports — not just the single best one. A live scrape logs
            # one bet (you place one slip at a time), but the sandbox is an
            # analytics surface: it should surface the whole opportunity
            # set the strategy exposed over the window.
            #
            # The BREAK_EVEN gate is on the slip *average*, so greedily
            # stacking the highest-prob legs into the first slip makes that
            # slip clear BE by a wide margin while every later slip falls
            # below it — the user sees ONE slip. Instead we (1) pick the
            # largest group count whose top legs still average above BE,
            # then (2) round-robin the legs across that many buckets so
            # each pairs strong legs with marginal ones, lifting the most
            # slips over the break-even line. Buckets are disjoint by
            # construction, so no leg is reused within the tick.
            slips_this_tick = 0
            n = config.slip_size
            g_max = len(avail) // n
            if g_max >= 1 and slip_be is not None:
                probs_avail = [
                    float(rows_all[i].get("calibrated_prob") or 0.0) for i in avail
                ]
                # Prefix means are non-increasing (avail is prob/EV-desc),
                # so the largest feasible group count maximises slips.
                ng = 0
                run_sum = 0.0
                for cand in range(1, g_max + 1):
                    run_sum = sum(probs_avail[: cand * n])
                    if (run_sum / (cand * n)) >= slip_be:
                        ng = cand
                if ng >= 1:
                    chosen = avail[: ng * n]
                    # Round-robin: bucket b gets positions b, b+ng, b+2ng…
                    buckets = [chosen[b::ng] for b in range(ng)]
                    for picks in buckets:
                        true_probs = [
                            float(rows_all[i].get("calibrated_prob") or 0.0)
                            for i in picks
                        ]
                        # Per-bucket gates: a bucket can still land below BE
                        # even when the global top-(ng*n) average cleared it.
                        if (sum(true_probs) / n) < slip_be:
                            continue
                        slip_ev = slip_ev_fn(true_probs)
                        if slip_ev is None or slip_ev <= 0:
                            continue
                        # Two-distinct-teams rule (PrizePicks): drop a slip
                        # that the partition left single-team.
                        if len({team_all[i] for i in picks}) < TWO_TEAMS_REQUIRED:
                            continue

                        selected_legs = [rows_all[i] for i in picks]
                        results = [r.get("result", "") for r in selected_legs]
                        payout_mult, n_eff, hits_eff, n_pushed = self._payout_with_push(
                            results, config.slip_type,
                        )
                        if config.use_kelly:
                            k_frac = self._calculate_kelly_fraction(
                                true_probs, config.slip_size, config.slip_type,
                            )
                            bet_size = max(0.0, float(config.bankroll) * k_frac)
                        else:
                            bet_size = config.bet_size

                        profit = (bet_size * payout_mult) - bet_size if bet_size > 0 else 0.0
                        first_leg = selected_legs[0]

                        sim_slips.append({
                            "timestamp": pd.Timestamp(tick).isoformat(),
                            "league":    first_leg.get("league"),
                            "hits":      hits_eff,
                            "n_eff":     n_eff,
                            "n_legs":    config.slip_size,
                            "n_pushed":  n_pushed,
                            "payout":    bet_size * payout_mult,
                            "bet_size":  bet_size,
                            "profit":    profit,
                            "legs": [
                                {
                                    "player":     r.get("player", ""),
                                    "prop":       r.get("prop", ""),
                                    "line":       r.get("line", ""),
                                    "side":       r.get("side", ""),
                                    "game_start": r.get("game_start", ""),
                                    "true_prob":  r.get("calibrated_prob"),
                                    "result":     r.get("result", ""),
                                }
                                for r in selected_legs
                            ],
                        })
                        placed.append((
                            tick,
                            [(p_keys_all[i], l_keys_all[i]) for i in picks],
                        ))
                        slips_this_tick += 1

            if slips_this_tick:
                funnel["ticks_with_slip"] += 1
            else:
                funnel["ticks_no_valid_slip"] += 1

        return sim_slips, funnel


    # ── Push-aware payout ───────────────────────────────────────────────

    @staticmethod
    def _payout_with_push(
        results: Iterable[str], slip_type: str,
    ) -> Tuple[float, int, int, int]:
        """Compute the slip's payout multiplier under PrizePicks push/dnp
        semantics. Mirrors the live API at web/app.py:2446-2458.

        Returns (payout_mult, n_eff, hits_eff, n_pushed).

        n_eff   = original n_legs - n_pushes - n_dnps (push and DNP both
                  refund the leg, reducing slip size).
        hits_eff = number of legs that actually hit (push/dnp don't count).
        payout = decimal multiplier on the original stake. payout=1.0
                 means stake refunded; payout > 1.0 is a win.
        """
        results = list(results)
        n_legs = len(results)
        effective = [r for r in results if r not in ("push", "dnp")]
        n_eff = len(effective)
        hits_eff = sum(1 for r in effective if r in ("hit", "won", "win"))
        n_pushed = n_legs - n_eff

        if n_eff == 0:
            # All legs pushed — full refund.
            return 1.0, 0, 0, n_pushed
        if n_eff == 1:
            return (1.0 if hits_eff == 1 else 0.0), n_eff, hits_eff, n_pushed
        if slip_type == "power":
            payout = POWER_PAYOUTS.get(n_eff, 0.0) if hits_eff == n_eff else 0.0
        else:  # flex
            if n_eff == 2:
                # PP treats 2-leg flex as Power 2.
                payout = POWER_PAYOUTS.get(2, 0.0) if hits_eff == 2 else 0.0
            else:
                payout = FLEX_PAYOUTS.get(n_eff, {}).get(hits_eff, 0.0)
        return float(payout), n_eff, hits_eff, n_pushed


    # ── Kelly (independent-bernoulli; same as v1) ───────────────────────

    def _calculate_kelly_fraction(self, probs: List[float], slip_size: int, slip_type: str) -> float:
        import itertools
        outcomes = list(itertools.product([0, 1], repeat=slip_size))
        ev = 0.0
        ev_sq = 0.0
        for outcome in outcomes:
            prob = 1.0
            for i in range(slip_size):
                prob *= probs[i] if outcome[i] == 1 else (1.0 - probs[i])
            hits = sum(outcome)
            mult = 0.0
            if slip_type == "power":
                if hits == slip_size:
                    mult = POWER_PAYOUTS.get(slip_size, 0.0)
            else:
                mult = FLEX_PAYOUTS.get(slip_size, {}).get(hits, 0.0)
            net_profit = mult - 1.0
            ev    += prob * net_profit
            ev_sq += prob * (net_profit ** 2)
        if ev <= 0:
            return 0.0
        variance = ev_sq - (ev ** 2)
        if variance <= 0:
            return 0.0
        return max(0.0, min((ev / variance) * 0.25, 1.0))   # quarter-Kelly


    # ── Bootstrap ───────────────────────────────────────────────────────

    @staticmethod
    def _bootstrap_metrics(
        slips: list[dict], n_resamples: int = BOOTSTRAP_RESAMPLES,
        bankroll: float = 100.0,
    ) -> dict:
        """Resample `slips` with replacement N times, computing total ROI,
        win rate, and max drawdown on each resample. Returns the 2.5 /
        97.5 percentiles for each metric so the UI can render
        `<point> [lo, hi]`.

        Drawdown is bankroll-based: peak starts at `bankroll`, drawdown
        is (peak - running_bank) / peak × 100. Same definition as the
        non-bootstrapped summary metric so the CI on max_drawdown_pct
        is in the same units the user sees on the card."""
        n = len(slips)
        # With <2 slips every bootstrap resample picks the same single
        # row, so the "CI" degenerates to [point, point] (e.g.
        # "+200%, +200%") which is mathematically vacuous and visually
        # misleading. Return empty so the UI renders "—" instead.
        if n < 2:
            return {}
        rng = random.Random(_BOOTSTRAP_SEED)

        rois:    list[float] = []
        wrs:     list[float] = []
        ddowns:  list[float] = []

        # Pre-extract for speed: bootstrap loops are hot.
        bet_arr    = [s["bet_size"] for s in slips]
        profit_arr = [s["profit"]   for s in slips]
        win_flag   = [1 if (s.get("payout", 0.0) > s.get("bet_size", 0.0)) else 0 for s in slips]

        for _ in range(n_resamples):
            idxs = [rng.randrange(n) for _ in range(n)]
            tot_bet = sum(bet_arr[i] for i in idxs)
            tot_prof = sum(profit_arr[i] for i in idxs)
            tot_wins = sum(win_flag[i] for i in idxs)
            roi = (tot_prof / tot_bet * 100.0) if tot_bet > 0 else 0.0
            wr  = (tot_wins / n * 100.0) if n > 0 else 0.0

            # Bankroll-based drawdown: same definition as the summary
            # metric. Resampled order approximates random ordering,
            # sufficient for tail-loss confidence.
            running = bankroll
            peak    = bankroll
            max_dd_pct = 0.0
            for i in idxs:
                running += profit_arr[i]
                if running > peak:
                    peak = running
                dd = ((peak - running) / peak * 100.0) if peak > 0 else 0.0
                if dd > max_dd_pct:
                    max_dd_pct = dd

            rois.append(roi)
            wrs.append(wr)
            ddowns.append(max_dd_pct)

        def _pct(arr, q):
            if not arr:
                return None
            arr_s = sorted(arr)
            k = int(round(q * (len(arr_s) - 1)))
            return arr_s[k]

        return {
            "roi_pct":          {"lo": round(_pct(rois, 0.025), 2),
                                 "hi": round(_pct(rois, 0.975), 2)},
            "win_rate_pct":     {"lo": round(_pct(wrs, 0.025), 2),
                                 "hi": round(_pct(wrs, 0.975), 2)},
            "max_drawdown_pct": {"lo": round(_pct(ddowns, 0.025), 2),
                                 "hi": round(_pct(ddowns, 0.975), 2)},
            "n_resamples":      n_resamples,
        }


    # ── Main entry ──────────────────────────────────────────────────────

    def run_simulation(self, config: StrategyConfig) -> Dict:
        if not self.db:
            return {"error": "Database not connected"}

        try:
            # Track the funnel — every filter step records its post-count
            # so the user can see exactly how many observations survived.
            funnel: dict[str, int] = {}

            # 1. Pull raw observatory rows.
            df = self._fetch_resolved_observatory(config.leagues)
            funnel["rows_pulled"] = len(df)
            if df.empty:
                return {"error": "No resolved data found matching league filter."}

            # 2. Date range.
            df["game_start_dt"] = pd.to_datetime(df["game_start"], errors="coerce", utc=True)
            df = df[df["game_start_dt"].notna()]
            funnel["after_game_start_parse"] = len(df)
            if config.start_date:
                start_dt = pd.to_datetime(config.start_date, utc=True)
                df = df[df["game_start_dt"] >= start_dt]
            if config.end_date:
                # End date is inclusive — bump to end-of-day.
                end_dt = pd.to_datetime(config.end_date, utc=True) + pd.Timedelta(days=1)
                df = df[df["game_start_dt"] < end_dt]
            funnel["after_date_filter"] = len(df)
            if df.empty:
                return {"error": "No rows in the selected date range.", "funnel": funnel}

            # 3. Prop includes/excludes.
            if config.excluded_props:
                df = df[~df["prop"].isin(config.excluded_props)]
            if config.included_props:
                df = df[df["prop"].isin(config.included_props)]
            funnel["after_prop_filter"] = len(df)
            if df.empty:
                return {"error": "Filters left no observations to evaluate.", "funnel": funnel}

            # 4. Apply current calibration to raw_true_prob.
            if config.use_calibration:
                df = self._apply_current_calibration(df)
            else:
                df = df.copy()
                df["calibrated_prob"] = pd.to_numeric(df["true_prob"], errors="coerce")
            df = df[df["calibrated_prob"].notna()]
            funnel["after_calibration"] = len(df)

            # 5. Min-prob filter (on calibrated).
            df = df[df["calibrated_prob"] >= config.min_prob]
            funnel["after_min_prob"] = len(df)
            if df.empty:
                return {
                    "error": (
                        f"No legs above Min True Prob {config.min_prob*100:.1f}%. "
                        f"Try lowering Min True Prob or picking more leagues."
                    ),
                    "funnel": funnel,
                }

            # 6. Build slips.
            #
            # live_replay → tick-based replay of BacktestLogger against
            # historical scrape moments (uses first_seen_at / last_seen_at
            # from migration_009). Produces the same slips the live
            # auto-builder would have logged on the same data, so users
            # can test settings (slip size, type, min_prob) without
            # spinning up a sidecar account.
            #
            # top_prob / top_ev → legacy slate-based (one slot per
            # calendar day). Preserved for backward compat; the v2
            # sandbox UI exposes them under "advanced".
            sim_slips: list[dict] = []
            cumulative_profit = 0.0
            total_bet = 0.0
            bankroll = config.bankroll
            push_legs_used = 0
            dnp_legs_used  = 0

            if config.slip_strategy == "live_replay":
                replay_slips, replay_funnel = self._replay_live_auto_builder(df, config)
                funnel.update(replay_funnel)
                sim_slips = replay_slips
                cumulative_profit = sum(s["profit"] for s in sim_slips)
                total_bet = sum(s["bet_size"] for s in sim_slips)
                push_legs_used = sum(s["n_pushed"] for s in sim_slips)
                dnp_legs_used = sum(
                    1 for s in sim_slips
                    for leg in s.get("legs", [])
                    if leg.get("result") == "dnp"
                )
                # Skip the slate loop and the post-build dedup pass:
                # _replay_live_auto_builder enforces dedup tick-by-tick
                # using the same canonical keys, so the invariant already
                # holds at emission time.
                slates_with_qualifying = 0
                slates_without_full_slip = 0
                n_dropped_post = 0
                run_used_pair = set()
                run_used_leg  = set()
                # Fall through to the equity / breakdown / response build.
                sorted_slate_ids = []
            else:
                df["slate_id"] = df["game_start_dt"].dt.date.astype(str)
                slates = df.groupby("slate_id")
                funnel["distinct_slates"] = int(df["slate_id"].nunique())
                sorted_slate_ids = df.sort_values(
                    "game_start_dt", kind="stable",
                )["slate_id"].unique()
                slates_with_qualifying = 0
                slates_without_full_slip = 0
                # Global dedup sets shared across every slate in this run.
                run_used_pair: set = set()
                run_used_leg:  set = set()

            for sid in sorted_slate_ids:
                slate_df = slates.get_group(sid)
                if len(slate_df) < config.slip_size:
                    slates_without_full_slip += 1
                    continue
                slates_with_qualifying += 1

                slips_for_slate = self._build_slips(
                    slate_df, config.slip_size, config.slip_strategy,
                    used_pair=run_used_pair, used_leg=run_used_leg,
                )
                if not slips_for_slate:
                    slates_without_full_slip += 1
                    continue

                # `selected_legs` is now a list of row-dicts (vectorized
                # away from per-row DataFrame.loc access — see
                # _select_ranked). Cross-slate dedup is enforced inside
                # _select_ranked via the shared `run_used_pair` /
                # `run_used_leg` sets — no additional emit-time check is
                # needed (and an additional check would over-fire,
                # because _select_ranked has already added these legs
                # to the shared sets by the time we see them here).
                for selected_legs in slips_for_slate:
                    if config.use_kelly:
                        probs = [r["calibrated_prob"] for r in selected_legs]
                        k_frac = self._calculate_kelly_fraction(
                            probs, config.slip_size, config.slip_type,
                        )
                        bet_size = bankroll * k_frac
                    else:
                        bet_size = config.bet_size

                    # Full leg record — line/side/game_start are required
                    # by the post-build dedup pass (engine.dedup) to
                    # reconstruct the canonical leg_key. Without them the
                    # invariant check at end-of-run would silently treat
                    # different lines on the same player+prop as
                    # duplicates (or worse, miss real duplicates because
                    # the keys collapse to `""` ).
                    leg_records = [
                        {
                            "player":     r.get("player", ""),
                            "prop":       r.get("prop", ""),
                            "line":       r.get("line", ""),
                            "side":       r.get("side", ""),
                            "game_start": r.get("game_start", ""),
                            "true_prob":  r.get("calibrated_prob"),
                            "result":     r.get("result", ""),
                        }
                        for r in selected_legs
                    ]
                    first = selected_legs[0]

                    if bet_size <= 0:
                        # Kelly sized this slip out — record it with bet=0 so
                        # the win-rate denominator (which excludes bet=0) sees it.
                        sim_slips.append({
                            "timestamp": first.get("game_start"),
                            "league":    first.get("league"),
                            "hits":      0,
                            "n_eff":     0,
                            "n_legs":    config.slip_size,
                            "n_pushed":  0,
                            "payout":    0.0,
                            "bet_size":  0.0,
                            "profit":    0.0,
                            "legs":      leg_records,
                        })
                        continue

                    results = [r.get("result", "") for r in selected_legs]
                    payout_mult, n_eff, hits_eff, n_pushed = self._payout_with_push(
                        results, config.slip_type,
                    )
                    n_dnp = sum(1 for r in results if r == "dnp")
                    push_legs_used += sum(1 for r in results if r == "push")
                    dnp_legs_used  += n_dnp

                    profit = (bet_size * payout_mult) - bet_size
                    cumulative_profit += profit
                    total_bet += bet_size
                    bankroll += profit

                    sim_slips.append({
                        "timestamp":  first.get("game_start"),
                        "league":     first.get("league"),
                        "hits":       hits_eff,
                        "n_eff":      n_eff,
                        "n_legs":     config.slip_size,
                        "n_pushed":   n_pushed,
                        "payout":     bet_size * payout_mult,
                        "bet_size":   bet_size,
                        "profit":     profit,
                        "legs":       leg_records,
                    })

            # ── Post-build dedup pass (belt-and-suspenders) ─────────
            # `_select_ranked` enforces dedup as it builds via the shared
            # `run_used_pair` / `run_used_leg` sets. This pass is the
            # invariant check on the final output: re-derive the keys
            # from the slip records and drop any slip that collides with
            # an earlier-kept one. Two reasons it's worth doing twice:
            #   1. Upstream selection has regressed silently three times
            #      (commits 6908c0a → dabc8de → 3e59a8b). A single check
            #      on emitted output cannot be bypassed by a future
            #      change to slip-construction logic.
            #   2. `_select_ranked` keys legs by their raw observatory
            #      strings. If a future refactor changes how rows are
            #      pre-processed (e.g. game_start normalization), the
            #      shared-set keys could drift from the emit-time keys.
            #      The post-build pass uses the canonical keys from
            #      engine.dedup, so the contract holds regardless.
            # `strict=True` enforces (player, sports_day) — the user
            # contract "no prop in two slips" reads at this granularity
            # since two bets on the same player in the same game are
            # near-perfectly correlated.
            sim_slips, n_dropped_post = drop_duplicate_slips(
                sim_slips, strict=True,
            )
            funnel["slates_with_qualifying"] = slates_with_qualifying
            funnel["slates_without_full_slip"] = slates_without_full_slip
            funnel["slips_dropped_post_dedup"] = n_dropped_post
            funnel["slips_formed"] = len(sim_slips)
            funnel["push_legs_in_slips"] = push_legs_used
            funnel["dnp_legs_in_slips"]  = dnp_legs_used

            if n_dropped_post > 0:
                logger.warning(
                    "Sandbox: post-build dedup dropped %d slip(s) — "
                    "_select_ranked emitted duplicates that the canonical "
                    "key check caught. Investigate the slip-builder path.",
                    n_dropped_post,
                )

            if not sim_slips:
                # Diagnose the *why* using the funnel so the user sees an
                # actionable error instead of a one-liner. The dominant
                # failure mode in production is the BREAK_EVEN gate: legs
                # pass the min_prob filter but every formed slip's
                # average prob lands below the slip type's break-even
                # (e.g. min_prob=54.1% but 2-Power BE=57.74%), so every
                # tick is silently rejected. Without this diagnosis the
                # user has no way to know to raise min_prob.
                be = BREAK_EVEN.get((str(config.slip_size), config.slip_type.lower()))
                be_pct = f"{be * 100:.2f}%" if be else "—"
                after_min = funnel.get("after_min_prob", 0)
                ticks_no_valid = funnel.get("ticks_no_valid_slip", 0)
                ticks_no_pool  = funnel.get("ticks_without_pool", 0)
                ticks_total    = funnel.get("distinct_scrape_ticks", 0)

                if after_min == 0:
                    msg = (
                        f"No legs above Min True Prob {config.min_prob*100:.1f}%. "
                        f"Try lowering Min True Prob."
                    )
                elif ticks_no_valid > 0 and ticks_total > 0 and ticks_no_valid == ticks_total:
                    msg = (
                        f"Built {ticks_total} candidate slip(s) but every one was "
                        f"rejected: average leg prob fell below the {config.slip_size}-leg "
                        f"{config.slip_type.title()} break-even ({be_pct}) or slip EV "
                        f"was <= 0. Raise Min True Prob to at least {be_pct}, or pick "
                        f"a slip size with a lower break-even (3-Power BE = 55.03%)."
                    )
                elif ticks_no_pool > 0 and ticks_total > 0 and ticks_no_pool == ticks_total:
                    msg = (
                        f"No tick had {config.slip_size} co-available legs. "
                        f"Pick more leagues or wait for additional scrape data."
                    )
                else:
                    msg = (
                        f"Could not form any {config.slip_size}-leg slips from history. "
                        f"Filters left {after_min} candidate legs across "
                        f"{ticks_total} ticks; {ticks_no_valid} were rejected by the "
                        f"break-even / EV gate ({be_pct})."
                    )
                return {"error": msg, "funnel": funnel}

            # Sort chronologically before any time-series rebuilding.
            def _ts(s):
                try:
                    return pd.to_datetime(s["timestamp"])
                except Exception:
                    return pd.Timestamp.max
            sim_slips.sort(key=_ts)

            # Equity / drawdown curves rebuilt in chronological order.
            equity_curve: list[dict] = []
            drawdown_curve: list[dict] = []
            running_cum = 0.0
            peak = config.bankroll
            running_bank = config.bankroll
            max_drawdown_pct = 0.0
            for s in sim_slips:
                running_cum += s["profit"]
                equity_curve.append({"x": s["timestamp"], "y": round(running_cum, 2)})
                running_bank += s["profit"]
                if running_bank > peak:
                    peak = running_bank
                dd_pct = ((peak - running_bank) / peak * 100.0) if peak > 0 else 0.0
                if dd_pct > max_drawdown_pct:
                    max_drawdown_pct = dd_pct
                drawdown_curve.append({"x": s["timestamp"], "y": round(dd_pct, 3)})

            # Aggregate metrics.
            roi = (cumulative_profit / total_bet) if total_bet > 0 else 0.0
            bet_slips = [s for s in sim_slips if s["bet_size"] > 0]
            wins_total = sum(1 for s in bet_slips if s["payout"] > s["bet_size"])
            win_rate = (wins_total / len(bet_slips)) if bet_slips else 0.0

            # Rolling ROI / win-rate (window sized to ~10% of slip count, clamped 20-100).
            window = max(20, min(100, len(sim_slips) // 10 or 20))
            rolling = []
            for i in range(len(sim_slips)):
                lo = max(0, i - window + 1)
                wnd = sim_slips[lo: i + 1]
                bet_sum    = sum(s["bet_size"] for s in wnd)
                profit_sum = sum(s["profit"] for s in wnd)
                wnd_bet    = [s for s in wnd if s["bet_size"] > 0]
                wins       = sum(1 for s in wnd_bet if s["payout"] > s["bet_size"])
                rolling.append({
                    "x":        wnd[-1]["timestamp"],
                    "roi":      round((profit_sum / bet_sum * 100.0) if bet_sum > 0 else 0.0, 2),
                    "win_rate": round((wins / len(wnd_bet) * 100.0) if wnd_bet else 0.0, 2),
                })

            # Monthly time-bucket: ROI per calendar month so users can see drift.
            monthly = self._monthly_buckets(sim_slips)

            # Bootstrap CIs on the aggregate metrics.
            ci = (
                self._bootstrap_metrics(bet_slips, bankroll=config.bankroll)
                if config.bootstrap and bet_slips else {}
            )

            # Breakdowns with bootstrap CI per row + thin-bucket flag.
            breakdowns = self._build_breakdowns(sim_slips, config.bootstrap)

            return {
                "summary": {
                    "total_slips":      len(sim_slips),
                    "bet_slips":        len(bet_slips),
                    "total_bet":        round(total_bet, 2),
                    "total_profit":     round(cumulative_profit, 2),
                    "roi_pct":          round(roi * 100, 2),
                    "win_rate_pct":     round(win_rate * 100, 2),
                    "max_drawdown_pct": round(max_drawdown_pct, 2),
                    "bankroll":         round(config.bankroll, 2),
                    "rolling_window":   window,
                    "ci":               ci,
                },
                "equity_curve":   equity_curve,
                "drawdown_curve": drawdown_curve,
                "rolling":        rolling,
                "monthly":        monthly,
                "breakdowns":     breakdowns,
                "funnel":         funnel,
                "slips":          sim_slips,
            }
        except Exception as e:
            logger.exception("Simulation failed")
            return {"error": str(e)}


    # ── Monthly bucketing & breakdowns ──────────────────────────────────

    @staticmethod
    def _monthly_buckets(slips: list[dict]) -> list[dict]:
        """ROI / win-rate per calendar month, oldest first. Useful for
        spotting performance decay over time."""
        if not slips:
            return []
        buckets: dict[str, dict] = {}
        for s in slips:
            try:
                dt = pd.to_datetime(s["timestamp"], utc=True)
            except Exception:
                continue
            key = dt.strftime("%Y-%m")
            b = buckets.setdefault(key, {
                "month": key, "slips": 0, "bet_slips": 0,
                "bet": 0.0, "profit": 0.0, "wins": 0,
            })
            b["slips"]  += 1
            b["bet"]    += s["bet_size"]
            b["profit"] += s["profit"]
            if s["bet_size"] > 0:
                b["bet_slips"] += 1
                if s["payout"] > s["bet_size"]:
                    b["wins"] += 1
        out = []
        for k in sorted(buckets):
            b = buckets[k]
            roi = (b["profit"] / b["bet"] * 100.0) if b["bet"] > 0 else 0.0
            wr  = (b["wins"] / b["bet_slips"] * 100.0) if b["bet_slips"] > 0 else 0.0
            out.append({
                "month":         k,
                "slips":         b["slips"],
                "bet":           round(b["bet"], 2),
                "profit":        round(b["profit"], 2),
                "roi_pct":       round(roi, 2),
                "win_rate_pct":  round(wr, 2),
            })
        return out

    @classmethod
    def _bucket_ci(cls, slips_subset: list[dict]) -> dict | None:
        """Bootstrap CI for one breakdown bucket. Returns None when the
        bucket is too thin to bootstrap meaningfully."""
        if len(slips_subset) < 2:
            return None
        return cls._bootstrap_metrics(slips_subset, n_resamples=BOOTSTRAP_RESAMPLES // 2)

    @classmethod
    def _build_breakdowns(cls, sim_slips: list[dict], with_ci: bool) -> dict:
        """Build by_league / by_hits / by_stat breakdowns with optional
        bootstrap CIs and an `is_thin` flag for low-n buckets."""
        def _empty():
            return {"slips": 0, "bet_slips": 0, "bet": 0.0, "profit": 0.0, "wins": 0,
                    "_slips": []}

        by_league: dict[str, dict] = {}
        by_hits:   dict[int, dict] = {}
        by_stat:   dict[str, dict] = {}

        for s in sim_slips:
            was_bet = s["bet_size"] > 0
            is_win  = was_bet and s["payout"] > s["bet_size"]

            lg = s["league"] or "Unknown"
            b = by_league.setdefault(lg, _empty())
            b["slips"] += 1; b["bet"] += s["bet_size"]; b["profit"] += s["profit"]
            if was_bet:
                b["bet_slips"] += 1; b["_slips"].append(s)
                if is_win: b["wins"] += 1

            h = int(s["hits"])
            b = by_hits.setdefault(h, _empty())
            b["slips"] += 1; b["bet"] += s["bet_size"]; b["profit"] += s["profit"]
            if was_bet:
                b["bet_slips"] += 1; b["_slips"].append(s)
                if is_win: b["wins"] += 1

            per_leg_bet = s["bet_size"] / s["n_legs"] if s["n_legs"] else 0.0
            per_leg_profit = s["profit"] / s["n_legs"] if s["n_legs"] else 0.0
            for leg in s.get("legs", []):
                prop = (leg.get("prop") or "Unknown").strip() or "Unknown"
                b = by_stat.setdefault(prop, {
                    "legs": 0, "bet": 0.0, "profit": 0.0, "hits": 0,
                })
                b["legs"]   += 1
                b["bet"]    += per_leg_bet
                b["profit"] += per_leg_profit
                if leg.get("result") == "hit":
                    b["hits"] += 1

        def _finalize_slip_group(d, key_label):
            out = []
            for k, v in d.items():
                bet = v.get("bet", 0.0)
                wr_num = v.get("wins", 0)
                wr_den = v.get("bet_slips", 0)
                slips_count = v.get("slips", 0)
                row = {
                    key_label: k,
                    "slips":   slips_count,
                    "bet":     round(bet, 2),
                    "profit":  round(v["profit"], 2),
                    "roi_pct": round((v["profit"] / bet * 100.0) if bet > 0 else 0.0, 2),
                    "win_rate_pct": round(
                        (wr_num / wr_den * 100.0) if wr_den else 0.0, 2,
                    ),
                    "is_thin": slips_count < MIN_BUCKET_SLIPS,
                }
                if with_ci and slips_count >= 2:
                    ci = cls._bucket_ci(v["_slips"])
                    if ci:
                        row["ci"] = ci
                out.append(row)
            return sorted(out, key=lambda r: r["profit"], reverse=True)

        def _finalize_stat_group(d):
            out = []
            for prop, v in d.items():
                bet  = v.get("bet", 0.0)
                legs = v.get("legs", 0)
                hits = v.get("hits", 0)
                out.append({
                    "stat_type": prop,
                    "slips":     legs,                # per-leg count, repurpose key
                    "bet":       round(bet, 2),
                    "profit":    round(v["profit"], 2),
                    "roi_pct":   round((v["profit"] / bet * 100.0) if bet > 0 else 0.0, 2),
                    "win_rate_pct": round(
                        (hits / legs * 100.0) if legs else 0.0, 2,
                    ),
                    "is_thin": legs < MIN_BUCKET_SLIPS,
                })
            return sorted(out, key=lambda r: r["profit"], reverse=True)

        return {
            "by_league": _finalize_slip_group(by_league, "league"),
            "by_hits":   _finalize_slip_group(by_hits,   "hits"),
            "by_stat":   _finalize_stat_group(by_stat),
        }


    # ── Optimizer ───────────────────────────────────────────────────────
    # Preserved from v1 but rewired to use the calibration-on-the-fly
    # data path so its output is consistent with run_simulation. Note:
    # this still has in-sample bias; the README and UI label it
    # accordingly. Use the held-out validation flow (future) for
    # generalizable threshold selection.

    # 0.005 step → 11 thresholds across [0.53, 0.58]. The previous 0.001
    # step (51 thresholds) was 5x slower for output that's strictly
    # noisier — bootstrap CI on each threshold's ROI dwarfs the
    # 0.001-vs-0.005 resolution gap, and the in-sample max-of-N bias
    # grows with N anyway, so a coarser sweep is also a more honest
    # one.
    _OPT_THRESHOLD_LO   = 0.53
    _OPT_THRESHOLD_HI   = 0.58
    _OPT_THRESHOLD_STEP = 0.005

    def _simulate_at_threshold(
        self, base_df: pd.DataFrame, threshold: float,
        slip_size: int, slip_type: str, bankroll: float, bet_size: float,
        use_kelly: bool, strategy: str,
    ) -> Optional[Dict]:
        df = base_df[base_df["calibrated_prob"] >= threshold]
        if df.empty or len(df) < slip_size:
            return None
        # `slate_id` is precomputed on `base_df` by `optimize_threshold`,
        # so the only per-threshold cost here is the boolean filter +
        # groupby — no per-row strftime.
        if "slate_id" not in df.columns:
            df = df.copy()
            df["slate_id"] = df["game_start_dt"].dt.date.astype(str)
        slates = df.groupby("slate_id")

        total_bet = 0.0
        total_profit = 0.0
        n_slips = 0
        n_zero_bets = 0
        running_bankroll = bankroll
        # Cross-slate dedup: same rationale as run_simulation — prevents
        # one leg from showing up in two slips across a single threshold
        # evaluation, even when slate-boundary edge cases would otherwise
        # leak it.
        opt_used_pair: set = set()
        opt_used_leg:  set = set()
        # Materialize every slip's leg-list so we can run the
        # post-build dedup pass before aggregating P&L. Aggregating
        # inline would prevent us from dropping a colliding slip
        # without un-doing its profit/bet contributions.
        emitted_slip_legs: list[list[dict]] = []
        for sid in df.sort_values("game_start_dt", kind="stable")["slate_id"].unique():
            slate_df = slates.get_group(sid)
            if len(slate_df) < slip_size:
                continue
            for selected_legs in self._build_slips(
                slate_df, slip_size, strategy,
                used_pair=opt_used_pair, used_leg=opt_used_leg,
            ):
                emitted_slip_legs.append(selected_legs)

        # Belt-and-suspenders: ensure no leg appears in two emitted slips
        # before they are scored. Same rationale as run_simulation's
        # post-build pass — see comment there.
        emitted_slip_legs, n_dropped_post = drop_duplicate_slips(
            emitted_slip_legs, strict=True,
        )
        if n_dropped_post > 0:
            logger.warning(
                "Sandbox optimizer: post-build dedup dropped %d slip(s) "
                "at threshold scan", n_dropped_post,
            )

        for selected_legs in emitted_slip_legs:
            # selected_legs is a list-of-dicts after the
            # vectorization rewrite of _select_ranked.
            if use_kelly:
                probs = [r["calibrated_prob"] for r in selected_legs]
                k_frac = self._calculate_kelly_fraction(probs, slip_size, slip_type)
                leg_bet_size = running_bankroll * k_frac
            else:
                leg_bet_size = bet_size
            if leg_bet_size <= 0:
                n_zero_bets += 1
                continue
            results = [r.get("result", "") for r in selected_legs]
            payout_mult, _n_eff, _hits_eff, _n_pushed = self._payout_with_push(results, slip_type)
            profit = (leg_bet_size * payout_mult) - leg_bet_size
            total_profit += profit
            total_bet    += leg_bet_size
            running_bankroll += profit
            n_slips += 1
        if total_bet <= 0:
            return None
        roi = (total_profit / total_bet) * 100.0
        return {
            "threshold":    round(float(threshold), 4),
            "roi":          roi,
            "slips":        n_slips,
            "zero_bets":    n_zero_bets,
            "total_bet":    round(total_bet, 2),
            "total_profit": round(total_profit, 2),
        }

    def optimize_threshold(self, config: StrategyConfig) -> Dict:
        if not self.db:
            return {"error": "Database not connected"}
        if config.slip_size not in (2, 3, 4, 5, 6):
            return {"error": f"slip_size must be one of 2..6 (got {config.slip_size})."}
        if config.slip_type not in ("power", "flex"):
            return {"error": f"slip_type must be 'power' or 'flex' (got {config.slip_type!r})."}
        if config.bankroll is None or config.bankroll <= 0:
            return {"error": "bankroll must be positive."}
        if not config.use_kelly and (config.bet_size is None or config.bet_size <= 0):
            return {"error": "bet_size must be positive when Kelly is disabled."}
        if config.slip_type == "flex" and config.slip_size < 3:
            return {"error": "Flex slips require at least 3 legs."}

        try:
            base_df = self._fetch_resolved_observatory(config.leagues)
            if base_df.empty:
                return {"error": "No resolved data found matching filters."}
            base_df["game_start_dt"] = pd.to_datetime(base_df["game_start"], errors="coerce", utc=True)
            base_df = base_df[base_df["game_start_dt"].notna()]
            if config.start_date:
                start_dt = pd.to_datetime(config.start_date, utc=True)
                base_df = base_df[base_df["game_start_dt"] >= start_dt]
            if config.end_date:
                end_dt = pd.to_datetime(config.end_date, utc=True) + pd.Timedelta(days=1)
                base_df = base_df[base_df["game_start_dt"] < end_dt]
            if config.excluded_props:
                base_df = base_df[~base_df["prop"].isin(config.excluded_props)]
            if config.included_props:
                base_df = base_df[base_df["prop"].isin(config.included_props)]
            if base_df.empty:
                return {"error": "Filters left no observations to evaluate."}

            base_df = self._apply_current_calibration(base_df) if config.use_calibration else base_df.assign(
                calibrated_prob=pd.to_numeric(base_df["true_prob"], errors="coerce")
            )
            base_df = base_df[base_df["calibrated_prob"].notna()]
            if base_df.empty:
                return {"error": "No usable rows after calibration step."}

            # Precompute slate_id once. The threshold sweep filters rows
            # but doesn't move them between slates, so doing strftime per
            # iteration was wasted work (was the dominant cost on the
            # threshold loop after the slip-builder vectorization).
            base_df = base_df.copy()
            base_df["slate_id"] = base_df["game_start_dt"].dt.date.astype(str)

            best_roi = -float("inf")
            best_threshold: Optional[float] = None
            results: list[Dict] = []
            n_skipped_volume = 0
            n_skipped_zero_kelly = 0

            n_steps = int(round(
                (self._OPT_THRESHOLD_HI - self._OPT_THRESHOLD_LO) / self._OPT_THRESHOLD_STEP
            )) + 1
            for t in np.linspace(self._OPT_THRESHOLD_LO, self._OPT_THRESHOLD_HI, n_steps):
                t_val = float(t)
                summary = self._simulate_at_threshold(
                    base_df=base_df, threshold=t_val,
                    slip_size=config.slip_size, slip_type=config.slip_type,
                    bankroll=config.bankroll, bet_size=config.bet_size,
                    use_kelly=config.use_kelly,
                    strategy=config.slip_strategy,
                )
                if summary is None:
                    df_at_t = base_df[base_df["calibrated_prob"] >= t_val]
                    if df_at_t.empty or len(df_at_t) < config.slip_size:
                        n_skipped_volume += 1
                    else:
                        n_skipped_zero_kelly += 1
                    continue
                results.append({"threshold": summary["threshold"], "roi": summary["roi"], "slips": summary["slips"]})
                if summary["roi"] > best_roi:
                    best_roi = summary["roi"]
                    best_threshold = summary["threshold"]

            if not results:
                parts: list[str] = []
                if n_skipped_zero_kelly > 0:
                    parts.append(f"Kelly sized $0 for {n_skipped_zero_kelly} threshold(s)")
                if n_skipped_volume > 0:
                    parts.append(f"insufficient legs to form a {config.slip_size}-leg slip "
                                 f"at {n_skipped_volume} threshold(s)")
                fix = (
                    "Try disabling Kelly or widening filters."
                    if config.use_kelly and n_skipped_zero_kelly > 0
                    else "Try lowering slip size or widening filters."
                )
                msg = ("; ".join(parts) + ". " + fix) if parts else (
                    f"Not enough resolved legs above any tested threshold. {fix}"
                )
                return {"error": msg}

            return {
                "best_threshold": round(float(best_threshold), 4),
                "best_roi":       round(best_roi, 2),
                "all_results":    results,
                "warning":        ("In-sample optimization — the reported best ROI is biased "
                                   "upward by multiple-comparisons. Use the held-out "
                                   "validation flow before acting on this number."),
            }
        except Exception as e:
            logger.exception("Optimization failed")
            return {"error": str(e)}
