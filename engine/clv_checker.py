"""
CLV (Closing Line Value) Tracker.

Periodically called to update the `closing_prob` in Supabase for pending bets.
As lines move, this records the last seen VWAP consensus probability before the
game starts and the line is pulled from the board.

Strategy:
  - ALWAYS update closing_prob for any pending bet where current odds are available,
    regardless of how far away the game is. This ensures that every pipeline run
    captures the latest market consensus. The last written value before lines
    disappear (at game start) becomes the de facto closing line.
  - On startup or when the app was down during a game start, a recovery pass will
    mark truly missed games (already finished, no odds available) so they don't
    remain stuck as empty forever.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

from engine.database import get_db
from engine.consensus import books_from_match_for_side, compute_true_probability

logger = logging.getLogger(__name__)

# Once the game starts, lines are no longer "closing" — any match we'd find
# after tip-off is live in-play pricing, not the closing line we want to
# compare against. Closing_prob is therefore frozen at the last value
# captured before start.
POST_START_GRACE_MINUTES = 0

# How long after game start (in hours) before we consider CLV as "missed" and
# finalize with a fallback. This prevents rows from being stuck empty forever
# when the app wasn't running during the tracking window.
MISSED_CUTOFF_HOURS = 1


class CLVTracker:
    def __init__(self):
        # simplify-v1: no calibration. The stored true_prob and the closing
        # prob are both the raw worst_case devig, so CLV compares like with
        # like — nothing to reload.
        pass

    def update_closing_lines(self, matches: list[Any]) -> int:
        """
        Updates pending backtest legs in Supabase with the latest true probability.

        `matches` is the list of MatchResult objects from app.py. Most callers
        should prefer `update_closing_lines_from_probs` with a precomputed dict
        so that the heavy MatchedProp list can be freed earlier.
        """
        current_probs = self._build_current_probs(matches)
        return self.update_closing_lines_from_probs(current_probs)

    def update_closing_lines_from_probs(
        self,
        current_probs: dict[tuple[str, str, str, float], float],
        current_book_probs: dict[tuple[str, str, str, float], dict[str, float]] | None = None,
    ) -> int:
        """
        Same as `update_closing_lines` but takes the precomputed
        (player, prop, side, line) -> worst_case_prob dict directly.
        Used by the main pipeline to avoid retaining `matches` until the
        background thread finishes.
        """
        db = get_db()
        if not db:
            return 0

        # Fetch pending legs from Supabase
        try:
            res = db.table("legs").select("*").eq("result", "pending").execute()
            rows = res.data or []
        except Exception as exc:
            logger.error("CLVTracker: cannot read pending legs from Supabase: %s", exc)
            return 0

        if not rows:
            return 0

        now_utc = datetime.now(timezone.utc)
        updated_count = 0

        # Pending: (player, league, prop, line, side, game_start) → raw closing
        # prob. Flushed at the end of the loop to market_observatory so the
        # calibration refit has CLV signal data to work with. We write the
        # *raw* value (pre-isotonic) here to avoid the circular dependency
        # where calibrated closing_prob would be used to refit calibration.
        observatory_writes: dict[tuple, float] = {}

        for row in rows:
            game_start_str = row.get("game_start", "")
            if not game_start_str:
                continue

            try:
                gs = datetime.fromisoformat(game_start_str.replace("Z", "+00:00"))
                if gs.tzinfo is None:
                    gs = gs.replace(tzinfo=timezone.utc)
            except Exception:
                continue

            mins_to_start = (gs - now_utc).total_seconds() / 60.0

            # Hard cutoff at game start: any price we'd see now is live
            # in-play, not closing. Freeze closing_prob at whatever was
            # last captured before tip-off.
            if mins_to_start <= 0:
                continue

            player = (row.get("player") or "").lower().strip()
            prop = (row.get("prop") or "").lower().strip()
            side = (row.get("side") or "").lower().strip()
            try:
                line = float(row.get("line", 0))
            except ValueError:
                line = 0.0

            key = (player, prop, side, line)
            if key in current_probs:
                new_cp_val = current_probs[key]
                old_cp_val = row.get("closing_prob")

                # Convert old value for comparison
                if old_cp_val is not None:
                    try:
                        old_cp_val = float(old_cp_val)
                    except (ValueError, TypeError):
                        old_cp_val = None

                # simplify-v1: closing prob is the raw worst_case devig — the
                # same space as the stored true_prob — so CLV reflects only
                # line movement. Same [0.001, 0.999] guard the live path uses.
                calibrated_cp = max(0.001, min(0.999, new_cp_val))

                # Update only when the calibrated value has moved materially
                # from whatever was last written.
                if old_cp_val is None or abs(calibrated_cp - old_cp_val) > 1e-4:
                    # CLV is measured raw-vs-raw: the closing prob is the raw
                    # consensus, so the entry must be raw_true_prob (pre-
                    # side-bias). Falling back to true_prob keeps legacy rows
                    # (which stored the same number in both) working.
                    try:
                        orig_true_prob = float(row.get("raw_true_prob") or row.get("true_prob", 0))
                    except (TypeError, ValueError):
                        orig_true_prob = float(row.get("true_prob", 0))
                    closing_prob = calibrated_cp
                    clv_pct = closing_prob - orig_true_prob

                    try:
                        sid = row.get("slip_id")
                        l_num = int(row.get("leg_num", 0))
                        # Capture-time quality (migration_012): record WHEN
                        # this close was written and how many minutes before
                        # tip-off. Lets the analytics tab distinguish a real
                        # near-tip close from a stale early one, and prefer
                        # the latest capture as the true closing line.
                        update_payload = {
                            "closing_prob":        round(closing_prob, 4),
                            "clv_pct":             round(clv_pct, 4),
                            "closing_captured_at": now_utc.isoformat(),
                            "closing_lead_min":    round(mins_to_start, 2),
                        }
                        # Per-book close for placed bets is captured on the
                        # matching market_observatory row (same market_key) —
                        # the legs table has no jsonb column to hold it and we
                        # cannot add one (no DDL access). Join legs→observatory
                        # on market_key to recover per-book entry/close.
                        try:
                            db.table("legs").update(update_payload).eq(
                                "slip_id", sid).eq("leg_num", l_num).execute()
                        except Exception:
                            # Pre-migration_012 schema — retry without the
                            # new quality columns so closing capture still
                            # works on un-migrated deploys.
                            db.table("legs").update({
                                "closing_prob": round(closing_prob, 4),
                                "clv_pct":      round(clv_pct, 4),
                            }).eq("slip_id", sid).eq("leg_num", l_num).execute()
                        updated_count += 1
                    except Exception as db_exc:
                        logger.error("CLVTracker DB update failed: %s", db_exc)

                    # Stage the *raw* closing prob for market_observatory so
                    # the calibration refit can use it as a CLV signal without
                    # the bias of double-calibration.
                    leg_player = row.get("player") or ""
                    leg_league = row.get("league") or ""
                    leg_prop = row.get("prop") or ""
                    leg_side = row.get("side") or ""
                    leg_game_start = row.get("game_start") or ""
                    if leg_player and leg_league and leg_game_start:
                        obs_key = (leg_player, leg_league, leg_prop, line, leg_side, leg_game_start)
                        observatory_writes[obs_key] = float(new_cp_val)

                    logger.debug(
                        "CLVTracker: Update %s %s %s @%s -> %.4f",
                        player, prop, side, line, new_cp_val
                    )

        if updated_count:
            logger.info("CLVTracker: updated %d pending bets", updated_count)

        # Flush observatory writes. If migration_003 hasn't been applied the
        # column won't exist; we swallow the error after the first failure
        # since it'll just keep failing for the rest of this batch.
        if observatory_writes:
            obs_failed = False
            for (p_, lg_, pr_, ln_, sd_, gs_), raw_cp in observatory_writes.items():
                if obs_failed:
                    break
                try:
                    db.table("market_observatory").update({
                        "closing_prob": round(raw_cp, 4)
                    }) \
                        .eq("player", p_) \
                        .eq("league", lg_) \
                        .eq("prop", pr_) \
                        .eq("line", ln_) \
                        .eq("side", sd_) \
                        .eq("game_start", gs_) \
                        .execute()
                except Exception as exc:
                    logger.debug(
                        "CLVTracker: market_observatory closing_prob write failed "
                        "(migration_003 not applied?): %s", exc,
                    )
                    obs_failed = True

        return updated_count

    def update_observatory_closing_lines(
        self,
        current_probs: dict[tuple[str, str, str, float], float],
        current_book_probs: dict[tuple[str, str, str, float], dict[str, float]] | None = None,
        *,
        capture_window_minutes: int = 240,
    ) -> int:
        """Standalone observatory closing-line capture (Phase 1A audit PR-3a).

        The legacy `update_closing_lines_from_probs` path only iterates the
        `legs` table, so an observatory row only ever receives a
        `closing_prob` if a logged bet exists for the same 6-tuple. With
        6,572 legs vs 47,653 observatory rows, that path leaves 95% of the
        training corpus without CLV signal. Result (measured): the dynamic
        CLV-weight estimator in isotonic_calibration._compute_clv_weight is
        operating on <2,000 rows even though 38,788 are settled.

        This method writes closing_prob to ALL pending observatory rows
        whose game_start is within `capture_window_minutes` of now (default
        4 hours pre-game). The window matters because:

          (a) Lines move materially in the last 1-2 hours pre-game. Writing
              closing_prob 6 hours out means the "closing" we measure is
              not the closing — it's the early-line consensus.
          (b) Writing on every scrape cycle (15 min) means a 4h-window row
              gets ~16 writes. Each write overwrites the previous, so the
              final value IS the closing line, and intermediate writes are
              just bookkeeping.

        Returns the number of observatory rows updated.

        Schema notes: writes the RAW pre-calibration consensus (the dict
        values from _build_current_probs are worst_case_prob already, but
        they're pre-isotonic). The calibration refit at
        isotonic_calibration._ingest_resolved_row reads this raw value and
        avoids the feedback loop documented at isotonic_calibration.py:746.
        """
        db = get_db()
        if not db:
            return 0
        if not current_probs:
            return 0

        now_utc = datetime.now(timezone.utc)
        window_end_iso = (
            now_utc + timedelta(minutes=capture_window_minutes)
        ).isoformat()
        now_iso = now_utc.isoformat()

        updated = 0
        page_size = 1000
        offset = 0
        # Paged scan over pending obs rows within the capture window.
        while True:
            try:
                res = (
                    db.table("market_observatory")
                      .select("id, player, prop, side, line, game_start, closing_prob, books")
                      .eq("result", "pending")
                      .gte("game_start", now_iso)
                      .lte("game_start", window_end_iso)
                      .range(offset, offset + page_size - 1)
                      .execute()
                )
            except Exception as exc:
                logger.warning(
                    "CLVTracker.observatory: page %d query failed: %s",
                    offset, exc,
                )
                return updated
            rows = res.data or []
            if not rows:
                break

            for row in rows:
                key = (
                    (row.get("player") or "").lower().strip(),
                    (row.get("prop") or "").lower().strip(),
                    (row.get("side") or "").lower().strip(),
                    float(row.get("line") or 0),
                )
                new_raw = current_probs.get(key)
                if new_raw is None:
                    continue
                try:
                    new_val = float(new_raw)
                except (TypeError, ValueError):
                    continue
                if not (0.0 < new_val < 1.0):
                    continue

                old_cp = row.get("closing_prob")
                if old_cp is not None:
                    try:
                        if abs(float(old_cp) - new_val) < 1e-4:
                            continue
                    except (TypeError, ValueError):
                        pass

                payload = {"closing_prob": round(new_val, 4)}

                # Per-book close + capture lead are stored INSIDE the existing
                # `books` jsonb (no DDL access to add dedicated columns). The
                # entry per-book devigs are the plain book keys (frozen at the
                # first scrape via ignore_duplicates); the close lives under
                # reserved "_close*" keys so an export can split them cleanly:
                #   {"fanduel":0.61,"pinnacle":0.62,        # entry
                #    "_close":{"fanduel":0.59,"pinnacle":0.60},
                #    "_close_lead_min":25.0,"_close_at":"2026-..."}
                entry_books = row.get("books")
                if not isinstance(entry_books, dict):
                    entry_books = {}
                merged = {
                    k: v for k, v in entry_books.items()
                    if not (isinstance(k, str) and k.startswith("_close"))
                }
                cb = current_book_probs.get(key) if current_book_probs else None
                if cb:
                    merged["_close"] = cb
                gs_str = row.get("game_start") or ""
                try:
                    gs = datetime.fromisoformat(gs_str.replace("Z", "+00:00"))
                    if gs.tzinfo is None:
                        gs = gs.replace(tzinfo=timezone.utc)
                    merged["_close_lead_min"] = round((gs - now_utc).total_seconds() / 60.0, 2)
                except Exception:
                    pass
                merged["_close_at"] = now_iso
                payload["books"] = merged

                try:
                    (db.table("market_observatory")
                       .update(payload)
                       .eq("id", row["id"])
                       .execute())
                    updated += 1
                except Exception:
                    # Fallback: at least persist the scalar consensus close.
                    try:
                        (db.table("market_observatory")
                           .update({"closing_prob": round(new_val, 4)})
                           .eq("id", row["id"])
                           .execute())
                        updated += 1
                    except Exception as exc2:
                        logger.warning(
                            "CLVTracker.observatory: write failed at obs_id=%s: %s",
                            row.get("id"), exc2,
                        )
                        return updated

            if len(rows) < page_size:
                break
            offset += page_size

        if updated:
            logger.info(
                "CLVTracker.observatory: updated closing_prob on %d pending "
                "observatory rows (window=%dmin)",
                updated, capture_window_minutes,
            )
        return updated

    def finalize_missed(self) -> int:
        """
        Recovery pass: for legs where a `closing_prob` was captured but the
        derived `clv_pct` never got written (partial-write state), fill in the
        diff against the recorded `true_prob`.

        We intentionally do NOT write a placeholder when closing_prob itself is
        missing — there's no way to recover the market's closing line after the
        fact, and writing `closing_prob = true_prob, clv_pct = 0` injects fake
        zeros that bias the CLV+ rate metric downward. Rows whose closing was
        never captured stay null, which the analytics loader correctly excludes.

        Returns the number of rows finalized.
        """
        db = get_db()
        if not db:
            return 0

        try:
            # Only the partial-write recovery path uses these fields — full
            # leg rows were 3x bigger and the rest is unused.
            cols = "slip_id, leg_num, closing_prob, clv_pct, true_prob, raw_true_prob, game_start"
            res = db.table("legs").select(cols).execute()
            rows = res.data or []
        except Exception as exc:
            logger.error("CLVTracker: cannot read legs from Supabase: %s", exc)
            return 0

        if not rows:
            return 0

        now_utc = datetime.now(timezone.utc)
        cutoff = timedelta(hours=MISSED_CUTOFF_HOURS)
        finalized = 0

        for row in rows:
            # Only target partial-write rows: closing_prob present but clv_pct missing.
            if row.get("closing_prob") is None or row.get("clv_pct") is not None:
                continue

            game_start_str = row.get("game_start", "")
            if not game_start_str:
                continue

            try:
                gs = datetime.fromisoformat(game_start_str.replace("Z", "+00:00"))
                if gs.tzinfo is None:
                    gs = gs.replace(tzinfo=timezone.utc)
            except Exception:
                continue

            if (now_utc - gs) <= cutoff:
                continue

            try:
                cp = float(row["closing_prob"])
                # Raw-vs-raw, same as the live path above.
                orig_true_prob = float(row.get("raw_true_prob") or row.get("true_prob", 0))
                clv_pct = round(cp - orig_true_prob, 4)
            except (ValueError, TypeError):
                continue

            try:
                sid = row.get("slip_id")
                l_num = int(row.get("leg_num", 0))
                db.table("legs").update({"clv_pct": clv_pct}) \
                    .eq("slip_id", sid).eq("leg_num", l_num).execute()
                finalized += 1
            except Exception as db_exc:
                logger.error("CLVTracker DB finalization failed: %s", db_exc)

        if finalized:
            logger.info("CLVTracker: finalized clv_pct on %d partial-write rows", finalized)

        return finalized

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _build_current_probs(self, matches: list[Any]) -> dict[tuple[str, str, str, float], float]:
        """
        Build a lookup: (player_lower, prop_lower, side, line) -> worst_case_prob.

        Mirrors the bet-placement pipeline in `_run_pipeline_body` exactly,
        including the per-side equivalent-line selection: for whole-number
        PP lines we accept book lines at PP±0.5 because they're
        mathematically equivalent under push-on-tie semantics. Placement
        and CLV must apply the same selection or the very first
        post-placement closing-line write produces a phantom delta.

        Subsequent calibration is applied identically on both sides
        (BetResult.__init__ at placement, update_closing_lines_from_probs
        on the closing side), so when nothing has moved between placement
        and the first CLV pass, clv_pct ≈ 0.
        """
        current_probs: dict[tuple[str, str, str, float], float] = {}
        for m in matches:
            if not getattr(m, "pp", None):
                continue

            player = m.pp.player_name.lower().strip()
            prop = m.pp.stat_type.lower().strip()
            line = float(m.pp.line_score)
            sides = ["over", "under"] if getattr(m.pp, "side", "both") == "both" else [m.pp.side]

            for side in sides:
                books = books_from_match_for_side(m, side)
                if not books:
                    continue
                consensus, _worst_case, _meta = compute_true_probability(
                    books, side,
                    league=getattr(m.pp, "league", None),
                    prop=getattr(m.pp, "stat_type", None),
                )
                # Use the same number the leg was logged with (consensus), so
                # CLV reflects line movement, not a devig-method gap.
                if consensus is not None:
                    current_probs[(player, prop, side, line)] = consensus

        return current_probs

    def _build_current_book_probs(
        self, matches: list[Any]
    ) -> dict[tuple[str, str, str, float], dict[str, float]]:
        """Like `_build_current_probs`, but PER BOOK:
        (player, prop, side, line) -> {book_name: devigged_prob}.

        Mirrors `_per_book_probs` in the +EV pipeline (web/app.py): two-sided
        exact lines use Power devig, single sides use the scaled single-sided
        devig. Captured at the closing pass so each book's close can be
        compared to its OWN entry (same instrument, same devig) — the only way
        the resulting CLV reflects genuine line movement rather than a
        book-composition or devig-method artifact.
        """
        from engine.devig import devig_power, devig_single_sided_scaled

        out: dict[tuple[str, str, str, float], dict[str, float]] = {}
        for m in matches:
            if not getattr(m, "pp", None):
                continue
            player = m.pp.player_name.lower().strip()
            prop = m.pp.stat_type.lower().strip()
            line = float(m.pp.line_score)
            sides = ["over", "under"] if getattr(m.pp, "side", "both") == "both" else [m.pp.side]
            for side in sides:
                books = books_from_match_for_side(m, side)
                if not books:
                    continue
                bp: dict[str, float] = {}
                for bk in books:
                    prob = None
                    if bk.both_sided and bk.over_odds is not None and bk.under_odds is not None:
                        t_o, t_u = devig_power(bk.over_odds, bk.under_odds)
                        prob = t_o if side == "over" else t_u
                    elif side == "over" and bk.over_odds is not None:
                        prob = devig_single_sided_scaled(bk.over_odds)
                    elif side == "under" and bk.under_odds is not None:
                        prob = devig_single_sided_scaled(bk.under_odds)
                    if prob is not None and 0.0 < prob < 1.0:
                        bp[bk.book_name] = round(float(prob), 4)
                if bp:
                    out[(player, prop, side, line)] = bp
        return out
