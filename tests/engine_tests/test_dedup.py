"""
Rigorous coverage of the slip-leg deduplication invariant.

The invariant the test suite enforces, end-to-end:

    No (player, game_start[:sports_day]) pair appears in two different
    slips emitted by a single run of the sandbox simulator (or the
    threshold optimizer). The same applies to live-pipeline auto-slip
    builds when the dedup window is observed.

Past dedup fixes (commits 6908c0a, dabc8de, 4591ee2, 3e59a8b) addressed
specific bug-classes (UTC date drift, line column omission, two-team
swap re-introducing collisions, single-slate scope) but never landed a
test. This file is the test-suite half of the contract:

  1. Key-construction unit tests — every drift mode that broke a prior
     fix (TZ format, line type, player spelling, missing columns).
  2. `_select_ranked` slip-builder unit tests — single slate, multi-
     slate, two-team swap.
  3. End-to-end `run_simulation` invariant test — drives the sandbox
     against a synthetic observatory DataFrame and asserts the post-
     build dedup invariant.
  4. `optimize_threshold` invariant test — same for the optimizer path.
  5. The `engine.dedup` helper modules — `drop_duplicate_slips`,
     `check_duplicate_pairs`, `assert_no_duplicate_legs`.

Run:  pytest tests/engine_tests/test_dedup.py -v
"""
from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from engine.backtest import (
    make_bet_key,
    make_leg_key,
)
from engine.dedup import (
    assert_no_duplicate_legs,
    assert_no_duplicate_pairs,
    check_duplicate_legs,
    check_duplicate_pairs,
    drop_duplicate_slips,
    leg_exact_key,
    leg_pair_key,
)


# ───────────────────────────────────────────────────────────────────────
# 1. Key-construction unit tests
# ───────────────────────────────────────────────────────────────────────

class MakeBetKeyTZDriftTests(unittest.TestCase):
    """Bug class fixed in commit 3e59a8b. The 12 h sports-day shift means
    a single late-ET game can't straddle UTC midnight — same game
    stored in any reasonable timestamp format collapses to one key."""

    PLAYER = "Breanna Stewart"

    def _key(self, ts: str) -> tuple:
        return make_bet_key(self.PLAYER, ts)

    def test_iso_with_offset_and_naive_utc_match(self):
        # 7 pm ET = 23 UTC, naive scrape stored same instant as UTC.
        with_offset = self._key("2025-05-13T19:00:00-04:00")
        naive_utc   = self._key("2025-05-13T23:00:00")
        self.assertEqual(with_offset, naive_utc)

    def test_late_et_tipoff_spilling_past_midnight_utc_one_key(self):
        # 10 pm ET → 02:00 UTC next day. Different formats for the SAME
        # game must produce the SAME sports_day. Without the 12 h shift
        # this was the original duplicate-leg root cause.
        offset = self._key("2025-05-13T22:00:00-04:00")
        naive  = self._key("2025-05-13 22:00:00")   # naive ET form
        utc_z  = self._key("2025-05-14T02:00:00Z")  # same instant, UTC w/ Z
        self.assertEqual(offset, utc_z)
        # The naive form is interpreted as UTC by make_bet_key (it has
        # no offset), but the 12 h shift still buckets it to the same
        # sports day as the offset-aware variant.
        self.assertEqual(offset, naive)

    def test_pacific_evening_collapses_with_eastern_evening_same_night(self):
        # 8 pm PT (=23 ET = 03:00 UTC next day) vs. 7 pm ET (=23 UTC):
        # for a same-night slate, both should bucket to the SAME
        # sports day. The 12 h shift puts midnight at 12:00 UTC ~07:00
        # ET — before any game starts.
        pt_evening = self._key("2025-05-13T20:00:00-07:00")
        et_evening = self._key("2025-05-13T19:00:00-04:00")
        self.assertEqual(pt_evening, et_evening)

    def test_different_dates_dont_collide(self):
        a = self._key("2025-05-13T19:00:00-04:00")
        b = self._key("2025-05-14T19:00:00-04:00")
        self.assertNotEqual(a, b)

    def test_missing_time_falls_through_safely(self):
        # No timestamp — function returns a deterministic "no_time"
        # bucket. Two legs with empty start_time both collide; that's
        # acceptable since they're indistinguishable.
        a = self._key("")
        b = self._key("")
        self.assertEqual(a, b)
        self.assertEqual(a[1], "no_time")


class MakeLegKeyLineNormalizationTests(unittest.TestCase):
    """Bug class fixed in commit 3e59a8b. `line` must be in the key
    AND must normalize across types."""

    def test_float_and_string_line_match(self):
        a = make_leg_key("LeBron", "Points", 22.5, "over", "2025-05-13T19:00:00Z")
        b = make_leg_key("LeBron", "Points", "22.5", "over", "2025-05-13T19:00:00Z")
        self.assertEqual(a, b)

    def test_int_and_float_line_match(self):
        a = make_leg_key("LeBron", "Points", 22, "over", "2025-05-13T19:00:00Z")
        b = make_leg_key("LeBron", "Points", 22.0, "over", "2025-05-13T19:00:00Z")
        self.assertEqual(a, b)

    def test_different_lines_differ(self):
        a = make_leg_key("LeBron", "Points", 22.5, "over", "2025-05-13T19:00:00Z")
        b = make_leg_key("LeBron", "Points", 23.5, "over", "2025-05-13T19:00:00Z")
        self.assertNotEqual(a, b)

    def test_sides_differentiate(self):
        a = make_leg_key("LeBron", "Points", 22.5, "over",  "2025-05-13T19:00:00Z")
        b = make_leg_key("LeBron", "Points", 22.5, "under", "2025-05-13T19:00:00Z")
        self.assertNotEqual(a, b)

    def test_prop_case_insensitive(self):
        a = make_leg_key("LeBron", "Points", 22.5, "over", "2025-05-13T19:00:00Z")
        b = make_leg_key("LeBron", "points", 22.5, "over", "2025-05-13T19:00:00Z")
        self.assertEqual(a, b)


class PlayerNormalizationTests(unittest.TestCase):
    """Bug class hinted at in commit 3e59a8b: scraper spelling drift
    shouldn't bypass dedup."""

    def test_initials_variants_collapse(self):
        # Boundary fix: "R.J. Barrett" / "RJ Barrett" / "R J Barrett"
        # all bucket to the same player.
        names = ["R.J. Barrett", "RJ Barrett", "R J Barrett", "rj barrett"]
        keys = [make_bet_key(n, "2025-05-13T19:00:00Z") for n in names]
        self.assertEqual(len(set(keys)), 1, f"Got keys: {keys}")

    def test_apostrophes_dont_split(self):
        a = make_bet_key("D'Angelo Russell", "2025-05-13T19:00:00Z")
        b = make_bet_key("DAngelo Russell",  "2025-05-13T19:00:00Z")
        self.assertEqual(a, b)

    def test_hyphen_becomes_space(self):
        a = make_bet_key("Jean-Luc Picard", "2025-05-13T19:00:00Z")
        b = make_bet_key("Jean Luc Picard", "2025-05-13T19:00:00Z")
        self.assertEqual(a, b)

    def test_unicode_accents_normalize(self):
        a = make_bet_key("Nikola Jokić", "2025-05-13T19:00:00Z")
        b = make_bet_key("Nikola Jokic", "2025-05-13T19:00:00Z")
        self.assertEqual(a, b)

    def test_different_players_dont_collide(self):
        a = make_bet_key("LeBron James", "2025-05-13T19:00:00Z")
        b = make_bet_key("Stephen Curry", "2025-05-13T19:00:00Z")
        self.assertNotEqual(a, b)


# ───────────────────────────────────────────────────────────────────────
# 2. engine.dedup field-alias unit tests
# ───────────────────────────────────────────────────────────────────────

class FieldAliasTests(unittest.TestCase):
    """The dedup helpers accept both the observatory shape (`player`,
    `prop`, `line`, `game_start`) and the live-pipeline shape
    (`player_name`, `prop_type`, `pp_line`, `start_time`). A silent
    convention mismatch contributed to prior dedup failures."""

    OBS_SHAPE = {
        "player":     "LeBron James",
        "prop":       "Points",
        "line":       22.5,
        "side":       "over",
        "game_start": "2025-05-13T19:00:00-04:00",
    }
    LIVE_SHAPE = {
        "player_name": "LeBron James",
        "prop_type":   "Points",
        "pp_line":     "22.5",
        "side":        "over",
        "start_time":  "2025-05-13T19:00:00-04:00",
    }

    def test_pair_key_matches_across_shapes(self):
        self.assertEqual(leg_pair_key(self.OBS_SHAPE), leg_pair_key(self.LIVE_SHAPE))

    def test_exact_key_matches_across_shapes(self):
        self.assertEqual(leg_exact_key(self.OBS_SHAPE), leg_exact_key(self.LIVE_SHAPE))

    def test_partial_dicts_use_available_alias(self):
        only_player = {"player": "LeBron James", "game_start": "2025-05-13T19:00:00Z"}
        only_pname  = {"player_name": "LeBron James", "start_time": "2025-05-13T19:00:00Z"}
        self.assertEqual(leg_pair_key(only_player), leg_pair_key(only_pname))


# ───────────────────────────────────────────────────────────────────────
# 3. drop_duplicate_slips / check_duplicate_pairs unit tests
# ───────────────────────────────────────────────────────────────────────

def _leg(player, prop, line, side, start):
    """Tiny helper — synthetic leg dict in the observatory shape."""
    return {
        "player": player, "prop": prop, "line": line,
        "side": side, "game_start": start,
        # Strategy_tester slips carry true_prob and result too.
        "true_prob": 0.6, "result": "hit",
    }


class DropDuplicateSlipsTests(unittest.TestCase):
    """The production filter. First-occurrence wins; the second slip is
    dropped entirely."""

    def test_no_duplicates_keeps_all(self):
        slip_a = [_leg("LeBron", "Points", 22.5, "over", "2025-05-13T19:00:00Z")]
        slip_b = [_leg("Curry",  "Points", 27.5, "over", "2025-05-13T19:00:00Z")]
        kept, dropped = drop_duplicate_slips([slip_a, slip_b], strict=True)
        self.assertEqual(len(kept), 2)
        self.assertEqual(dropped, 0)

    def test_same_player_same_game_drops_second(self):
        slip_a = [_leg("LeBron", "Points",   22.5, "over", "2025-05-13T19:00:00Z")]
        slip_b = [_leg("LeBron", "Rebounds", 8.5,  "over", "2025-05-13T19:00:00Z")]
        kept, dropped = drop_duplicate_slips([slip_a, slip_b], strict=True)
        self.assertEqual(len(kept), 1)
        self.assertEqual(dropped, 1)

    def test_same_player_different_game_kept(self):
        slip_a = [_leg("LeBron", "Points", 22.5, "over", "2025-05-13T19:00:00Z")]
        # Two days later — different sports_day.
        slip_b = [_leg("LeBron", "Points", 22.5, "over", "2025-05-15T19:00:00Z")]
        kept, dropped = drop_duplicate_slips([slip_a, slip_b], strict=True)
        self.assertEqual(len(kept), 2)

    def test_tz_drift_in_same_game_still_collapsed(self):
        # Same game, two storage formats. The first slip stores the
        # game as offset-aware; the second naive ET-as-UTC. They MUST
        # be treated as the same game and the second slip dropped.
        slip_a = [_leg("LeBron", "Points", 22.5, "over",
                       "2025-05-13T22:00:00-04:00")]
        slip_b = [_leg("LeBron", "Points", 22.5, "over",
                       "2025-05-14T02:00:00Z")]
        kept, dropped = drop_duplicate_slips([slip_a, slip_b], strict=True)
        self.assertEqual(len(kept), 1)
        self.assertEqual(dropped, 1)

    def test_non_strict_allows_different_props_same_player_game(self):
        slip_a = [_leg("LeBron", "Points",   22.5, "over", "2025-05-13T19:00:00Z")]
        slip_b = [_leg("LeBron", "Rebounds", 8.5,  "over", "2025-05-13T19:00:00Z")]
        kept, dropped = drop_duplicate_slips([slip_a, slip_b], strict=False)
        self.assertEqual(len(kept), 2)

    def test_within_slip_duplicate_drops_whole_slip(self):
        # If a slip somehow has the same leg twice inside it, that slip
        # is invalid — a future code path shouldn't be able to bypass
        # this check by hand-constructing a duplicate-internal slip.
        bad_slip = [
            _leg("LeBron", "Points", 22.5, "over", "2025-05-13T19:00:00Z"),
            _leg("LeBron", "Points", 22.5, "over", "2025-05-13T19:00:00Z"),
        ]
        good_slip = [_leg("Curry", "Points", 27.5, "over", "2025-05-13T19:00:00Z")]
        kept, dropped = drop_duplicate_slips([bad_slip, good_slip])
        self.assertEqual(len(kept), 1)
        self.assertEqual(dropped, 1)

    def test_first_occurrence_wins_order_preserved(self):
        slip_a = [_leg("LeBron", "Points",   22.5, "over", "2025-05-13T19:00:00Z")]
        slip_b = [_leg("LeBron", "Rebounds", 8.5,  "over", "2025-05-13T19:00:00Z")]
        slip_c = [_leg("LeBron", "Assists",  6.5,  "over", "2025-05-13T19:00:00Z")]
        kept, dropped = drop_duplicate_slips([slip_a, slip_b, slip_c], strict=True)
        self.assertEqual(kept, [slip_a])
        self.assertEqual(dropped, 2)

    def test_dict_form_slips_supported(self):
        # Strategy_tester emits slips as dicts with a "legs" field.
        sa = {"legs": [_leg("LeBron", "Points",   22.5, "over", "2025-05-13T19:00:00Z")]}
        sb = {"legs": [_leg("LeBron", "Rebounds", 8.5,  "over", "2025-05-13T19:00:00Z")]}
        kept, dropped = drop_duplicate_slips([sa, sb], strict=True)
        self.assertEqual(len(kept), 1)


class CheckDuplicateAssertionTests(unittest.TestCase):
    """The invariant-check helpers — used by tests and by the post-build
    pass to log violations."""

    def test_check_duplicate_legs_empty_when_clean(self):
        slip_a = [_leg("LeBron", "Points", 22.5, "over", "2025-05-13T19:00:00Z")]
        slip_b = [_leg("Curry",  "Points", 27.5, "over", "2025-05-13T19:00:00Z")]
        self.assertEqual(check_duplicate_legs([slip_a, slip_b]), [])

    def test_check_duplicate_legs_reports_collision(self):
        slip_a = [_leg("LeBron", "Points", 22.5, "over", "2025-05-13T19:00:00Z")]
        slip_b = [_leg("LeBron", "Points", 22.5, "over", "2025-05-13T19:00:00Z")]
        cols = check_duplicate_legs([slip_a, slip_b])
        self.assertEqual(len(cols), 1)
        self.assertEqual((cols[0][0], cols[0][1]), (0, 1))

    def test_check_duplicate_pairs_stricter_than_legs(self):
        slip_a = [_leg("LeBron", "Points",   22.5, "over", "2025-05-13T19:00:00Z")]
        slip_b = [_leg("LeBron", "Rebounds", 8.5,  "over", "2025-05-13T19:00:00Z")]
        self.assertEqual(check_duplicate_legs([slip_a, slip_b]), [],
                         "different prop → different leg_key")
        self.assertNotEqual(check_duplicate_pairs([slip_a, slip_b]), [],
                            "same player+game → pair_key collision")

    def test_assert_no_duplicate_legs_raises(self):
        slip_a = [_leg("LeBron", "Points", 22.5, "over", "2025-05-13T19:00:00Z")]
        slip_b = [_leg("LeBron", "Points", 22.5, "over", "2025-05-13T19:00:00Z")]
        with self.assertRaises(AssertionError):
            assert_no_duplicate_legs([slip_a, slip_b])

    def test_assert_no_duplicate_pairs_raises(self):
        slip_a = [_leg("LeBron", "Points",   22.5, "over", "2025-05-13T19:00:00Z")]
        slip_b = [_leg("LeBron", "Rebounds", 8.5,  "over", "2025-05-13T19:00:00Z")]
        with self.assertRaises(AssertionError):
            assert_no_duplicate_pairs([slip_a, slip_b])


# ───────────────────────────────────────────────────────────────────────
# 8. Live BacktestLogger pre-insert invariant
# ───────────────────────────────────────────────────────────────────────

class _FakeQuery:
    """Tiny PostgREST-ish stub. Only the methods backtest.py actually
    calls are implemented; chaining always returns self."""
    def __init__(self, rows=None):
        self._rows = list(rows or [])
        self.inserts: list[list[dict]] = []

    def select(self, *_a, **_k):     return self
    def gte(self, *_a, **_k):        return self
    def eq(self, *_a, **_k):         return self
    def in_(self, *_a, **_k):        return self
    def order(self, *_a, **_k):      return self
    def limit(self, *_a, **_k):      return self
    def execute(self):
        class _Res:
            def __init__(self, data):
                self.data = data
        return _Res(self._rows)

    def insert(self, rows):
        self.inserts.append(rows if isinstance(rows, list) else [rows])
        return self


class _FakeDB:
    """Maps `table(name)` to a per-table FakeQuery. Pre-seed with
    existing slip + leg rows to simulate the 48 h dedup window."""

    def __init__(self, existing_slips=None, existing_legs=None):
        self._slips_q = _FakeQuery(rows=existing_slips or [])
        self._legs_q  = _FakeQuery(rows=existing_legs  or [])

    def table(self, name):
        if name == "slips": return self._slips_q
        if name == "legs":  return self._legs_q
        raise KeyError(name)


class BacktestLoggerPreInsertTests(unittest.TestCase):
    """The new pre-insert invariant check inside _try_log_slip_locked.

    Catches: within-slip pair_key duplicate after the two-team swap,
    and cross-slip collisions that slipped in between the top-of-method
    read and the actual insert (the race the per-user lock is supposed
    to prevent — but the assertion is the only thing that fails
    *loudly* if the lock is ever bypassed)."""

    def _bet(self, *, player, prop, line, side, start, team, ev=0.05, prob=0.6):
        return {
            "player_name":  player,
            "prop_type":    prop,
            "pp_line":      line,
            "side":         side,
            "start_time":   start,
            "team":         team,
            "true_prob":    prob,
            "raw_true_prob": prob,
            "individual_ev_pct": ev,
        }

    def test_normal_slip_writes_successfully(self):
        from engine.backtest import BacktestLogger
        bets = [
            self._bet(player=f"P{i}", prop="Points", line=20.5, side="over",
                      start="2025-05-13T19:00:00Z",
                      team="ATL" if i < 3 else "BOS",
                      ev=0.05 + 0.001 * i)
            for i in range(6)
        ]
        # Sort descending — _try_log_slip_locked sorts internally too.
        db = _FakeDB()
        bl = BacktestLogger(user_id="u1", db_client=db)
        result = bl.try_log_slip(bets, slip_type="Power", n_legs=6)
        self.assertIsNotNone(result)
        # One slip header + one legs batch inserted.
        self.assertEqual(len(db._slips_q.inserts), 1)
        self.assertEqual(len(db._legs_q.inserts), 1)

    def test_cross_slip_collision_blocks_insert(self):
        """Pre-existing slip in the 48 h window contains player P0 in the
        same game. The pre-insert check must abort the new slip."""
        from engine.backtest import BacktestLogger
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        existing_slips = [{"id": "S-OLD", "timestamp": now}]
        existing_legs = [{
            "slip_id":    "S-OLD",
            "player":     "P0",
            "game_start": "2025-05-13T19:00:00Z",
            "prop":       "Points",
            "line":       20.5,
            "side":       "over",
        }]
        bets = [
            self._bet(player=f"P{i}", prop="Points", line=20.5, side="over",
                      start="2025-05-13T19:00:00Z",
                      team="ATL" if i < 3 else "BOS",
                      ev=0.05 + 0.001 * i)
            for i in range(6)
        ]
        # P0 is the top-EV bet — but the existing slip already used it.
        # _try_log_slip_locked's top-of-method dedup read will already
        # skip P0 from the candidate pool. So the slip we build is on
        # P1..P5 — only 5 legs. With n_legs=6 we should get None.
        db = _FakeDB(existing_slips=existing_slips, existing_legs=existing_legs)
        bl = BacktestLogger(user_id="u1", db_client=db)
        result = bl.try_log_slip(bets, slip_type="Power", n_legs=6)
        self.assertIsNone(result, "should not log a slip when not enough non-colliding bets")
        # Nothing inserted.
        self.assertEqual(len(db._slips_q.inserts), 0)
        self.assertEqual(len(db._legs_q.inserts), 0)

    def test_padded_candidate_pool_logs_around_collision(self):
        """If there are enough non-colliding bets, the slip just uses
        them. Verifies the collision check is at the leg level not the
        whole-input level."""
        from engine.backtest import BacktestLogger
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        # Existing slip used P0.
        db = _FakeDB(
            existing_slips=[{"id": "S-OLD", "timestamp": now}],
            existing_legs=[{
                "slip_id":    "S-OLD",
                "player":     "P0",
                "game_start": "2025-05-13T19:00:00Z",
                "prop":       "Points",
                "line":       20.5,
                "side":       "over",
            }],
        )
        # 8 candidates, 4 per team — P0 is excluded by dedup, but
        # there are still 7 remaining → enough for a 6-leg slip.
        bets = [
            self._bet(player=f"P{i}", prop="Points", line=20.5, side="over",
                      start="2025-05-13T19:00:00Z",
                      team="ATL" if i < 4 else "BOS",
                      ev=0.05 + 0.001 * i)
            for i in range(8)
        ]
        bl = BacktestLogger(user_id="u1", db_client=db)
        result = bl.try_log_slip(bets, slip_type="Power", n_legs=6)
        self.assertIsNotNone(result)
        # The written slip must NOT contain P0.
        written_legs = db._legs_q.inserts[0]
        players_in_slip = [r["player"] for r in written_legs]
        self.assertNotIn("P0", players_in_slip)
        self.assertEqual(len(players_in_slip), 6)


if __name__ == "__main__":
    unittest.main(verbosity=2)
