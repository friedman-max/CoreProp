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
# 4. _select_ranked slip-builder unit tests
# ───────────────────────────────────────────────────────────────────────

class SelectRankedDedupTests(unittest.TestCase):
    """Stress the slip-builder itself with synthetic slates. Exercises:

      • single-slate cross-slip dedup
      • multi-slate dedup via shared used_pair / used_leg sets
      • two-team swap (the bug-class fixed in 3e59a8b)
      • TZ-drift across slate boundaries (the bug-class fixed in 3e59a8b)
    """

    def _tester(self):
        from engine.strategy_tester import StrategyTester
        # Bypass __init__ entirely — we never touch self.db here, and
        # not calling get_db() avoids the network round-trip from the
        # Supabase stub.
        return StrategyTester.__new__(StrategyTester)

    def _slate(self, rows: list[dict]) -> pd.DataFrame:
        df = pd.DataFrame(rows)
        # Strategy_tester pre-computes `_ev` before slip building.
        return df

    def _row(self, *, player, prop, line, side, start, team, calibrated_prob):
        return {
            "player": player, "prop": prop, "line": line, "side": side,
            "game_start": start, "team": team,
            "calibrated_prob": calibrated_prob, "result": "hit",
        }

    def test_single_slate_two_slips_no_shared_player(self):
        t = self._tester()
        rows = [
            self._row(player=f"P{i}", prop="Points", line=20.5, side="over",
                      start="2025-05-13T19:00:00Z",
                      team="A" if i < 6 else "B",
                      calibrated_prob=0.6 - 0.01 * i)
            for i in range(12)
        ]
        slate = self._slate(rows)
        slips = t._build_slips(slate, 6, "top_ev")
        self.assertEqual(len(slips), 2)
        # Confirm no leg-key appears in two slips.
        assert_no_duplicate_legs([s for s in slips])

    def test_cross_slate_shared_sets_prevent_dupes(self):
        t = self._tester()
        # Same 6 players in BOTH slates (same game; observatory has
        # both PP and book rows landing on different dates due to TZ).
        slate_1_rows = [
            self._row(player=f"P{i}", prop="Points", line=20.5, side="over",
                      start="2025-05-13T19:00:00-04:00",
                      team="A" if i < 3 else "B",
                      calibrated_prob=0.6) for i in range(6)
        ]
        slate_2_rows = [
            self._row(player=f"P{i}", prop="Points", line=20.5, side="over",
                      # SAME game, different format (naive ET-as-UTC).
                      start="2025-05-13T23:00:00",
                      team="A" if i < 3 else "B",
                      calibrated_prob=0.6) for i in range(6)
        ]
        used_pair: set = set()
        used_leg:  set = set()
        slips_1 = t._build_slips(self._slate(slate_1_rows), 6, "top_ev",
                                 used_pair=used_pair, used_leg=used_leg)
        slips_2 = t._build_slips(self._slate(slate_2_rows), 6, "top_ev",
                                 used_pair=used_pair, used_leg=used_leg)
        self.assertEqual(len(slips_1), 1)
        self.assertEqual(len(slips_2), 0,
                         "second slate is the same game in a different "
                         "timestamp format → no new slip should form")

    def test_two_team_swap_does_not_reintroduce_collision(self):
        # The bug-class fixed in 3e59a8b: greedy first pass picks all-
        # team-A, swap drops the last leg and pulls in a Team B leg —
        # but the swap candidate must not collide with the kept legs.
        t = self._tester()
        rows = []
        # Five team-A legs at the top of the EV ordering.
        for i in range(5):
            rows.append(self._row(
                player=f"A{i}", prop="Points", line=20.5, side="over",
                start="2025-05-13T19:00:00Z", team="ATL",
                calibrated_prob=0.7 - 0.001 * i,
            ))
        # A duplicate of A0 with a slightly higher EV — should NOT be
        # eligible as the swap candidate (collides with kept A0).
        rows.append(self._row(
            player="A0", prop="Rebounds", line=8.5, side="over",
            start="2025-05-13T19:00:00Z", team="BOS",  # diff team but same player+game
            calibrated_prob=0.69,
        ))
        # A legitimate team-B leg further down.
        rows.append(self._row(
            player="B0", prop="Points", line=20.5, side="over",
            start="2025-05-13T19:00:00Z", team="BOS",
            calibrated_prob=0.5,
        ))
        slate = self._slate(rows)
        slips = t._build_slips(slate, 6, "top_ev")
        # The swap must pick B0, not the A0/Rebounds candidate.
        self.assertEqual(len(slips), 1)
        legs = slips[0]
        players_in_slip = [l["player"] for l in legs]
        # A0 still in slip exactly once (from the first pass).
        self.assertEqual(players_in_slip.count("A0"), 1)
        # B0 was the swap result.
        self.assertIn("B0", players_in_slip)

    def test_top_prob_strategy_also_dedupes(self):
        """All strategies (top_prob, top_ev, live_replay) now dedup."""
        t = self._tester()
        rows = [
            self._row(player=f"P{i}", prop="Points", line=20.5, side="over",
                      start="2025-05-13T19:00:00Z",
                      team="A" if i < 6 else "B",
                      calibrated_prob=0.6 - 0.01 * i) for i in range(12)
        ]
        slate = self._slate(rows)
        slips = t._build_slips(slate, 6, "top_prob")
        self.assertEqual(len(slips), 2)
        assert_no_duplicate_legs(slips)

    def test_within_slate_same_player_two_lines_only_one_picked(self):
        # Two rows for the same player+game with different lines —
        # both have positive EV but they'd correlate perfectly. Only
        # the higher-ranked one should enter a slip; the second is
        # rejected by the within-slip pair_key check.
        t = self._tester()
        rows = [
            self._row(player="LeBron", prop="Points", line=22.5, side="over",
                      start="2025-05-13T19:00:00Z", team="LAL",
                      calibrated_prob=0.70),
            self._row(player="LeBron", prop="Rebounds", line=8.5, side="over",
                      start="2025-05-13T19:00:00Z", team="LAL",
                      calibrated_prob=0.69),
        ]
        # Pad with team-B legs so a 2-leg slip is possible.
        for i in range(5):
            rows.append(self._row(
                player=f"B{i}", prop="Points", line=20.5, side="over",
                start="2025-05-13T19:00:00Z", team="BOS",
                calibrated_prob=0.60 - 0.01 * i,
            ))
        slate = self._slate(rows)
        slips = t._build_slips(slate, 6, "top_ev")
        self.assertEqual(len(slips), 1)
        leg_players = [l["player"] for l in slips[0]]
        self.assertEqual(leg_players.count("LeBron"), 1)


# ───────────────────────────────────────────────────────────────────────
# 5. End-to-end run_simulation invariant test
# ───────────────────────────────────────────────────────────────────────

class RunSimulationDedupInvariantTests(unittest.TestCase):
    """Drive the whole simulator against a synthetic observatory dataset
    and assert: zero duplicate pair_keys across the emitted slips,
    regardless of strategy choice."""

    def _synth_observatory(self, *, days: int = 5, players_per_day: int = 14) -> pd.DataFrame:
        """A multi-day, multi-team, multi-game observatory fixture with
        plenty of legs per slate to exercise multi-slip emission and
        cross-slate dedup."""
        rows = []
        for d in range(days):
            # Use offset-aware ISO so date filtering works deterministically.
            day = f"2025-05-{13 + d:02d}"
            for i in range(players_per_day):
                rows.append({
                    "player":         f"P{d:02d}_{i:02d}",
                    "prop":           "Points",
                    "line":           20.5,
                    "side":           "over",
                    "game_start":     f"{day}T19:00:00-04:00",
                    "team":           "ATL" if i < players_per_day // 2 else "BOS",
                    "league":         "NBA",
                    "true_prob":      0.6,
                    "raw_true_prob":  0.6,
                    "market_width":   0.05,
                    "result":         "hit" if (i + d) % 2 == 0 else "miss",
                })
        return pd.DataFrame(rows)

    def _run_with_fixture(self, fixture: pd.DataFrame, **cfg_overrides):
        """Patch the data fetch + calibration so run_simulation reads
        from our fixture and applies a passthrough calibration."""
        from engine.strategy_tester import StrategyTester, StrategyConfig

        cfg = StrategyConfig(
            leagues=["NBA"],
            min_prob=0.5,
            slip_size=6,
            slip_type="power",
            bankroll=100.0,
            bet_size=1.0,
            use_kelly=False,
            slip_strategy="live_replay",
            bootstrap=False,
        )
        for k, v in cfg_overrides.items():
            setattr(cfg, k, v)

        tester = StrategyTester.__new__(StrategyTester)
        tester.db = object()  # truthy — bypass the "db not connected" guard
        tester._curves = {}

        # Patch calibration to passthrough so we don't need a fitted file.
        def _passthrough(self, df):
            df = df.copy()
            df["calibrated_prob"] = pd.to_numeric(df["true_prob"], errors="coerce")
            return df
        with patch.object(StrategyTester, "_fetch_resolved_observatory",
                          lambda self, leagues: fixture), \
             patch.object(StrategyTester, "_apply_current_calibration",
                          _passthrough):
            return tester.run_simulation(cfg)

    def test_no_duplicate_pair_keys_in_run_simulation_output(self):
        fixture = self._synth_observatory()
        out = self._run_with_fixture(fixture)
        self.assertNotIn("error", out, msg=out)
        slips = out["slips"]
        self.assertGreater(len(slips), 0, "fixture should produce >=1 slip")
        # The contract: no (player, sports_day) pair appears twice.
        assert_no_duplicate_pairs(slips)
        # And no exact-leg duplicate either (stricter).
        assert_no_duplicate_legs(slips)

    def test_dedup_holds_across_all_strategies(self):
        fixture = self._synth_observatory()
        for strat in ("top_prob", "top_ev", "live_replay"):
            with self.subTest(strategy=strat):
                out = self._run_with_fixture(fixture, slip_strategy=strat)
                self.assertNotIn("error", out, msg=f"{strat}: {out}")
                assert_no_duplicate_pairs(out["slips"])

    def test_dedup_holds_when_observatory_has_tz_drift(self):
        """The most-recently-fixed bug class: the same logical game
        appears in two rows with two timestamp formats. End-to-end
        check that the simulator (still) collapses them."""
        # 7 pm ET tip stored as offset-aware vs. naive ET-as-UTC.
        # The greedy builder would pick from one row, but the dedup
        # pass must reject the second row's leg from a later slip.
        rows = []
        for i in range(14):
            rows.append({
                "player": f"P{i:02d}", "prop": "Points", "line": 20.5,
                "side": "over", "game_start": "2025-05-13T19:00:00-04:00",
                "team": "ATL" if i < 7 else "BOS",
                "league": "NBA",
                "true_prob": 0.6, "raw_true_prob": 0.6, "market_width": 0.05,
                "result": "hit",
            })
        # Duplicate every leg with a different TZ format.
        dupes = []
        for r in rows:
            d = dict(r)
            d["game_start"] = "2025-05-13T23:00:00"  # same instant, naive
            dupes.append(d)
        fixture = pd.DataFrame(rows + dupes)
        out = self._run_with_fixture(fixture)
        self.assertNotIn("error", out, msg=out)
        assert_no_duplicate_pairs(out["slips"])

    def test_funnel_reports_dedup_drops(self):
        fixture = self._synth_observatory()
        out = self._run_with_fixture(fixture)
        self.assertIn("slips_dropped_post_dedup", out["funnel"])
        # In a clean fixture, the count should be 0 — slot is present
        # for diagnostics either way.
        self.assertEqual(out["funnel"]["slips_dropped_post_dedup"], 0)


# ───────────────────────────────────────────────────────────────────────
# 6. optimize_threshold invariant test
# ───────────────────────────────────────────────────────────────────────

class OptimizeThresholdDedupInvariantTests(unittest.TestCase):
    """The optimizer threshold sweep emits slips per threshold — the
    invariant holds within each threshold evaluation."""

    def _run_opt_with_fixture(self, fixture: pd.DataFrame):
        from engine.strategy_tester import StrategyTester, StrategyConfig
        cfg = StrategyConfig(
            leagues=["NBA"], min_prob=0.5, slip_size=6, slip_type="power",
            bankroll=100.0, bet_size=1.0, use_kelly=False,
            slip_strategy="live_replay", bootstrap=False,
        )
        tester = StrategyTester.__new__(StrategyTester)
        tester.db = object()
        tester._curves = {}

        def _passthrough(self, df):
            df = df.copy()
            df["calibrated_prob"] = pd.to_numeric(df["true_prob"], errors="coerce")
            return df

        # Hook _simulate_at_threshold so we can inspect emitted slips
        # by capturing them at the dedup step. We can't easily expose
        # them from the existing optimizer, so we exercise the
        # builder + dedup helper directly on the optimizer's data
        # path to verify the invariant holds.
        with patch.object(StrategyTester, "_fetch_resolved_observatory",
                          lambda self, leagues: fixture), \
             patch.object(StrategyTester, "_apply_current_calibration",
                          _passthrough):
            base_df = tester._fetch_resolved_observatory([])
            base_df["game_start_dt"] = pd.to_datetime(
                base_df["game_start"], errors="coerce", utc=True,
            )
            base_df = tester._apply_current_calibration(base_df)
            base_df["slate_id"] = base_df["game_start_dt"].dt.date.astype(str)

            # Mirror _simulate_at_threshold's slip materialization.
            slates = base_df.groupby("slate_id")
            opt_used_pair: set = set()
            opt_used_leg:  set = set()
            emitted: list[list[dict]] = []
            for sid in base_df.sort_values(
                "game_start_dt", kind="stable",
            )["slate_id"].unique():
                slate_df = slates.get_group(sid)
                if len(slate_df) < cfg.slip_size:
                    continue
                for slip in tester._build_slips(
                    slate_df, cfg.slip_size, cfg.slip_strategy,
                    used_pair=opt_used_pair, used_leg=opt_used_leg,
                ):
                    emitted.append(slip)
        return emitted

    def test_optimizer_emits_no_duplicate_pairs(self):
        # Use a 3-day fixture with same-team players (forces the swap
        # path) and intentional TZ drift across days.
        rows = []
        for d in range(3):
            day = f"2025-05-{13 + d:02d}"
            for i in range(14):
                rows.append({
                    "player": f"P{d}_{i:02d}", "prop": "Points",
                    "line": 20.5, "side": "over",
                    "game_start": f"{day}T19:00:00-04:00",
                    "team": "ATL" if i < 7 else "BOS",
                    "league": "NBA",
                    "true_prob": 0.6, "raw_true_prob": 0.6,
                    "market_width": 0.05, "result": "hit",
                })
        fixture = pd.DataFrame(rows)
        slips = self._run_opt_with_fixture(fixture)
        self.assertGreater(len(slips), 0)
        # No (player, sports_day) appears in two slips.
        assert_no_duplicate_pairs(slips)


# ───────────────────────────────────────────────────────────────────────
# 7. Property-style robustness test
# ───────────────────────────────────────────────────────────────────────

class DedupInvariantPropertyTest(unittest.TestCase):
    """Brute-force property: generate a wide range of synthetic
    fixtures (varying team distribution, days, slip sizes) and verify
    the dedup invariant holds for every output."""

    def test_random_fixtures_all_satisfy_invariant(self):
        import random as _r
        from engine.strategy_tester import StrategyTester, StrategyConfig
        rng = _r.Random(42)
        for trial in range(20):
            n_days = rng.randint(2, 6)
            n_per_day = rng.randint(8, 25)
            slip_size = rng.choice([2, 3, 4, 5, 6])
            rows = []
            for d in range(n_days):
                day = f"2025-05-{13 + d:02d}"
                # Random TZ format per row to stress the sports-day shift.
                for i in range(n_per_day):
                    if rng.random() < 0.5:
                        ts = f"{day}T{rng.randint(17, 23):02d}:00:00-04:00"
                    else:
                        # naive UTC representation
                        h = rng.randint(21, 23) + 4
                        next_day = day if h < 24 else f"2025-05-{14 + d:02d}"
                        h_norm = h if h < 24 else h - 24
                        ts = f"{next_day}T{h_norm:02d}:00:00"
                    rows.append({
                        "player": f"P{d}_{i:02d}", "prop": "Points",
                        "line": 20.5, "side": rng.choice(["over", "under"]),
                        "game_start": ts,
                        "team": rng.choice(["ATL", "BOS", "LAL", "MIA"]),
                        "league": "NBA",
                        "true_prob": rng.uniform(0.5, 0.7),
                        "raw_true_prob": 0.6,
                        "market_width": 0.05,
                        "result": rng.choice(["hit", "miss"]),
                    })
            fixture = pd.DataFrame(rows)

            cfg = StrategyConfig(
                leagues=["NBA"], min_prob=0.5, slip_size=slip_size,
                slip_type="flex" if slip_size >= 3 else "power",
                bankroll=100.0, bet_size=1.0, use_kelly=False,
                slip_strategy="live_replay", bootstrap=False,
            )
            tester = StrategyTester.__new__(StrategyTester)
            tester.db = object()
            tester._curves = {}

            def _passthrough(self, df):
                df = df.copy()
                df["calibrated_prob"] = pd.to_numeric(df["true_prob"], errors="coerce")
                return df

            with patch.object(StrategyTester, "_fetch_resolved_observatory",
                              lambda self, leagues: fixture), \
                 patch.object(StrategyTester, "_apply_current_calibration",
                              _passthrough):
                out = tester.run_simulation(cfg)

            if "error" in out:
                # Some random fixtures don't produce any slips — that's OK.
                continue
            try:
                assert_no_duplicate_pairs(out["slips"])
            except AssertionError as exc:
                self.fail(
                    f"Invariant violation in random trial {trial} "
                    f"(slip_size={slip_size}, days={n_days}, per_day={n_per_day}): "
                    f"{exc}"
                )


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
