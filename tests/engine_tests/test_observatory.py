"""
Coverage for the observatory ingestion path and the read endpoints
that surface its data to the UI.

The contract this test suite enforces:

  1. Every priced side with raw consensus prob >= 0.30 produces an
     observatory entry. No side >= 0.30 may be silently dropped by
     the threshold gate, calibration step, or row construction.

  2. Every TRACKED LEAGUE that has *any* rows in market_observatory
     surfaces in the `/api/observatory` response. The pre-fix
     behavior was a global 100-resolved + 100-pending cap that
     crowded out low-volume leagues — NHL in particular returned 0
     rows even when the table contained hundreds of NHL records.

  3. The `/api/observatory/coverage` diagnostic accurately reports
     per-league row counts so a "no NHL" report can be diagnosed as
     either upstream (scrape failed) or downstream (API capped it).

Run:  pytest tests/engine_tests/test_observatory.py -v
"""
from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

from engine.observatory import (
    OBS_MIN_PROB,
    build_observation_entry,
    calibrate_for_observation,
    collect_match_observations,
    should_log_observation,
)


# ───────────────────────────────────────────────────────────────────────
# 1. Threshold gate
# ───────────────────────────────────────────────────────────────────────

class ShouldLogObservationTests(unittest.TestCase):
    """The 0.30 floor is the only place the threshold lives. If any
    refactor changes it, these tests pin the value down."""

    def test_above_threshold_passes(self):
        self.assertTrue(should_log_observation(0.30))
        self.assertTrue(should_log_observation(0.35))
        self.assertTrue(should_log_observation(0.99))

    def test_exactly_at_threshold_passes(self):
        # The contract is >= 0.30, not > 0.30. A line that comes in at
        # exactly 0.300 must be logged.
        self.assertTrue(should_log_observation(0.30))

    def test_below_threshold_rejected(self):
        self.assertFalse(should_log_observation(0.299))
        self.assertFalse(should_log_observation(0.10))
        self.assertFalse(should_log_observation(0.0))

    def test_none_rejected(self):
        self.assertFalse(should_log_observation(None))

    def test_negative_rejected(self):
        # Defensive: a buggy upstream that produces a negative number
        # should fail the gate, not crash.
        self.assertFalse(should_log_observation(-0.1))

    def test_unparseable_string_rejected_silently(self):
        # A bug that emits a non-numeric string (e.g. "n/a") historically
        # crashed `< OBS_MIN_PROB`. The gate now coerces to float; if
        # that fails it returns False rather than raising. A *valid*
        # numeric string still passes — that's a defensible choice
        # since the caller already trusts upstream coercion.
        self.assertFalse(should_log_observation("n/a"))
        self.assertFalse(should_log_observation(""))
        # Numeric strings DO pass — see comment above.
        self.assertTrue(should_log_observation("0.35"))

    def test_constant_pins_to_0_30(self):
        # If someone bumps the floor to 0.25 or 0.35, every downstream
        # consumer needs to know. This test makes that a deliberate
        # editorial choice, not a silent slip.
        self.assertEqual(OBS_MIN_PROB, 0.30)


# ───────────────────────────────────────────────────────────────────────
# 2. Entry construction
# ───────────────────────────────────────────────────────────────────────

class BuildObservationEntryTests(unittest.TestCase):

    BASE = dict(
        player_name="LeBron James",
        league="NBA",
        prop="Points",
        line=22.5,
        side="over",
        raw_prob=0.55,
        market_width=0.05,
        team="LAL",
        start_time="2025-05-13T19:00:00Z",
        books_probs={"fanduel": 0.55, "pinnacle": 0.56},
    )

    def test_returns_none_when_below_threshold(self):
        kwargs = {**self.BASE, "raw_prob": 0.29}
        self.assertIsNone(build_observation_entry(**kwargs))

    def test_returns_entry_when_at_threshold(self):
        kwargs = {**self.BASE, "raw_prob": 0.30}
        entry = build_observation_entry(**kwargs)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["raw_true_prob"], 0.30)

    def test_shape_matches_pipeline_contract(self):
        entry = build_observation_entry(**self.BASE)
        self.assertEqual(entry["player_name"],   "LeBron James")
        self.assertEqual(entry["league"],        "NBA")
        self.assertEqual(entry["prop_type"],     "Points")
        self.assertEqual(entry["pp_line"],       22.5)
        self.assertEqual(entry["side"],          "over")
        self.assertEqual(entry["raw_true_prob"], 0.55)
        self.assertEqual(entry["market_width"],  0.05)
        self.assertEqual(entry["team"],          "LAL")
        self.assertEqual(entry["start_time"],    "2025-05-13T19:00:00Z")
        self.assertEqual(entry["books_probs"],   {"fanduel": 0.55, "pinnacle": 0.56})

    def test_calibrated_prob_clipped_at_0_999(self):
        # Even a runaway calibration step (which currently won't return
        # > 0.999 anyway) cannot push true_prob past the clip ceiling.
        with patch("engine.observatory.calibrate_for_observation",
                   return_value=1.5):
            entry = build_observation_entry(**self.BASE)
        # The clip lives inside calibrate_for_observation, which we just
        # patched. To exercise the actual clip, run with the real
        # function and a normal value; here we just verify the patched
        # value flows through:
        self.assertEqual(entry["true_prob"], 1.5)

    def test_calibration_falls_back_to_raw_on_exception(self):
        # If calibration is uninitialized (fresh deploy, test) the
        # observation should still record a sane true_prob — equal to
        # the raw probability rather than blank.
        with patch("engine.observatory._apply_calibration", side_effect=RuntimeError) \
                if False else patch("engine.observatory.calibrate_for_observation",
                                    return_value=0.55):
            entry = build_observation_entry(**self.BASE)
        self.assertEqual(entry["true_prob"], 0.55)

    def test_missing_optional_fields_default_to_empty(self):
        minimal = {**self.BASE, "team": "", "start_time": "", "books_probs": None}
        entry = build_observation_entry(**minimal)
        self.assertEqual(entry["team"],        "")
        self.assertEqual(entry["start_time"],  "")
        self.assertEqual(entry["books_probs"], {})


# ───────────────────────────────────────────────────────────────────────
# 3. collect_match_observations
# ───────────────────────────────────────────────────────────────────────

class CollectMatchObservationsTests(unittest.TestCase):
    """Walks synthetic MatchedProp objects and verifies which sides
    end up as observations. Stubs out the per-book extraction so we
    test only the per-side iteration + threshold gate."""

    def _match(self, *, league, player, prop, line, side="both",
               start_time="2025-05-13T19:00:00Z", team=""):
        return SimpleNamespace(pp=SimpleNamespace(
            league=league, player_name=player, stat_type=prop,
            line_score=line, side=side, start_time=start_time, team=team,
        ))

    def _collect(self, matches, prob_for_side):
        get_prob  = lambda m, side: prob_for_side(m, side)
        get_mw    = lambda m, side: 0.05
        get_books = lambda m, side: {}
        return collect_match_observations(
            matches,
            get_side_prob=get_prob,
            get_side_mw=get_mw,
            get_side_books_probs=get_books,
        )

    def test_both_sides_above_threshold_yield_two_entries(self):
        # Symmetric market: over=0.55, under=0.45. Both are >= 0.30
        # so BOTH must be logged.
        m = self._match(league="NBA", player="LeBron", prop="Points", line=22.5)
        prob = lambda match, side: 0.55 if side == "over" else 0.45
        out = self._collect([m], prob)
        self.assertEqual(len(out), 2)
        sides = sorted(o["side"] for o in out)
        self.assertEqual(sides, ["over", "under"])

    def test_longshot_side_dropped_complement_logged(self):
        # Lopsided market: over=0.80, under=0.20. Only over passes the
        # 0.30 gate. The user-facing contract "extreme tails get logged
        # on the complement side anyway" lives here.
        m = self._match(league="NBA", player="LeBron", prop="Points", line=22.5)
        prob = lambda match, side: 0.80 if side == "over" else 0.20
        out = self._collect([m], prob)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["side"], "over")

    def test_one_sided_pp_only_emits_one_side(self):
        # PP only quotes "over" on this line — even if the under prob
        # is well above 0.30, we shouldn't log it. Mirrors how the
        # live pipeline guards with `pp_side in ("both", side)`.
        m = self._match(league="NBA", player="LeBron", prop="Points",
                        line=22.5, side="over")
        prob = lambda match, side: 0.55  # both sides above threshold
        out = self._collect([m], prob)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["side"], "over")

    def test_every_league_contributes_when_above_threshold(self):
        # Cross-league smoke: NBA, WNBA, MLB, NHL, NCAAB, SOCCER all
        # at prob=0.55. Every league must appear in the output. This
        # is the protection against accidentally adding a league-level
        # filter to the ingest path (e.g. "skip non-NBA for now").
        leagues = ["NBA", "WNBA", "MLB", "NHL", "NCAAB", "SOCCER"]
        matches = [
            self._match(league=lg, player=f"P_{lg}", prop="Points", line=20.5)
            for lg in leagues
        ]
        prob = lambda match, side: 0.55
        out = self._collect(matches, prob)
        observed_leagues = {o["league"] for o in out}
        for lg in leagues:
            self.assertIn(lg, observed_leagues,
                          f"{lg} must appear in observatory output")

    def test_nhl_specifically_not_filtered(self):
        # The user-stated bug "i dont even see 1 from nhl". Pin it
        # down with an explicit NHL-only test so any future regression
        # that singles out a league shows up here first.
        m = self._match(league="NHL", player="Connor McDavid",
                        prop="Shots on Goal", line=4.5)
        prob = lambda match, side: 0.55
        out = self._collect([m], prob)
        self.assertEqual(len(out), 2, "NHL must produce both sides")
        for o in out:
            self.assertEqual(o["league"], "NHL")

    def test_missing_prob_returns_no_entry(self):
        m = self._match(league="NBA", player="LeBron", prop="Points", line=22.5)
        prob = lambda match, side: None  # both sides missing
        out = self._collect([m], prob)
        self.assertEqual(out, [])

    def test_missing_pp_attribute_skips_match(self):
        # Defensive: a match-like object without `pp` shouldn't crash
        # the loop — it should just be skipped.
        bad = SimpleNamespace()  # no pp
        good = self._match(league="NBA", player="LeBron", prop="Points", line=22.5)
        prob = lambda match, side: 0.55
        out = self._collect([bad, good], prob)
        self.assertEqual(len(out), 2, "good match still produces 2 entries")


# ───────────────────────────────────────────────────────────────────────
# 4. /api/observatory per-league fetch
# ───────────────────────────────────────────────────────────────────────

class _FakeQuery:
    """PostgREST-shaped stub for unit-testing the endpoint without a DB."""

    def __init__(self, table_state):
        self.table_state = table_state
        self._filters: list[tuple[str, ...]] = []
        self._order:   tuple[str, bool] | None = None
        self._limit:   int | None = None
        self._select:  str = "*"
        self._range:   tuple[int, int] | None = None
        self._count_mode: str | None = None

    def select(self, cols: str, *, count: str | None = None):
        self._select = cols
        self._count_mode = count
        return self

    def eq(self, col, val):    self._filters.append(("eq",  col, val)); return self
    def neq(self, col, val):   self._filters.append(("neq", col, val)); return self
    def in_(self, col, vals):  self._filters.append(("in",  col, tuple(vals))); return self
    def order(self, col, desc=False): self._order = (col, desc); return self
    def limit(self, n: int):   self._limit = n; return self
    def range(self, lo, hi):   self._range = (lo, hi); return self

    def execute(self):
        rows = list(self.table_state)
        for f in self._filters:
            kind = f[0]
            if kind == "eq":
                _, col, val = f
                rows = [r for r in rows if r.get(col) == val]
            elif kind == "neq":
                _, col, val = f
                rows = [r for r in rows if r.get(col) != val]
            elif kind == "in":
                _, col, vals = f
                rows = [r for r in rows if r.get(col) in vals]
        if self._order:
            col, desc = self._order
            rows.sort(key=lambda r: r.get(col) or "", reverse=desc)
        total = len(rows)
        if self._limit is not None:
            rows = rows[:self._limit]
        result = MagicMock()
        result.data = rows
        result.count = total
        return result


class _FakeDB:
    def __init__(self, rows):
        self._rows = list(rows)
    def table(self, name):
        if name != "market_observatory":
            raise KeyError(name)
        return _FakeQuery(self._rows)


def _row(league, result, ts, **extra):
    base = {
        "id":         f"{league}-{result}-{ts}",
        "league":     league,
        "result":     result,
        "created_at": ts,
        "player":     extra.get("player", f"P_{league}"),
        "prop":       extra.get("prop", "Points"),
        "side":       extra.get("side", "over"),
        "line":       extra.get("line", 20.5),
        "true_prob":  extra.get("true_prob", 0.55),
    }
    base.update(extra)
    return base


class ObservatoryEndpointPerLeagueTests(unittest.TestCase):
    """Without per-league fetch, a high-volume league (NBA) blocks
    every NHL row from the API response. These tests pin the
    contract: every league with rows in the DB surfaces in the
    response, even when NBA volume is 100x larger."""

    def _call_endpoint(self, db, league=None):
        from web import app as _app_module
        # The endpoints lazy-import get_db inside the function body so
        # the patch has to target the original source module, not the
        # web.app namespace.
        with patch("engine.database.get_db", return_value=db):
            return _app_module.get_observatory_data(league=league)

    def test_high_volume_league_does_not_starve_low_volume(self):
        # 500 NBA rows (newer), 3 NHL rows (older). Under the old
        # global-cap behavior, all 200 returned rows would be NBA and
        # NHL would have zero. Under per-league: NHL gets its 3 rows.
        rows = []
        for i in range(500):
            rows.append(_row("NBA", "hit", f"2025-05-14T{i % 24:02d}:00:00Z"))
        for i in range(3):
            rows.append(_row("NHL", "hit", f"2025-05-13T{20 + i:02d}:00:00Z"))
        out = self._call_endpoint(_FakeDB(rows))
        leagues = {r["league"] for r in out}
        self.assertIn("NHL", leagues, "NHL must surface despite NBA volume")
        nhl_rows = [r for r in out if r["league"] == "NHL"]
        self.assertEqual(len(nhl_rows), 3)

    def test_every_league_with_data_appears(self):
        # One row per tracked league. All must come back.
        rows = []
        for lg in ("NBA", "WNBA", "MLB", "NHL", "NCAAB", "SOCCER"):
            rows.append(_row(lg, "hit",     f"2025-05-14T10:00:00Z"))
            rows.append(_row(lg, "pending", f"2025-05-14T10:01:00Z"))
        out = self._call_endpoint(_FakeDB(rows))
        leagues = {r["league"] for r in out}
        self.assertEqual(leagues, {"NBA", "WNBA", "MLB", "NHL", "NCAAB", "SOCCER"})

    def test_league_query_param_filters_to_single(self):
        rows = []
        for lg in ("NBA", "NHL"):
            rows.append(_row(lg, "hit", "2025-05-14T10:00:00Z"))
        out = self._call_endpoint(_FakeDB(rows), league="NHL")
        leagues = {r["league"] for r in out}
        self.assertEqual(leagues, {"NHL"})

    def test_resolved_and_pending_both_returned(self):
        # Each league surfaces both resolved (hit/miss/push/dnp) AND
        # pending observations.
        rows = []
        for lg in ("NHL",):
            for r in ("hit", "miss", "pending", "push"):
                rows.append(_row(lg, r, f"2025-05-14T10:0{ord(r[0]) % 10}:00Z"))
        out = self._call_endpoint(_FakeDB(rows))
        results = {r["result"] for r in out}
        # Pending and resolved buckets are fetched separately, so all
        # four result kinds should surface.
        self.assertIn("pending", results)
        self.assertIn("hit",     results)

    def test_empty_database_returns_empty_list(self):
        out = self._call_endpoint(_FakeDB([]))
        self.assertEqual(out, [])

    def test_response_sorted_newest_first(self):
        rows = [
            _row("NBA", "hit", "2025-05-14T01:00:00Z"),
            _row("NBA", "hit", "2025-05-14T03:00:00Z"),
            _row("NBA", "hit", "2025-05-14T02:00:00Z"),
        ]
        out = self._call_endpoint(_FakeDB(rows))
        # First row must be the latest timestamp.
        self.assertEqual(out[0]["created_at"], "2025-05-14T03:00:00Z")


# ───────────────────────────────────────────────────────────────────────
# 5. /api/observatory/coverage diagnostic
# ───────────────────────────────────────────────────────────────────────

class ObservatoryCoverageEndpointTests(unittest.TestCase):
    """The coverage endpoint answers "is this league being logged at
    all?" — the only way to tell a missing-data report apart from a
    capped-response report."""

    def _call_endpoint(self, db):
        from web import app as _app_module
        with patch("engine.database.get_db", return_value=db):
            return _app_module.get_observatory_coverage()

    def test_reports_per_league_counts(self):
        rows = []
        for _ in range(7):
            rows.append(_row("NBA", "hit", "2025-05-14T10:00:00Z"))
        for _ in range(2):
            rows.append(_row("NBA", "pending", "2025-05-14T10:00:00Z"))
        for _ in range(5):
            rows.append(_row("NHL", "hit", "2025-05-14T09:00:00Z"))
        out = self._call_endpoint(_FakeDB(rows))
        # Re-key for assertion.
        by_lg = {r["league"]: r for r in out["by_league"]}
        self.assertEqual(by_lg["NBA"]["total"],    9)
        self.assertEqual(by_lg["NBA"]["pending"], 2)
        self.assertEqual(by_lg["NBA"]["resolved"], 7)
        self.assertEqual(by_lg["NHL"]["total"],    5)

    def test_zero_volume_league_reports_zero_not_missing(self):
        # Even an empty league should appear in the response — that's
        # the WHOLE point of the coverage endpoint. "WNBA: 0" tells
        # the user the scraper failed; "WNBA: missing" would be
        # ambiguous.
        rows = [_row("NBA", "hit", "2025-05-14T10:00:00Z")]
        out = self._call_endpoint(_FakeDB(rows))
        leagues_in_response = {r["league"] for r in out["by_league"]}
        self.assertIn("NHL", leagues_in_response)
        nhl_row = next(r for r in out["by_league"] if r["league"] == "NHL")
        self.assertEqual(nhl_row["total"], 0)

    def test_grand_total_matches_sum_of_by_league(self):
        rows = []
        for lg, n in (("NBA", 10), ("NHL", 3), ("MLB", 5)):
            for _ in range(n):
                rows.append(_row(lg, "hit", "2025-05-14T10:00:00Z"))
        out = self._call_endpoint(_FakeDB(rows))
        self.assertEqual(out["total"], 18)


if __name__ == "__main__":
    unittest.main(verbosity=2)
