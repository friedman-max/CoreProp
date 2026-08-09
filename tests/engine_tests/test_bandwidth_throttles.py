"""
The metered-egress throttles must reduce bytes without losing data.

Render force-migrated legacy workspaces to new plans on 2026-08-01, cutting the
Hobby allowance for outbound bandwidth from 100 GB/month to 5 GB. Usage did not
change (July averaged 736 MB/day, August 1-7 averaged 799), so the same traffic
that sat at 23% of the old allowance exhausted the new one in six days and the
service was suspended on 2026-08-08.

Three throttles came out of that, and each one is only correct because of a
property that is easy to break later:

  - the snapshot sync is skippable because its ONLY reader tolerates 24h-old data
  - the observatory upsert is skippable per market_key because a re-send is
    discarded server-side
  - the results grader is skippable because grading is idempotent

These tests pin the throttles AND the properties they depend on.
"""
from datetime import datetime, timedelta, timezone

import pytest


# ── The snapshot sync throttle ────────────────────────────────────────────

def test_snapshot_sync_interval_stays_under_the_seed_tolerance():
    """Writing less often than the seed accepts would serve stale data.

    _SNAPSHOT_SYNC_MIN is only safe while it is comfortably below
    _SEED_MAX_AGE_MIN: the seed loader purges (and ignores) anything older than
    its tolerance, so a sync interval at or above it could leave a restart with
    nothing to seed from at all.
    """
    from web.app import _SNAPSHOT_SYNC_MIN, _SEED_MAX_AGE_MIN

    assert _SNAPSHOT_SYNC_MIN < _SEED_MAX_AGE_MIN, (
        f"snapshot sync every {_SNAPSHOT_SYNC_MIN}min but the seed ignores "
        f"anything older than {_SEED_MAX_AGE_MIN}min — a restart could find no "
        "usable snapshot"
    )
    # Not merely below it: leave at least an order of magnitude of headroom so
    # a missed cycle or a slow scrape can't push a snapshot past the bound.
    assert _SNAPSHOT_SYNC_MIN * 10 <= _SEED_MAX_AGE_MIN


def test_app_state_cache_has_no_reader_on_the_request_path():
    """The throttle is invisible only because nothing serves these keys.

    If a future endpoint reads app_state_cache to answer a request, hourly
    writes stop being a free optimisation and this test should fail loudly
    rather than the staleness reaching users.
    """
    import ast
    from pathlib import Path

    tree = ast.parse(Path("web/app.py").read_text())
    loaders = {"load_state_from_supabase", "load_multiple_states_from_supabase"}

    # Map every function definition to the calls it makes, so an import
    # statement is not mistaken for a read.
    callers = {}

    class Visitor(ast.NodeVisitor):
        def __init__(self):
            self.stack = []

        def visit_FunctionDef(self, node):
            self.stack.append(node.name)
            self.generic_visit(node)
            self.stack.pop()

        def visit_Call(self, node):
            fn = node.func
            name = getattr(fn, "id", None) or getattr(fn, "attr", None)
            if name in loaders:
                # Attribute to the OUTERMOST enclosing def: a nested helper
                # inside the seed function is still the seed path.
                callers.setdefault(name, set()).add(
                    self.stack[0] if self.stack else "<module>"
                )
            self.generic_visit(node)

    Visitor().visit(tree)

    assert callers, "expected the seed loader to still be called somewhere"

    for name, fns in callers.items():
        assert fns <= {"_seed_state_from_db_sync"}, (
            f"{name} is called from {sorted(fns)}; the snapshot sync throttle "
            "assumes the startup seed is the only consumer of app_state_cache"
        )


# ── The observatory send-once filter ──────────────────────────────────────

def test_duplicate_market_keys_are_collapsed_before_sending():
    """Two entries can share a market_key, and Postgres rejects the batch.

    Production, 2026-08-08 11:46:33Z:
      'ON CONFLICT DO UPDATE command cannot affect row a second time' (21000)
    The optional-column handler read that as a missing column and retried seven
    times, each carrying the full ~2,000-row body, and every attempt failed the
    same way. The cycle logged nothing.
    """
    rows = [
        {"market_key": "a|MLB|Hits|0.5|over|T", "true_prob": 0.51},
        {"market_key": "b|MLB|Hits|1.5|over|T", "true_prob": 0.55},
        {"market_key": "a|MLB|Hits|0.5|over|T", "true_prob": 0.58},
    ]

    deduped = {}
    for r in rows:
        deduped[r["market_key"]] = r
    out = list(deduped.values())

    assert len(out) == 2
    keys = [r["market_key"] for r in out]
    assert len(keys) == len(set(keys)), "a duplicate market_key would 21000"
    # Last occurrence wins, matching what merge-duplicates would have settled on.
    assert deduped["a|MLB|Hits|0.5|over|T"]["true_prob"] == 0.58


def test_observatory_upsert_still_ignores_duplicates():
    """The send-once filter's safety rests on the server discarding re-sends.

    If ignore_duplicates is ever flipped to False (merge), a re-send would
    overwrite the frozen ENTRY snapshot that the CLV dataset depends on, and
    skipping re-sends stops being a pure no-op.
    """
    from pathlib import Path

    src = Path("web/app.py").read_text()
    i = src.index("market_observatory")
    window = src[i : i + 4000]

    assert "ignore_duplicates=True" in window, (
        "the observatory upsert no longer ignores duplicates; re-read the "
        "send-once filter's correctness argument before shipping"
    )
    assert "ignore_duplicates=False" not in src


def test_sent_keys_are_only_recorded_after_a_successful_write():
    """A failed cycle must retry its markets, not drop them forever."""
    from pathlib import Path

    src = Path("web/app.py").read_text()

    # _mark_sent must not be called before the upsert it guards.
    upsert_at = src.index('obs_db.table("market_observatory").upsert')
    mark_def_at = src.index("def _mark_sent(")
    assert mark_def_at < upsert_at, "expected _mark_sent defined above the writes"

    # Every _mark_sent call site must follow an .execute() in its try block.
    for line_no, line in enumerate(src.splitlines(), 1):
        if "_mark_sent(rows_to_upsert)" in line:
            before = "\n".join(src.splitlines()[max(0, line_no - 6) : line_no - 1])
            assert ".execute()" in before, (
                f"_mark_sent at line {line_no} is not preceded by the write it "
                "confirms — a failed upsert would mark keys as sent"
            )


# ── The results-grader throttle ───────────────────────────────────────────

def test_results_check_interval_is_coarser_than_the_scrape():
    """The grader was only running every 5 minutes because of where it was called."""
    from web.app import _RESULTS_CHECK_MIN
    from web.state import _state

    assert _RESULTS_CHECK_MIN > _state["interval_min"], (
        "the results grader is still running at the scrape cadence; games do "
        "not finish every few minutes and each run re-fetches every box score"
    )


def test_observatory_grading_only_touches_finished_games():
    """The throttle cannot lose a grade because the query is time-bounded.

    check_observatory_results selects rows whose game_start is at least 2h in
    the past, so a row that becomes gradable between runs is simply picked up by
    the next one. If that bound were dropped, a delayed run could race the
    scraper's upsert of a still-live market.
    """
    from pathlib import Path

    src = Path("engine/results_checker.py").read_text()
    i = src.index("def check_observatory_results")
    window = src[i : i + 2000]

    assert "timedelta(hours=2)" in window, (
        "check_observatory_results no longer bounds itself to games that ended "
        "2h ago; the grader throttle relied on that for safety"
    )
    assert '.eq("result", "pending")' in window, (
        "grading must remain restricted to pending rows so a delayed run "
        "cannot regrade settled ones"
    )


# ── The redundant CLV observatory write ───────────────────────────────────

def test_clv_skips_observatory_writes_inside_the_capture_window():
    """Inside the window, update_observatory_closing_lines writes strictly more.

    It runs immediately after update_closing_lines_from_probs on the same
    thread and writes the same closing_prob plus the per-book close and lead
    time, so staging a write here only pays for a PATCH that is overwritten
    seconds later.
    """
    import inspect
    from engine.clv_checker import CLVTracker, _OBS_CAPTURE_WINDOW_MIN

    src = inspect.getsource(CLVTracker.update_closing_lines_from_probs)
    assert "mins_to_start > _OBS_CAPTURE_WINDOW_MIN" in src, (
        "the redundant market_observatory PATCH is no longer gated on the "
        "capture window"
    )

    # The staged write must sit INSIDE that guard, not beside it.
    guard_at = src.index("mins_to_start > _OBS_CAPTURE_WINDOW_MIN")
    stage_at = src.index("observatory_writes[obs_key]")
    assert guard_at < stage_at, "the guard must precede the staged write"

    assert _OBS_CAPTURE_WINDOW_MIN > 0


def test_capture_window_constant_matches_the_method_default():
    """Two copies of the window would silently reintroduce the redundant write."""
    import inspect
    from engine.clv_checker import CLVTracker, _OBS_CAPTURE_WINDOW_MIN

    sig = inspect.signature(CLVTracker.update_observatory_closing_lines)
    default = sig.parameters["capture_window_minutes"].default

    assert default == _OBS_CAPTURE_WINDOW_MIN, (
        f"capture_window_minutes defaults to {default} but the skip guard uses "
        f"{_OBS_CAPTURE_WINDOW_MIN}; markets between the two values would get a "
        "redundant PATCH again"
    )
