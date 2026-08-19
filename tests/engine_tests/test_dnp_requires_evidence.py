"""
A leg must never be graded DNP because the stats source was unreachable.

Regression for the 2026-08-05 incident: `site.api.espn.com` began returning 403
to every request. Every stat lookup returned None, and the grader's
`hours_since_end >= 6` fallback then marked entire slates DNP — 567 legs across
MLB and WNBA, each pushing at 1.0x, which silently erased the real P&L. Players
who plainly played (Cal Raleigh 1 H, Riley Greene 3H/2R/1RBI) were recorded as
having not appeared.

The distinction that matters: "the box score says this player didn't appear" is
evidence; "I could not fetch the box score" is not. Only the former may produce
a DNP.
"""
from datetime import datetime, timedelta, timezone

import pytest

from engine.results_checker import ESPNResultsChecker


@pytest.fixture
def rc():
    return ESPNResultsChecker()


def test_uses_a_reachable_espn_host(rc):
    """The blocked host must not come back via a copy-paste."""
    from engine.results_checker import ESPN_SCOREBOARD, ESPN_SUMMARY

    for url in list(ESPN_SCOREBOARD.values()) + list(ESPN_SUMMARY.values()):
        assert "site.api.espn.com" not in url, (
            f"{url} uses site.api.espn.com, which returns 403 to every request "
            "regardless of headers. Use site.web.api.espn.com."
        )


def test_failed_fetch_is_recorded(rc, monkeypatch):
    """A scoreboard error must be remembered, not swallowed into an empty dict."""
    class Boom:
        def get(self, *a, **k):
            raise RuntimeError("403 Forbidden")

    monkeypatch.setattr(rc, "_session", Boom())
    out = rc._fetch_all_stats("MLB", "20260804")

    assert out == {}
    assert ("MLB", "20260804") in rc._fetch_failed, (
        "a failed fetch returned {} indistinguishably from 'no games that day'"
    )


def test_summary_failure_is_recorded(rc, monkeypatch):
    """A box-score error must be remembered too, not just a scoreboard error.

    Regression for 2026-08-09: the scoreboard kept answering 200 while the
    summary endpoint failed, so `_fetch_failed` stayed empty, the guard passed,
    and 162 legs were written DNP overnight. The scoreboard tells you a game
    happened; the summary is the only thing that tells you who played.
    """
    class ScoreboardOkSummaryBoom:
        def __init__(self):
            self.calls = 0

        def get(self, url, *a, **k):
            self.calls += 1
            if self.calls == 1:                     # scoreboard
                class R:
                    @staticmethod
                    def raise_for_status():
                        return None

                    @staticmethod
                    def json():
                        return {"events": [{"id": "401585183"}]}
                return R()
            raise RuntimeError("403 Forbidden")     # summary

    monkeypatch.setattr(rc, "_session", ScoreboardOkSummaryBoom())
    out = rc._fetch_all_stats("MLB", "20260808")

    assert out == {}
    assert ("MLB", "20260808") in rc._fetch_failed, (
        "the scoreboard succeeded but every box score failed; the date was not "
        "flagged, so the DNP guard would pass and grade the slate DNP"
    )


def test_window_failure_is_detected_for_either_date(rc):
    """The guard covers both dates in the UTC-boundary lookup window."""
    gs = datetime(2026, 8, 5, 1, 40, tzinfo=timezone.utc)

    assert rc._window_fetch_failed("MLB", gs) is False

    # The prior ET date is the one that actually holds a 01:40 UTC game.
    rc._fetch_failed.add(("MLB", "20260804"))
    assert rc._window_fetch_failed("MLB", gs) is True

    rc._fetch_failed.clear()
    rc._fetch_failed.add(("MLB", "20260805"))
    assert rc._window_fetch_failed("MLB", gs) is True

    # A different league's outage must not taint this one.
    rc._fetch_failed.clear()
    rc._fetch_failed.add(("WNBA", "20260804"))
    assert rc._window_fetch_failed("MLB", gs) is False


def test_successful_fetch_clears_a_prior_failure(rc, monkeypatch):
    """A recovered date must not stay poisoned for the rest of the run."""
    rc._fetch_failed.add(("MLB", "20260804"))

    class Ok:
        def get(self, *a, **k):
            class R:
                @staticmethod
                def raise_for_status():
                    return None

                @staticmethod
                def json():
                    return {"events": []}
            return R()

    monkeypatch.setattr(rc, "_session", Ok())
    rc._fetch_all_stats("MLB", "20260804")

    assert ("MLB", "20260804") not in rc._fetch_failed


def test_dnp_branch_is_guarded_by_the_window_check():
    """The grading loop must consult the guard before writing a DNP.

    Asserted against the source: the DNP write is deep inside a long loop with
    live DB and network calls, so exercising it end-to-end here would be far
    more fragile than checking that the guard precedes it.
    """
    import inspect

    src = inspect.getsource(ESPNResultsChecker.check_pending_results)
    guard = src.index("_window_fetch_failed")
    dnp_write = src.index('"result":      "dnp"')

    assert guard < dnp_write, (
        "the DNP write is no longer preceded by the _window_fetch_failed guard; "
        "an unreachable stats source can mark whole slates as DNP again"
    )
