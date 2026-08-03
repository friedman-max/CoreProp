"""The marketing pages must not state numbers they can't source.

The landing page shipped for months claiming "2,847 bets scanned today",
"4,200+ sharps building edge with CoreProp", a "58.4% backtested hit rate"
over "1,284 bets", "+11.7% ROI", "14,823 +EV bets surfaced this month" and
"+1,287 more edges available now" — alongside sixteen fabricated
"Yesterday's Winners" carrying invented box-score results, stamped with a
`new Date()` so they always read as *yesterday's* real games, and three
invented customer testimonials attributed to named people.

None of it came from anywhere. Two claims were falsifiable and false:

  * "Refreshed every 30 seconds" / "Lines update every 30 seconds" — the
    scheduler runs on `_state["interval_min"]`, 5 minutes by default and
    user-configurable (`POST /api/config`). Nothing in the codebase has ever
    polled at 30s.
  * NFL, advertised in the league list on both the landing and pricing pages.
    `config.ACTIVE_LEAGUES` is NBA/WNBA/MLB/NHL/NCAAB and has never had an
    NFL flag, so no NFL prop has ever been scraped.

Plus one number that was wrong in the same way this repo has already been
burned once: the slip visual printed "BE 54.07%", the pre-37.5x 6-Power
break-even. `tests/engine_tests/test_payout_table_mirror.py` bans that literal
in the app tabs precisely because a hit rate between 54.07% and the true 54.66%
renders as profitable while being EV-negative — but that test's file list
didn't cover landing.jsx.

This test is the regression guard. It is deliberately a blunt substring ban
rather than a rendering check: the failure mode isn't subtle logic, it's
someone pasting an impressive-looking number into JSX because the page felt
empty. `/api/public/coverage` (web/routers/public.py) exists so there is an
honest way to fill that space.

Both the `.jsx` sources and the committed `dist/*.js` bundles are checked --
`dist/` is what the browser actually runs (CLAUDE.md, "The `.jsx` -> `dist/`
build contract"), so a source-only check would pass while production still
served the old claims.
"""
from __future__ import annotations

import re
from pathlib import Path

import config as cfg

_ROOT = Path(__file__).resolve().parents[2]
_STATIC = _ROOT / "web" / "static"

# The marketing surfaces: what a signed-out visitor can read before paying.
_MARKETING = (
    _STATIC / "landing.jsx",
    _STATIC / "pricing.jsx",
    _STATIC / "dist" / "landing.js",
    _STATIC / "dist" / "pricing.js",
)

# Every invented figure, verbatim as it shipped. Keyed by claim -> why it's banned.
_BANNED_CLAIMS = {
    "2,847":     "invented 'bets scanned today' counter",
    "4,200":     "invented user count ('4,200+ sharps')",
    "58.4":      "invented backtested hit rate",
    "1,284":     "invented backtest sample size",
    "11.7":      "invented ROI",
    "14,823":    "invented monthly +EV bet count",
    "1,287":     "invented 'more edges available now' paywall teaser",
    "54.07":     "stale pre-37.5x 6-Power break-even (real value: 54.66%)",
    "0.5407":    "stale pre-37.5x 6-Power break-even as a probability",
    "30 seconds": "false refresh cadence (scheduler runs on interval_min, default 5 min)",
    "30 sec":    "false refresh cadence",
    "five seasons": "claims a play-by-play-trained model that does not exist",
}


def _strip_js_comments(src: str) -> str:
    """Drop `//` and `/* */` comment text.

    Same precedent as `test_payout_table_mirror._strip_js_comments`: naming a
    banned number in a comment that EXPLAINS why it's banned is legitimate, and
    landing.jsx's header comment lists every claim it removed so the next person
    doesn't reintroduce one. The ban applies to executable code and JSX text
    only. (The `dist/*.js` bundles are minified with comments already stripped,
    so this is a no-op there.)
    """
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
    src = re.sub(r"^\s*//.*$", "", src, flags=re.MULTILINE)
    return src


def _sources():
    for path in _MARKETING:
        assert path.exists(), (
            f"missing marketing surface {path}. If a page was renamed, update "
            "_MARKETING -- do not let the ban list silently cover nothing."
        )
        yield path, _strip_js_comments(path.read_text(encoding="utf-8"))


def test_the_file_list_is_not_empty():
    """Guard the premise: a typo'd path list would make every test below vacuous."""
    assert len(list(_sources())) == len(_MARKETING) == 4


def test_comment_stripping_does_not_eat_the_whole_file():
    """The stripper is the one thing that could make every ban vacuous. Each
    source must still carry real code after it runs."""
    for path, src in _sources():
        assert "React.createElement" in src or "className" in src, (
            f"{path.name}: nothing recognisable survived comment stripping -- "
            "the regex is too greedy and every ban below is passing on an "
            "empty string"
        )


def test_no_fabricated_statistics_on_the_marketing_pages():
    offenders = []
    for path, src in _sources():
        for claim, why in _BANNED_CLAIMS.items():
            if claim in src:
                offenders.append(f"{path.relative_to(_ROOT)}: {claim!r} -- {why}")
    assert not offenders, (
        "fabricated statistic(s) on a marketing page. Every number shown to a "
        "signed-out visitor must come from /api/public/coverage, be derived from "
        "engine/constants.py, or not be shown:\n  " + "\n  ".join(offenders)
    )


def test_marketing_pages_do_not_advertise_inactive_leagues():
    """A league named in sales copy must be one the pipeline actually scrapes."""
    active = {lg for lg, on in cfg.ACTIVE_LEAGUES.items() if on}
    inactive = {"NFL", "NCAAF", "MLS", "UFC", "PGA", "ATP", "EPL"} - active
    offenders = []
    for path, src in _sources():
        for league in sorted(inactive):
            if re.search(rf"\b{league}\b", src):
                offenders.append(f"{path.relative_to(_ROOT)}: advertises {league}")
    assert not offenders, (
        "marketing copy names a league config.ACTIVE_LEAGUES doesn't scrape, so "
        f"no such prop has ever appeared on the board. Active: {sorted(active)}\n  "
        + "\n  ".join(offenders)
    )


def test_no_invented_testimonials():
    """The three quotes attributed to 'Mike R.' / 'Devon S.' / 'Priya K.' were
    written by us. Attributing words to a named person who didn't say them is a
    different category of problem from an optimistic statistic."""
    offenders = []
    for path, src in _sources():
        for name in ("Mike R.", "Devon S.", "Priya K."):
            if name in src:
                offenders.append(f"{path.relative_to(_ROOT)}: {name}")
    assert not offenders, "invented testimonial(s):\n  " + "\n  ".join(offenders)


def test_landing_derives_the_six_power_break_even_from_the_payout_table():
    """The one number the landing page is allowed to state on its own is the
    per-leg break-even, and only because it's COMPUTED from the payout constant
    the same way ev-page.jsx computes it. Pin both halves: the payout literal
    must match engine/constants.py, and the page must do the arithmetic rather
    than hardcode the result."""
    from engine.constants import BREAK_EVEN, POWER_PAYOUTS

    src = (_STATIC / "landing.jsx").read_text(encoding="utf-8")
    m = re.search(r"LP_POWER_6_PAYOUT\s*=\s*([\d.]+)", src)
    assert m, "landing.jsx no longer defines LP_POWER_6_PAYOUT"
    assert float(m.group(1)) == POWER_PAYOUTS[6], (
        f"landing.jsx 6-Power payout {m.group(1)} != engine/constants.py "
        f"{POWER_PAYOUTS[6]}"
    )
    assert re.search(r"Math\.pow\(1\s*/\s*LP_POWER_6_PAYOUT", src), (
        "landing.jsx must DERIVE the break-even from the payout "
        "(Math.pow(1/payout, 1/6)), not hardcode it -- that's how 54.07 "
        "survived the 40x -> 37.5x change"
    )
    # And the value it will render must be the real one.
    assert round((1 / POWER_PAYOUTS[6]) ** (1 / 6), 4) == BREAK_EVEN[("6", "power")]
