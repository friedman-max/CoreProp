"""The PrizePicks payout tables are duplicated in five places; they must agree.

`engine/constants.py` (`POWER_PAYOUTS`, `FLEX_PAYOUTS`, `BREAK_EVEN`) is the
source of truth for every server-side EV number. The browser recomputes EV and
break-even client-side so a slip's numbers update as the user edits it, so the
same tables are hand-copied into `web/static/ev-page.jsx`
(`EV_POWER_PAYOUTS` / `EV_FLEX_PAYOUTS`) and `web/static/page-backtest.jsx`
(`BT_POWER_PAYOUTS` / `BT_FLEX_PAYOUTS`) — both files carry a "KEEP IN SYNC
with engine/constants.py" comment, which is exactly the kind of contract that
holds until it doesn't.

Why this is load-bearing rather than tidiness:

  * PrizePicks has already moved a number under us — the 6-Power top payout
    dropped from 40x to 37.5x, which lifted the 6-Power per-leg break-even from
    0.5407 to 0.5466. A one-sided edit produces no error, no log line, and no
    failing test: the backend gates on one table while the UI shows EV and
    break-even computed from the other. Every displayed EV% is silently wrong,
    and the wrongness looks plausible.
  * `web/static/dist/*.js` is what the browser actually runs. Render's build env
    is pip-only, so `dist/` is committed by hand via `./build.sh` (see CLAUDE.md,
    "The `.jsx` -> `dist/` build contract"). Editing a `.jsx` without rebuilding
    ships a no-op to production, so the compiled bundles are checked too.

There is no 2-leg Flex: it degenerates to a 2-leg Power (the same "both must
hit" payoff), so it has no `FLEX_PAYOUTS` row and no `BREAK_EVEN` entry, and
both frontends must fall through to the Power-2 branch for n == 2. A stray
2-leg Flex row would invent a product PrizePicks does not sell.

The tables are parsed out of the JS as text rather than executed: the `.jsx`
files are React components that touch `React`/`window`, so there is nothing to
import from Python. Static parsing also keeps the check dependency-free (no
node needed), which is why the frontend half of this contract was previously
unguarded — the old `tests/frontend/test_sandbox_live.mjs` needed node, was
never collected by pytest (`python_files = test_*.py`), and read a
`page-sandbox.jsx` that no longer exists.
"""
from __future__ import annotations

import re
from math import comb
from pathlib import Path

from engine.constants import BREAK_EVEN, FLEX_PAYOUTS, POWER_PAYOUTS

_ROOT = Path(__file__).resolve().parents[2]
_STATIC = _ROOT / "web" / "static"

# (path, power-table name, flex-table name) for every mirror of the tables.
_MIRRORS = (
    (_STATIC / "ev-page.jsx", "EV_POWER_PAYOUTS", "EV_FLEX_PAYOUTS"),
    (_STATIC / "page-backtest.jsx", "BT_POWER_PAYOUTS", "BT_FLEX_PAYOUTS"),
    (_STATIC / "dist" / "ev-page.js", "EV_POWER_PAYOUTS", "EV_FLEX_PAYOUTS"),
    (_STATIC / "dist" / "page-backtest.js", "BT_POWER_PAYOUTS", "BT_FLEX_PAYOUTS"),
)

# `2: 3.0`, `6:37.5`, and the minifier's `5:.4` all have to parse.
_ENTRY_RE = re.compile(r"(\d+)\s*:\s*(\d*\.?\d+)")


def _object_literal(src: str, name: str, path: Path) -> str:
    """Return the `{...}` text assigned to `name`, brace-balanced.

    Works on both the readable source and the esbuild-minified bundle (which
    keeps top-level names because the output is a plain global script, not a
    module).
    """
    m = re.search(rf"\b{re.escape(name)}\s*=\s*\{{", src)
    assert m, f"{path.name} no longer defines {name}"
    start = m.end() - 1
    depth = 0
    for i in range(start, len(src)):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                return src[start : i + 1]
    raise AssertionError(f"unbalanced braces in {name} in {path.name}")


def _parse_power(src: str, name: str, path: Path) -> dict[int, float]:
    """`{2: 3.0, 3: 6.0, ...}` -> `{2: 3.0, 3: 6.0, ...}`."""
    body = _object_literal(src, name, path)
    return {int(k): float(v) for k, v in _ENTRY_RE.findall(body)}


def _parse_flex(src: str, name: str, path: Path) -> dict[int, dict[int, float]]:
    """`{3: {2: 1.0, 3: 3.0}, ...}` -> nested dict, one level deep."""
    body = _object_literal(src, name, path)
    inner = re.compile(r"(\d+)\s*:\s*\{([^{}]*)\}")
    out: dict[int, dict[int, float]] = {}
    for n_legs, tiers in inner.findall(body):
        out[int(n_legs)] = {int(k): float(v) for k, v in _ENTRY_RE.findall(tiers)}
    return out


def _mirror_tables():
    """Yield `(label, power_table, flex_table)` for each frontend mirror."""
    for path, power_name, flex_name in _MIRRORS:
        assert path.exists(), f"missing frontend mirror: {path}"
        src = path.read_text(encoding="utf-8")
        label = f"{path.parent.name}/{path.name}"
        yield label, _parse_power(src, power_name, path), _parse_flex(src, flex_name, path)


# ── Parser guards ─────────────────────────────────────────────────────────────
# Without these, a regex that stops matching would turn every assertion below
# into an empty-vs-empty comparison that passes while the contract rots.


def test_the_parser_actually_finds_entries_in_every_mirror():
    for label, power, flex in _mirror_tables():
        assert len(power) == 5, f"{label}: parsed {len(power)} power entries, expected 5"
        assert len(flex) == 4, f"{label}: parsed {len(flex)} flex rows, expected 4"
        assert all(tiers for tiers in flex.values()), f"{label}: an empty flex row parsed"


def test_constants_tables_are_populated():
    """Guard the Python side too — an emptied table must not pass by symmetry."""
    assert len(POWER_PAYOUTS) == 5, POWER_PAYOUTS
    assert len(FLEX_PAYOUTS) == 4, FLEX_PAYOUTS
    assert len(BREAK_EVEN) == 9, BREAK_EVEN  # power 2-6 + flex 3-6


# ── The mirror ────────────────────────────────────────────────────────────────


def test_power_payouts_match_engine_constants():
    for label, power, _flex in _mirror_tables():
        assert power == POWER_PAYOUTS, (
            f"{label} power payouts drifted from engine/constants.py POWER_PAYOUTS: "
            f"{power} != {POWER_PAYOUTS}. Every EV% and break-even shown to users "
            "is computed from this table client-side."
        )


def test_flex_payouts_match_engine_constants():
    for label, _power, flex in _mirror_tables():
        assert flex == FLEX_PAYOUTS, (
            f"{label} flex payouts drifted from engine/constants.py FLEX_PAYOUTS: "
            f"{flex} != {FLEX_PAYOUTS}"
        )


def test_six_power_still_pays_37_5x_everywhere():
    """The regression that motivated the mirror: PrizePicks cut 6-Power from 40x
    to 37.5x. Pinned explicitly so a revert to 40x fails loudly instead of
    quietly re-inflating every 6-leg EV."""
    assert POWER_PAYOUTS[6] == 37.5, POWER_PAYOUTS
    for label, power, _flex in _mirror_tables():
        assert power[6] == 37.5, f"{label}: 6-Power pays {power[6]}x, expected 37.5x"


# ── No 2-leg Flex ─────────────────────────────────────────────────────────────


def test_there_is_no_two_leg_flex():
    assert 2 not in FLEX_PAYOUTS, "FLEX_PAYOUTS gained a 2-leg row"
    assert ("2", "flex") not in BREAK_EVEN, "BREAK_EVEN gained a ('2', 'flex') entry"
    assert ("2", "power") in BREAK_EVEN, "BREAK_EVEN lost its 2-leg Power entry"
    for label, power, flex in _mirror_tables():
        assert 2 not in flex, (
            f"{label} defines a 2-leg flex payout row; a 2-leg flex degenerates "
            "to a 2-leg power and is not a product PrizePicks sells"
        )
        assert 2 in power, f"{label} lost its 2-leg power payout"


def test_flex_rows_cover_exactly_three_through_six_legs():
    assert sorted(FLEX_PAYOUTS) == [3, 4, 5, 6], FLEX_PAYOUTS
    for label, _power, flex in _mirror_tables():
        assert sorted(flex) == [3, 4, 5, 6], f"{label} flex leg counts: {sorted(flex)}"


def test_both_frontends_route_two_leg_flex_to_the_power_table():
    """`n == 2` must short-circuit to the Power-2 payout before the Flex table
    lookup, in both the EV and the break-even helper. Otherwise a 2-leg Flex
    slip reads a missing row and renders null instead of the Power-2 number."""
    for path, power_name, _flex_name in _MIRRORS:
        src = path.read_text(encoding="utf-8")
        label = f"{path.parent.name}/{path.name}"
        # `if (n === 2) return (dist[2] * EV_POWER_PAYOUTS[2] - 1) * 100;` and
        # `if (slipType === "power" || n === 2)`, minified to `n===2`.
        hits = len(re.findall(rf"===\s*2\b[^;]{{0,120}}{re.escape(power_name)}\s*\[\s*2\s*\]", src))
        hits += len(re.findall(rf"{re.escape(power_name)}\s*\[\s*2\s*\][^;]{{0,120}}===\s*2\b", src))
        hits += len(re.findall(r"\|\|\s*\w+\s*===\s*2\b|\w+\s*===\s*2\s*\|\|", src))
        assert hits, f"{label} has no n === 2 fall-through to {power_name}[2]"


# ── BREAK_EVEN must be derivable from the payout tables ───────────────────────
# This is the half the payout-mirror alone cannot catch: someone updates a
# payout on both sides but leaves BREAK_EVEN at the old number, so the display
# agrees with itself while the gate the pipeline uses is stale. The frontend
# derives break-even from the payout tables at runtime (closed form for Power,
# bisection for Flex); constants.py hard-codes the results to 4dp, so the two
# only agree as long as the hard-coded values are the real roots.

_BE_TOLERANCE = 5e-5  # constants.py rounds to 4 decimal places


def _flex_expected_payout(p: float, n_legs: int, tiers: dict[int, float]) -> float:
    """E[payout] for `n_legs` independent legs each at probability `p`."""
    return sum(
        comb(n_legs, k) * p**k * (1 - p) ** (n_legs - k) * tiers.get(k, 0.0)
        for k in range(n_legs + 1)
    )


def _flex_break_even(n_legs: int, tiers: dict[int, float]) -> float:
    """Bisect for the p where E[payout] == 1 — mirrors slipBreakEvenPct()."""
    lo, hi = 0.0, 1.0
    for _ in range(200):
        mid = (lo + hi) / 2
        if _flex_expected_payout(mid, n_legs, tiers) < 1.0:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def test_power_break_evens_are_derived_from_the_power_payouts():
    for n_legs, payout in POWER_PAYOUTS.items():
        expected = (1.0 / payout) ** (1.0 / n_legs)  # p^n * payout == 1
        actual = BREAK_EVEN[(str(n_legs), "power")]
        assert abs(actual - expected) <= _BE_TOLERANCE, (
            f"BREAK_EVEN[('{n_legs}', 'power')] is {actual} but a {payout}x payout "
            f"breaks even at {expected:.6f}. Update the break-even with the payout."
        )


def test_flex_break_evens_are_derived_from_the_flex_payouts():
    for n_legs, tiers in FLEX_PAYOUTS.items():
        expected = _flex_break_even(n_legs, tiers)
        actual = BREAK_EVEN[(str(n_legs), "flex")]
        assert abs(actual - expected) <= _BE_TOLERANCE, (
            f"BREAK_EVEN[('{n_legs}', 'flex')] is {actual} but the payout schedule "
            f"{tiers} breaks even at {expected:.6f}"
        )
