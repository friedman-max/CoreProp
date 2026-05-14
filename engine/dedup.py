"""
Slip-leg deduplication: canonical key construction and invariant
enforcement.

A leg has two signatures:

    pair_key = (player_norm, sports_day)
        — a player in a single game (the "sports day" of the game-start
          timestamp, shifted back 12 h before extracting the UTC date so
          a late-ET tip-off doesn't straddle midnight). Two legs sharing
          this signature MUST NOT appear in two different slips.

    leg_key  = (player_norm, prop_norm, line_norm, side_norm, sports_day)
        — an exact leg. Stricter than pair_key. Two legs sharing this
          signature are literally the same bet.

Both signatures are computed by `engine.backtest.make_bet_key` /
`make_leg_key`. This module wraps them so:

  - Every call site uses the same construction, regardless of whether
    rows came from the live pipeline (`prop_type`/`pp_line`/`start_time`)
    or the observatory (`prop`/`line`/`game_start`).
  - A single function `check_duplicate_legs` walks an emitted slip list
    and returns the collisions, so it's trivial to assert the
    end-to-end invariant.
  - `drop_duplicate_slips` is the production filter — given a list of
    emitted slips, returns a copy with the second-and-later occurrence
    of any leg removed (slip dropped wholesale). Belt-and-suspenders:
    even if upstream selection silently misbehaves, the final output is
    guaranteed clean.

Why a post-emit pass and not just trust _select_ranked? The greedy
builder's invariant is established by mutating shared `used_pair` /
`used_leg` sets as it goes. That's brittle to refactors and has broken
silently three times already (see commits 6908c0a → dabc8de → 3e59a8b).
A *single* invariant check on the actual emitted output cannot be
silently weakened by an upstream change to slip construction.
"""
from __future__ import annotations

import logging
from typing import Iterable, Optional

from engine.backtest import make_bet_key, make_leg_key

logger = logging.getLogger(__name__)


# Field-name aliases. The same logical attribute appears under different
# keys depending on the source:
#   • engine.strategy_tester emits leg dicts with `player` / `prop` /
#     `line` / `side` / `game_start` (observatory column names).
#   • engine.backtest / web.app live-pipeline dicts use `player_name` /
#     `prop_type` / `pp_line` / `side` / `start_time`.
# All four key extractors check both — silent drift between conventions
# was a contributor to prior dedup failures (one call site used `prop`
# while another used `prop_type`, so the keys never matched).
_PLAYER_FIELDS    = ("player", "player_name")
_PROP_FIELDS      = ("prop",   "prop_type")
_LINE_FIELDS      = ("line",   "pp_line")
_SIDE_FIELDS      = ("side",)
_START_FIELDS     = ("game_start", "start_time")


def _first(d: dict, names: tuple[str, ...]) -> str:
    """Return the first non-empty value from `d` for any name in `names`,
    or "" if none are present. Treats None and "" identically."""
    for n in names:
        v = d.get(n)
        if v is None:
            continue
        # Numeric line values are valid — only filter out empty strings.
        if isinstance(v, str) and not v:
            continue
        return v
    return ""


def leg_pair_key(leg: dict) -> tuple:
    """Canonical (player, sports_day) signature for one leg dict.

    Accepts both the observatory shape (`player`, `game_start`) and the
    pipeline shape (`player_name`, `start_time`). See module docstring
    for the rationale."""
    player = _first(leg, _PLAYER_FIELDS)
    start  = _first(leg, _START_FIELDS)
    return make_bet_key(str(player or ""), str(start or ""))


def leg_exact_key(leg: dict) -> tuple:
    """Canonical (player, prop, line, side, sports_day) signature for one
    leg dict. Same field-alias handling as `leg_pair_key`."""
    player = _first(leg, _PLAYER_FIELDS)
    prop   = _first(leg, _PROP_FIELDS)
    line   = _first(leg, _LINE_FIELDS)
    side   = _first(leg, _SIDE_FIELDS)
    start  = _first(leg, _START_FIELDS)
    return make_leg_key(
        str(player or ""),
        str(prop   or ""),
        line,                            # _normalize_line handles type
        str(side   or ""),
        str(start  or ""),
    )


def iter_slip_leg_keys(slip: Iterable[dict]) -> list[tuple]:
    """Return the canonical leg_key list for every leg dict in a slip."""
    return [leg_exact_key(leg) for leg in slip]


def iter_slip_pair_keys(slip: Iterable[dict]) -> list[tuple]:
    """Return the canonical pair_key list for every leg dict in a slip."""
    return [leg_pair_key(leg) for leg in slip]


# ── Invariant checks ───────────────────────────────────────────────────

def check_duplicate_legs(slips: list[dict | list]) -> list[tuple[int, int, tuple]]:
    """Walk the emitted slip list and report any leg_key appearing in
    two different slips.

    `slips` may be a list of slip dicts (each with a `"legs"` field) or
    a list of leg-lists. Returns a list of `(slip_idx_a, slip_idx_b,
    leg_key)` tuples — empty when the invariant holds. Every collision
    is reported (not just the first) so callers can log all of them.

    Pair-key collisions are NOT reported here — leg_key implies
    pair_key, and the two-team rule deliberately allows the same
    player on multiple slips when they're in different games. The
    contract is "the same exact leg never repeats."
    """
    seen: dict[tuple, int] = {}
    collisions: list[tuple[int, int, tuple]] = []
    for idx, slip in enumerate(slips):
        legs = _slip_legs(slip)
        for key in iter_slip_leg_keys(legs):
            prior = seen.get(key)
            if prior is not None and prior != idx:
                collisions.append((prior, idx, key))
            else:
                # Only set once — keep the FIRST occurrence so the
                # reported `prior` is meaningful.
                seen.setdefault(key, idx)
    return collisions


def check_duplicate_pairs(slips: list[dict | list]) -> list[tuple[int, int, tuple]]:
    """Same shape as `check_duplicate_legs` but uses pair_key (player +
    sports_day). Stricter — a player in the same game can only appear
    in one slip — matches the user-facing contract "no prop in two
    slips" most directly, since two different bets on the same player
    in the same game are nearly perfectly correlated and so the user
    expects them treated as one."""
    seen: dict[tuple, int] = {}
    collisions: list[tuple[int, int, tuple]] = []
    for idx, slip in enumerate(slips):
        legs = _slip_legs(slip)
        for key in iter_slip_pair_keys(legs):
            prior = seen.get(key)
            if prior is not None and prior != idx:
                collisions.append((prior, idx, key))
            else:
                seen.setdefault(key, idx)
    return collisions


def assert_no_duplicate_legs(slips: list[dict | list]) -> None:
    """Test-friendly assertion: raise AssertionError on any leg_key
    duplicate across slips. Production code should use
    `drop_duplicate_slips` (which logs and filters)."""
    collisions = check_duplicate_legs(slips)
    if collisions:
        raise AssertionError(
            f"Found {len(collisions)} duplicate leg(s) across slips: "
            f"first few = {collisions[:5]}"
        )


def assert_no_duplicate_pairs(slips: list[dict | list]) -> None:
    """Stricter variant — raises if any (player, sports_day) pair is
    used in two slips. Use this for the user-facing contract."""
    collisions = check_duplicate_pairs(slips)
    if collisions:
        raise AssertionError(
            f"Found {len(collisions)} duplicate pair(s) across slips: "
            f"first few = {collisions[:5]}"
        )


# ── Production filter ───────────────────────────────────────────────────

def drop_duplicate_slips(
    slips: list[dict | list],
    *,
    strict: bool = True,
) -> tuple[list, int]:
    """Return `(kept_slips, n_dropped)`. A slip is dropped entirely (not
    edited in place) if any of its legs collide with a leg from an
    earlier-kept slip.

    `strict=True` (default) compares pair_keys — same player + same
    sports day blocks. `strict=False` compares exact leg_keys —
    different prop/line/side on the same player+game is allowed.
    Production paths use strict=True to enforce the user-stated
    contract.

    The function is order-preserving: the first slip wins. Callers that
    care about which slip wins should pre-sort by their preferred
    criterion (e.g. EV descending) before calling.
    """
    key_fn = leg_pair_key if strict else leg_exact_key
    seen: set[tuple] = set()
    kept: list = []
    dropped = 0
    for slip in slips:
        legs = _slip_legs(slip)
        keys = [key_fn(leg) for leg in legs]
        # Within-slip duplicates (two legs with the same key inside one
        # slip) collapse the slip too — that's also a contract violation
        # for the live slip builder, where it would mean betting the
        # same leg twice on a single slip. Such slips never made sense
        # to emit.
        if len(set(keys)) != len(keys):
            logger.warning(
                "Dedup: dropping slip with within-slip duplicate legs "
                "(keys=%s)", keys,
            )
            dropped += 1
            continue
        if any(k in seen for k in keys):
            colliding = [k for k in keys if k in seen]
            logger.warning(
                "Dedup: dropping slip whose leg(s) %s already appeared in "
                "an earlier slip", colliding,
            )
            dropped += 1
            continue
        for k in keys:
            seen.add(k)
        kept.append(slip)
    return kept, dropped


def _slip_legs(slip: dict | list) -> list[dict]:
    """Extract the leg list from either a slip dict (with `legs` key)
    or a raw leg-list. Strategy_tester emits the dict form into
    `sim_slips`; tests and internal callers may pass leg-lists."""
    if isinstance(slip, dict):
        return list(slip.get("legs") or [])
    return list(slip)


# ── Cross-slip used-set builder ────────────────────────────────────────

def build_used_sets(
    emitted: Iterable[dict | list],
) -> tuple[set[tuple], set[tuple]]:
    """Return (pair_keys, leg_keys) covering every leg in every emitted
    slip. Used by callers that want to seed `_select_ranked`'s shared
    dedup sets from previously-emitted output (e.g. a second pass over
    the same data with different filters)."""
    pair_keys: set[tuple] = set()
    leg_keys:  set[tuple] = set()
    for slip in emitted:
        for leg in _slip_legs(slip):
            pair_keys.add(leg_pair_key(leg))
            leg_keys.add(leg_exact_key(leg))
    return pair_keys, leg_keys


# ── Diagnostic helpers ─────────────────────────────────────────────────

def find_legs_with_key(
    slips: list[dict | list],
    key: tuple,
    *,
    strict: bool = True,
) -> list[tuple[int, dict]]:
    """For debugging duplicate-dedup reports: find every (slip_idx, leg)
    where leg matches the given key."""
    key_fn = leg_pair_key if strict else leg_exact_key
    out: list[tuple[int, dict]] = []
    for idx, slip in enumerate(slips):
        for leg in _slip_legs(slip):
            if key_fn(leg) == key:
                out.append((idx, leg))
    return out
