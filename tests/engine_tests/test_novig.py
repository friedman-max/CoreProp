"""Novig parser: exchange order-book markets -> FanDuelProp.

Covers the conversion (ask probabilities -> American over/under odds), the
illiquidity guard (drop wide bid/ask spreads), stat-type mapping, and the
skips (futures/no-player, one-sided, out-of-range prices).
"""
from __future__ import annotations

from scrapers.novig import _market_to_prop, _to_float, NOVIG_PROP_MAP

MLB = NOVIG_PROP_MAP["MLB"]


def _market(mtype, strike, over_ask, under_ask, player="Test Player", team="DET"):
    outcomes = []
    if over_ask is not None:
        outcomes.append({"type": "Over", "available": over_ask})
    if under_ask is not None:
        outcomes.append({"type": "Under", "available": under_ask})
    return {
        "type": mtype,
        "strike": strike,
        "player": ({"full_name": player,
                    "player_competitors": [{"competitor": {"short_name": team}}]}
                   if player else None),
        "event": {"scheduled_start": "2026-06-21T20:10:00+00:00"},
        "outcomes": outcomes,
    }


def test_to_float_bounds():
    assert _to_float(0.375) == 0.375
    assert _to_float("0.685") == 0.685
    assert _to_float(0) is None
    assert _to_float(1) is None
    assert _to_float(1.2) is None
    assert _to_float("x") is None


def test_valid_market_maps_to_prop():
    p = _market_to_prop(_market("HITS", 0.5, 0.375, 0.685), "MLB", MLB)
    assert p is not None
    assert p.league == "MLB"
    assert p.prop_type == "Hits"          # HITS -> PrizePicks label
    assert p.line == 0.5
    assert p.both_sided is True
    assert p.player_name == "Test Player"
    assert p.start_time.startswith("2026-06-21")
    # over ask < 0.5 -> plus odds; under ask > 0.5 -> minus odds
    assert p.over_odds > 0
    assert p.under_odds < 0


def test_wide_spread_is_dropped():
    # over+under asks = 1.20 -> 20% spread, over the 12% guard.
    assert _market_to_prop(_market("HITS", 0.5, 0.60, 0.60), "MLB", MLB) is None


def test_crossed_book_is_dropped():
    # asks sum < 1 (negative spread) -> reject as malformed.
    assert _market_to_prop(_market("HITS", 0.5, 0.40, 0.50), "MLB", MLB) is None


def test_unmapped_stat_type_skipped():
    # Futures/awards aren't in the prop map -> skipped.
    assert _market_to_prop(_market("NL_CY_YOUNG_WINNER", 0, 0.10, 0.95), "MLB", MLB) is None


def test_no_player_skipped():
    assert _market_to_prop(_market("HITS", 0.5, 0.375, 0.685, player=None), "MLB", MLB) is None


def test_one_sided_skipped():
    assert _market_to_prop(_market("HITS", 0.5, 0.375, None), "MLB", MLB) is None


def test_out_of_range_price_skipped():
    # available must be a probability in (0, 1).
    assert _market_to_prop(_market("HITS", 0.5, 1.2, 0.4), "MLB", MLB) is None
