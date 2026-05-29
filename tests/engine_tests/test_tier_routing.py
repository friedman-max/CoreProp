"""Unit tests for engine/tier.py.

Tests cover every (slip_type, n_legs, tier) combination plus the
calibration_halted short-circuit and the effective_min_prob composition
with user overrides + slip break-even.
"""
from engine.tier import (
    TIER_A_MIN_PROB,
    TIER_B_MIN_PROB,
    TIER_C_MIN_PROB,
    effective_min_prob,
    filter_legs_for_slip,
    tier_eligible_for_slip,
    tier_for_leg,
    tier_for_prob,
    tier_summary,
)


# ─────────────────────────────────────────────────────────────────────────
# tier_for_prob — probability bands + halt short-circuit
# ─────────────────────────────────────────────────────────────────────────

def test_tier_a_at_threshold():
    assert tier_for_prob(0.65) == "A"
    assert tier_for_prob(0.70) == "A"
    assert tier_for_prob(0.999) == "A"


def test_tier_b_band():
    assert tier_for_prob(0.60) == "B"
    assert tier_for_prob(0.6499) == "B"


def test_tier_c_band():
    assert tier_for_prob(0.55) == "C"
    assert tier_for_prob(0.5999) == "C"


def test_reject_below_floor():
    assert tier_for_prob(0.54) == "REJECT"
    assert tier_for_prob(0.0) == "REJECT"


def test_halt_always_rejects():
    # A halted cell is REJECT regardless of probability — the calibrator
    # is on hold for that cell, so a high prob is exactly the wrong signal.
    assert tier_for_prob(0.95, calibration_halted=True) == "REJECT"
    assert tier_for_prob(0.65, calibration_halted=True) == "REJECT"
    assert tier_for_prob(0.55, calibration_halted=True) == "REJECT"


def test_none_or_invalid_prob_rejects():
    assert tier_for_prob(None) == "REJECT"
    assert tier_for_prob("not a number") == "REJECT"  # type: ignore[arg-type]


# ─────────────────────────────────────────────────────────────────────────
# tier_for_leg dict shape
# ─────────────────────────────────────────────────────────────────────────

def test_tier_for_leg_dict():
    assert tier_for_leg({"true_prob": 0.66}) == "A"
    assert tier_for_leg({"true_prob": 0.62}) == "B"
    assert tier_for_leg({"true_prob": 0.56}) == "C"
    assert tier_for_leg({"true_prob": 0.50}) == "REJECT"


def test_tier_for_leg_halted():
    assert tier_for_leg({"true_prob": 0.70, "calibration_halted": True}) == "REJECT"


# ─────────────────────────────────────────────────────────────────────────
# tier_eligible_for_slip — the routing table
# ─────────────────────────────────────────────────────────────────────────

def test_tier_a_eligible_everywhere():
    for n in (2, 3, 4, 5, 6):
        assert tier_eligible_for_slip("A", "power", n) is True
    for n in (3, 4, 5, 6):
        assert tier_eligible_for_slip("A", "flex", n) is True


def test_tier_b_only_long_slips():
    # Power-2/3/4 forbidden for Tier B (3-Power BE=0.55 > Tier B floor 0.60? wrong direction:
    # 3-Power BE=0.5503 LESS than 0.60, so technically a 0.60 leg covers it on its own;
    # BUT the slip avg matters: with one Tier-B leg at 0.60 and others at the floor,
    # the slip avg can dip below 3-Power BE. We forbid Tier B from 3/4-pick to keep
    # the average safely above BE without per-slip avg math).
    assert tier_eligible_for_slip("B", "power", 2) is False
    assert tier_eligible_for_slip("B", "power", 3) is False
    assert tier_eligible_for_slip("B", "power", 4) is False
    assert tier_eligible_for_slip("B", "power", 5) is True
    assert tier_eligible_for_slip("B", "power", 6) is True
    # Flex: forbid 3-Flex (BE=0.577 > Tier B floor), allow 4/5/6
    assert tier_eligible_for_slip("B", "flex", 3) is False
    assert tier_eligible_for_slip("B", "flex", 4) is True
    assert tier_eligible_for_slip("B", "flex", 5) is True
    assert tier_eligible_for_slip("B", "flex", 6) is True


def test_tier_c_never_auto_logged():
    for st in ("power", "flex"):
        for n in (2, 3, 4, 5, 6):
            assert tier_eligible_for_slip("C", st, n) is False


def test_reject_never_eligible():
    for st in ("power", "flex"):
        for n in (2, 3, 4, 5, 6):
            assert tier_eligible_for_slip("REJECT", st, n) is False


# ─────────────────────────────────────────────────────────────────────────
# filter_legs_for_slip
# ─────────────────────────────────────────────────────────────────────────

def test_filter_drops_halted():
    legs = [
        {"true_prob": 0.70, "calibration_halted": False},
        {"true_prob": 0.70, "calibration_halted": True},   # drop
        {"true_prob": 0.65, "calibration_halted": False},
    ]
    out = filter_legs_for_slip(legs, "power", 6)
    assert len(out) == 2


def test_filter_drops_tier_c_for_short_slip():
    legs = [
        {"true_prob": 0.70},  # A — keep
        {"true_prob": 0.60},  # B — drop on 3-Power
        {"true_prob": 0.55},  # C — drop
    ]
    out = filter_legs_for_slip(legs, "power", 3)
    assert len(out) == 1
    assert out[0]["true_prob"] == 0.70


def test_filter_keeps_tier_b_for_long_slip():
    legs = [
        {"true_prob": 0.70},  # A
        {"true_prob": 0.60},  # B
        {"true_prob": 0.55},  # C — drop
    ]
    out = filter_legs_for_slip(legs, "power", 6)
    assert len(out) == 2
    assert {l["true_prob"] for l in out} == {0.70, 0.60}


def test_filter_preserves_input_order():
    legs = [
        {"true_prob": 0.70, "bet_id": "a"},
        {"true_prob": 0.66, "bet_id": "b"},
        {"true_prob": 0.65, "bet_id": "c"},
    ]
    out = filter_legs_for_slip(legs, "power", 6)
    assert [l["bet_id"] for l in out] == ["a", "b", "c"]


# ─────────────────────────────────────────────────────────────────────────
# tier_summary
# ─────────────────────────────────────────────────────────────────────────

def test_tier_summary_counts():
    legs = [
        {"true_prob": 0.66},                      # A
        {"true_prob": 0.61},                      # B
        {"true_prob": 0.61, "calibration_halted": True},  # REJECT (halt)
        {"true_prob": 0.56},                      # C
        {"true_prob": 0.40},                      # REJECT
    ]
    s = tier_summary(legs)
    assert s == {"A": 1, "B": 1, "C": 1, "REJECT": 2}


# ─────────────────────────────────────────────────────────────────────────
# effective_min_prob
# ─────────────────────────────────────────────────────────────────────────

def test_effective_min_prob_user_override_respected():
    # User explicitly set 0.70 → that's the floor (above all defaults)
    assert effective_min_prob(0.70, "power", 6) == 0.70


def test_effective_min_prob_user_below_break_even_is_floored():
    # User sets 0.40, slip 3-Power BE = 0.5503 → we use the BE.
    assert effective_min_prob(0.40, "power", 3) >= 0.55


def test_effective_min_prob_routes_to_tier_b_for_long_slip():
    # No user override + slip is 6-Power → Tier B floor (0.60).
    assert effective_min_prob(None, "power", 6) == TIER_B_MIN_PROB


def test_effective_min_prob_routes_to_tier_a_for_short_slip():
    # No user override + slip is 3-Power → Tier A floor (0.65)
    # because 3-Power is not Tier-B-eligible.
    assert effective_min_prob(None, "power", 3) == TIER_A_MIN_PROB


def test_effective_min_prob_3_flex_routes_to_tier_a():
    # 3-Flex BE = 0.5774 → forbidden for Tier B → routes to Tier A.
    assert effective_min_prob(None, "flex", 3) == TIER_A_MIN_PROB
