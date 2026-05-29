"""Phase 1A audit C2: side-keyed league posterior.

Tests that the calibrator now prefers the (league, side) league-tier bin
over the side-pooled bin, with backward compat when the side-keyed bin is
absent (legacy state).

Strategy: hand-craft a `curves` dict with both bin shapes coexisting and
assert calibrate() picks the right one.
"""
from engine.isotonic_calibration import calibrate


# Helper — make a flat curve at value `y` (PAV representative point at x=0.5).
def _flat(y: float, *, n_eff: float = 1000.0) -> dict:
    return {
        "curve": [(0.5, y)],
        "n_eff": n_eff,
        "n_obs": int(n_eff),
    }


def _curves(global_y, leagues, props=None):
    return {
        "global":  _flat(global_y),
        "leagues": {k: _flat(v) for k, v in leagues.items()},
        "props":   {k: _flat(v) for k, v in (props or {}).items()},
    }


def test_side_keyed_league_bin_preferred_when_present():
    # Both bins exist; side-keyed should win.
    curves = _curves(
        global_y=0.50,
        leagues={
            "NBA":         0.50,  # side-pooled (legacy, washes out asymmetry)
            "NBA|over":    0.45,  # side-keyed: model overstates OVER
            "NBA|under":   0.58,  # side-keyed: model understates UNDER
        },
    )
    out_over  = calibrate(curves, "NBA", None, "over",  0.50)
    out_under = calibrate(curves, "NBA", None, "under", 0.50)

    # Expect calibrator to pull OVER predictions DOWN and UNDER predictions UP,
    # not the pooled 0.50.
    assert out_over  < 0.50, f"OVER should pull toward side-keyed bin (0.45), got {out_over}"
    assert out_under > 0.50, f"UNDER should pull toward side-keyed bin (0.58), got {out_under}"


def test_falls_back_to_side_pooled_when_side_keyed_missing():
    # Only the side-pooled bin exists (legacy state).
    curves = _curves(
        global_y=0.50,
        leagues={"NBA": 0.55},  # side-pooled only
    )
    out_over  = calibrate(curves, "NBA", None, "over",  0.50)
    out_under = calibrate(curves, "NBA", None, "under", 0.50)

    # Both sides shrink toward the same pooled value (no directional signal).
    assert abs(out_over - out_under) < 1e-6
    assert out_over > 0.50  # shrunk toward 0.55


def test_unknown_side_falls_back_to_pooled():
    # Legacy data: side="both" rows existed; calibrate() should still work.
    curves = _curves(
        global_y=0.50,
        leagues={"NBA": 0.55, "NBA|over": 0.40},
    )
    out_both = calibrate(curves, "NBA", None, "both", 0.50)
    # Should fall back to "NBA" pooled because "NBA|both" doesn't exist.
    assert out_both > 0.50 and out_both < 0.55 + 1e-3


def test_other_leagues_not_affected():
    # Side-keyed NBA bin must not bleed into MLB results.
    curves = _curves(
        global_y=0.50,
        leagues={"NBA|over": 0.40, "MLB": 0.55},
    )
    out_mlb_over = calibrate(curves, "MLB", None, "over", 0.50)
    # MLB side-keyed bin absent → falls back to "MLB" pooled.
    assert out_mlb_over > 0.50


def test_unknown_league_falls_back_to_global():
    curves = _curves(
        global_y=0.55,
        leagues={"NBA|over": 0.40},
    )
    out_unknown = calibrate(curves, "WNBA", None, "over", 0.50)
    # WNBA neither side-keyed nor pooled → returns global.
    assert abs(out_unknown - 0.55) < 1e-6


def test_side_keyed_with_prop_child_chains_correctly():
    # End-to-end: global → league|side → prop|side
    curves = _curves(
        global_y=0.50,
        leagues={"NBA|over": 0.55},
        props={"NBA|Points|over": 0.65},
    )
    # Prop bucket should pull the result toward 0.65, but shrunk by κ.
    out = calibrate(curves, "NBA", "Points", "over", 0.50)
    assert 0.55 < out < 0.65


def test_empty_curves_passthrough():
    out = calibrate({}, "NBA", "Points", "over", 0.55)
    assert out == 0.55
