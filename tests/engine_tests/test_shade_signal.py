"""Maybe Cool Fix C1: PP shade signal + anti-public filter."""
import json
import os
import tempfile
from unittest.mock import patch

import pytest

from engine import shade_signal as ss
from engine.constants import OPTIMAL_BREAK_EVEN


# ─────────────────────────────────────────────────────────────────────────
# pp_shade
# ─────────────────────────────────────────────────────────────────────────

def test_pp_shade_zero_at_break_even():
    assert abs(ss.pp_shade(OPTIMAL_BREAK_EVEN)) < 1e-9


def test_pp_shade_positive_above_break_even():
    assert ss.pp_shade(0.65) > 0.10
    assert ss.pp_shade(0.70) > 0.15


def test_pp_shade_negative_below_break_even():
    assert ss.pp_shade(0.50) < 0
    assert ss.pp_shade(0.40) < -0.10


def test_pp_shade_handles_none_and_invalid():
    assert ss.pp_shade(None) == 0.0
    assert ss.pp_shade("not a number") == 0.0
    assert ss.pp_shade(0.0) == 0.0
    assert ss.pp_shade(1.0) == 0.0
    assert ss.pp_shade(-0.5) == 0.0


# ─────────────────────────────────────────────────────────────────────────
# shade_bucket
# ─────────────────────────────────────────────────────────────────────────

def test_shade_bucket_boundaries():
    assert ss.shade_bucket(-0.20) == "very_negative"
    assert ss.shade_bucket(-0.04) == "negative"
    assert ss.shade_bucket( 0.01) == "neutral_low"
    assert ss.shade_bucket( 0.04) == "neutral_high"
    assert ss.shade_bucket( 0.08) == "positive"
    assert ss.shade_bucket( 0.15) == "very_positive"


def test_shade_bucket_handles_invalid():
    assert ss.shade_bucket("nope") == "neutral_low"


# ─────────────────────────────────────────────────────────────────────────
# is_anti_public + reload_anti_public
# ─────────────────────────────────────────────────────────────────────────

def _write_deny_list(tmp_path: str, denied: dict) -> str:
    p = os.path.join(tmp_path, "anti_public_cells.json")
    with open(p, "w") as f:
        json.dump({
            "updated_at": "2026-05-29T00:00:00Z",
            "min_obs": 50,
            "min_neg_clv_pp": -0.03,
            "denied": denied,
        }, f)
    return p


def test_is_anti_public_empty_when_no_deny_list(tmp_path):
    with patch.object(ss, "ANTI_PUBLIC_FILE", os.path.join(str(tmp_path), "missing.json")):
        ss.reload_anti_public()
        assert ss.is_anti_public({"league": "NBA", "side": "over", "true_prob": 0.70}) is False


def test_is_anti_public_flags_matching_cell(tmp_path):
    deny = {"NBA|over|very_positive": {"n": 210, "mean_clv": -0.149}}
    fn = _write_deny_list(str(tmp_path), deny)
    with patch.object(ss, "ANTI_PUBLIC_FILE", fn):
        ss.reload_anti_public()
        # 0.70 → shade ≈ 0.16 → very_positive bucket → flagged
        leg = {"league": "NBA", "side": "over", "true_prob": 0.70}
        assert ss.is_anti_public(leg) is True


def test_is_anti_public_does_not_flag_non_matching_cell(tmp_path):
    deny = {"NBA|over|very_positive": {"n": 210, "mean_clv": -0.149}}
    fn = _write_deny_list(str(tmp_path), deny)
    with patch.object(ss, "ANTI_PUBLIC_FILE", fn):
        ss.reload_anti_public()
        # NBA under not in deny-list
        leg_under = {"league": "NBA", "side": "under", "true_prob": 0.70}
        assert ss.is_anti_public(leg_under) is False
        # MLB over not in deny-list
        leg_mlb = {"league": "MLB", "side": "over", "true_prob": 0.70}
        assert ss.is_anti_public(leg_mlb) is False


def test_is_anti_public_respects_shade_threshold(tmp_path):
    deny = {"NBA|over|neutral_high": {"n": 500, "mean_clv": -0.04}}
    fn = _write_deny_list(str(tmp_path), deny)
    with patch.object(ss, "ANTI_PUBLIC_FILE", fn):
        ss.reload_anti_public()
        # shade ~ 0.04 (neutral_high) but threshold default 0.06 → not flagged
        leg = {"league": "NBA", "side": "over", "true_prob": 0.58}
        assert ss.is_anti_public(leg) is False
        # Lower threshold → flagged
        assert ss.is_anti_public(leg, shade_threshold=0.0) is True


def test_is_anti_public_handles_none_leg():
    assert ss.is_anti_public(None) is False
    assert ss.is_anti_public({}) is False


# ─────────────────────────────────────────────────────────────────────────
# annotate
# ─────────────────────────────────────────────────────────────────────────

def test_annotate_adds_shade_fields():
    leg = {"league": "NBA", "side": "over", "true_prob": 0.70, "bet_id": "abc"}
    out = ss.annotate(leg)
    assert "pp_shade" in out
    assert "shade_bucket" in out
    assert out["bet_id"] == "abc"
    assert out["pp_shade"] > 0.10
    assert out["shade_bucket"] == "very_positive"


def test_annotate_does_not_mutate_input():
    leg = {"true_prob": 0.65}
    _ = ss.annotate(leg)
    assert "pp_shade" not in leg  # original untouched
