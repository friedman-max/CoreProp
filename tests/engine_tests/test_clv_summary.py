"""Holy Fix C5: CLV summary (moved/stale split, coverage, no hex gate)."""
import pytest

from engine.calibration import _summarize_clv, CLV_STALE_EPS


def test_empty():
    s = _summarize_clv([])
    assert s["n_clv_tracked"] == 0
    assert s["avg_clv_pct"] is None
    assert s["clv_plus_rate"] is None
    assert s["avg_clv_pct_moved"] is None


def test_moved_vs_stale_split():
    rows = [
        {"closing_prob": 0.60, "clv_pct": 0.05},    # moved +
        {"closing_prob": 0.58, "clv_pct": -0.03},   # moved -
        {"closing_prob": 0.55, "clv_pct": 0.0},     # stale (exact zero)
        {"closing_prob": 0.55, "clv_pct": 0.0},     # stale
    ]
    s = _summarize_clv(rows)
    assert s["n_clv_tracked"] == 4
    assert s["n_clv_moved"] == 2
    assert s["n_clv_stale"] == 2


def test_avg_all_includes_stale_zeros():
    """avg_clv_pct is over ALL tracked (incl stale 0s) — biased toward 0."""
    rows = [
        {"closing_prob": 0.60, "clv_pct": 0.10},
        {"closing_prob": 0.55, "clv_pct": 0.0},
        {"closing_prob": 0.55, "clv_pct": 0.0},
    ]
    s = _summarize_clv(rows)
    # (0.10 + 0 + 0) / 3
    assert abs(s["avg_clv_pct"] - 0.10/3) < 1e-9
    # moved-only average is the honest signal: just 0.10
    assert abs(s["avg_clv_pct_moved"] - 0.10) < 1e-9


def test_plus_rate_over_moved_only():
    """+CLV rate excludes stale zeros from the denominator."""
    rows = [
        {"closing_prob": 0.60, "clv_pct": 0.05},    # moved +
        {"closing_prob": 0.58, "clv_pct": -0.03},   # moved -
        {"closing_prob": 0.55, "clv_pct": 0.0},     # stale — excluded
    ]
    s = _summarize_clv(rows)
    # 1 of 2 moved legs is positive
    assert abs(s["clv_plus_rate"] - 0.5) < 1e-9


def test_all_stale_gives_none_moved_metrics():
    rows = [
        {"closing_prob": 0.55, "clv_pct": 0.0},
        {"closing_prob": 0.55, "clv_pct": 0.0},
    ]
    s = _summarize_clv(rows)
    assert s["n_clv_moved"] == 0
    assert s["n_clv_stale"] == 2
    assert s["avg_clv_pct"] == 0.0       # all zeros average to 0
    assert s["avg_clv_pct_moved"] is None
    assert s["clv_plus_rate"] is None


def test_stale_eps_boundary():
    """A clv just above eps is moved; just below is stale."""
    rows = [
        {"closing_prob": 0.5, "clv_pct": CLV_STALE_EPS * 2},   # moved
        {"closing_prob": 0.5, "clv_pct": CLV_STALE_EPS / 2},   # stale
    ]
    s = _summarize_clv(rows)
    assert s["n_clv_moved"] == 1
    assert s["n_clv_stale"] == 1


def test_no_hex_gate_in_load_clv_rows_logic():
    """Regression: _load_clv_rows must NOT filter by START_SLIP_ID hex sort.
    We can't hit the DB here, but we assert the broken sort-and-gate code is
    gone by checking the function source doesn't reference found_start."""
    import inspect
    from engine import calibration
    src = inspect.getsource(calibration._load_clv_rows)
    # The broken logic was a found_start flag + sort-by-slip_id gate.
    assert "found_start" not in src
    assert "sorted(" not in src
    # The gate compared each row's slip_id to START_SLIP_ID; that comparison
    # must be gone (a docstring mention of the constant name is fine).
    assert "== START_SLIP_ID" not in src
    assert 'r.get("slip_id") == START_SLIP_ID' not in src
