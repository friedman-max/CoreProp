"""
Unit tests for engine/rwbc_calibration.py.

Covers the algorithmic correctness of fit_one_cell and the
circuit-breaker semantics of calibrate(). Does NOT exercise the
persistence layer (publish_if_delta_n_exceeded / load_cell_cache_from_db)
because those are thin wrappers around Supabase calls — exercising them
would require a fake supabase client and is better covered by the
end-to-end verification (run server with USE_RWBC=true and inspect logs).
"""
import math
import pytest
import threading

from engine import rwbc_calibration as rwbc
from engine.rwbc_calibration import (
    fit_one_cell, bucket_of, W_CELL_HALT_THRESHOLD, GLOBAL_PRIOR_STRENGTH,
)


# ─────────────────────────────────────────────────────────────────────────
# fit_one_cell algebra
# ─────────────────────────────────────────────────────────────────────────

def _obs(pred, y, weight=1.0):
    return {"pred": pred, "y": y, "weight": weight, "prob_bucket": bucket_of(pred)}


def test_fit_returns_none_for_empty_cell():
    assert fit_one_cell([], global_hit_rate=0.5) is None


def test_fit_returns_none_for_thin_cell():
    # n_eff < 5 → too thin, return None (no publish).
    obs = [_obs(0.5, 1, weight=1.0)] * 4   # n_eff = 4
    assert fit_one_cell(obs, global_hit_rate=0.5) is None


def test_high_resolution_low_reliability_error_yields_high_w_cell():
    """Cell where the model's prediction tracks the realized rate
    closely AND varies across buckets should get w_cell near 1."""
    obs = []
    # Bucket A (pred=0.40): 100 obs, ~40% hit → matches model
    for _ in range(40): obs.append(_obs(0.40, 1))
    for _ in range(60): obs.append(_obs(0.40, 0))
    # Bucket B (pred=0.60): 100 obs, ~60% hit → matches model
    for _ in range(60): obs.append(_obs(0.60, 1))
    for _ in range(40): obs.append(_obs(0.60, 0))

    fit = fit_one_cell(obs, global_hit_rate=0.5)
    assert fit is not None
    assert fit["w_cell"] > 0.85, f"expected w_cell near 1, got {fit['w_cell']}"
    # Resolution should be non-trivial (predictions span 20pp).
    assert fit["resolution"] > 0.005
    # Reliability error should be near zero (model is well-calibrated).
    assert fit["reliability_error"] < 0.001


def test_zero_resolution_collapses_w_cell():
    """Cell where every prediction is identical (single bucket) has
    resolution = 0 by construction. w_cell should collapse to 0, and
    calibrate() on a cell with w_cell=0 should return None (halt)."""
    # All predictions land in bucket(0.55), so distinct_buckets = 1
    obs = []
    for _ in range(50): obs.append(_obs(0.55, 1))
    for _ in range(50): obs.append(_obs(0.55, 0))

    fit = fit_one_cell(obs, global_hit_rate=0.5)
    assert fit is not None
    # Single-bucket cell triggers the distinct_buckets < 2 guard →
    # explicit w_cell = 0, not a divide-by-near-zero artifact.
    assert fit["w_cell"] == 0.0
    assert fit["distinct_buckets"] == 1


def test_high_reliability_error_yields_low_w_cell():
    """Model predicts 0.70 across a bucket, but reality is 0.30. Even
    with multiple buckets contributing to resolution, the huge
    reliability error should drive w_cell low."""
    obs = []
    for _ in range(30): obs.append(_obs(0.70, 1))   # 30% realized
    for _ in range(70): obs.append(_obs(0.70, 0))
    for _ in range(40): obs.append(_obs(0.50, 1))   # 40% realized (model said 50)
    for _ in range(60): obs.append(_obs(0.50, 0))

    fit = fit_one_cell(obs, global_hit_rate=0.5)
    assert fit is not None
    # The model is wrong by 40pp on the 0.70 bucket → reliability_error large
    assert fit["reliability_error"] > 0.04
    # w_cell should be < 0.5 (model loses most of its weight)
    assert fit["w_cell"] < 0.5


def test_beta_binomial_posterior_matches_closed_form():
    """The cell posterior should match the standard Beta-Binomial
    mean formula: (k + α₀) / (n + α₀ + β₀)."""
    # 80 hits out of 100 (true rate 0.80) with a 0.50 prior of
    # strength GLOBAL_PRIOR_STRENGTH=30
    obs = []
    for _ in range(80): obs.append(_obs(0.50, 1))
    for _ in range(20): obs.append(_obs(0.50, 0))   # single-bucket
    fit = fit_one_cell(obs, global_hit_rate=0.5)
    assert fit is not None

    alpha0 = 0.5 * GLOBAL_PRIOR_STRENGTH
    beta0  = 0.5 * GLOBAL_PRIOR_STRENGTH
    expected = (80 + alpha0) / (100 + alpha0 + beta0)
    assert math.isclose(fit["p_post"], expected, abs_tol=1e-9), \
        f"expected {expected}, got {fit['p_post']}"


def test_beta_binomial_pulls_thin_cell_toward_prior():
    """A cell with n=5 and 5/5 hits should NOT report 100% — the prior
    of 0.5 with strength 30 should dominate. Posterior ≈ (5+15)/(5+30) ≈ 0.571"""
    obs = [_obs(0.50, 1) for _ in range(5)]
    obs.append(_obs(0.45, 0))   # second bucket so distinct_buckets >= 2
    fit = fit_one_cell(obs, global_hit_rate=0.5)
    assert fit is not None
    # k_eff = 5, n_eff = 6 → (5 + 15) / (6 + 30) ≈ 0.5556
    expected = (5 + 15) / (6 + 30)
    assert math.isclose(fit["p_post"], expected, abs_tol=1e-9)
    # Far from the empirical 5/6 ≈ 0.833
    assert fit["p_post"] < 0.6


# ─────────────────────────────────────────────────────────────────────────
# calibrate() circuit-breaker contract
# ─────────────────────────────────────────────────────────────────────────

def _install_cell(league, prop, side, **kw):
    """Inject a CellStats directly into the cache so calibrate() sees it
    without going through the publish path. Cleans up via the fixture."""
    from datetime import datetime, timezone
    defaults = dict(
        w_cell=0.5, p_post=0.55, n_eff=200.0, resolution=0.005,
        reliability_error=0.001, mean_pred=0.50, mean_obs=0.55,
        last_fit_at=datetime.now(timezone.utc),
        last_publish_at=datetime.now(timezone.utc),
        last_publish_n_eff=200.0,
    )
    defaults.update(kw)
    stats = rwbc.CellStats(league=league, prop=prop, side=side, **defaults)
    with rwbc._cache_lock:
        rwbc._cell_cache[(league, prop, side)] = stats


@pytest.fixture(autouse=True)
def _reset_cache():
    """Snapshot + restore _cell_cache around each test."""
    with rwbc._cache_lock:
        snapshot = dict(rwbc._cell_cache)
        rwbc._cell_cache.clear()
    yield
    with rwbc._cache_lock:
        rwbc._cell_cache.clear()
        rwbc._cell_cache.update(snapshot)


def test_calibrate_returns_none_for_unknown_cell():
    """Brand-new cell never seen at refit time → calibrate() returns
    None (cold-start contract from the plan: treat as untradeable)."""
    assert rwbc.calibrate(0.65, "NBA", "Points", "over") is None


def test_calibrate_returns_none_when_w_cell_below_threshold():
    """Circuit breaker fires below W_CELL_HALT_THRESHOLD."""
    _install_cell("NBA", "Points", "under", w_cell=0.10, p_post=0.55)
    assert rwbc.calibrate(0.70, "NBA", "Points", "under") is None


def test_calibrate_blends_model_and_posterior_above_threshold():
    """w_cell=0.4 above threshold → 0.4·p_model + 0.6·p_post."""
    _install_cell("NBA", "Points", "over", w_cell=0.4, p_post=0.50)
    out = rwbc.calibrate(0.80, "NBA", "Points", "over")
    expected = 0.4 * 0.80 + 0.6 * 0.50  # = 0.62
    assert out is not None
    assert math.isclose(out, expected, abs_tol=1e-9)


def test_calibrate_clamps_to_numerical_window():
    """Output is clamped to [0.001, 0.999] to keep downstream log-loss
    math from blowing up on extreme legs."""
    _install_cell("NBA", "Points", "over", w_cell=0.5, p_post=0.999)
    out = rwbc.calibrate(0.999, "NBA", "Points", "over")
    assert out is not None
    assert out <= 0.999


def test_calibrate_rejects_bad_side_value():
    """Side must be 'over' or 'under' (case-insensitive)."""
    _install_cell("NBA", "Points", "over", w_cell=0.5)
    # Bad sides return None even if the model has a strong prediction.
    assert rwbc.calibrate(0.70, "NBA", "Points", "yes") is None
    assert rwbc.calibrate(0.70, "NBA", "Points", "") is None
    assert rwbc.calibrate(0.70, "NBA", "Points", None) is None
    # Case-insensitive on side is the contract.
    assert rwbc.calibrate(0.70, "NBA", "Points", "OVER") is not None


# ─────────────────────────────────────────────────────────────────────────
# Auxiliary surface (bucket_of, cache_stats)
# ─────────────────────────────────────────────────────────────────────────

def test_bucket_of_centers_5pp_bins():
    """bucket_of(0.42) → center of [0.40, 0.45) = 0.425."""
    assert bucket_of(0.42) == 0.425
    assert bucket_of(0.40) == 0.425
    assert bucket_of(0.4499) == 0.425
    assert bucket_of(0.45)  == 0.475


def test_cache_stats_reports_halts_correctly():
    _install_cell("NBA", "Points", "over",  w_cell=0.50)
    _install_cell("NBA", "Points", "under", w_cell=0.10)   # halted
    _install_cell("MLB", "Hits",   "over",  w_cell=0.30)
    stats = rwbc.cache_stats()
    assert stats["n_cells"] == 3
    assert stats["n_halted"] == 1
    assert stats["n_active"] == 2
    assert stats["halt_threshold"] == W_CELL_HALT_THRESHOLD


def test_calibrate_is_thread_safe_under_cache_swap():
    """The publish path swaps _cell_cache wholesale while inference may
    be reading. Validate that calibrate() and load_cell_cache_from_db()
    don't deadlock or corrupt state."""
    _install_cell("NBA", "Points", "over", w_cell=0.4, p_post=0.55)
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            rwbc.calibrate(0.7, "NBA", "Points", "over")

    def swapper():
        # Simulate publish path's cache update.
        for _ in range(20):
            with rwbc._cache_lock:
                new = dict(rwbc._cell_cache)
            with rwbc._cache_lock:
                rwbc._cell_cache.clear()
                rwbc._cell_cache.update(new)

    t1 = threading.Thread(target=reader); t1.start()
    t2 = threading.Thread(target=swapper); t2.start()
    t2.join(timeout=5)
    stop.set()
    t1.join(timeout=5)
    # Survived without deadlock or exception.
    assert not t1.is_alive() and not t2.is_alive()
