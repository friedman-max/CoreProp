"""Maybe Cool Fix C2: Beta calibration fit + apply tests."""
import os
import tempfile
from unittest.mock import patch

import numpy as np
import pytest

from engine import beta_calibration as bc


# ─────────────────────────────────────────────────────────────────────────
# Identity recovery: y = s exactly → fitted params should give q ≈ s
# ─────────────────────────────────────────────────────────────────────────

def test_identity_recovery_no_shade():
    """When the outcome is exactly s, beta-cal must learn approximately
    (a=1, b=-1, c=0, d=0) i.e. the logit transformation."""
    rng = np.random.default_rng(42)
    n = 500
    s = rng.uniform(0.20, 0.80, size=n)
    # Stochastic y so the MLE has unique gradient; expected fit ≈ identity.
    y = (rng.uniform(size=n) < s).astype(float)
    params = bc.fit_beta_cal(s, y)
    # After fit, applying to a test s should be close to s on average.
    test_s = np.linspace(0.30, 0.70, 9)
    q = np.array([bc.apply_beta_cal(params, float(x)) for x in test_s])
    # Mean abs error should be small
    assert np.mean(np.abs(q - test_s)) < 0.05


# ─────────────────────────────────────────────────────────────────────────
# Calibrator pulls predictions down on over-confident data
# ─────────────────────────────────────────────────────────────────────────

def test_overconfidence_correction():
    """Simulate D1's overconfidence pattern: when model says 0.70, reality
    is 0.55. Beta-cal must learn to push 0.70 → ~0.55."""
    rng = np.random.default_rng(7)
    n = 1000
    s = rng.uniform(0.4, 0.8, size=n)
    # True probability: linear shrinkage toward 0.5.
    p_true = 0.5 + 0.5 * (s - 0.5)
    y = (rng.uniform(size=n) < p_true).astype(float)
    params = bc.fit_beta_cal(s, y)
    # At s=0.70 the true probability is 0.60; calibrated output should
    # land much closer to 0.60 than the input 0.70.
    q70 = bc.apply_beta_cal(params, 0.70)
    assert q70 < 0.70 - 0.04


# ─────────────────────────────────────────────────────────────────────────
# Shade conditioning
# ─────────────────────────────────────────────────────────────────────────

def test_shade_conditioning_changes_output():
    """Fit with shade as a feature INDEPENDENT of s.

    In production, shade = s - 0.5408 is a deterministic function of s, so
    the c coefficient is not identifiable independently of (a, b, d) (the
    fit is collinear). The shade feature only adds information when it
    carries signal beyond what s already encodes — e.g., shade at log
    time vs. shade at close.

    This test uses an INDEPENDENT shade so the fit can isolate c's effect.
    """
    rng = np.random.default_rng(11)
    n = 800
    s = rng.uniform(0.40, 0.70, size=n)
    # Independent shade — not derived from s.
    shade = rng.uniform(-0.10, 0.10, size=n)
    # Simulated reality: high (independent) shade → lower hit rate.
    p_true = np.clip(s - 0.6 * np.maximum(0, shade), 0.05, 0.95)
    y = (rng.uniform(size=n) < p_true).astype(float)
    params = bc.fit_beta_cal(s, y, shade=shade)
    assert params.has_shade
    # At the SAME input s, higher shade should reduce q.
    q_zero  = bc.apply_beta_cal(params, 0.60, shade=0.0)
    q_high  = bc.apply_beta_cal(params, 0.60, shade=0.15)
    assert q_high < q_zero, f"expected q_high < q_zero; got {q_high} vs {q_zero}"


# ─────────────────────────────────────────────────────────────────────────
# Thin-cell fallback
# ─────────────────────────────────────────────────────────────────────────

def test_thin_cell_returns_identity_params():
    """Below MIN_FIT_N samples, return identity (a=1, b=-1) to avoid
    overfitting a sparse cell."""
    n_thin = int(bc.MIN_FIT_N) - 5
    s = np.full(n_thin, 0.55)
    y = np.ones(n_thin)
    params = bc.fit_beta_cal(s, y)
    assert params.a == 1.0
    assert params.b == -1.0


# ─────────────────────────────────────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────────────────────────────────────

def test_save_load_round_trip(tmp_path):
    fn = os.path.join(str(tmp_path), "beta_cal.json")
    with patch.object(bc, "BETA_CAL_FILE", fn):
        params = bc.BetaCalParams(a=1.2, b=-1.1, c=-0.3, d=0.05,
                                  n_eff=150.0, has_shade=True)
        ok = bc.save_params({("NBA", "Points", "over"): params})
        assert ok
        loaded = bc.load_params()
        assert ("NBA", "Points", "over") in loaded
        out = loaded[("NBA", "Points", "over")]
        assert abs(out.a - 1.2) < 1e-9
        assert out.has_shade


def test_calibrate_returns_none_for_unknown_cell(tmp_path):
    fn = os.path.join(str(tmp_path), "missing.json")
    with patch.object(bc, "BETA_CAL_FILE", fn):
        bc.load_params()
        assert bc.calibrate("NBA", "Points", "over", 0.65) is None


def test_calibrate_returns_value_for_known_cell(tmp_path):
    fn = os.path.join(str(tmp_path), "beta_cal.json")
    with patch.object(bc, "BETA_CAL_FILE", fn):
        params = bc.BetaCalParams(a=1.0, b=-1.0, c=0.0, d=0.0,
                                  n_eff=100.0, has_shade=False)
        bc.save_params({("NBA", "Points", "over"): params})
        bc.load_params()
        out = bc.calibrate("NBA", "Points", "over", 0.65)
        assert out is not None
        # Identity params → output ≈ input
        assert abs(out - 0.65) < 1e-4


# ─────────────────────────────────────────────────────────────────────────
# Apply bounds
# ─────────────────────────────────────────────────────────────────────────

def test_apply_bounded_to_001_999():
    """Extreme params shouldn't push the output outside the (0.001, 0.999)
    guard that downstream EV math expects."""
    extreme = bc.BetaCalParams(a=10.0, b=10.0, c=0.0, d=20.0,
                               n_eff=1000.0, has_shade=False)
    out_low = bc.apply_beta_cal(extreme, 0.01)
    out_hi  = bc.apply_beta_cal(extreme, 0.99)
    assert 0.001 <= out_low <= 0.999
    assert 0.001 <= out_hi  <= 0.999
