"""HTTP contract for the sandbox router (web/routers/sandbox.py).

Covers the two pure helpers (`_config_from_req`, `_sandbox_cache_key`) and
both endpoints (`/api/sandbox/run`, `/api/sandbox/optimize`) end-to-end
through FastAPI's TestClient. The `StrategyTester` is patched so these
tests never touch the database or calibration — they pin the request →
config mapping, the auth dependency, and the error → HTTP 400 translation,
not the simulation math (that's covered in the engine tests).
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from web.routers.sandbox import (
    SandboxRequest, _config_from_req, _sandbox_cache_key,
)


# ── Pure helpers ────────────────────────────────────────────────────────

class TestRequestDefaults:
    def test_defaults_match_v1_behavior(self):
        req = SandboxRequest()
        assert req.min_prob == pytest.approx(0.5408)
        assert req.slip_size == 6
        assert req.slip_type == "flex"
        assert req.bet_size == 1.0
        assert req.use_kelly is False
        assert req.slip_strategy == "live_replay"
        assert req.bootstrap is True
        assert req.leagues == []


class TestConfigMapping:
    def test_every_field_maps_through(self):
        req = SandboxRequest(
            leagues=["NBA", "MLB"], min_prob=0.56, slip_size=4,
            slip_type="power", bet_size=2.0, use_kelly=True,
            included_props=["Points"], slip_strategy="top_ev",
            start_date="2026-01-01", end_date="2026-02-01", bootstrap=False,
        )
        cfg = _config_from_req(req)
        assert cfg.leagues == ["NBA", "MLB"]
        assert cfg.min_prob == pytest.approx(0.56)
        assert cfg.slip_size == 4
        assert cfg.slip_type == "power"
        assert cfg.bet_size == 2.0
        assert cfg.use_kelly is True
        assert cfg.included_props == ["Points"]
        assert cfg.slip_strategy == "top_ev"
        assert cfg.start_date == "2026-01-01"
        assert cfg.end_date == "2026-02-01"
        assert cfg.bootstrap is False


class TestCacheKey:
    """The key must be stable for identical configs and differ when any
    output-affecting field changes — otherwise we'd serve stale results
    or never hit the cache."""

    def _patched_cal(self):
        # Pin the calibration fingerprint so the key depends only on the
        # request fields under test.
        return patch(
            "engine.isotonic_calibration.load_isotonic_calibration",
            return_value={"fitted_at": "2026-05-01T00:00:00Z"},
        )

    def test_identical_requests_hash_equal(self):
        with self._patched_cal():
            a = _sandbox_cache_key(SandboxRequest(leagues=["NBA", "MLB"]))
            b = _sandbox_cache_key(SandboxRequest(leagues=["MLB", "NBA"]))
        assert a == b, "league order must not affect the key (sorted)"

    @pytest.mark.parametrize("field,value", [
        ("min_prob", 0.6), ("slip_size", 3), ("slip_type", "power"),
        ("bet_size", 5.0), ("use_kelly", True), ("slip_strategy", "top_ev"),
        ("start_date", "2026-01-01"), ("bootstrap", False),
    ])
    def test_changing_a_field_changes_the_key(self, field, value):
        with self._patched_cal():
            base = _sandbox_cache_key(SandboxRequest())
            changed = _sandbox_cache_key(SandboxRequest(**{field: value}))
        assert base != changed, f"{field} must participate in the cache key"

    def test_calibration_refit_invalidates_key(self):
        with patch("engine.isotonic_calibration.load_isotonic_calibration",
                   return_value={"fitted_at": "2026-05-01T00:00:00Z"}):
            old = _sandbox_cache_key(SandboxRequest())
        with patch("engine.isotonic_calibration.load_isotonic_calibration",
                   return_value={"fitted_at": "2026-05-02T00:00:00Z"}):
            new = _sandbox_cache_key(SandboxRequest())
        assert old != new, "a refit (new fitted_at) must invalidate the key"


# ── Endpoint contract ───────────────────────────────────────────────────

@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    from web.app import app
    from web.auth import get_current_user
    app.dependency_overrides[get_current_user] = lambda: {"id": "test-user"}
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.pop(get_current_user, None)


def _fake_tester(*, run=None, optimize=None):
    """A StrategyTester() stand-in whose methods return canned payloads."""
    inst = MagicMock()
    if run is not None:
        inst.run_simulation.return_value = run
    if optimize is not None:
        inst.optimize_threshold.return_value = optimize
    factory = MagicMock(return_value=inst)
    return factory


class TestRunEndpoint:
    def test_run_returns_payload_on_success(self, client):
        payload = {"summary": {"total_slips": 1, "roi_pct": 500.0}, "slips": []}
        with patch("web.routers.sandbox._sandbox_cache_get", return_value=None), \
             patch("web.routers.sandbox._sandbox_cache_put"), \
             patch("engine.strategy_tester.StrategyTester",
                   _fake_tester(run=payload)):
            r = client.post("/api/sandbox/run", json={"leagues": ["NBA"]})
        assert r.status_code == 200
        assert r.json()["summary"]["roi_pct"] == 500.0

    def test_run_translates_engine_error_to_400(self, client):
        with patch("web.routers.sandbox._sandbox_cache_get", return_value=None), \
             patch("web.routers.sandbox._sandbox_cache_put"), \
             patch("engine.strategy_tester.StrategyTester",
                   _fake_tester(run={"error": "Raise Min True Prob to 57.74%"})):
            r = client.post("/api/sandbox/run", json={"leagues": ["NBA"]})
        assert r.status_code == 400
        assert r.json()["detail"] == "Raise Min True Prob to 57.74%"

    def test_run_serves_from_cache_without_calling_tester(self, client):
        cached = {"summary": {"total_slips": 7}}
        factory = _fake_tester(run={"summary": {"total_slips": 0}})
        with patch("web.routers.sandbox._sandbox_cache_get", return_value=cached), \
             patch("engine.strategy_tester.StrategyTester", factory):
            r = client.post("/api/sandbox/run", json={"leagues": ["NBA"]})
        assert r.status_code == 200
        body = r.json()
        assert body["summary"]["total_slips"] == 7
        assert body["_meta"]["from_cache"] is True
        factory.assert_not_called()

    def test_run_requires_auth(self):
        # No dependency override → the real get_current_user runs and
        # rejects the unauthenticated request.
        from fastapi.testclient import TestClient
        from web.app import app
        with TestClient(app) as c:
            r = c.post("/api/sandbox/run", json={"leagues": ["NBA"]})
        assert r.status_code in (401, 403)


class TestOptimizeEndpoint:
    def test_optimize_returns_payload_on_success(self, client):
        payload = {"best_threshold": 0.55, "best_roi": 12.3, "all_results": []}
        with patch("engine.strategy_tester.StrategyTester",
                   _fake_tester(optimize=payload)):
            r = client.post("/api/sandbox/optimize", json={"slip_size": 3, "slip_type": "power"})
        assert r.status_code == 200
        assert r.json()["best_threshold"] == 0.55

    def test_optimize_translates_error_to_400(self, client):
        with patch("engine.strategy_tester.StrategyTester",
                   _fake_tester(optimize={"error": "Flex slips require at least 3 legs."})):
            r = client.post("/api/sandbox/optimize", json={"slip_size": 2, "slip_type": "flex"})
        assert r.status_code == 400
        assert "Flex slips require at least 3 legs." in r.json()["detail"]
