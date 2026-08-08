"""Every route that serves the paid payload must carry the subscription gate,
and every ops route must carry the admin gate.

These exist because of a real production incident. `/api/bootstrap` was a
back-compat shim that proxied `/api/bootstrap/core` byte for byte but was
declared without the `user` dependency and without `_user_has_access`. While
BILLING_ENFORCE was false that omission was invisible — the gate is a no-op
when enforcement is off, so the endpoint behaved identically to its gated
twin. The moment BILLING_ENFORCE went true in production, every other data
route returned 402 and this one still served ~350KB of the full +EV list
(true_prob, edge, individual_ev_pct) to an unauthenticated curl.

A spot-check of "the" data endpoint would not have caught it, so these tests
enumerate the route table instead of naming endpoints: a newly added alias for
a paid payload fails here until it is gated.
"""
from __future__ import annotations

import inspect

import pytest


# Routes that serve a cached payload built from the paid pipeline. Anything
# calling _cached_response with one of these keys is selling the product.
_PAID_CACHE_KEYS = {"bets", "matches", "core", "pp_lines", "fd_lines",
                    "dk_lines", "pin_lines"}

# Ops routes: they start real work (scrape / refit / backtest sweep) on a
# single-worker dyno, or publish process internals.
_OPS_PATH_MARKERS = ("/api/admin/", "/refresh")


def _routes():
    from web.app import app
    out = []
    for r in app.routes:
        fn = getattr(r, "endpoint", None)
        path = getattr(r, "path", None)
        if fn is None or path is None:
            continue
        try:
            src = inspect.getsource(fn)
        except (OSError, TypeError):
            src = ""
        out.append((path, sorted(getattr(r, "methods", []) or []), fn, src))
    return out


def test_every_paid_payload_route_checks_access():
    """Any route that returns a paid cache payload must call _user_has_access."""
    offenders = []
    for path, methods, fn, src in _routes():
        if "_cached_response" not in src:
            continue
        serves_paid = any(f'"{k}"' in src or f"'{k}'" in src for k in _PAID_CACHE_KEYS)
        if serves_paid and "_user_has_access" not in src:
            offenders.append(f"{','.join(methods)} {path} ({fn.__name__})")
    assert not offenders, (
        "These routes serve a paid payload without a subscription gate — this "
        "is the /api/bootstrap bypass class of bug:\n  " + "\n  ".join(offenders)
    )


def test_paid_payload_routes_take_the_user_dependency():
    """_user_has_access(user) is meaningless if `user` was never injected."""
    offenders = []
    for path, methods, fn, src in _routes():
        if "_user_has_access" not in src:
            continue
        params = inspect.signature(fn).parameters
        if "user" not in params:
            offenders.append(f"{','.join(methods)} {path} ({fn.__name__})")
    assert not offenders, (
        "These routes call _user_has_access but never inject `user`:\n  "
        + "\n  ".join(offenders)
    )


def test_every_ops_route_requires_admin():
    """Scrape/refit/diagnostic routes must sit behind require_admin."""
    offenders = []
    for path, methods, fn, src in _routes():
        if not any(m in path for m in _OPS_PATH_MARKERS):
            continue
        params = inspect.signature(fn).parameters
        gated = any("require_admin" in str(p.default) for p in params.values())
        if not gated:
            offenders.append(f"{','.join(methods)} {path} ({fn.__name__})")
    assert not offenders, (
        "Unauthenticated ops routes (free DoS lever on a 512MB single-worker "
        "dyno):\n  " + "\n  ".join(offenders)
    )


def test_admin_gate_fails_closed_without_a_token(monkeypatch):
    """With ADMIN_TOKEN unset the gate must deny, not wave everyone through."""
    from fastapi import HTTPException
    import web.auth as auth

    monkeypatch.setattr(auth, "ADMIN_TOKEN", "")
    with pytest.raises(HTTPException) as ei:
        auth.require_admin(x_admin_token="anything")
    assert ei.value.status_code == 403


def test_admin_gate_rejects_a_wrong_token(monkeypatch):
    from fastapi import HTTPException
    import web.auth as auth

    monkeypatch.setattr(auth, "ADMIN_TOKEN", "correct-horse")
    with pytest.raises(HTTPException) as ei:
        auth.require_admin(x_admin_token="wrong-horse")
    assert ei.value.status_code == 403
    # And accepts the right one.
    assert auth.require_admin(x_admin_token="correct-horse") is True
