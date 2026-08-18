"""Web Push send path for the installable PWA.

The whole feature is gated on VAPID keys (is_configured): with no keys set this
module's public functions are no-ops, so shipping it changes nothing in
production until the keys are configured — the same env-gating discipline as
SIDE_BIAS / billing.

Isolation: send_to_user reads ONE user's subscriptions with the service-role
client PLUS an explicit `.eq("user_id", …)` — the two-layer isolation the rest
of the codebase uses (RLS is the other layer). Dead endpoints (the push service
returns 404/410 once a subscription is gone) are pruned so they aren't retried.

Delivery is isolated in `_deliver` so the pywebpush import stays lazy (it pulls
in cryptography) and so tests can stub it without the library installed.
"""
from __future__ import annotations

import json
import logging
import threading
import time

import config as cfg
from engine.database import get_db
from engine.writer import writer

logger = logging.getLogger(__name__)


def is_configured() -> bool:
    """True only when both VAPID keys are present. Read live (not a cached
    constant) so a runtime/env change — and tests — take effect."""
    return bool(cfg.VAPID_PUBLIC_KEY and cfg.VAPID_PRIVATE_KEY)


def _deliver(subscription_info: dict, payload: str) -> None:
    """A single Web Push delivery. Isolated so the pywebpush import is lazy and
    so tests can stub this without pywebpush installed. Raises on failure; the
    caller inspects the exception's response status to decide about pruning."""
    from pywebpush import webpush

    webpush(
        subscription_info=subscription_info,
        data=payload,
        vapid_private_key=cfg.VAPID_PRIVATE_KEY,
        vapid_claims={"sub": cfg.VAPID_SUBJECT},
        timeout=10,
    )


def send_to_user(user_id: str, title: str, body: str, url: str = "/") -> int:
    """Push `title`/`body` to every registered endpoint for `user_id`.

    Returns the number of successful deliveries. Never raises — push is a
    best-effort notification, never a reason to break the caller (the
    auto-backtest worker). No-op (returns 0) when unconfigured or the user has
    no subscriptions.
    """
    if not is_configured():
        return 0
    db = get_db()
    if db is None:
        return 0
    try:
        rows = (
            db.table("push_subscriptions")
            .select("endpoint,p256dh,auth")
            .eq("user_id", user_id)          # explicit tenant scope (layer 2 = RLS)
            .execute()
            .data
        ) or []
    except Exception as exc:
        logger.warning("push: could not read subscriptions for %s: %s", user_id, exc)
        return 0
    if not rows:
        return 0

    payload = json.dumps({"title": title, "body": body, "url": url, "tag": "coreprop-slips"})
    sent = 0
    dead: list[str] = []
    for r in rows:
        info = {
            "endpoint": r.get("endpoint"),
            "keys": {"p256dh": r.get("p256dh"), "auth": r.get("auth")},
        }
        try:
            _deliver(info, payload)
            sent += 1
        except Exception as exc:
            code = getattr(getattr(exc, "response", None), "status_code", None)
            if code in (404, 410):
                # The subscription is gone (unsubscribed / uninstalled). Prune it
                # so we stop paying to retry a dead endpoint every cycle.
                dead.append(r.get("endpoint"))
            else:
                logger.warning("push: delivery failed (%s): %s", code, exc)

    if dead:
        try:
            writer("push.prune").table("push_subscriptions").delete().in_("endpoint", dead).execute()
        except Exception as exc:
            logger.warning("push: could not prune %d dead endpoint(s): %s", len(dead), exc)

    return sent


# ── APNs (native iOS app) ────────────────────────────────────────────────────
# The native iOS app can't receive Web Push, so this is the Apple Push path.
# Same discipline as the VAPID path above: env-gated (apns_is_configured), never
# raises, prunes dead tokens (410 Unregistered / 400 BadDeviceToken), and keeps
# the httpx HTTP/2 import lazy so the module loads without h2 installed.

# APNs provider JWTs are valid up to 60 min and Apple throttles frequent
# regeneration, so we sign once and reuse for 45 min.
_APNS_JWT_CACHE: dict = {"token": None, "iat": 0.0}
_apns_jwt_lock = threading.Lock()
_APNS_JWT_TTL_SEC = 2700  # 45 min


def apns_is_configured() -> bool:
    """True only when the full APNs token-auth set is present. Read live so a
    runtime/env change — and tests — take effect."""
    return bool(
        cfg.APNS_AUTH_KEY and cfg.APNS_KEY_ID and cfg.APNS_TEAM_ID and cfg.APNS_BUNDLE_ID
    )


def _apns_jwt() -> str:
    """Signed, cached ES256 provider token for APNs. Header carries the key id;
    claims carry the team id (iss) + issued-at. Cached 45 min (< Apple's 60)."""
    now = time.time()
    with _apns_jwt_lock:
        tok = _APNS_JWT_CACHE["token"]
        if tok and (now - _APNS_JWT_CACHE["iat"]) < _APNS_JWT_TTL_SEC:
            return tok
        import jwt as _jwt  # PyJWT[crypto] — already a dependency (web/auth.py)
        token = _jwt.encode(
            {"iss": cfg.APNS_TEAM_ID, "iat": int(now)},
            cfg.APNS_AUTH_KEY,
            algorithm="ES256",
            headers={"kid": cfg.APNS_KEY_ID, "alg": "ES256"},
        )
        # PyJWT>=2 returns str; be defensive if a bytes slips through.
        if isinstance(token, bytes):
            token = token.decode("ascii")
        _APNS_JWT_CACHE["token"] = token
        _APNS_JWT_CACHE["iat"] = now
        return token


def _apns_host(environment: str) -> str:
    return "api.sandbox.push.apple.com" if environment == "sandbox" else "api.push.apple.com"


def _deliver_apns(device_token: str, environment: str, payload: bytes, jwt_token: str):
    """One APNs delivery. Returns (status_code, reason|None). Raises on transport
    failure. Isolated so the httpx (HTTP/2) import stays lazy and tests can stub
    it without h2 installed. APNs requires HTTP/2."""
    import httpx

    url = f"https://{_apns_host(environment)}/3/device/{device_token}"
    headers = {
        "authorization": f"bearer {jwt_token}",
        "apns-topic": cfg.APNS_BUNDLE_ID,
        "apns-push-type": "alert",
        "apns-priority": "10",
    }
    with httpx.Client(http2=True, timeout=10) as client:
        resp = client.post(url, content=payload, headers=headers)
        reason = None
        if resp.status_code != 200:
            try:
                reason = (resp.json() or {}).get("reason")
            except Exception:
                reason = None
        return resp.status_code, reason


# Reasons that mean the token is permanently invalid → prune it.
_APNS_DEAD_REASONS = {"BadDeviceToken", "DeviceTokenNotForTopic", "Unregistered"}


def send_apns_to_user(user_id: str, title: str, body: str) -> int:
    """Send an alert to every registered APNs device for `user_id`.

    Returns the number of successful deliveries. Never raises — push is
    best-effort. No-op (returns 0) when unconfigured or the user has no tokens.
    Reads with the service-role client PLUS an explicit .eq("user_id", …) — the
    two-layer isolation used across the codebase (RLS is the other layer)."""
    if not apns_is_configured():
        return 0
    db = get_db()
    if db is None:
        return 0
    try:
        rows = (
            db.table("apns_tokens")
            .select("device_token,environment")
            .eq("user_id", user_id)          # explicit tenant scope (layer 2 = RLS)
            .execute()
            .data
        ) or []
    except Exception as exc:
        logger.warning("apns: could not read tokens for %s: %s", user_id, exc)
        return 0
    if not rows:
        return 0

    try:
        jwt_token = _apns_jwt()
    except Exception as exc:
        logger.warning("apns: could not sign provider JWT: %s", exc)
        return 0

    payload = json.dumps({"aps": {"alert": {"title": title, "body": body}, "sound": "default"}}).encode("utf-8")
    sent = 0
    dead: list[str] = []
    for r in rows:
        tok = r.get("device_token")
        env = r.get("environment") or "production"
        if not tok:
            continue
        try:
            status, reason = _deliver_apns(tok, env, payload, jwt_token)
        except Exception as exc:
            logger.warning("apns: delivery error: %s", exc)
            continue
        if status == 200:
            sent += 1
        elif status == 410 or (status == 400 and reason in _APNS_DEAD_REASONS):
            dead.append(tok)
        else:
            logger.warning("apns: status %s reason=%s for token …%s", status, reason, tok[-6:])

    if dead:
        try:
            writer("push.apns_prune").table("apns_tokens").delete().in_("device_token", dead).execute()
        except Exception as exc:
            logger.warning("apns: could not prune %d dead token(s): %s", len(dead), exc)

    return sent
