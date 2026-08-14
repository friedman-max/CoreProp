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
