"""Web Push delivery for auto-backtest slip notifications.

Scope is deliberately narrow: store a browser's push subscription, and send
that browser a notification when the auto-backtest worker logs slips for its
owner. There is no general notification framework here and shouldn't be one
until there is a second thing worth notifying about.

WHY WEB PUSH AND NOT APNs
    The iOS app in ios/ is a WKWebView wrapper signed with a free Apple
    developer account, and free provisioning cannot use APNs at all. Web Push
    works on iOS 16.4+ with no Apple account, no $99, and no review — but ONLY
    when the site is opened from a Home Screen icon. A Safari tab gets nothing,
    silently. That constraint is Apple's, not ours, and it is why the UI copy
    tells the user to add to Home Screen first.
"""

import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# VAPID identifies this server to the push services. The public key is handed
# to the browser at subscribe time and is not a secret; the private key signs
# every send and is. Generate a pair with:
#
#     python -c "from py_vapid import Vapid01; v=Vapid01(); v.generate_keys(); \
#         print(v.public_key_urlsafe_base64(), v.private_key_urlsafe_base64())"
#
# Both must persist: regenerating them invalidates every stored subscription,
# because the browser bound its endpoint to the public key it saw at subscribe
# time. That is a silent failure -- sends start 403ing and nothing rings.
VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "").strip()
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "").strip()
# Push services require a contact for the sender. mailto: or an https URL.
VAPID_SUBJECT = os.getenv("VAPID_SUBJECT", "mailto:support@coreprop.me").strip()


def push_configured() -> bool:
    """True when we have enough VAPID config to actually send.

    Mirrors _billing_configured()'s shape on purpose. Unlike billing, this one
    fails CLOSED -- an unconfigured server simply never sends, which is a
    missing notification rather than a security hole.
    """
    return bool(VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY)


def send_to_user(db, user_id: str, title: str, body: str, url: str = "/") -> int:
    """Send one notification to every device registered to `user_id`.

    Returns the number of devices that accepted it. Never raises: this is
    called from the auto-backtest background thread, where an exception would
    kill slip logging -- the thing the user actually cares about -- to deliver
    a notification about it.
    """
    if not push_configured():
        return 0

    try:
        from pywebpush import webpush, WebPushException
    except ImportError:
        logger.warning("push    pywebpush not installed — skipping notification")
        return 0

    try:
        rows = (
            db.table("push_subscriptions")
            .select("endpoint,p256dh,auth")
            .eq("user_id", user_id)
            .execute()
            .data
            or []
        )
    except Exception as e:
        logger.error("push    could not read subscriptions for user=%s: %s", user_id, e)
        return 0

    if not rows:
        return 0

    payload = json.dumps({"title": title, "body": body, "url": url})
    sent = 0
    dead: list[str] = []

    for row in rows:
        endpoint = row.get("endpoint") or ""
        try:
            webpush(
                subscription_info={
                    "endpoint": endpoint,
                    "keys": {"p256dh": row.get("p256dh"), "auth": row.get("auth")},
                },
                data=payload,
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={"sub": VAPID_SUBJECT},
                timeout=10,
            )
            sent += 1
        except WebPushException as e:
            # 404/410 is the push service saying this endpoint no longer
            # exists -- the user cleared the site, or (on iOS) deleted the Home
            # Screen icon. That is terminal, so drop the row instead of
            # retrying it every cycle forever.
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (404, 410):
                dead.append(endpoint)
            else:
                logger.warning("push    send failed (%s) for user=%s: %s", status, user_id, e)
        except Exception as e:
            logger.warning("push    unexpected send error for user=%s: %s", user_id, e)

    if dead:
        try:
            db.table("push_subscriptions").delete().in_("endpoint", dead).execute()
            logger.info("push    pruned %d expired subscription(s)", len(dead))
        except Exception as e:
            logger.warning("push    could not prune expired subscriptions: %s", e)

    return sent


def slip_notification_text(count: int, label: Optional[str] = None) -> tuple[str, str]:
    """Copy for the 'your slips were logged' notification.

    Batched per cycle rather than one-per-slip: auto-backtest can log up to
    MAX_AUTO_SLIPS_PER_CYCLE (default 10) in a single refresh, and ten
    notifications arriving at once is how you get muted.
    """
    plural = "slip" if count == 1 else "slips"
    suffix = f" ({label})" if label else ""
    return (
        "New backtest slip" if count == 1 else f"{count} new backtest slips",
        f"Auto-backtest logged {count} {plural}{suffix}. Tap to view.",
    )
