"""Web Push subscription endpoints for the installable PWA.

A signed-in user's browser POSTs its PushSubscription here after granting
notification permission; the auto-backtest worker (web/app.py) then pushes that
user a notification when it logs slips for them.

Owner-scoped: the row is tied to the authenticated user's id, and unsubscribe
carries an explicit `.eq("user_id", …)` so it is an ownership check, not just a
visibility one (migration_022 + RLS are the second layer). Writes go through
engine/writer.py. The feature is a no-op unless VAPID keys are set.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from engine import push as push_svc
from engine.writer import writer
from web.auth import get_current_user

router = APIRouter(prefix="/api/push", tags=["push"])


class _Keys(BaseModel):
    p256dh: str
    auth: str


class PushSubscriptionIn(BaseModel):
    endpoint: str
    keys: _Keys


class UnsubscribeIn(BaseModel):
    endpoint: str


class ApnsRegisterIn(BaseModel):
    device_token: str
    environment: str = "production"   # "production" | "sandbox"
    bundle_id: str | None = None


class ApnsUnregisterIn(BaseModel):
    device_token: str


@router.get("/vapid-public-key")
def vapid_public_key():
    """Public — the browser needs the applicationServerKey to subscribe. Also
    injected into the page config by web/app.py::root, so the client normally
    doesn't fetch this; it exists for completeness / non-injected contexts."""
    import config as cfg
    return {"vapid_public_key": cfg.VAPID_PUBLIC_KEY, "enabled": push_svc.is_configured()}


@router.post("/subscribe")
def push_subscribe(sub: PushSubscriptionIn, user: dict = Depends(get_current_user)):
    if not push_svc.is_configured():
        raise HTTPException(status_code=503, detail="Push notifications are not configured.")
    # Bound the fields: an authenticated but hostile client shouldn't be able to
    # stuff arbitrary blobs into the table. Real values are well under these.
    if len(sub.endpoint) > 2000 or len(sub.keys.p256dh) > 300 or len(sub.keys.auth) > 200:
        raise HTTPException(status_code=400, detail="Malformed subscription.")
    writer("push.subscribe").table("push_subscriptions").upsert(
        {
            "user_id": user["id"],
            "endpoint": sub.endpoint,
            "p256dh": sub.keys.p256dh,
            "auth": sub.keys.auth,
        },
        on_conflict="endpoint",
    ).execute()
    return {"ok": True}


@router.post("/unsubscribe")
def push_unsubscribe(body: UnsubscribeIn, user: dict = Depends(get_current_user)):
    writer("push.unsubscribe").table("push_subscriptions").delete() \
        .eq("endpoint", body.endpoint).eq("user_id", user["id"]).execute()
    return {"ok": True}


# ── APNs (native iOS app) ────────────────────────────────────────────────────
# The native app registers its APNs device token here; the auto-backtest worker
# then sends it Apple Push alongside the Web Push path. Owner-scoped exactly like
# /subscribe: the row carries the authenticated user's id, unregister carries an
# explicit .eq("user_id", …) (ownership check, not just visibility), and writes
# go through engine/writer.py. No-op unless the APNS_* keys are configured.

@router.post("/apns/register")
def apns_register(body: ApnsRegisterIn, user: dict = Depends(get_current_user)):
    if not push_svc.apns_is_configured():
        raise HTTPException(status_code=503, detail="Apple Push is not configured.")
    token = (body.device_token or "").strip()
    # APNs device tokens are hex; 64 chars historically, longer for newer tokens.
    # Bound the field so an authenticated but hostile client can't stuff blobs.
    if not token or len(token) > 200 or not all(c in "0123456789abcdefABCDEF" for c in token):
        raise HTTPException(status_code=400, detail="Malformed device token.")
    environment = body.environment if body.environment in ("production", "sandbox") else "production"
    writer("push.apns_register").table("apns_tokens").upsert(
        {
            "user_id": user["id"],
            "device_token": token,
            "environment": environment,
            "bundle_id": body.bundle_id,
        },
        on_conflict="device_token",
    ).execute()
    return {"ok": True}


@router.post("/apns/unregister")
def apns_unregister(body: ApnsUnregisterIn, user: dict = Depends(get_current_user)):
    writer("push.apns_unregister").table("apns_tokens").delete() \
        .eq("device_token", body.device_token).eq("user_id", user["id"]).execute()
    return {"ok": True}
