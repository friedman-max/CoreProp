"""Stripe webhook: signature handling and SDK-object normalization.

Written after a production-only 500 on the money path. The handler used
`stripe.Webhook.construct_event`, which both verifies the signature AND returns
a parsed `StripeObject`. In stripe-python <10 that object subclassed `dict`, so
`event.get("type")` worked. From v10 on its MRO is `StripeObject -> object` and
`.get` raises `AttributeError: get` — and that call sat OUTSIDE the handler's
try/except, so Stripe received a 500, retried forever, and `subscription_status`
was never written. A customer would be charged and still see 402.

requirements.txt asks for `stripe>=9.0.0` with no upper bound, so which object
model you get depends only on when pip last resolved. Worse, the bug could not
reproduce in development: the no-secret branch parsed the body with json.loads
and yielded a real dict, so only a deploy with STRIPE_WEBHOOK_SECRET set could
hit it.

These tests therefore pin the two properties that actually prevent recurrence:
  1. the handler reads the event from the raw JSON body, never from an SDK object
  2. _stripe_to_dict flattens anything the SDK hands back into a plain dict
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from stripe._stripe_object import StripeObject

import web.app as app_mod

SECRET = "whsec_test_secret"


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(app_mod, "STRIPE_SECRET_KEY", "sk_test_x")
    monkeypatch.setattr(app_mod, "STRIPE_PRICE_MONTHLY", "price_m")
    monkeypatch.setattr(app_mod, "STRIPE_PRICE_YEARLY", "price_y")
    monkeypatch.setattr(app_mod, "STRIPE_WEBHOOK_SECRET", SECRET)
    # Never touch Supabase from a test.
    monkeypatch.setattr(app_mod, "_sync_subscription_to_db", lambda sub: None)
    with TestClient(app_mod.app) as c:
        yield c


def _signed(body: bytes, secret: str = SECRET, ts: str | None = None) -> dict:
    ts = ts or str(int(time.time()))
    sig = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return {"stripe-signature": f"t={ts},v1={sig}", "Content-Type": "application/json"}


def _event(etype: str = "customer.subscription.created") -> bytes:
    return json.dumps({
        "id": "evt_1", "object": "event", "type": etype,
        "data": {"object": {
            "id": "sub_1", "object": "subscription", "customer": "cus_1",
            "status": "active", "current_period_end": int(time.time()) + 86400,
            "items": {"data": [{"price": {"id": "price_m"}}]},
        }},
    }).encode()


# --- the regression itself ------------------------------------------------

@pytest.mark.parametrize("etype", [
    "customer.subscription.created",
    "customer.subscription.updated",
    "customer.subscription.deleted",
    "invoice.payment_failed",
    "checkout.session.completed",
    "customer.updated",            # not handled, but must not error
])
def test_signed_events_return_200(client, etype):
    """Every event Stripe is subscribed to must return 2xx.

    A 500 here is the exact production failure: Stripe retries, the customer
    stays unentitled, and nothing surfaces in the UI.
    """
    body = _event(etype)
    r = client.post("/api/billing/webhook", content=body, headers=_signed(body))
    assert r.status_code == 200, f"{etype} -> {r.status_code} {r.text}"


def test_handler_does_not_depend_on_stripe_sdk_object_model(client, monkeypatch):
    """The event must come from the raw body, not from a StripeObject.

    Guards the specific regression: if someone reinstates construct_event's
    return value, `.get` breaks on stripe>=10. We assert the handler still
    works even when construct_event would raise outright.
    """
    def _boom(*a, **k):
        raise AssertionError("handler must not call construct_event for parsing")

    monkeypatch.setattr(app_mod._stripe().Webhook, "construct_event", _boom, raising=False)
    body = _event()
    r = client.post("/api/billing/webhook", content=body, headers=_signed(body))
    assert r.status_code == 200


# --- signature verification must not regress ------------------------------

def test_forged_signature_rejected(client):
    body = _event()
    bad = {"stripe-signature": f"t={int(time.time())},v1={'0' * 64}",
           "Content-Type": "application/json"}
    assert client.post("/api/billing/webhook", content=body, headers=bad).status_code == 400


def test_wrong_secret_rejected(client):
    body = _event()
    r = client.post("/api/billing/webhook", content=body,
                    headers=_signed(body, secret="whsec_other"))
    assert r.status_code == 400


def test_stale_timestamp_rejected(client):
    """Outside Stripe's default 300s tolerance — replay protection."""
    body = _event()
    r = client.post("/api/billing/webhook", content=body, headers=_signed(body, ts="1"))
    assert r.status_code == 400


def test_missing_signature_header_rejected(client):
    body = _event()
    r = client.post("/api/billing/webhook", content=body,
                    headers={"Content-Type": "application/json"})
    assert r.status_code == 400


@pytest.mark.parametrize("body", [b"{not json", b"[1,2,3]", b'"a string"'])
def test_malformed_body_with_valid_signature_is_400_not_500(client, body):
    """Correctly signed but unusable. Must not reach the handler as a non-dict."""
    r = client.post("/api/billing/webhook", content=body, headers=_signed(body))
    assert r.status_code == 400


# --- the normalization boundary -------------------------------------------

def test_stripe_object_has_no_get_on_modern_sdk():
    """Documents the upstream change this whole module defends against.

    If this ever fails, stripe-python has gone back to dict subclassing and the
    original bug becomes dormant rather than fixed — the boundary should stay.
    """
    assert not hasattr(StripeObject.construct_from({"a": 1}, "k"), "get")


def test_stripe_to_dict_flattens_sdk_objects():
    sub = StripeObject.construct_from({
        "id": "sub_1", "customer": "cus_1", "status": "trialing",
        "metadata": {"supabase_user_id": "uuid-1"},
        "items": {"data": [{"price": {"id": "price_m"}}]},
    }, "k")
    d = app_mod._stripe_to_dict(sub)
    assert isinstance(d, dict)
    assert d.get("customer") == "cus_1"
    # Nested access is what _sync_subscription_to_db actually does.
    assert (d.get("metadata") or {}).get("supabase_user_id") == "uuid-1"
    assert (d.get("items") or {}).get("data")[0]["price"]["id"] == "price_m"


def test_stripe_to_dict_passes_plain_dicts_through_unchanged():
    src = {"customer": "cus_1"}
    assert app_mod._stripe_to_dict(src) is src


def test_stripe_to_dict_never_raises():
    """_sync_subscription_to_db is called from the webhook; a throw here would
    turn a survivable bad payload into a retry storm."""
    assert app_mod._stripe_to_dict(object()) == {}
    assert app_mod._stripe_to_dict(None) == {}


# --- current_period_end moved onto subscription items (2025-03-31.basil) ---

@pytest.fixture
def captured_upsert(monkeypatch):
    seen = {}
    monkeypatch.setattr(app_mod, "_find_user_id_by_customer", lambda c: "uuid-1")
    monkeypatch.setattr(app_mod, "_upsert_user_billing",
                        lambda uid, fields: seen.update({"uid": uid, **fields}))
    monkeypatch.setattr(app_mod, "STRIPE_PRICE_MONTHLY", "price_m")
    return seen


def test_current_period_end_is_read_from_the_subscription_item(captured_upsert):
    """The modern (basil and later) shape. Both the pinned endpoint version
    2026-04-22.dahlia and the SDK's 2026-06-24.dahlia render this."""
    period_end = int(time.time()) + 86400
    app_mod._sync_subscription_to_db({
        "customer": "cus_1", "status": "active",
        "items": {"data": [{"price": {"id": "price_m"},
                            "current_period_end": period_end}]},
    })
    assert captured_upsert.get("current_period_end"), \
        "current_period_end never written — a cancelled user loses their paid month"
    expected = datetime.fromtimestamp(period_end, timezone.utc).isoformat()
    assert captured_upsert["current_period_end"] == expected


def test_current_period_end_falls_back_to_the_subscription_root(captured_upsert):
    """Pre-basil shape, e.g. a replayed old event or a re-pin to an older
    API version. Must still work."""
    period_end = int(time.time()) + 86400
    app_mod._sync_subscription_to_db({
        "customer": "cus_1", "status": "active",
        "current_period_end": period_end,
        "items": {"data": [{"price": {"id": "price_m"}}]},
    })
    assert captured_upsert.get("current_period_end")


def test_cancelled_user_keeps_access_until_the_period_they_paid_for(monkeypatch):
    """The reason current_period_end matters at all. With the column NULL,
    _user_has_access returns False the instant a subscription is cancelled."""
    monkeypatch.setattr(app_mod, "BILLING_ENFORCE", True)
    monkeypatch.setattr(app_mod, "STRIPE_SECRET_KEY", "sk_test_x")
    monkeypatch.setattr(app_mod, "STRIPE_PRICE_MONTHLY", "price_m")
    monkeypatch.setattr(app_mod, "STRIPE_PRICE_YEARLY", "price_y")
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", set())
    user = {"id": "99999999-8888-7777-6666-555555555555", "email": "payer@example.com"}

    future = (datetime.now(timezone.utc) + timedelta(days=20)).isoformat()

    monkeypatch.setattr(app_mod, "_read_user_billing", lambda u: {
        "subscription_status": "canceled", "current_period_end": future})
    assert app_mod._user_has_access(user) is True

    monkeypatch.setattr(app_mod, "_read_user_billing", lambda u: {
        "subscription_status": "canceled", "current_period_end": None})
    assert app_mod._user_has_access(user) is False


# --- webhook must fail closed without a signing secret --------------------

def test_unsigned_webhook_refused_when_no_signing_secret(monkeypatch):
    """Without this the route is an unauthenticated, RLS-bypassing write onto
    any Supabase uuid supplied in data.object.metadata.supabase_user_id."""
    monkeypatch.setattr(app_mod, "STRIPE_SECRET_KEY", "sk_test_x")
    monkeypatch.setattr(app_mod, "STRIPE_PRICE_MONTHLY", "price_m")
    monkeypatch.setattr(app_mod, "STRIPE_PRICE_YEARLY", "price_y")
    monkeypatch.setattr(app_mod, "STRIPE_WEBHOOK_SECRET", "")
    monkeypatch.delenv("COREPROP_ALLOW_UNSIGNED_WEBHOOKS", raising=False)

    called = []
    monkeypatch.setattr(app_mod, "_sync_subscription_to_db", lambda s: called.append(s))
    with TestClient(app_mod.app) as c:
        r = c.post("/api/billing/webhook", content=_event(),
                   headers={"Content-Type": "application/json"})
    assert r.status_code == 503
    assert not called, "handler ran on an unverified event"


def test_unsigned_webhook_allowed_only_behind_explicit_dev_flag(monkeypatch):
    monkeypatch.setattr(app_mod, "STRIPE_SECRET_KEY", "sk_test_x")
    monkeypatch.setattr(app_mod, "STRIPE_PRICE_MONTHLY", "price_m")
    monkeypatch.setattr(app_mod, "STRIPE_PRICE_YEARLY", "price_y")
    monkeypatch.setattr(app_mod, "STRIPE_WEBHOOK_SECRET", "")
    monkeypatch.setenv("COREPROP_ALLOW_UNSIGNED_WEBHOOKS", "true")
    monkeypatch.setattr(app_mod, "_sync_subscription_to_db", lambda s: None)
    with TestClient(app_mod.app) as c:
        r = c.post("/api/billing/webhook", content=_event(),
                   headers={"Content-Type": "application/json"})
    assert r.status_code == 200


# --- a failed write must not be acknowledged ------------------------------

def test_handler_failure_returns_5xx_so_stripe_retries(client, monkeypatch):
    """customer.subscription.deleted is terminal: no later event repairs the
    row, so acknowledging a failed write loses it permanently."""
    def _boom(sub):
        raise RuntimeError("No DB connection for billing upsert.")
    monkeypatch.setattr(app_mod, "_sync_subscription_to_db", _boom)
    body = _event("customer.subscription.deleted")
    r = client.post("/api/billing/webhook", content=body, headers=_signed(body))
    assert r.status_code >= 500, "a 2xx tells Stripe the event was handled"


def test_sync_accepts_a_stripe_sdk_object(monkeypatch):
    """checkout.session.completed and invoice.payment_failed both pass the
    result of stripe.Subscription.retrieve() — an SDK object, not a dict."""
    captured = {}
    monkeypatch.setattr(app_mod, "_find_user_id_by_customer", lambda c: "uuid-1")
    monkeypatch.setattr(app_mod, "_upsert_user_billing",
                        lambda uid, fields: captured.update({"uid": uid, **fields}))
    sub = StripeObject.construct_from({
        "id": "sub_1", "customer": "cus_1", "status": "active",
        "current_period_end": int(time.time()) + 86400,
        "items": {"data": [{"price": {"id": "price_m"}}]},
    }, "k")
    monkeypatch.setattr(app_mod, "STRIPE_PRICE_MONTHLY", "price_m")
    app_mod._sync_subscription_to_db(sub)
    assert captured.get("subscription_status") == "active"
    assert captured.get("stripe_customer_id") == "cus_1"
    assert captured.get("subscription_plan") == "monthly"
