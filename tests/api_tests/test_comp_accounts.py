"""COMP_ACCOUNTS — permanent free access for founders/staff/testers.

Exists because switching BILLING_ENFORCE on locked the owner out of his own
app. The comp list is deliberately an env var rather than a hand-written
subscription_status='active' row in user_config: that row would assert a
Stripe subscription that does not exist, and _sync_subscription_to_db would
overwrite it the first time that account ever touched Stripe.
"""
from __future__ import annotations

import pytest

import web.app as app_mod


@pytest.fixture
def enforced(monkeypatch):
    """Billing configured AND enforced — the only state where access can fail."""
    monkeypatch.setattr(app_mod, "BILLING_ENFORCE", True)
    monkeypatch.setattr(app_mod, "STRIPE_SECRET_KEY", "sk_test_x")
    monkeypatch.setattr(app_mod, "STRIPE_PRICE_MONTHLY", "price_m")
    monkeypatch.setattr(app_mod, "STRIPE_PRICE_YEARLY", "price_y")
    # No billing row for anyone unless a test says otherwise.
    monkeypatch.setattr(app_mod, "_read_user_billing", lambda u: {
        "stripe_customer_id": None, "subscription_status": None,
        "subscription_plan": None, "current_period_end": None,
    })


def _u(uid="11111111-2222-3333-4444-555555555555", email="owner@example.com"):
    return {"id": uid, "email": email, "jwt": "x"}


def test_no_comp_list_means_no_access_without_a_subscription(enforced, monkeypatch):
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", set())
    assert app_mod._user_has_access(_u()) is False


def test_comp_by_email_grants_access(enforced, monkeypatch):
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", {"owner@example.com"})
    assert app_mod._user_has_access(_u()) is True


def test_comp_by_user_uuid_grants_access(enforced, monkeypatch):
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS",
                        {"11111111-2222-3333-4444-555555555555"})
    assert app_mod._user_has_access(_u()) is True


def test_comp_matching_is_case_insensitive_and_trims(enforced, monkeypatch):
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", {"owner@example.com"})
    assert app_mod._user_has_access(_u(email="  OWNER@Example.COM  ")) is True


def test_a_non_comped_account_is_still_denied(enforced, monkeypatch):
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", {"owner@example.com"})
    assert app_mod._user_has_access(_u(uid="other-uuid",
                                       email="stranger@example.com")) is False


def test_comp_does_not_grant_access_to_anonymous(enforced, monkeypatch):
    """An empty/None user must never match, even with a comp list configured."""
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", {"owner@example.com"})
    assert app_mod._user_has_access(None) is False
    assert app_mod._is_comped(None) is False


def test_blank_fields_never_match_a_comp_entry(enforced, monkeypatch):
    """A user with no email must not match a stray empty string in the list —
    ''.strip() entries are filtered at parse time, but guard the runtime path
    too so a None email can't collide with anything."""
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", {"owner@example.com", ""})
    assert app_mod._is_comped({"id": None, "email": None}) is False
    assert app_mod._is_comped({"id": "", "email": ""}) is False


def test_comp_survives_having_no_user_config_row(enforced, monkeypatch):
    """A comped account should not need a billing row at all — and the comp
    check must run BEFORE the Supabase read so it costs no round-trip."""
    def _boom(_u):
        raise AssertionError("_read_user_billing must not be called for a comp")
    monkeypatch.setattr(app_mod, "_read_user_billing", _boom)
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", {"owner@example.com"})
    assert app_mod._user_has_access(_u()) is True


# ── grandfathered accounts (the billing-launch cutoff) ────────────────────
#
# Policy, fixed on 2026-08-08: every account that existed when billing was
# switched on keeps CoreProp Pro free forever; everyone who signs up after
# pays. The list is closed — these tests fail if it is ever extended, because
# growing it silently would give away paid access.

_A_GRANDFATHERED_UUID = "a541fb5d-d0fe-403c-ab0f-b2ab9d72a422"


def test_all_nine_launch_accounts_are_grandfathered(enforced, monkeypatch):
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", set())
    assert len(app_mod._GRANDFATHERED_ACCOUNTS) == 9
    for uid in app_mod._GRANDFATHERED_ACCOUNTS:
        assert app_mod._user_has_access({"id": uid, "email": "whatever@x.com"}) is True


def test_grandfather_list_is_closed(enforced):
    """Guards the policy itself: new free accounts must go through
    COMP_ACCOUNTS, never by appending here. If you are intentionally changing
    the historical cutoff, update this number and say why in the commit."""
    assert len(app_mod._GRANDFATHERED_ACCOUNTS) == 9, (
        "The grandfather list is a one-time historical fact (accounts existing "
        "at billing launch). Comp new accounts via COMP_ACCOUNTS instead."
    )


def test_a_future_signup_must_pay(enforced, monkeypatch):
    """The other half of the policy: anyone not on either list is denied."""
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", set())
    future = {"id": "00000000-dead-beef-0000-000000000001",
              "email": "newcustomer@example.com"}
    assert app_mod._user_has_access(future) is False


def test_a_future_signup_gets_access_once_subscribed(enforced, monkeypatch):
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", set())
    monkeypatch.setattr(app_mod, "_read_user_billing", lambda u: {
        "stripe_customer_id": "cus_x", "subscription_status": "active",
        "subscription_plan": "monthly", "current_period_end": None,
    })
    future = {"id": "00000000-dead-beef-0000-000000000002",
              "email": "paid@example.com"}
    assert app_mod._user_has_access(future) is True


def test_grandfather_matches_uuid_only_not_email(enforced, monkeypatch):
    """The baked-in list holds UUIDs. An attacker who registers a founder's
    email must not inherit the grandfather — that is exactly why emails are
    not in this list."""
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", set())
    impostor = {"id": "00000000-dead-beef-0000-000000000003",
                "email": "benjaminfriedman01@gmail.com"}
    assert app_mod._user_has_access(impostor) is False


def test_grandfather_short_circuits_the_billing_read(enforced, monkeypatch):
    """Grandfathered users must not touch user_config at all — this is what
    kept them working while migration_010 was unapplied and the billing
    columns did not exist."""
    def _boom(_u):
        raise AssertionError("billing read must not happen for a grandfathered user")
    monkeypatch.setattr(app_mod, "_read_user_billing", _boom)
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", set())
    assert app_mod._user_has_access({"id": _A_GRANDFATHERED_UUID,
                                     "email": "x@x.com"}) is True


def test_comp_is_irrelevant_when_enforcement_is_off(monkeypatch):
    """Sanity: with enforcement off everyone already has access."""
    monkeypatch.setattr(app_mod, "BILLING_ENFORCE", False)
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", set())
    assert app_mod._user_has_access(_u()) is True
    assert app_mod._user_has_access(None) is True
