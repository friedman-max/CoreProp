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


def test_comp_is_irrelevant_when_enforcement_is_off(monkeypatch):
    """Sanity: with enforcement off everyone already has access."""
    monkeypatch.setattr(app_mod, "BILLING_ENFORCE", False)
    monkeypatch.setattr(app_mod, "COMP_ACCOUNTS", set())
    assert app_mod._user_has_access(_u()) is True
    assert app_mod._user_has_access(None) is True
