"""load_artefact correctness: prefer-newer behavior between disk and Supabase.

This was the root cause of the local-vs-prod data divergence (fixed in
`Sandbox preload + artefact-loader parity` commit). Pin the behavior so
nobody re-introduces a "disk-first, Supabase-fallback" loader by accident.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone

import pytest

from engine import persistence


@pytest.fixture
def tmp_artefact(tmp_path):
    """Write a known disk artefact and return (path, payload)."""
    path = tmp_path / "artefact.json"
    payload = {"version": 2, "marker": "disk"}
    path.write_text(json.dumps(payload))
    # Backdate so the Supabase copy is newer in the relevant test.
    old = datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()
    os.utime(path, (old, old))
    return str(path), payload


def test_supabase_wins_when_newer(monkeypatch, tmp_artefact):
    path, _ = tmp_artefact
    monkeypatch.setattr(
        persistence, "load_state_from_supabase",
        lambda key: ({"version": 2, "marker": "supabase"}, "2026-05-13T00:00:00Z"),
    )
    out = persistence.load_artefact("artefact", path)
    assert out["marker"] == "supabase"
    # Disk should have been rewritten to the chosen payload.
    on_disk = json.loads(open(path).read())
    assert on_disk["marker"] == "supabase"


def test_disk_wins_when_newer(monkeypatch, tmp_path):
    path = tmp_path / "artefact.json"
    payload = {"version": 2, "marker": "disk"}
    path.write_text(json.dumps(payload))
    # Touch to a recent timestamp so disk is newer than the stub Supabase ts.
    now = datetime.now(timezone.utc).timestamp()
    os.utime(path, (now, now))
    monkeypatch.setattr(
        persistence, "load_state_from_supabase",
        lambda key: ({"version": 2, "marker": "supabase"}, "2024-01-01T00:00:00Z"),
    )
    out = persistence.load_artefact("artefact", str(path))
    assert out["marker"] == "disk"


def test_supabase_only_when_disk_missing(monkeypatch, tmp_path):
    path = tmp_path / "missing.json"  # never written
    monkeypatch.setattr(
        persistence, "load_state_from_supabase",
        lambda key: ({"version": 2, "marker": "supabase"}, "2026-01-01T00:00:00Z"),
    )
    out = persistence.load_artefact("artefact", str(path))
    assert out["marker"] == "supabase"
    # Disk was hydrated from Supabase so subsequent reads stay local.
    assert os.path.exists(path)


def test_returns_none_when_both_absent(monkeypatch, tmp_path):
    path = tmp_path / "missing.json"
    monkeypatch.setattr(persistence, "load_state_from_supabase", lambda key: (None, None))
    assert persistence.load_artefact("artefact", str(path)) is None


def test_validator_rejects_bad_supabase(monkeypatch, tmp_artefact):
    """A corrupt Supabase payload must not replace a healthy disk file."""
    path, _ = tmp_artefact
    monkeypatch.setattr(
        persistence, "load_state_from_supabase",
        lambda key: ({"version": 99, "marker": "supabase"}, "2026-05-13T00:00:00Z"),
    )
    out = persistence.load_artefact(
        "artefact", path,
        validator=lambda p: isinstance(p, dict) and p.get("version") == 2,
    )
    assert out["marker"] == "disk"
