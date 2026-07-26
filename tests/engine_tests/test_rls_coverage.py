"""Every table in the schema must have RLS enabled.

CoreProp publishes `SUPABASE_URL` + `SUPABASE_ANON_KEY` to the browser
(web/app.py injects them into index.html, and GET /api/ui-config serves them),
so the Postgres REST endpoint is directly reachable by anyone who loads the
site. Any table WITHOUT row-level security is therefore readable — and, absent
a policy denying it, writable — with the published anon key, bypassing FastAPI
entirely.

`app_state_cache` is the sharpest case: `_seed_state_from_db_sync()` trusts it
on boot, so a poisoned row is served to every user on first paint.

This test parses migrations/schema.sql (the consolidated bootstrap that a fresh
project runs) and asserts each `create table` has a matching
`enable row level security`. It is a static check on purpose: it needs no live
database, and it fails when someone adds a table and forgets the RLS line —
which is exactly when the gap gets introduced.

Enabling RLS with NO policy denies all anon/authenticated access by default,
while the service-role client (engine/database.py) bypasses RLS and keeps
working — which is what the server-side pipeline needs.
"""
from __future__ import annotations

import re
from pathlib import Path

_MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations"
_SCHEMA = _MIGRATIONS / "schema.sql"
_M018 = _MIGRATIONS / "migration_018.sql"


def _strip_comments(text: str) -> str:
    # Strip line comments so a table name mentioned in prose can't count as a
    # declaration or as an RLS enablement.
    return "\n".join(line.split("--", 1)[0] for line in text.splitlines())


def _sql() -> str:
    return _strip_comments(_SCHEMA.read_text(encoding="utf-8"))


def _declared_tables(sql: str) -> set[str]:
    return set(re.findall(r"create\s+table\s+(?:if\s+not\s+exists\s+)?(\w+)", sql, re.I))


def _rls_enabled_tables(sql: str) -> set[str]:
    return set(
        re.findall(r"alter\s+table\s+(\w+)\s+enable\s+row\s+level\s+security", sql, re.I)
    )


def test_schema_declares_at_least_the_known_tables():
    """Guard the parser itself — if this regex stops matching, the coverage
    assertion below would vacuously pass."""
    tables = _declared_tables(_sql())
    assert {"slips", "legs", "user_config", "app_state_cache"} <= tables, tables


def test_every_table_has_row_level_security_enabled():
    sql = _sql()
    missing = _declared_tables(sql) - _rls_enabled_tables(sql)
    assert not missing, (
        "tables reachable with the published anon key because RLS is not enabled: "
        f"{sorted(missing)}"
    )


def test_user_owned_tables_scope_to_the_authenticated_user():
    """The owner policies are the actual isolation boundary for user data."""
    sql = _sql()
    for table in ("slips", "legs", "user_config"):
        pattern = (
            rf"create\s+policy\s+\"?\w+\"?\s+on\s+{table}\b.*?"
            r"using\s*\(\s*user_id\s*=\s*auth\.uid\(\)\s*\)"
        )
        assert re.search(pattern, sql, re.I | re.S), (
            f"{table} has no `user_id = auth.uid()` owner policy"
        )


def test_app_state_cache_is_not_publicly_writable():
    """market_observatory is intentionally public-READ (`for select using (true)`).
    Nothing else may carry a permissive policy, and app_state_cache in
    particular must not — the boot seed reads it."""
    sql = _sql()
    permissive = re.findall(
        r"create\s+policy\s+\"?[\w\s-]+\"?\s+on\s+(\w+)(.*?);", sql, re.I | re.S
    )
    for table, body in permissive:
        if re.search(r"using\s*\(\s*true\s*\)", body, re.I):
            assert table == "market_observatory", (
                f"{table} has a permissive `using (true)` policy"
            )
            assert re.search(r"for\s+select", body, re.I), (
                "market_observatory's public policy must be SELECT-only"
            )


# ── migration_018 applies to EXISTING projects ────────────────────────────────
# Unlike schema.sql (which creates all eight tables in the same pass),
# migration_018 runs against a live project that may never have applied the
# migrations that created its targets: calibration_cells / calibration_history
# come from migration_010b_rwbc.sql and strategy_performance_compare from
# migration_013.sql, both of which are marked OPTIONAL / not-yet-applied in this
# repo. An unconditional `alter table` on a missing relation aborts the whole
# script with 42P01 — and because Supabase's SQL editor runs it as one
# transaction, the tables that DO exist get rolled back too, so nothing is
# secured. Each statement must therefore be guarded on the table existing.

_M018_TARGETS = (
    "app_state_cache",
    "calibration_cells",
    "calibration_history",
    "strategy_performance_compare",
)


def test_migration_018_guards_every_target_against_a_missing_table():
    sql = _strip_comments(_M018.read_text(encoding="utf-8"))

    # No bare `alter table <target> enable row level security` — each must sit
    # inside an existence check.
    for table in _M018_TARGETS:
        bare = re.search(
            rf"^\s*alter\s+table\s+{table}\s+enable\s+row\s+level\s+security",
            sql,
            re.I | re.M,
        )
        assert not bare, (
            f"migration_018 unconditionally alters {table}; it aborts with 42P01 "
            "on a project that never created that table"
        )


def test_migration_018_still_covers_all_four_tables():
    """Guarding must not silently drop a table from the migration."""
    sql = _strip_comments(_M018.read_text(encoding="utf-8"))
    for table in _M018_TARGETS:
        assert re.search(rf"'{table}'", sql), (
            f"migration_018 no longer targets {table}"
        )


def test_migration_018_existence_check_precedes_the_alter():
    """The lockdown must be reachable only after a to_regclass() check, and it
    must skip (not abort) when the relation is absent."""
    sql = _strip_comments(_M018.read_text(encoding="utf-8"))

    assert re.search(r"to_regclass\s*\(", sql, re.I), (
        "no to_regclass existence check — a missing table will abort with 42P01"
    )
    guard = re.search(r"to_regclass\s*\([^)]*\)\s*is\s+null", sql, re.I)
    assert guard, "to_regclass result is not compared against null"
    # The skip must `continue` past the alter, not raise.
    after_guard = sql[guard.end():]
    assert re.search(r"\bcontinue\b", after_guard, re.I), (
        "the missing-table branch does not `continue` — it would fall through "
        "to the alter and abort"
    )
    assert re.search(r"enable\s+row\s+level\s+security", sql, re.I), (
        "migration no longer enables RLS at all"
    )
    assert re.search(r"revoke\s+all", sql, re.I), (
        "migration no longer revokes anon/authenticated grants"
    )
