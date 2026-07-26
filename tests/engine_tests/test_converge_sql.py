"""`migrations/converge.sql` must be safe to run against a project in ANY state.

The problem it solves
---------------------
`migrations/migration_0NN.sql` are the source of truth and history;
`migrations/schema.sql` is the flattened fresh-project bootstrap and its own
header says "use this OR the numbered migrations, not both". Neither answers the
only question an operator actually has in front of a live project: *am I up to
date, and how do I get there from here?*

That gap has already cost us once. migration_018 aborted with
``ERROR: 42P01: relation "strategy_performance_compare" does not exist`` because
migration_013 had never been applied — and because Supabase's SQL editor runs a
script as ONE transaction, the abort rolled back the tables that DID exist, so
nothing was secured.

`converge.sql` is the answer: one idempotent, non-destructive, existence-guarded
script that converges structure and then reports the resulting state.

Why these assertions are static
-------------------------------
There is no local Postgres in this repo or in CI, so this file never executes
the SQL — it validates the properties that make the SQL *safe to execute*:

  * nothing destructive (a converge script that can drop a table or delete rows
    is more dangerous than the drift it fixes),
  * no DML at all — one-time data repairs must not be re-run (migration_009's
    backfill is the reason migration_014 exists; re-running it re-breaks the
    rows 014 repaired),
  * every DDL statement guarded on its target existing, so a missing relation
    SKIPS instead of aborting the transaction and taking its siblings with it,
  * every existence check schema-qualified — an unqualified ``to_regclass()``
    resolves through ``search_path``, so without ``public`` every check returns
    NULL and the script silently converges NOTHING while reporting success,
  * Supabase-only roles (``anon`` / ``authenticated``) guarded on ``pg_roles``
    so a plain Postgres restore doesn't abort,
  * and coverage: every table, column and index `schema.sql` declares is also
    handled by `converge.sql`. That last one is what keeps the file honest as
    the schema grows — converge can't silently forget a table.
"""
from __future__ import annotations

import re
from pathlib import Path

_MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations"
_CONVERGE = _MIGRATIONS / "converge.sql"
_SCHEMA = _MIGRATIONS / "schema.sql"


def _strip_comments(text: str) -> str:
    """Drop line comments so prose can't satisfy (or violate) an assertion."""
    return "\n".join(line.split("--", 1)[0] for line in text.splitlines())


def _raw() -> str:
    """converge.sql WITH comments — for the documentation assertions."""
    return _CONVERGE.read_text(encoding="utf-8")


def _sql() -> str:
    return _strip_comments(_raw())


def _schema_sql() -> str:
    return _strip_comments(_SCHEMA.read_text(encoding="utf-8"))


def _blank_literals(text: str) -> tuple[str, list[str]]:
    """Blank out every single-quoted literal; return (blanked, literals).

    Needed because ``raise notice 'converge.sql will not set not null'`` is
    prose, not a statement — a naive scan for destructive keywords would flag
    the very comment that documents the safety property. Dollar-quoted bodies
    are left intact: they carry real DDL.
    """
    out: list[str] = []
    literals: list[str] = []
    i, n = 0, len(text)
    while i < n:
        if text[i] != "'":
            out.append(text[i])
            i += 1
            continue
        j = i + 1
        buf: list[str] = []
        while j < n:
            if text[j] == "'":
                if j + 1 < n and text[j + 1] == "'":  # '' is an escaped quote
                    buf.append("''")
                    j += 2
                    continue
                break
            buf.append(text[j])
            j += 1
        body = "".join(buf)
        literals.append(body)
        out.append("'" + " " * len(body) + "'")
        i = j + 1
    return "".join(out), literals


def _statements() -> str:
    """converge.sql with comments AND string literals removed — what Postgres
    would actually parse as statements."""
    return _blank_literals(_sql())[0]


def _dynamic_sql() -> list[str]:
    """Every literal that is reached via ``execute`` — i.e. SQL this file runs
    dynamically. A destructive statement hidden in an execute string is just as
    destructive as a static one, so these are scanned too."""
    sql = _sql()
    blanked, _ = _blank_literals(sql)
    found: list[str] = []
    for m in re.finditer(r"'( *)'", blanked):
        raw_literal = sql[m.start() + 1 : m.end() - 1]
        # Look back to the start of the statement this literal belongs to.
        prev = blanked.rfind(";", 0, m.start())
        if re.search(r"\bexecute\b", blanked[prev + 1 : m.start()], re.I):
            found.append(raw_literal)
    # Dollar-quoted bodies are executed DDL too. Match each tag SEPARATELY:
    # a single `(\$\w+\$)(.*?)\1` sweep starts at the outermost tag and its
    # non-greedy body swallows every nested `$ddl$` block, hiding them.
    # Outer `do $tag$` bodies are skipped — _statements() already covers them.
    outer = {t.lower() for t in re.findall(r"^do\s+(\$\w*\$)", sql, re.I | re.M)}
    for tag in {t.lower() for t in re.findall(r"\$\w+\$", sql)} - outer:
        found += re.findall(re.escape(tag) + r"(.*?)" + re.escape(tag), sql, re.S)
    # Literals inside those bodies are prose too (raise messages), so blank them.
    return [_blank_literals(f)[0] for f in found]


def _do_blocks(sql: str) -> list[str]:
    """Bodies of every top-level ``do $tag$ … $tag$;`` block.

    The backreference makes nesting work: a ``$fn$``-quoted function body inside
    a ``$converge$`` block does not terminate the outer block.
    """
    return [body for _tag, body in re.findall(r"^do\s+(\$\w*\$)(.*?)\1\s*;", sql, re.I | re.M | re.S)]


def _split_args(text: str) -> list[str]:
    """Split a `raise` argument tail on commas at paren-depth 0, so
    ``array_to_string(a, ', ')`` counts as ONE argument, not two."""
    out: list[str] = []
    depth = 0
    buf: list[str] = []
    for ch in text:
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        if ch == "," and depth == 0:
            out.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    out.append("".join(buf))
    return out


def _lines_containing(sql: str, pattern: str) -> list[str]:
    rx = re.compile(pattern, re.I)
    return [ln for ln in sql.splitlines() if rx.search(ln)]


# ── the file exists and parses the way the rest of this module assumes ────────


def test_converge_sql_exists():
    assert _CONVERGE.is_file(), (
        f"{_CONVERGE} is missing — operators have no way to answer "
        '"am I up to date?"'
    )


def test_parser_finds_do_blocks():
    """Guard the parser itself: if the block splitter stops matching, every
    'each DDL block is guarded' assertion below would vacuously pass."""
    blocks = _do_blocks(_sql())
    assert len(blocks) >= 5, f"only {len(blocks)} do-blocks parsed"
    assert any("to_regclass" in b for b in blocks)


def test_literal_stripper_is_not_vacuous():
    """The destructive-statement scan runs against literal-stripped SQL, so the
    stripper itself has to work — otherwise a real `drop table` inside a string
    would be missed, and prose would be flagged as code."""
    blanked, literals = _blank_literals("select 'drop table x' , foo; -- k")
    assert "drop table" not in blanked
    assert "drop table x" in literals
    # Doubled quotes are an escape, not a terminator.
    blanked2, lits2 = _blank_literals("a 'it''s truncate' b")
    assert "truncate" not in blanked2 and "it''s truncate" in lits2
    # And the real file must still contain parseable statements.
    assert "to_regclass" in _statements()
    assert len(_dynamic_sql()) >= 8, "execute'd SQL is not being collected"


# ── 1. never destructive ──────────────────────────────────────────────────────

# `drop policy` / `drop trigger` / `drop constraint` are deliberately allowed:
# Postgres has no CREATE POLICY IF NOT EXISTS or ADD CONSTRAINT IF NOT EXISTS,
# so drop-then-recreate is the only idempotent idiom and it is what every
# existing migration in this directory already does.
_DESTRUCTIVE = {
    "drop table": r"\bdrop\s+table\b",
    "drop column": r"\bdrop\s+column\b",
    "drop index": r"\bdrop\s+index\b",
    "drop schema": r"\bdrop\s+schema\b",
    "drop database": r"\bdrop\s+database\b",
    "drop function": r"\bdrop\s+function\b",
    "drop type": r"\bdrop\s+type\b",
    "truncate": r"\btruncate\b",
    "delete from": r"\bdelete\s+from\b",
}


def test_converge_contains_no_destructive_statement():
    """Scans STATEMENTS (literals blanked) so the prose that documents the
    safety property isn't mistaken for a violation, plus every dynamically
    execute'd string, where a hidden `drop table` would be just as fatal."""
    statements = _statements()
    found = {name: _lines_containing(statements, rx) for name, rx in _DESTRUCTIVE.items()}
    found = {k: v for k, v in found.items() if v}
    assert not found, (
        "converge.sql must never destroy schema or data; found: "
        + "; ".join(f"{k} -> {v}" for k, v in found.items())
    )

    for snippet in _dynamic_sql():
        for name, rx in _DESTRUCTIVE.items():
            assert not re.search(rx, snippet, re.I), (
                f"execute'd SQL contains {name}: {snippet.strip()[:120]}"
            )


def test_converge_never_sets_a_column_not_null():
    """migration_001 leaves `user_id … set not null` as a MANUAL step on
    purpose — it fails on any legacy row with a NULL user_id, and on Supabase
    that abort rolls back the whole script. converge.sql reports the condition
    instead of forcing it."""
    assert not _lines_containing(_statements(), r"\bset\s+not\s+null\b"), (
        "converge.sql tries to SET NOT NULL; that aborts on pre-existing NULLs"
    )
    for snippet in _dynamic_sql():
        assert not re.search(r"\bset\s+not\s+null\b", snippet, re.I), (
            f"execute'd SQL sets NOT NULL: {snippet.strip()[:120]}"
        )


def test_converge_runs_no_dml():
    """One-time data repairs (migration_005/006/009/011 backfills,
    migration_014, repair_duplicate_legs.sql) must NOT be re-run. converge.sql
    converges STRUCTURE only.

    `select count(*)` inside the report is fine — reads are not mutations.
    """
    statements = _statements()
    for pattern, why in (
        (r"^\s*update\s+", "an UPDATE re-runs a one-time backfill"),
        (r"^\s*insert\s+into\s+", "an INSERT mutates data"),
    ):
        offenders = [ln for ln in statements.splitlines() if re.search(pattern, ln, re.I)]
        assert not offenders, f"{why}: {offenders}"

    for snippet in _dynamic_sql():
        assert not re.search(r"^\s*(update|insert)\b", snippet.strip(), re.I), (
            f"execute'd SQL mutates data: {snippet.strip()[:120]}"
        )


def test_converge_documents_how_it_treats_one_time_repairs():
    raw = _raw()
    for token in ("migration_014", "repair_duplicate_legs", "migration_011"):
        assert token in raw, (
            f"converge.sql does not say what it does about {token}; an operator "
            "cannot tell whether the data repairs were handled"
        )


# ── 2. every DDL statement is existence-guarded ───────────────────────────────


def test_no_bare_alter_table():
    """A top-level `alter table` on a relation that does not exist raises 42P01
    and, inside Supabase's single-transaction editor, rolls back everything the
    script had already converged."""
    sql = _sql()
    bare = [ln for ln in sql.splitlines() if re.match(r"\s*alter\s+table\b", ln, re.I)]
    assert not bare, f"unguarded top-level alter table: {bare}"

    # Every remaining occurrence must be built through format() inside a block.
    for line in _lines_containing(sql, r"\balter\s+table\b"):
        assert "format(" in line.lower(), f"alter table not built via format(): {line.strip()}"


def test_no_bare_create_or_drop_on_a_relation():
    """Same failure mode for indexes, policies and triggers: all three abort
    with 42P01 when the table is absent."""
    sql = _sql()
    for pattern, label in (
        (r"\s*create\s+(unique\s+)?index\b", "create index"),
        (r"\s*(create|drop)\s+policy\b", "policy"),
        (r"\s*(create|drop)\s+trigger\b", "trigger"),
    ):
        bare = [ln for ln in sql.splitlines() if re.match(pattern, ln, re.I)]
        assert not bare, f"unguarded top-level {label}: {bare}"


def test_every_ddl_block_checks_its_target_exists():
    blocks = _do_blocks(_sql())
    ddl = re.compile(
        r"\b(alter\s+table|create\s+(unique\s+)?index|create\s+policy|create\s+trigger)\b",
        re.I,
    )
    for i, body in enumerate(blocks):
        if ddl.search(body):
            assert "to_regclass" in body.lower(), (
                f"do-block #{i} emits DDL without a to_regclass() existence check"
            )


def test_missing_relation_skips_instead_of_aborting():
    sql = _sql()
    guard = re.search(r"to_regclass\s*\([^)]*\)\s*is\s+null", sql, re.I)
    assert guard, "no `to_regclass(...) is null` guard — a missing table aborts"
    assert re.search(r"\bcontinue\b", sql[guard.end():], re.I), (
        "the missing-relation branch never `continue`s past the DDL"
    )
    assert re.search(r"raise\s+notice", sql, re.I), "skips are silent"


def test_create_table_is_if_not_exists():
    for m in re.finditer(r"create\s+table\s+(\S+)", _sql(), re.I):
        assert m.group(1).lower() == "if", (
            f"`create table {m.group(1)}` is not IF NOT EXISTS — it aborts on re-run"
        )


def test_constraint_and_index_adds_tolerate_hostile_existing_data():
    """`add constraint … check` and `create unique index` both abort when
    pre-existing rows violate them. Those must degrade to a warning, not take
    the transaction down."""
    sql = _sql()
    assert re.search(r"\bexception\s+when\b", sql, re.I), (
        "no PL/pgSQL exception handler — a check constraint that existing data "
        "violates would abort the whole converge run"
    )
    assert re.search(r"drop\s+constraint\s+if\s+exists", sql, re.I), (
        "constraints are not dropped before being re-added; Postgres has no "
        "ADD CONSTRAINT IF NOT EXISTS, so this is not idempotent"
    )


# ── 3. schema-qualified everywhere ────────────────────────────────────────────


def test_every_to_regclass_argument_is_schema_qualified():
    """An unqualified to_regclass() resolves via search_path. If `public` were
    absent from it, EVERY check would return NULL, every table would look
    missing, and converge.sql would report success having done nothing."""
    calls = re.findall(r"to_regclass\s*\(\s*'([^']*)'", _sql(), re.I)
    assert calls, "no to_regclass() calls found at all"
    unqualified = [c for c in calls if "." not in c]
    assert not unqualified, f"unqualified to_regclass() argument(s): {unqualified}"


def test_ddl_names_are_schema_qualified():
    sql = _sql()
    for line in _lines_containing(sql, r"\b(alter\s+table|create\s+(unique\s+)?index)\b"):
        assert "public." in line, f"object name is not schema-qualified: {line.strip()}"


def test_warns_when_public_is_not_resolvable():
    """The silent-no-op failure mode has to be called out loudly, exactly as
    migration_018 does."""
    sql = _sql()
    assert re.search(r"to_regnamespace\s*\(\s*'public'\s*\)", sql, re.I), (
        "converge.sql never checks that the `public` schema is resolvable"
    )
    assert re.search(r"raise\s+warning", sql, re.I), "no raise warning anywhere"


# ── 4. Supabase-only roles and schemas are guarded ────────────────────────────


def test_role_grants_are_pg_roles_guarded():
    sql = _sql()
    assert _lines_containing(sql, r"\brevoke\b"), "converge.sql revokes nothing"
    assert not [ln for ln in sql.splitlines() if re.match(r"\s*revoke\b", ln, re.I)], (
        "top-level revoke — aborts on a Postgres without Supabase's roles"
    )
    for i, body in enumerate(_do_blocks(sql)):
        if re.search(r"\brevoke\b", body, re.I):
            assert re.search(r"pg_roles\s+where\s+rolname", body, re.I), (
                f"do-block #{i} revokes without a pg_roles existence check"
            )


def test_auth_schema_dependency_is_guarded():
    """`references auth.users(id)` and `auth.uid()` only exist on Supabase."""
    sql = _sql()
    assert re.search(r"to_regclass\s*\(\s*'auth\.users'\s*\)", sql, re.I), (
        "the auth.users foreign key is not guarded on auth.users existing"
    )


# ── 5. coverage — converge cannot silently forget part of the schema ──────────


def _schema_tables() -> set[str]:
    return set(
        re.findall(r"create\s+table\s+(?:if\s+not\s+exists\s+)?(\w+)", _schema_sql(), re.I)
    )


def _schema_added_columns() -> set[str]:
    return set(
        re.findall(r"add\s+column\s+if\s+not\s+exists\s+(\w+)", _schema_sql(), re.I)
    )


def _schema_indexes() -> set[str]:
    return set(
        re.findall(
            r"create\s+(?:unique\s+)?index\s+(?:if\s+not\s+exists\s+)?(\w+)",
            _schema_sql(),
            re.I,
        )
    )


def test_coverage_parsers_are_not_vacuous():
    assert {"slips", "legs", "user_config", "app_state_cache"} <= _schema_tables()
    assert {"dedup_key", "raw_true_prob", "closing_books"} <= _schema_added_columns()
    assert {"idx_legs_slip", "legs_user_dedup_key_unique"} <= _schema_indexes()


def test_converge_handles_every_table_schema_declares():
    sql = _sql()
    missing = sorted(t for t in _schema_tables() if not re.search(rf"\b{t}\b", sql))
    assert not missing, f"converge.sql never mentions table(s): {missing}"


def test_converge_enables_rls_on_every_table_schema_declares():
    """Same invariant test_rls_coverage pins on schema.sql: a table reachable
    with the published anon key and no RLS is readable and writable by any
    visitor."""
    sql = _sql()
    block = next(
        (b for b in _do_blocks(sql) if re.search(r"enable\s+row\s+level\s+security", b, re.I)),
        None,
    )
    assert block, "converge.sql never enables row level security"
    missing = sorted(t for t in _schema_tables() if f"'{t}'" not in block)
    assert not missing, f"RLS not converged for: {missing}"


def test_converge_adds_every_column_schema_adds():
    sql = _sql()
    missing = sorted(c for c in _schema_added_columns() if not re.search(rf"\b{c}\b", sql))
    assert not missing, f"converge.sql never adds column(s): {missing}"


def test_converge_creates_every_index_schema_creates():
    sql = _sql()
    missing = sorted(i for i in _schema_indexes() if not re.search(rf"\b{i}\b", sql))
    assert not missing, f"converge.sql never creates index(es): {missing}"


def test_converge_carries_the_user_config_check_constraints():
    """Scoped to the block that actually CONVERGES constraints. Checking the
    whole file would pass on a constraint that only survives in the
    verification report's expected-list — reported as missing forever, never
    added. (That exact hole was found by a mutation check.)"""
    sql = _sql()
    converging = [b for b in _do_blocks(sql) if re.search(r"drop\s+constraint\s+if\s+exists", b, re.I)]
    assert converging, "no do-block drops-and-re-adds constraints"
    body = "\n".join(converging)
    expected = set(re.findall(r"add\s+constraint\s+(\w+)", _schema_sql(), re.I))
    assert expected, "schema.sql declares no constraints — assertion is vacuous"
    missing = sorted(n for n in expected if n not in body)
    assert not missing, f"converge.sql never adds check constraint(s): {missing}"

    # And the report must expect every one of them, so drift is visible.
    report = _do_blocks(sql)[-1]
    unreported = sorted(n for n in expected if n not in report)
    assert not unreported, f"verification report never checks: {unreported}"


def test_converge_converges_the_migration_011_default_bump():
    """migration_004 created auto_slip_min_prob with default 0.5407 and
    migration_011 raised it to 0.60. `add column if not exists` is a no-op on a
    project that already ran 004, so the default has to be set explicitly or
    the project never converges."""
    sql = _sql()
    assert re.search(r"alter\s+column\s+%?I?\s*", sql, re.I)
    assert re.search(r"set\s+default", sql, re.I), "no `set default` — 011 never converges"
    assert "0.60" in sql or "0.6" in sql


def test_converge_installs_the_final_trigger_body():
    """migration_017 fixed migration_009's trigger: an already-graded row stays
    frozen but pending -> graded is allowed. The pre-017 body silently killed
    observatory resolution for a month."""
    sql = _sql()
    assert "market_observatory_upsert_guard" in sql
    assert re.search(
        r"if\s+old\.result\s+is\s+distinct\s+from\s+'pending'\s+then", sql, re.I
    ), "the trigger body is migration_009's, which blocks grading (migration_017)"
    assert re.search(r"create\s+or\s+replace\s+function", sql, re.I)


def test_converge_keeps_the_owner_policies_and_the_public_read_policy():
    sql = _sql()
    assert re.search(r"user_id\s*=\s*auth\.uid\(\)", sql, re.I), (
        "no `user_id = auth.uid()` owner policy — slips/legs/user_config would "
        "be readable across tenants"
    )
    assert re.search(r"for\s+select\s+using\s*\(\s*true\s*\)", sql, re.I), (
        "market_observatory's deliberate public-read policy is missing"
    )


def test_converge_does_not_revoke_the_frontend_tables():
    """The browser talks to slips / legs / user_config / market_observatory
    directly with the anon key + the user's JWT. Revoking those grants would
    break the app; only the four server-only tables get revoked."""
    sql = _sql()
    block = next((b for b in _do_blocks(sql) if re.search(r"\brevoke\b", b, re.I)), None)
    assert block, "nothing is revoked"
    for table in ("slips", "legs", "user_config", "market_observatory"):
        assert f"'{table}'" not in block, (
            f"converge.sql revokes anon/authenticated grants on {table}; the "
            "frontend reads it with the published anon key"
        )


# ── 6. the verification report ────────────────────────────────────────────────


def test_converge_ends_with_a_verification_report():
    """The operator must be able to tell "I am current" from the output alone."""
    sql = _sql()
    tail = sql[len(sql) // 2 :]
    for probe, why in (
        (r"relrowsecurity|rowsecurity", "RLS state is never reported"),
        (r"information_schema\.columns|pg_attribute", "column presence is never reported"),
        (r"pg_policy|pg_policies", "policy presence is never reported"),
        (r"pg_index|pg_indexes|pg_class", "index presence is never reported"),
        (r"pg_trigger", "trigger presence is never reported"),
        (r"raise\s+warning", "nothing is flagged loudly"),
    ):
        assert re.search(probe, tail, re.I), f"verification report: {why}"


def test_report_checks_exactly_what_converge_creates():
    """The report's expected-index list and the converge block's create-index
    list must be the same set, or the report lies in one direction or the other
    (silently ignoring a missing index, or forever reporting a phantom)."""
    sql = _sql()
    split = sql.index(_do_blocks(sql)[-1])
    converged = set(re.findall(r"\('(idx_\w+|legs_user_dedup_key_unique)',\s*'", sql[:split]))
    reported = set(re.findall(r"'(idx_\w+|legs_user_dedup_key_unique)'", sql[split:]))
    assert converged, "no indexes parsed from the converge block"
    assert converged == reported, (
        f"converge-only: {sorted(converged - reported)}; report-only: {sorted(reported - converged)}"
    )


def test_report_hardcoded_counts_match_its_lists():
    """`all 19 expected indexes present` must not drift from the actual list."""
    raw = _raw()
    report = raw[raw.index("VERIFICATION REPORT") :]
    for label, pattern in (
        ("indexes", r"all (\d+) expected indexes"),
        ("constraints", r"all (\d+) user_config checks"),
    ):
        claimed = re.search(pattern, report)
        assert claimed, f"report no longer states a total for {label}"
        if label == "indexes":
            actual = len(set(re.findall(r"'(idx_\w+|legs_user_dedup_key_unique)'", report)))
        else:
            actual = len(set(re.findall(r"'(user_config_\w+_chk)'", report)))
        assert int(claimed.group(1)) == actual, (
            f"report claims {claimed.group(1)} {label} but lists {actual}"
        )


def test_verification_report_states_a_verdict():
    raw = _raw().upper()
    assert "MISSING" in raw
    assert "CONVERG" in raw


def test_report_flags_the_data_repairs_it_refuses_to_run():
    """converge.sql runs no DML, so it must at least tell the operator when a
    one-time repair still looks necessary."""
    raw = _raw()
    assert "first_seen_at" in raw and "game_start" in raw, (
        "the migration_014 first_seen_at artifact is never detected"
    )
    assert re.search(r"leg_num", raw), (
        "duplicate-leg detection (repair_duplicate_legs.sql) is never reported"
    )


# ── 7. documentation the operator depends on ──────────────────────────────────


def test_converge_documents_the_reconstructed_base_tables():
    """slips / legs / app_state_cache predate migration_001 and were created in
    the Supabase dashboard; their DDL here is reverse-engineered from app code,
    not the original. An operator must not be told otherwise."""
    raw = _raw().lower()
    assert "reconstruct" in raw
    for table in ("slips", "legs", "app_state_cache"):
        assert table in raw
    assert "original ddl" in raw or "not original" in raw or "app code" in raw


def test_converge_documents_that_it_is_re_runnable():
    raw = _raw().lower()
    assert "idempotent" in raw
    assert "re-run" in raw or "rerun" in raw


def test_dollar_quoted_delimiters_balance():
    """An odd count means the rest of the file is inside a string literal and
    silently does nothing."""
    raw = _raw()
    for tag in set(re.findall(r"\$\w*\$", raw)):
        count = len(re.findall(re.escape(tag), raw))
        assert count % 2 == 0, f"unbalanced dollar-quote {tag}: {count} occurrences"

    sql = _sql()
    opens = len(re.findall(r"^do\s+\$\w*\$", sql, re.I | re.M))
    blocks = len(_do_blocks(sql))
    assert opens == blocks, f"{opens} `do $tag$` openers but only {blocks} closed blocks"


def test_foreach_loop_variables_are_scalars():
    """`FOREACH x IN ARRAY` requires x to be a SCALAR of the array's element
    type. Declaring it `record` is accepted by the parser and fails at RUNTIME
    with "FOREACH ... IN ARRAY loop variable must be of a scalar type" — which,
    inside Supabase's single-transaction editor, aborts the whole run. Found by
    a hand audit; pinned here so it cannot come back."""
    sql = _sql()
    offenders: list[str] = []
    for m in re.finditer(r"foreach\s+(\w+)\s+in\s+array", sql, re.I):
        var = m.group(1)
        start = sql.rfind("declare", 0, m.start())
        block = sql[start : sql.find("begin", start)]
        decl = re.search(rf"^\s*{var}\s+([^\s;:]+)", block, re.M | re.I)
        if decl and decl.group(1).lower() in {"record", "row"}:
            offenders.append(f"{var} declared {decl.group(1)}")
    assert not offenders, f"FOREACH IN ARRAY over a non-scalar variable: {offenders}"


def _raise_statements() -> list[tuple[str, list[str]]]:
    """Every ``raise notice|warning`` as (format_string, [args]).

    Parsed against literal-blanked text so a ``;`` or a quote INSIDE a message
    can't end the statement early, and so the format string is only the LEADING
    run of adjacent literals (Postgres concatenates those) — a literal appearing
    later, like the separator in ``array_to_string(a, ', ')``, is part of an
    argument, not of the message.
    """
    sql = _sql()
    blanked, _ = _blank_literals(sql)
    spans = [(m.start(), m.end()) for m in re.finditer(r"'[^']*'", blanked)]

    def literal_at(pos: int) -> tuple[int, int] | None:
        return next((s for s in spans if s[0] == pos), None)

    out: list[tuple[str, list[str]]] = []
    for m in re.finditer(r"\braise\s+(?:notice|warning)\s+", blanked, re.I):
        end = blanked.find(";", m.end())
        stmt_end = len(blanked) if end == -1 else end
        # Walk the leading run of adjacent string literals.
        i = m.end()
        parts: list[str] = []
        while True:
            while i < stmt_end and blanked[i] in " \t\r\n":
                i += 1
            span = literal_at(i)
            if span is None or span[1] > stmt_end:
                break
            parts.append(sql[span[0] + 1 : span[1] - 1].replace("''", "'"))
            i = span[1]
        rest = sql[i:stmt_end]
        args = [a for a in _split_args(rest.lstrip().lstrip(",")) if a.strip()]
        out.append(("".join(parts), args))
    return out


def test_raise_placeholder_count_matches_its_arguments():
    """PL/pgSQL `raise` uses bare `%` placeholders and aborts at RUNTIME with
    "too many parameters specified for RAISE" (or too few) on a mismatch.
    Inside Supabase's single-transaction editor that abort rolls back the whole
    converge run, so the counts have to be right."""
    stmts = _raise_statements()
    assert len(stmts) > 40, f"only {len(stmts)} raise statements parsed — vacuous"
    mismatches = [
        f"{len(re.findall(r'(?<!%)%(?!%)', msg))} placeholder(s) vs "
        f"{len(args)} arg(s): {msg[:70]!r}"
        for msg, args in stmts
        if len(re.findall(r"(?<!%)%(?!%)", msg)) != len(args)
    ]
    assert not mismatches, "raise argument mismatch -> runtime abort: " + "; ".join(mismatches)


def test_every_loop_is_closed():
    """`foreach … loop` / `for … loop` each contribute two `loop` tokens (the
    opener and the `end loop`), so opener count must equal `end loop` count."""
    sql = _statements().lower()
    end_loop = len(re.findall(r"\bend\s+loop\b", sql))
    all_loop = len(re.findall(r"\bloop\b", sql))
    assert end_loop > 0, "no loops parsed — the balance check would be vacuous"
    assert all_loop == 2 * end_loop, (
        f"{all_loop - end_loop} loop opener(s) but {end_loop} `end loop`"
    )


def test_every_if_is_closed():
    """Every `then` closes with `end if`, EXCEPT the ones belonging to
    `exception when … then` (closed by the sub-block's own `end`) and `elsif`
    (which shares its parent's `end if`)."""
    sql = _statements().lower()
    then = len(re.findall(r"\bthen\b", sql))
    end_if = len(re.findall(r"\bend\s+if\b", sql))
    elsif = len(re.findall(r"\belsif\b", sql))
    exc = len(re.findall(r"\bexception\s+when\b", sql))
    assert end_if > 0, "no if-blocks parsed — the balance check would be vacuous"
    assert then == end_if + elsif + exc, (
        f"unbalanced: {then} `then` vs {end_if} `end if` + {elsif} elsif + {exc} exception"
    )


def test_every_block_is_closed():
    """`declare`/`begin` … `end` nesting: each `begin` needs one `end`."""
    sql = _statements().lower()
    begins = len(re.findall(r"\bbegin\b", sql))
    # `end;`, `end $tag$` (outer do-blocks) and `end` before a language clause.
    ends = len(re.findall(r"\bend\s*(;|\$\w*\$)", sql))
    assert begins > 0
    assert begins == ends, f"{begins} `begin` vs {ends} closing `end`"
