"""Single chokepoint for service-role writes.

Why this module exists
----------------------
`engine.database.get_db()` returns a PostgREST client built with the
Supabase **service key**, which bypasses RLS. Every module in the codebase
imports it and writes freely; there's no audit trail and no single seam
where a future "two-pod split" (API pod + writer pod) could be enforced.

`writer()` is a drop-in replacement for `get_db()` *for write paths only*.
It returns a thin wrapper that:

  1. Logs every `.upsert / .insert / .update / .delete` call with table
     name, row count, and the caller's filename:lineno. Filtered to INFO,
     so the production log stream shows exactly which code paths write.
  2. Tags the write with the *purpose* string the caller supplies, so a
     later audit can answer "what wrote to legs at 03:14 UTC?" without
     having to grep the codebase.
  3. Is the seam at which a future restructure can flip a feature flag
     to route writes to a separate service. Code that already calls
     `writer(...)` is forward-compatible with that change; code that
     calls `get_db()` directly is not.

Migration policy
----------------
- **New code** writing user data, observatory rows, or app_state_cache:
  use `writer("purpose")`. Reads stay on `get_db()`.
- **Old code** can be migrated opportunistically when it's touched for
  other reasons. There's no big-bang sweep planned because the audit
  value is realized progressively as call sites move over.
- **Reads** never go through this module — they're cheaper through the
  raw client and don't need the audit overhead.
"""
from __future__ import annotations

import inspect
import logging
import os
from typing import Any

from engine.database import get_db

logger = logging.getLogger(__name__)

# Toggle structured audit logging. Off by default — flip via env in
# environments where you want to observe writes (staging, debugging).
# Production stays quiet so we don't bloat the log stream.
_AUDIT_ENABLED = os.environ.get("COREPROP_WRITER_AUDIT", "0") == "1"


def _caller_location(skip: int = 3) -> str:
    """Walk back up the stack to the first frame outside this module
    and the postgrest internals. Returns 'file.py:lineno' or '?:?'."""
    try:
        frame = inspect.currentframe()
        for _ in range(skip):
            if frame is None:
                return "?:?"
            frame = frame.f_back
        while frame is not None:
            fname = frame.f_code.co_filename
            if "engine\\writer.py" in fname or "engine/writer.py" in fname:
                frame = frame.f_back
                continue
            if "postgrest" in fname:
                frame = frame.f_back
                continue
            return f"{os.path.basename(fname)}:{frame.f_lineno}"
    except Exception:
        pass
    return "?:?"


class _WriteAuditedTable:
    """Wraps a postgrest table builder. Intercepts the four write verbs
    so audit metadata can be captured; everything else delegates straight
    through. Designed so a caller can drop `writer("x").table(...)` into
    code that previously had `db.table(...)` without other changes."""

    __slots__ = ("_inner", "_purpose", "_table")

    def __init__(self, inner, purpose: str, table_name: str):
        self._inner = inner
        self._purpose = purpose
        self._table = table_name

    def _audit(self, verb: str, n_rows: int | None = None):
        if not _AUDIT_ENABLED:
            return
        loc = _caller_location()
        rows_part = f" rows={n_rows}" if n_rows is not None else ""
        logger.info(
            "writer audit: %s.%s [%s]%s from %s",
            self._table, verb, self._purpose, rows_part, loc,
        )

    def __getattr__(self, name):
        # Delegate reads / filters / etc. unchanged.
        return getattr(self._inner, name)

    # ── Write verbs (intercepted) ─────────────────────────────────────

    def insert(self, rows, *args, **kwargs):
        try:
            n = len(rows) if isinstance(rows, (list, tuple)) else 1
        except Exception:
            n = None
        self._audit("insert", n)
        return self._inner.insert(rows, *args, **kwargs)

    def upsert(self, rows, *args, **kwargs):
        try:
            n = len(rows) if isinstance(rows, (list, tuple)) else 1
        except Exception:
            n = None
        self._audit("upsert", n)
        return self._inner.upsert(rows, *args, **kwargs)

    def update(self, payload, *args, **kwargs):
        self._audit("update")
        return self._inner.update(payload, *args, **kwargs)

    def delete(self, *args, **kwargs):
        self._audit("delete")
        return self._inner.delete(*args, **kwargs)


class _Writer:
    """Top-level facade returned by `writer(purpose)`. Mirrors the small
    slice of the postgrest API that production code uses for writes."""

    __slots__ = ("_inner", "_purpose")

    def __init__(self, inner, purpose: str):
        self._inner = inner
        self._purpose = purpose

    def table(self, name: str) -> _WriteAuditedTable:
        if self._inner is None:
            # No-op shim so callers can write `writer("x").table("y").upsert(...)`
            # without checking for None. The underlying execute() will raise
            # the same way `get_db().table(...)` would.
            return _WriteAuditedTable(None, self._purpose, name)
        return _WriteAuditedTable(self._inner.table(name), self._purpose, name)

    def rpc(self, *args, **kwargs):
        # Stored procedures called via PostgREST RPC. Currently unused but
        # the audit path is here for free once we migrate writes to
        # security-definer functions (the recommended end-state).
        if _AUDIT_ENABLED:
            logger.info("writer audit: rpc [%s] %s", self._purpose, args[0] if args else "?")
        if self._inner is None:
            raise RuntimeError("Supabase client unavailable")
        return self._inner.rpc(*args, **kwargs)


def writer(purpose: str) -> _Writer:
    """Return an audited write surface tagged with `purpose`.

    Usage:
        writer("observatory.log").table("market_observatory").upsert(rows).execute()

    `purpose` should be a short dotted name that identifies the *write
    use case*, not the function it's called from. Examples:
        - "observatory.log"     — pipeline logging matched lines
        - "observatory.resolve" — results_checker setting hit/miss/push
        - "user_config.update"  — user preference write
        - "clv.update_closing"  — CLV tracker setting closing_prob

    Multiple call sites for the same purpose share a tag; the audit log
    then aggregates by purpose, not by callsite.
    """
    return _Writer(get_db(), purpose)
