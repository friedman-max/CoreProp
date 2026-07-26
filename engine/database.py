import os
import logging
from postgrest import SyncPostgrestClient
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env from project root
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# ── The anon key is PUBLIC. The service key must never stand in for it. ─────
# SUPABASE_ANON_KEY is served to every visitor: web/app.py::root() injects it
# into index.html and GET /api/ui-config returns it. This line used to read
#     os.environ.get("SUPABASE_ANON_KEY", SUPABASE_KEY)
# so one missing env var silently published the SERVICE-ROLE key, which
# BYPASSES row-level security — full read/write on every table for anyone who
# viewed page source, with nothing logged to notice it by. There is no safe
# degraded mode for that, so we refuse to start: a failed boot costs one
# deploy, a leaked service-role key costs a rotation and can be harvested
# quietly first. Pasting the service key into SUPABASE_ANON_KEY is the same
# hole, so identity with SUPABASE_KEY is rejected too.
_anon_key = os.environ.get("SUPABASE_ANON_KEY") or None
if SUPABASE_KEY and (_anon_key is None or _anon_key == SUPABASE_KEY):
    raise RuntimeError(
        "SUPABASE_ANON_KEY is "
        + ("missing" if _anon_key is None else "set to the SUPABASE_SERVICE_KEY value")
        + ". It is published to the browser, so it must be the project's anon "
        "(publishable) key — never the service-role key, which bypasses RLS. "
        "Set SUPABASE_ANON_KEY from Supabase -> Project Settings -> API -> "
        "'anon public' and restart. Refusing to start rather than exposing the "
        "service key to every visitor."
    )

SUPABASE_ANON_KEY = _anon_key
if SUPABASE_ANON_KEY is None:
    # No service key either, so there is no secret to leak — this is an
    # unconfigured environment (bare import, analysis script). get_user_db()
    # returns None, and callers already handle that.
    logger.warning(
        "SUPABASE_ANON_KEY missing; per-user (RLS-scoped) clients unavailable."
    )

# ── Per-call clients (reverted from the shared connection pool) ────────────
# We build a fresh SyncPostgrestClient (each with its own httpx.Client) per
# call. An earlier optimization shared ONE httpx.Client across every
# service-role and per-user client to skip the per-request TLS handshake.
# That shared pool is the prime suspect for a regression where slips stopped
# reaching the backtest AND analytics/backtest reads came back empty: under
# the production threading model (gunicorn --workers 1 --threads 1 plus the
# scraper ThreadPoolExecutor and the auto-log / CLV / results / observatory
# daemon threads all sharing one pool), a single poisoned/exhausted pooled
# connection can make concurrent DB calls fail — and those failures are
# swallowed by the callers' broad try/except, so writes silently no-op and
# reads silently return []. Per-call clients cannot cross-contaminate that
# way, which is the state that was reliably logging slips before. The minor
# per-request handshake cost is not worth risking the core feature.

db = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        # Construct the REST URL (usually SU_URL + /rest/v1)
        rest_url = f"{SUPABASE_URL.rstrip('/')}/rest/v1"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates"
        }
        db = SyncPostgrestClient(rest_url, headers=headers)
        logger.info("Supabase PostgREST client initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize PostgREST client: {e}")
else:
    logger.warning("SUPABASE_URL or SUPABASE_SERVICE_KEY missing from environment.")

def get_db() -> SyncPostgrestClient:
    """Service-role client (bypasses RLS)"""
    return db

def get_user_db(jwt: str) -> SyncPostgrestClient:
    """Supabase client scoped to a user's JWT — RLS applies.

    The `apikey` header MUST be the anon key: it is what makes this client
    RLS-scoped instead of all-tenant. Without one we return None (callers
    already treat that as "no database") rather than fall back to anything
    with wider privileges.
    """
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return None
    rest_url = f"{SUPABASE_URL.rstrip('/')}/rest/v1"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates"
    }
    return SyncPostgrestClient(rest_url, headers=headers)
