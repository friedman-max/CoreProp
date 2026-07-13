import os
import logging
from postgrest import SyncPostgrestClient
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env from project root
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", SUPABASE_KEY) # Fallback to service key if anon key missing, but RLS works with JWT.

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
    """Supabase client scoped to a user's JWT — RLS applies."""
    if not SUPABASE_URL:
        return None
    rest_url = f"{SUPABASE_URL.rstrip('/')}/rest/v1"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates"
    }
    return SyncPostgrestClient(rest_url, headers=headers)
