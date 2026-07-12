import os
import logging
import httpx
from postgrest import SyncPostgrestClient
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env from project root
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", SUPABASE_KEY) # Fallback to service key if anon key missing, but RLS works with JWT.

_REST_URL = f"{SUPABASE_URL.rstrip('/')}/rest/v1" if SUPABASE_URL else None

# ── Shared connection pool ────────────────────────────────────────────────
# A single httpx.Client (keep-alive pool, HTTP/2) reused by EVERY PostgREST
# client we build — the service-role client below and every per-request
# user-scoped client from get_user_db(). Previously each call to
# get_user_db(jwt) constructed its own SyncPostgrestClient, and each of those
# spun up a brand-new httpx.Client → a fresh TLS + HTTP/2 handshake to Supabase
# on every authenticated request (/api/config, /api/backtest/keys,
# /api/analytics, /api/backtest/slips, deletes, billing reads …), plus a slow
# socket leak as those one-shot clients were never closed.
#
# Auth is applied PER REQUEST: PostgREST's request builder merges each client's
# own headers (apikey + Authorization) into every outgoing request, so two
# users sharing this transport never see each other's credentials — the pool
# only carries raw TCP/TLS connections, not auth state. httpx.Client is
# thread-safe for concurrent requests, which is what our ThreadPoolExecutor
# scrapers and background daemon threads need.
#
# HTTP/1.1 pool (NOT HTTP/2) on purpose. FastAPI runs our sync endpoints in a
# large threadpool, and background daemon threads (results checker, CLV,
# observatory, auto-log) hit Supabase too — so many requests run concurrently.
# Over HTTP/2 they would all multiplex onto ONE shared TCP connection; when the
# Cloudflare/PgBouncer edge in front of Supabase reset or GOAWAY'd that idle-
# then-reused connection, the paginated analytics scans failed and their errors
# were swallowed by evaluate_analytics's per-section try/except → the Analytics
# tab silently showed no data. An HTTP/1.1 keep-alive pool hands each concurrent
# request its own warm connection, so a single bad connection only affects one
# request (httpx discards it and reconnects) and can't blank the whole tab —
# while still eliminating the per-request TLS handshake this pool exists for.
_shared_http_client = None
if _REST_URL:
    try:
        # retries=2 reconnects when a pooled connection turns out to be stale
        # (the edge closed it while idle) before the request is sent, so a
        # reused-but-dead keep-alive connection doesn't surface as a failed
        # request. Only connection-establishment errors are retried; a real
        # HTTP error response is returned as-is.
        _transport = httpx.HTTPTransport(
            http1=True,
            http2=False,
            retries=2,
            limits=httpx.Limits(
                max_keepalive_connections=20,
                max_connections=100,
                keepalive_expiry=60.0,
            ),
        )
        _shared_http_client = httpx.Client(
            base_url=_REST_URL,
            follow_redirects=True,
            transport=_transport,
            # Generous read budget for the paginated analytics scans; fail fast
            # on connect so a Supabase blip doesn't hang a request for 120 s.
            timeout=httpx.Timeout(60.0, connect=10.0),
        )
    except Exception as e:  # pragma: no cover - defensive
        logger.error(f"Failed to create shared httpx client: {e}")
        _shared_http_client = None

db = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates"
        }
        db = SyncPostgrestClient(_REST_URL, headers=headers, http_client=_shared_http_client)
        logger.info("Supabase PostgREST client initialized successfully (shared pool).")
    except Exception as e:
        logger.error(f"Failed to initialize PostgREST client: {e}")
else:
    logger.warning("SUPABASE_URL or SUPABASE_SERVICE_KEY missing from environment.")

def get_db() -> SyncPostgrestClient:
    """Service-role client (bypasses RLS)"""
    return db

def get_user_db(jwt: str) -> SyncPostgrestClient:
    """Supabase client scoped to a user's JWT — RLS applies.

    Cheap to construct: it reuses the process-wide `_shared_http_client`
    connection pool, so no new socket/TLS handshake is created here. Only the
    per-user Authorization header differs, and that's carried on the returned
    client's own headers and merged into each request.
    """
    if not SUPABASE_URL:
        return None
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates"
    }
    return SyncPostgrestClient(_REST_URL, headers=headers, http_client=_shared_http_client)
