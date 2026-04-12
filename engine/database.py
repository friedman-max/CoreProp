import os
import logging
from postgrest import SyncPostgrestClient
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env from project root
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

db = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        # Construct the REST URL (usually SU_URL + /rest/v1)
        rest_url = f"{SUPABASE_URL.rstrip('/')}/rest/v1"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }
        db = SyncPostgrestClient(rest_url, headers=headers)
        logger.info("Supabase PostgREST client initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize PostgREST client: {e}")
else:
    logger.warning("SUPABASE_URL or SUPABASE_SERVICE_KEY missing from environment.")

def get_db() -> SyncPostgrestClient:
    return db
