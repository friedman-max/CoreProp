"""
Apply migration_010.sql to the local Supabase project via the service-role
key. This script is intentionally outside the repo's deploy path — keep it
local. Production deploys still apply migrations via the Supabase SQL editor.

Usage:
    python3 analysis/apply_migration_010.py

What it does:
  1. Loads .env to read SUPABASE_URL + SUPABASE_SERVICE_KEY
  2. Reads migration_010.sql from the repo root
  3. Submits the SQL via the Supabase REST `/rpc/exec_sql` if available,
     otherwise prints the SQL with instructions to paste into the SQL
     editor manually (Supabase API doesn't always expose raw DDL via the
     REST surface).
  4. Verifies the two new tables exist by trying a trivial SELECT.

Safe to run multiple times — the migration is fully idempotent
(`create table if not exists`, `create index if not exists`).
"""
import os
import sys
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

if not (SUPABASE_URL and SUPABASE_KEY):
    print("✗ SUPABASE_URL or SUPABASE_SERVICE_KEY missing from .env", file=sys.stderr)
    sys.exit(1)

migration_path = ROOT / "migration_010.sql"
if not migration_path.exists():
    print(f"✗ migration_010.sql not found at {migration_path}", file=sys.stderr)
    sys.exit(1)

sql = migration_path.read_text()
print(f"Loaded migration ({len(sql):,} bytes) from {migration_path}\n")

# --- Try to run the migration via PostgREST's RPC if a helper is exposed. ---
# Most projects don't ship an exec_sql RPC by default; we attempt it but
# fall back to manual instructions on 404 / 401.

import requests

rest_url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/rpc/exec_sql"
headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}
try:
    r = requests.post(rest_url, json={"sql": sql}, headers=headers, timeout=30)
except Exception as exc:
    print(f"RPC call failed: {exc}\n")
    r = None

if r is not None and r.status_code in (200, 204):
    print(f"✓ Migration applied via exec_sql RPC (status {r.status_code}).\n")
else:
    if r is not None:
        print(f"✗ exec_sql RPC not available (status {r.status_code}, body: {r.text[:200]})\n")
    print("Falling back to manual application. Copy the SQL below into the")
    print("Supabase dashboard SQL editor and run it:\n")
    print("─" * 70)
    print(sql)
    print("─" * 70)
    print("\nAfter pasting + running, re-run this script to verify table creation.\n")

# --- Verify both tables exist via REST. ---
from engine.database import get_db
db = get_db()

print("Verifying calibration_cells…")
try:
    res = db.table("calibration_cells").select("league", count="exact").limit(1).execute()
    print(f"  ✓ calibration_cells reachable; row count: {res.count if res.count is not None else 'unknown'}")
except Exception as exc:
    print(f"  ✗ calibration_cells NOT reachable: {exc}")
    print("  → If you haven't pasted the SQL into Supabase yet, do that first.")

print("Verifying calibration_history…")
try:
    res = db.table("calibration_history").select("id", count="exact").limit(1).execute()
    print(f"  ✓ calibration_history reachable; row count: {res.count if res.count is not None else 'unknown'}")
except Exception as exc:
    print(f"  ✗ calibration_history NOT reachable: {exc}")
    print("  → If you haven't pasted the SQL into Supabase yet, do that first.")

print("\nDone. If verification passed, you can start the server with")
print("    USE_RWBC=true python3 main.py")
print("to route live calibration through RWBC.")
