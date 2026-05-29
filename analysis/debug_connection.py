"""Connection sanity check — try the simplest possible queries."""
from common import get_db

db = get_db()
print("db client:", db)

print("\n--- market_observatory: any rows at all? ---")
try:
    r = db.table("market_observatory").select("id, league, result", count="exact").limit(5).execute()
    print(f"  count={r.count}  sample rows: {r.data}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n--- legs: any rows at all? ---")
try:
    r = db.table("legs").select("id, league, result", count="exact").limit(5).execute()
    print(f"  count={r.count}  sample rows: {r.data}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n--- slips: any rows at all? ---")
try:
    r = db.table("slips").select("id, slip_type, n_legs", count="exact").limit(5).execute()
    print(f"  count={r.count}  sample rows: {r.data}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n--- distinct result values on market_observatory ---")
try:
    r = db.table("market_observatory").select("result").limit(2000).execute()
    from collections import Counter
    c = Counter(row.get("result") for row in (r.data or []))
    print(f"  {dict(c)}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n--- distinct league values on market_observatory ---")
try:
    r = db.table("market_observatory").select("league").limit(2000).execute()
    from collections import Counter
    c = Counter(row.get("league") for row in (r.data or []))
    print(f"  {dict(c)}")
except Exception as e:
    print(f"  ERROR: {e}")
