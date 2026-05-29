"""
ANALYSIS 10 — Expected RWBC slip volume from the past 10 days.

Approach:
  1. Fit RWBC cells (same as probe 06) from the full settled observatory
  2. Pull every slip + leg logged in the past 10 days, joined to user_config
     to get each user's min_prob threshold
  3. For each leg, re-score through RWBC using raw_true_prob + (league,
     prop, side). Mark calibration_halted if the cell halted. Mark
     fails_threshold if calibrated prob < user's min_prob.
  4. A slip "survives RWBC" iff ALL its legs survive (no halt + clear threshold)
  5. Aggregate per-day per-user: how many original slips, how many survive,
     survival rate. Project forward to "expected slips per day under RWBC."

Caveats (stated up front):
  - Lower bound. RWBC could build DIFFERENT slips from the same daily
    candidate pool that don't match what isotonic logged. The true RWBC
    daily volume is `survived ≤ RWBC ≤ original × pool_efficiency`.
  - Per-user threshold uses CURRENT auto_slip_min_prob, not the threshold
    that was active when each historical slip was logged (the user may
    have changed it). Approximation.
"""
from common import fetch_all, attach_y, settled, bucket_prob, beta_binomial_posterior, pretty_print_table
import pandas as pd
from datetime import datetime, timedelta, timezone

print("Loading settled market_observatory…")
obs = fetch_all(
    "market_observatory", "league, prop, side, true_prob, raw_true_prob, result",
    filters={"result": ("neq", "pending"), "league": ("in_", ["NBA", "WNBA", "MLB", "NHL"])},
    order_col="resolved_at",
)
obs = settled(attach_y(obs))
obs = obs[obs["true_prob"].notna() & obs["side"].notna()].copy()
obs["p"] = obs["true_prob"].astype(float)
obs["side_u"] = obs["side"].str.upper()
obs["bucket"] = obs["p"].apply(lambda p: bucket_prob(p, 0.05))
GLOBAL_HIT = obs["y"].mean()

HALT = 0.20
def fit_cell(g):
    if len(g) < 5: return None
    mp = g["p"].mean()
    bg = g.groupby("bucket").agg(n=("y","size"), pp=("p","mean"), po=("y","mean")).reset_index()
    if len(bg) < 2: return {"w": 0.0, "p_post": (g["y"].mean()*len(g) + GLOBAL_HIT*30)/(len(g)+30)}
    res = (bg["n"] * (bg["pp"] - mp) ** 2).sum() / len(g)
    rel = (bg["n"] * (bg["pp"] - bg["po"]) ** 2).sum() / len(g)
    w = float(max(0.0, min(1.0, res / (res + rel + 1e-6))))
    k = int(g["y"].sum())
    pm, _, _ = beta_binomial_posterior(k, len(g), GLOBAL_HIT, 30.0)
    return {"w": w, "p_post": pm}
cells = {}
for key, g in obs.groupby(["league", "prop", "side_u"]):
    s = fit_cell(g)
    if s: cells[key] = s
print(f"  RWBC cells fit: {len(cells)}")

# ── Last 10 days of slips + legs ──────────────────────────────────────
cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
print(f"\nLoading slips logged since {cutoff_iso[:10]}…")
slips = fetch_all("slips", "id, user_id, timestamp, slip_type, n_legs",
                  filters={"timestamp": ("gte", cutoff_iso)}, order_col="timestamp")
legs  = fetch_all("legs", "slip_id, user_id, player, league, prop, side, line, true_prob, raw_true_prob",
                  filters={}, order_col="slip_id", order_desc=False)
print(f"  slips: {len(slips):,}    legs total: {len(legs):,}")

slips_df = pd.DataFrame(slips)
legs_df  = pd.DataFrame(legs)
if not len(slips_df) or not len(legs_df):
    print("No data — exiting.")
    raise SystemExit(0)
slips_df["date"] = pd.to_datetime(slips_df["timestamp"], errors="coerce").dt.date

# Join legs to slips so we have per-leg slip_id and date
legs_in = legs_df[legs_df["slip_id"].isin(slips_df["id"])].copy()
print(f"  legs in window: {len(legs_in):,}")

# Per-user min_prob
print("\nLoading user_config thresholds…")
ucfg = fetch_all("user_config", "user_id, auto_slip_min_prob, auto_slip_legs, auto_slip_type", filters={})
ucfg_df = pd.DataFrame(ucfg)
ucfg_df["auto_slip_min_prob"] = pd.to_numeric(ucfg_df["auto_slip_min_prob"], errors="coerce").fillna(0.5407)
user_thresh = dict(zip(ucfg_df["user_id"], ucfg_df["auto_slip_min_prob"]))

# ── Re-score each leg through RWBC ──────────────────────────────────────
def rwbc_score(row):
    raw = row.get("raw_true_prob")
    if raw is None or pd.isna(raw):
        # Fallback: use the stored isotonic-calibrated true_prob as a stand-in
        raw = row.get("true_prob")
    if raw is None or pd.isna(raw):
        return None, True   # treated as halt
    side_u = str(row["side"] or "").upper()
    c = cells.get((row["league"], row["prop"], side_u))
    if c is None or c["w"] < HALT:
        return float(raw), True
    p_cal = c["w"] * float(raw) + (1 - c["w"]) * c["p_post"]
    return float(max(0.001, min(0.999, p_cal))), False

scores = legs_in.apply(rwbc_score, axis=1)
legs_in["p_rwbc"]   = [s[0] for s in scores]
legs_in["halted"]   = [s[1] for s in scores]
# Per-leg threshold pass: use the slip's user threshold
legs_in = legs_in.merge(slips_df[["id", "user_id", "date"]], left_on="slip_id", right_on="id", suffixes=("", "_slip"))
legs_in["user_min"] = legs_in["user_id"].map(user_thresh).fillna(0.5407)
legs_in["fails_threshold"] = (~legs_in["halted"]) & (legs_in["p_rwbc"] < legs_in["user_min"])
legs_in["leg_survives"]    = (~legs_in["halted"]) & (~legs_in["fails_threshold"])

# ── Per-slip survival ───────────────────────────────────────────────────
slip_surv = (
    legs_in.groupby("slip_id")
           .agg(n_legs=("leg_survives", "size"),
                n_survive=("leg_survives", "sum"),
                n_halted=("halted", "sum"),
                n_fail_thresh=("fails_threshold", "sum"))
           .reset_index()
)
slip_surv["slip_survives"] = slip_surv["n_survive"] == slip_surv["n_legs"]
slip_surv = slip_surv.merge(slips_df[["id", "user_id", "date"]], left_on="slip_id", right_on="id")

# ── Aggregate per day per user ──────────────────────────────────────────
print("\n" + "═" * 72)
print("  HISTORICAL VOLUME (current isotonic) vs RWBC FORECAST")
print("  By user, daily averages over the past 10 days.")
print("═" * 72)
rows = []
for uid, sub in slip_surv.groupby("user_id"):
    n_days = sub["date"].nunique()
    n_original = len(sub)
    n_surv     = int(sub["slip_survives"].sum())
    legs_total = int(sub["n_legs"].sum())
    legs_halt  = int(sub["n_halted"].sum())
    legs_fail  = int(sub["n_fail_thresh"].sum())
    rows.append({
        "user_id": uid[:8] + "…",
        "min_prob": round(float(user_thresh.get(uid, 0.5407)), 3),
        "days_active": n_days,
        "original_slips_total": n_original,
        "original_slips_per_day": round(n_original / max(1, n_days), 2),
        "RWBC_survivors": n_surv,
        "RWBC_per_day": round(n_surv / max(1, n_days), 2),
        "survival_pct": f"{n_surv/max(1,n_original)*100:.1f}%",
        "legs_halted_pct": f"{legs_halt/max(1,legs_total)*100:.1f}%",
        "legs_below_thresh_pct": f"{legs_fail/max(1,legs_total)*100:.1f}%",
    })
pretty_print_table(pd.DataFrame(rows), "Per-user RWBC volume forecast")

# ── Global summary ──────────────────────────────────────────────────────
g_days = slip_surv["date"].nunique()
g_orig = len(slip_surv)
g_surv = int(slip_surv["slip_survives"].sum())
g_legs_total = int(slip_surv["n_legs"].sum())
g_legs_halt  = int(slip_surv["n_halted"].sum())
g_legs_fail  = int(slip_surv["n_fail_thresh"].sum())
print("\n" + "═" * 72)
print("  HEADLINE — total system over the past 10 days")
print("═" * 72)
print(f"    Days with logged activity        : {g_days}")
print(f"    Slips logged (all users)         : {g_orig:,}  ({g_orig/max(1,g_days):.1f} per day avg)")
print(f"    Slips that survive RWBC scoring  : {g_surv:,}  ({g_surv/max(1,g_days):.1f} per day avg)")
print(f"    Survival rate                    : {g_surv/max(1,g_orig)*100:.1f}%")
print()
print(f"    Total legs across all slips      : {g_legs_total:,}")
print(f"    Legs halted by circuit breaker   : {g_legs_halt:,} ({g_legs_halt/max(1,g_legs_total)*100:.1f}%)")
print(f"    Legs below user threshold        : {g_legs_fail:,} ({g_legs_fail/max(1,g_legs_total)*100:.1f}%)")

# ── Daily timeline ──────────────────────────────────────────────────────
print("\n  ── Daily timeline (last 10 days) ──")
daily = (
    slip_surv.groupby("date")
             .agg(slips_original=("slip_id", "count"),
                  slips_RWBC=("slip_survives", "sum"))
             .reset_index()
             .sort_values("date")
)
daily["survival_pct"] = (daily["slips_RWBC"] / daily["slips_original"] * 100).round(1)
pretty_print_table(daily, "Daily slip volume")

# ── Per-user breakdown for THE user (bpersonalfinance123) ───────────────
TARGET = "ec2ad8e5-7620-4302-ae71-a3e99b387f80"
sub = slip_surv[slip_surv["user_id"] == TARGET]
if len(sub):
    print("\n" + "═" * 72)
    print(f"  YOUR ACCOUNT ({TARGET[:8]}…) — bpersonalfinance123")
    print("═" * 72)
    print(f"    Threshold:           {user_thresh.get(TARGET, 0.5407):.4f}")
    print(f"    Days active:         {sub['date'].nunique()}")
    print(f"    Slips under isotonic: {len(sub):,}  ({len(sub)/max(1,sub['date'].nunique()):.1f}/day)")
    print(f"    Slips under RWBC:    {int(sub['slip_survives'].sum()):,}  ({int(sub['slip_survives'].sum())/max(1,sub['date'].nunique()):.2f}/day)")
    print(f"    Survival rate:       {int(sub['slip_survives'].sum())/max(1,len(sub))*100:.1f}%")
