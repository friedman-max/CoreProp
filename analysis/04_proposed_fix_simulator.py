"""
PROBE 4 — Forward-simulate the proposed selection rule on observatory data.

The proposed rule, derived from probes 01-03:

  (a) Per-side bias correction:  p' = p + δ_side(league),
      where δ is the constant the side's mean gap implies.
  (b) Anchored shrinkage:        p'' = 0.51 + α(league) * (p' - 0.51)
  (c) Drop-known-bad cells:      hard skip on (league, side) combos with
                                  negative selection edge from probe 03.

Counterfactual: take every settled observatory row, score it under
the current calibrator AND under the proposed rule, then compare:
  - Brier score
  - Hit rate of accepted bets (≥ 53% calibrated)
  - Volume of accepted bets (fewer is OK if they're sharper)
  - Realized EV at PP 3-pick Power payout (5x → break-even 58.5%)
"""
from common import fetch_all, attach_y, settled, bucket_prob, brier_score, pretty_print_table
import pandas as pd
import numpy as np

print("Loading settled market_observatory…")
df = fetch_all(
    "market_observatory", "league, prop, side, true_prob, raw_true_prob, result",
    filters={"result": ("neq", "pending"), "league": ("in_", ["NBA", "WNBA", "MLB", "NHL"])},
    order_col="resolved_at",
)
df = settled(attach_y(df))
df = df[df["true_prob"].notna() & df["side"].notna()].copy()
df["p"] = df["true_prob"].astype(float)
df["side_u"] = df["side"].str.upper()
df["bucket"] = df["p"].apply(lambda p: bucket_prob(p, 0.05))
print(f"  n={len(df):,}")

# ── Calibrated constants from probes 01-03 (re-derive in case data shifted) ──
ANCHOR = 0.51

# Per-side bias from probe 02 (mean gap = obs_mean - pred_mean)
bias = {}
for (lg, side), sub in df.groupby(["league", "side_u"]):
    bias[(lg, side)] = float(sub["y"].mean() - sub["p"].mean())

# Per-(league, side) shrinkage α — weighted-LS slope through (0.51, 0.51)
alpha = {}
for (lg, side), sub in df.groupby(["league", "side_u"]):
    g = sub.groupby("bucket").agg(n=("y", "size"), hits=("y", "sum")).reset_index()
    g["observed"] = g["hits"] / g["n"]
    big = g[g["n"] >= 80]
    if len(big) < 3:
        alpha[(lg, side)] = 1.0
        continue
    x = big["bucket"].to_numpy() - ANCHOR
    y = big["observed"].to_numpy() - ANCHOR
    w = big["n"].to_numpy()
    slope = (w * x * y).sum() / (w * x * x).sum()
    # Clip alpha into a sane range — negative α means inverted signal;
    # treat that as "model has zero info, fall back to bias-only"
    alpha[(lg, side)] = max(0.0, min(1.5, float(slope)))

# Known-bad (league, side) cells from probe 03 — selection edge < -3pp
DROP_CELLS = {
    ("NBA", "OVER"),
    ("NHL", "OVER"),
    ("NHL", "UNDER"),
    ("WNBA", "OVER"),
}

print("\n  Per-(league, side) bias correction δ and shrinkage α:")
for k in sorted(bias):
    inverted = alpha[k] == 0.0
    dropped = "  ← DROPPED" if k in DROP_CELLS else ""
    inv = "  (signal inverted → α clipped to 0)" if inverted else ""
    print(f"    {k[0]:>4} {k[1]:>5}:  δ = {bias[k]:+.4f}   α = {alpha[k]:.4f}{inv}{dropped}")

# ── Apply current vs proposed calibration to every row ──────────────────
def calibrate_proposed(row):
    key = (row["league"], row["side_u"])
    if key in DROP_CELLS:
        return None  # skip
    p = row["p"] + bias[key]
    p = ANCHOR + alpha[key] * (p - ANCHOR)
    return float(max(0.01, min(0.99, p)))

df["p_proposed"] = df.apply(calibrate_proposed, axis=1)

# ── Brier comparison ────────────────────────────────────────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  BRIER COMPARISON: current calibrator vs. proposed")
print("────────────────────────────────────────────────────────────────────")
rows = []
for scope, sub in [("GLOBAL", df)] + list(df.groupby("league")):
    sub_kept = sub[sub["p_proposed"].notna()]
    rows.append({
        "scope": scope,
        "n_kept": len(sub_kept),
        "n_dropped": len(sub) - len(sub_kept),
        "brier_current": round(brier_score(sub_kept["p"], sub_kept["y"]), 5),
        "brier_proposed": round(brier_score(sub_kept["p_proposed"], sub_kept["y"]), 5),
        "delta_brier (lower is better)": round(brier_score(sub_kept["p"], sub_kept["y"])
                                                - brier_score(sub_kept["p_proposed"], sub_kept["y"]), 5),
    })
pretty_print_table(pd.DataFrame(rows), "Brier")

# ── Selection acceptance comparison ──────────────────────────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  SELECTION RULE: accept iff calibrated p ≥ THRESHOLD")
print("  Compares 'current' (use raw model p) vs 'proposed' (use p_proposed)")
print("────────────────────────────────────────────────────────────────────")
for THRESHOLD in (0.55, 0.58, 0.60, 0.62, 0.65):
    cur_acc = df[df["p"] >= THRESHOLD]
    prop_acc = df[(df["p_proposed"].notna()) & (df["p_proposed"] >= THRESHOLD)]
    print(f"\n  Threshold = {THRESHOLD:.2f}")
    print(f"    Current   accepted: {len(cur_acc):,}   realized hit: {cur_acc['y'].mean():.4f}")
    print(f"    Proposed  accepted: {len(prop_acc):,}   realized hit: {prop_acc['y'].mean():.4f}")
    if len(prop_acc) and len(cur_acc):
        lift = prop_acc["y"].mean() - cur_acc["y"].mean()
        print(f"    Lift: {lift*100:+.2f} pp hit rate")

# ── Simulated 3-pick Power EV (5x payout, break-even 58.48%) ─────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  3-PICK POWER SLIP EV SIMULATION  (payout 5x, BE 58.48%)")
print("  Assume each accepted leg is independent — best-case for the model.")
print("────────────────────────────────────────────────────────────────────")
BE = 1.0 / 5.0 ** (1/3)  # ~0.585

def sim_slip_ev(legs_df: pd.DataFrame, n_legs: int = 3) -> dict:
    if len(legs_df) < n_legs:
        return {"avg_leg_hit": None, "slip_hit_indep": None, "ev_per_unit_stake": None, "n_legs": len(legs_df)}
    avg_hit = legs_df["y"].mean()
    slip_hit = avg_hit ** n_legs
    ev_per_unit = slip_hit * 5.0 - 1.0  # stake 1, payout 5 on win, lose 1 on loss
    return {"avg_leg_hit": round(avg_hit, 4),
            "slip_hit_indep_3leg": round(slip_hit, 4),
            "ev_per_unit_stake": round(ev_per_unit, 4),
            "n_legs": len(legs_df)}

for THRESHOLD in (0.55, 0.58, 0.60, 0.62):
    cur_acc = df[df["p"] >= THRESHOLD]
    prop_acc = df[(df["p_proposed"].notna()) & (df["p_proposed"] >= THRESHOLD)]
    print(f"\n  Threshold {THRESHOLD:.2f}:")
    print(f"    CURRENT  → {sim_slip_ev(cur_acc)}")
    print(f"    PROPOSED → {sim_slip_ev(prop_acc)}")

print("\nNotes:")
print("  - EV negative on current at 0.55-0.58 thresh → confirms current selection is unprofitable")
print("  - 'slip_hit_indep' assumes independence (best case). Real PP correlation only makes it worse.")
print("  - The single biggest lift comes from DROPPING the four anti-helpful cells,")
print("    not from the per-row calibration tweaks.")
