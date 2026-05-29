"""
PROBE 7 — Per-(league × side × 5pp bucket) gap audit under RWBC vs current.

The user's literal ask: after RWBC, is the predicted ≈ realized for EVERY
(league, side, 5pp bucket) cell? Answer empirically — not by global Brier
or pooled bucket tables, but by the full Cartesian grid.

Output a grid where each row is one cell with:
   league | side | bucket | n | current_predicted | current_realized | current_gap
                                  | rwbc_predicted | rwbc_realized | rwbc_gap
                                  | wilson_low | wilson_high   (95% CI on realized)
                                  | verdict ("✓ equalized" if |rwbc_gap| < CI half-width)

Then summary: % of cells where RWBC gap is inside the 95% CI on realized
(i.e. statistically indistinguishable from perfect calibration).
"""
from common import (
    fetch_all, attach_y, settled, bucket_prob, beta_binomial_posterior,
    wilson_ci, brier_score, pretty_print_table,
)
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

GLOBAL_HIT = df["y"].mean()

# ── Refit the (league, prop, side) cells for RWBC (same as probe 06) ──
def fit_cell(g, prior_strength=30.0):
    if len(g) < 5: return None
    mean_pred = g["p"].mean()
    bg = g.groupby("bucket").agg(n=("y","size"), pp=("p","mean"), po=("y","mean")).reset_index()
    res = (bg["n"] * (bg["pp"] - mean_pred) ** 2).sum() / len(g)
    rel = (bg["n"] * (bg["pp"] - bg["po"]) ** 2).sum() / len(g)
    w = res / (res + rel + 1e-6)
    w = float(max(0.0, min(1.0, w)))
    k = int(g["y"].sum())
    pm, _, _ = beta_binomial_posterior(k, len(g), GLOBAL_HIT, prior_strength)
    return {"w": w, "p_post": pm}

cells = {}
for key, g in df.groupby(["league", "prop", "side_u"]):
    s = fit_cell(g)
    if s: cells[key] = s

def rwbc(row):
    c = cells.get((row["league"], row["prop"], row["side_u"]))
    if c is None: return row["p"]
    return c["w"] * row["p"] + (1 - c["w"]) * c["p_post"]

df["p_rwbc"] = df.apply(rwbc, axis=1)
df["rwbc_bucket"] = df["p_rwbc"].apply(lambda p: bucket_prob(p, 0.05))

# ── Per-(league, side, BUCKET) gap audit ──
print("\n────────────────────────────────────────────────────────────────────")
print("  PER-(league × side × 5pp bucket) GAP AUDIT")
print("  ✓ = realized inside CI on predicted → statistically equalized")
print("  ✗ = realized outside CI → real miscalibration remains")
print("────────────────────────────────────────────────────────────────────")

def grid(df, p_col, bucket_col, min_n=30):
    g = (
        df.groupby(["league", "side_u", bucket_col])
          .agg(n=("y", "size"), predicted=(p_col, "mean"), realized=("y", "mean"),
               hits=("y", "sum"))
          .reset_index()
    )
    g = g[g["n"] >= min_n].copy()
    cis = g.apply(lambda r: wilson_ci(int(r["hits"]), int(r["n"])), axis=1)
    g["ci_low"] = [c[0] for c in cis]
    g["ci_high"] = [c[1] for c in cis]
    g["gap"] = g["realized"] - g["predicted"]
    g["equalized"] = (g["predicted"] >= g["ci_low"]) & (g["predicted"] <= g["ci_high"])
    return g.sort_values(["league", "side_u", bucket_col]).reset_index(drop=True)

cur_grid  = grid(df, "p",       "bucket")
rwbc_grid = grid(df, "p_rwbc",  "rwbc_bucket")

# Side-by-side comparison rolled up
print("\n  Cells per league × side with ≥30 observations:")
sz = pd.concat([
    cur_grid.groupby(["league", "side_u"]).size().rename("current_cells"),
    rwbc_grid.groupby(["league", "side_u"]).size().rename("rwbc_cells"),
], axis=1).reset_index()
pretty_print_table(sz, "Cell count per league × side")

# Calibration pass rate per (league, side)
print("\n  Calibration pass rate per (league × side):")
def pass_summary(g, label):
    r = g.groupby(["league", "side_u"]).agg(
        cells=("equalized", "size"),
        equalized=("equalized", "sum"),
        mean_abs_gap=("gap", lambda x: x.abs().mean()),
    ).reset_index()
    r["pass_rate"] = (r["equalized"] / r["cells"] * 100).round(1)
    r["mean_abs_gap_pp"] = (r["mean_abs_gap"] * 100).round(2)
    r["calibrator"] = label
    return r[["calibrator", "league", "side_u", "cells", "equalized", "pass_rate", "mean_abs_gap_pp"]]

cur_pass = pass_summary(cur_grid, "current")
rwbc_pass = pass_summary(rwbc_grid, "RWBC")
combined = pd.concat([cur_pass, rwbc_pass]).sort_values(["league", "side_u", "calibrator"])
pretty_print_table(combined, "Cells passing calibration (predicted inside 95% CI of realized)")

# Headline number: % cells that are statistically equalized
print("\n────────────────────────────────────────────────────────────────────")
print("  HEADLINE  —  fraction of cells statistically calibrated")
print("────────────────────────────────────────────────────────────────────")
def fraction(g, lbl):
    print(f"    {lbl:>10}:  {g['equalized'].sum()} / {len(g)} cells equalized  "
          f"({g['equalized'].mean()*100:.1f}%)   mean |gap| = {g['gap'].abs().mean()*100:.2f}pp")
fraction(cur_grid, "current")
fraction(rwbc_grid, "RWBC")

# ── Show every per-cell row for direct inspection ──
print("\n────────────────────────────────────────────────────────────────────")
print("  FULL GRID (RWBC) — every cell with n ≥ 30")
print("  Look at the 'equalized' column — what fraction are ✓?")
print("────────────────────────────────────────────────────────────────────")
disp = rwbc_grid[["league", "side_u", "rwbc_bucket", "n", "predicted", "realized",
                  "ci_low", "ci_high", "gap", "equalized"]].copy()
disp.columns = ["league", "side", "bucket", "n", "predicted", "realized",
                "ci_low", "ci_high", "gap", "equalized"]
pretty_print_table(disp, "RWBC per-cell calibration")

# Outliers — cells where RWBC still fails calibration
fails = rwbc_grid[~rwbc_grid["equalized"]].copy()
if len(fails):
    print("\n────────────────────────────────────────────────────────────────────")
    print(f"  RWBC RESIDUAL FAILURES — {len(fails)} cells outside 95% CI")
    print("  These are where 'almost exactly correct' breaks down, and why.")
    print("────────────────────────────────────────────────────────────────────")
    fails["abs_gap"] = fails["gap"].abs()
    fails_disp = fails.sort_values("abs_gap", ascending=False)[
        ["league", "side_u", "rwbc_bucket", "n", "predicted", "realized",
         "ci_low", "ci_high", "gap"]
    ]
    fails_disp.columns = ["league", "side", "bucket", "n", "predicted",
                          "realized", "ci_low", "ci_high", "gap"]
    pretty_print_table(fails_disp, "Cells where RWBC still misses")
