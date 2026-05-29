"""
PROBE 1 — per-(league, prop, side, prob_bucket) calibration on the
*unbiased observatory stream*. This is the chart you've been looking at,
sliced finer.

What it answers:
  - Where exactly does the model say X% and the world says Y%?
  - Which (league, prop, side) cells have the largest disparities?
  - How many observations are in each cell? (so we know which divergences
    are real vs. noise)
  - What's the realized hit rate per 5-pt model-prob bucket?

This is the loser-pattern question rephrased: a 70-75% model bucket whose
actual hit rate is 55% is a population of losers. Find those cells.
"""
from common import fetch_all, attach_y, settled, bucket_prob, beta_binomial_posterior, brier_score, pretty_print_table
import pandas as pd

print("Loading settled market_observatory…")
df = fetch_all(
    "market_observatory",
    "league, prop, side, true_prob, raw_true_prob, result",
    filters={"result": ("neq", "pending"), "league": ("in_", ["NBA", "WNBA", "MLB", "NHL"])},
    order_col="resolved_at",
)
df = settled(attach_y(df))
df = df[df["true_prob"].notna()].copy()
df["p"] = df["true_prob"].astype(float)
df["bucket"] = df["p"].apply(lambda p: bucket_prob(p, 0.05))
df["league_prop_side"] = df["league"] + " | " + df["prop"].fillna("") + " | " + df["side"].fillna("").str.upper()
print(f"  using n={len(df):,} settled observations")

# ── (1) Global per-bucket calibration table ──────────────────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  GLOBAL per-bucket calibration (all leagues pooled)")
print("────────────────────────────────────────────────────────────────────")
g = df.groupby("bucket").agg(n=("y", "size"), hits=("y", "sum")).reset_index()
g["observed"] = g["hits"] / g["n"]
g["gap"] = g["observed"] - g["bucket"]
g["realized_over_predicted_dev"] = (g["observed"] - 0.51) / (g["bucket"] - 0.51).replace(0, float("nan"))
pretty_print_table(g, "P(hit) per model-prob bucket (the calibration curve)")

# Save the implicit shrinkage factor — slope of (observed − 0.51) on (predicted − 0.51).
big_buckets = g[g["n"] >= 200]
if len(big_buckets) >= 3:
    import numpy as np
    x = big_buckets["bucket"].to_numpy() - 0.51
    y = big_buckets["observed"].to_numpy() - 0.51
    w = big_buckets["n"].to_numpy()
    slope = (w * x * y).sum() / (w * x * x).sum()  # weighted least squares through origin
    print(f"\n  Implicit shrinkage factor α (slope of obs-0.51 on pred-0.51, weighted by n):")
    print(f"     α_global ≈ {slope:.4f}")
    print(f"  → A calibrated_prob = 0.51 + {slope:.3f} * (model_prob - 0.51) would flatten the global curve.")

# ── (2) Per-league shrinkage factor ──────────────────────────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  PER-LEAGUE shrinkage factor (α)")
print("────────────────────────────────────────────────────────────────────")
rows = []
for lg, sub in df.groupby("league"):
    gg = sub.groupby("bucket").agg(n=("y", "size"), hits=("y", "sum")).reset_index()
    gg["observed"] = gg["hits"] / gg["n"]
    big = gg[gg["n"] >= 100]
    if len(big) < 3:
        rows.append({"league": lg, "buckets_with_n>=100": len(big), "alpha": None, "anchor": 0.51, "n_total": len(sub)})
        continue
    import numpy as np
    x = big["bucket"].to_numpy() - 0.51
    y = big["observed"].to_numpy() - 0.51
    w = big["n"].to_numpy()
    slope = (w * x * y).sum() / (w * x * x).sum()
    rows.append({"league": lg, "buckets_with_n>=100": len(big), "alpha": round(slope, 4),
                 "anchor": 0.51, "n_total": len(sub)})
pretty_print_table(pd.DataFrame(rows), "Per-league shrinkage")

# ── (3) Find the worst per-(league, prop, side) cells ────────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  WORST-CALIBRATED CELLS  (n>=80 obs, sorted by absolute gap)")
print("  Each row = (league, prop, side, bucket). 'gap' = observed - bucket center.")
print("────────────────────────────────────────────────────────────────────")
cells = df.groupby(["league", "prop", "side", "bucket"]).agg(
    n=("y", "size"), hits=("y", "sum")
).reset_index()
cells["observed"] = cells["hits"] / cells["n"]
cells["gap"] = cells["observed"] - cells["bucket"]
worst = cells[cells["n"] >= 80].copy()
worst["abs_gap"] = worst["gap"].abs()
worst = worst.sort_values("abs_gap", ascending=False).head(25)
worst = worst[["league", "prop", "side", "bucket", "n", "hits", "observed", "gap"]]
pretty_print_table(worst, "Top 25 worst-calibrated cells")

# ── (4) Brier score breakdown — current model vs anchored shrinkage ──────
print("\n────────────────────────────────────────────────────────────────────")
print("  CURRENT-vs-PROPOSED Brier score (smaller is better)")
print("────────────────────────────────────────────────────────────────────")
ANCHOR = 0.51
brier_rows = []
for lg, sub in [("GLOBAL", df)] + list(df.groupby("league")):
    p_current = sub["p"].astype(float)
    y = sub["y"].astype(float)
    # Per-league alpha when available, else global
    big = sub.groupby("bucket").agg(n=("y", "size"), hits=("y", "sum")).reset_index()
    big["observed"] = big["hits"] / big["n"]
    big = big[big["n"] >= 80]
    if len(big) >= 3:
        import numpy as np
        x = big["bucket"].to_numpy() - ANCHOR
        yy = big["observed"].to_numpy() - ANCHOR
        w = big["n"].to_numpy()
        alpha = float((w * x * yy).sum() / (w * x * x).sum())
    else:
        alpha = 1.0
    p_proposed = ANCHOR + alpha * (p_current - ANCHOR)
    p_proposed = p_proposed.clip(0.01, 0.99)
    brier_rows.append({
        "scope": lg,
        "n": len(sub),
        "alpha": round(alpha, 4),
        "brier_current": round(brier_score(p_current, y), 6),
        "brier_proposed": round(brier_score(p_proposed, y), 6),
        "delta": round(brier_score(p_current, y) - brier_score(p_proposed, y), 6),
    })
pretty_print_table(pd.DataFrame(brier_rows),
                   "Brier score: current calibrator vs. anchored shrinkage at 0.51")

print("\nInterpretation guide:")
print("  - alpha < 1   → current model overstates its departure from 0.51")
print("  - alpha > 1   → current model understates its departure from 0.51 (under-confident)")
print("  - alpha ≈ 1   → already calibrated, shrinkage won't help")
print("  - brier delta > 0 → proposed is strictly better on this slice")
