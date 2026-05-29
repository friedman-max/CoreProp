"""
PROBE 2 — Over/Under asymmetry.

The worst-cells list from probe 01 was dominated by:
  - UNDERs hitting MORE than the model predicts (positive gap)
  - OVERs hitting LESS than the model predicts (negative gap)

If true at the population level, this is a structural bias the model is
inheriting from somewhere — likely vig direction in the consensus devig,
or asymmetric line-shading by PrizePicks that we don't undo.

This probe:
  (a) plots per-league mean gap split by OVER vs UNDER
  (b) fits separate shrinkage factors α_over and α_under per league
  (c) reports the population-level OVER and UNDER hit rates by bucket
"""
from common import fetch_all, attach_y, settled, bucket_prob, pretty_print_table
import numpy as np
import pandas as pd

print("Loading settled market_observatory…")
df = fetch_all(
    "market_observatory",
    "league, prop, side, true_prob, raw_true_prob, result",
    filters={"result": ("neq", "pending"), "league": ("in_", ["NBA", "WNBA", "MLB", "NHL"])},
    order_col="resolved_at",
)
df = settled(attach_y(df))
df = df[df["true_prob"].notna() & df["side"].notna()].copy()
df["p"] = df["true_prob"].astype(float)
df["side_u"] = df["side"].str.upper()
df["bucket"] = df["p"].apply(lambda p: bucket_prob(p, 0.05))
print(f"  n={len(df):,}")

# ── (a) Per-league mean gap by side ──────────────────────────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  MEAN MISCALIBRATION GAP (observed - predicted) by league × side")
print("────────────────────────────────────────────────────────────────────")
asym = df.groupby(["league", "side_u"]).agg(
    n=("y", "size"),
    mean_pred=("p", "mean"),
    mean_obs=("y", "mean"),
).reset_index()
asym["gap"] = asym["mean_obs"] - asym["mean_pred"]
pretty_print_table(asym, "Per (league, side) calibration gap")

# Pivot for easy comparison
piv = asym.pivot(index="league", columns="side_u", values="gap")
piv["asymmetry (OVER − UNDER)"] = piv["OVER"] - piv["UNDER"]
print("\n  Gap pivot — negative OVER row + positive UNDER row = the bias we saw:")
print(piv.to_string())

# ── (b) Per-league α split by side ───────────────────────────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  Per-league shrinkage factor α SPLIT BY SIDE")
print("  (slope of (observed - 0.51) on (predicted - 0.51), weighted by n)")
print("────────────────────────────────────────────────────────────────────")
ANCHOR = 0.51
rows = []
for (lg, side), sub in df.groupby(["league", "side_u"]):
    g = sub.groupby("bucket").agg(n=("y", "size"), hits=("y", "sum")).reset_index()
    g["observed"] = g["hits"] / g["n"]
    big = g[g["n"] >= 80]
    if len(big) < 3:
        rows.append({"league": lg, "side": side, "buckets": len(big), "alpha": None, "n_total": len(sub)})
        continue
    x = big["bucket"].to_numpy() - ANCHOR
    y = big["observed"].to_numpy() - ANCHOR
    w = big["n"].to_numpy()
    slope = (w * x * y).sum() / (w * x * x).sum()
    rows.append({"league": lg, "side": side, "buckets": len(big),
                 "alpha": round(float(slope), 4), "n_total": len(sub)})
pretty_print_table(pd.DataFrame(rows), "α per (league × side)")

# ── (c) Bucket-level calibration tables, OVER and UNDER side by side ─────
print("\n────────────────────────────────────────────────────────────────────")
print("  POOLED ALL-LEAGUES: per-bucket observed hit rate, OVER vs UNDER")
print("────────────────────────────────────────────────────────────────────")
side_buckets = (
    df.groupby(["side_u", "bucket"])
      .agg(n=("y", "size"), hits=("y", "sum")).reset_index()
)
side_buckets["observed"] = side_buckets["hits"] / side_buckets["n"]
ov = side_buckets[side_buckets["side_u"] == "OVER"][["bucket", "n", "observed"]].rename(columns={"n": "n_over", "observed": "over_hit"})
un = side_buckets[side_buckets["side_u"] == "UNDER"][["bucket", "n", "observed"]].rename(columns={"n": "n_under", "observed": "under_hit"})
merged = ov.merge(un, on="bucket", how="outer").sort_values("bucket")
merged["over_gap"] = merged["over_hit"] - merged["bucket"]
merged["under_gap"] = merged["under_hit"] - merged["bucket"]
pretty_print_table(merged, "Bucket-level OVER vs UNDER")

# ── (d) Conclusion ───────────────────────────────────────────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  HEADLINE")
print("────────────────────────────────────────────────────────────────────")
total_over = df[df.side_u == "OVER"]
total_under = df[df.side_u == "UNDER"]
print(f"  P(hit | OVER)  = {total_over['y'].mean():.4f}   on n={len(total_over):,}")
print(f"  P(hit | UNDER) = {total_under['y'].mean():.4f}   on n={len(total_under):,}")
gap = total_under["y"].mean() - total_over["y"].mean()
print(f"  UNDER edge over OVER: {gap*100:+.2f} pp")
print()
print("  If this number is materially positive (≥1pp on n>30k), PrizePicks lines")
print("  are systematically shaded toward OVERs and our model isn't correcting")
print("  for it. The fix is a constant additive correction per side, fit from")
print("  this number directly: subtract gap/2 from p_over, add gap/2 to p_under.")
