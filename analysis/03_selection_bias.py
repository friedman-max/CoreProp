"""
PROBE 3 — Is our selection rule helping?

Pull settled `legs` (the auto-backtest's logged decisions) and compare
hit rate to settled `market_observatory` legs in the SAME (league, prop,
side, bucket) cell. If our logged legs hit at the same rate as a random
observatory leg from the cell, the selection rule adds no information.
If they hit LESS, our selection is anti-correlated with truth.
"""
from common import fetch_all, attach_y, settled, bucket_prob, pretty_print_table
import pandas as pd

print("Loading settled legs + observatory…")
legs = fetch_all(
    "legs", "league, prop, side, true_prob, raw_true_prob, result",
    filters={"result": ("neq", "pending")}, order_col="resolved_at",
)
obs = fetch_all(
    "market_observatory", "league, prop, side, true_prob, raw_true_prob, result",
    filters={"result": ("neq", "pending"), "league": ("in_", ["NBA", "WNBA", "MLB", "NHL"])},
    order_col="resolved_at",
)
legs = settled(attach_y(legs))
obs = settled(attach_y(obs))
legs = legs[legs["true_prob"].notna() & legs["side"].notna()].copy()
obs = obs[obs["true_prob"].notna() & obs["side"].notna()].copy()
legs["bucket"] = legs["true_prob"].astype(float).apply(lambda p: bucket_prob(p, 0.05))
obs["bucket"] = obs["true_prob"].astype(float).apply(lambda p: bucket_prob(p, 0.05))
legs["side_u"] = legs["side"].str.upper()
obs["side_u"] = obs["side"].str.upper()

print(f"  legs settled: {len(legs):,}   observatory settled: {len(obs):,}")

# ── (a) Side-level: did our selection improve P(hit) over the cell base rate? ──
print("\n────────────────────────────────────────────────────────────────────")
print("  Logged-leg hit rate vs same-bucket observatory base rate")
print("  Positive 'edge' = selection beat random; negative = selection hurt us")
print("────────────────────────────────────────────────────────────────────")

# Group observatory into a lookup table of {(league, prop, side, bucket) → base_rate, n}.
obs_lookup = (
    obs.groupby(["league", "prop", "side_u", "bucket"])
       .agg(n_obs=("y", "size"), p_obs=("y", "mean")).reset_index()
)
# Same grouping on legs
leg_g = (
    legs.groupby(["league", "prop", "side_u", "bucket"])
        .agg(n_legs=("y", "size"), p_legs=("y", "mean")).reset_index()
)

merged = leg_g.merge(obs_lookup, on=["league", "prop", "side_u", "bucket"], how="left")
merged["selection_edge_pp"] = (merged["p_legs"] - merged["p_obs"]) * 100

# League × side summary, weighted by leg sample
print("\n  Per (league, side) — aggregated over all cells the selector visited:")
print("  (compares each logged leg to the matched cell's observatory base rate)")
agg = (
    merged.dropna(subset=["p_obs"]).copy()
)
# Weight by leg count to compute the realized "selection edge"
def weighted_mean(df, vcol, wcol):
    return (df[vcol] * df[wcol]).sum() / df[wcol].sum()

rows = []
for (lg, side), sub in agg.groupby(["league", "side_u"]):
    if sub["n_legs"].sum() == 0:
        continue
    rows.append({
        "league": lg,
        "side": side,
        "n_legs_logged": int(sub["n_legs"].sum()),
        "p_obs_baseline (weighted)": round(weighted_mean(sub, "p_obs", "n_legs"), 4),
        "p_legs_realized": round(weighted_mean(sub, "p_legs", "n_legs"), 4),
        "selection_edge_pp": round(weighted_mean(sub, "selection_edge_pp", "n_legs"), 2),
    })
pretty_print_table(pd.DataFrame(rows), "Selection edge by (league × side)")

# ── (b) Same thing rolled up to league level (ignoring side) ─────────────
print("\n  Per league (rolling up side):")
rows = []
for lg in sorted(legs["league"].unique()):
    sub = agg[agg["league"] == lg]
    if sub["n_legs"].sum() == 0:
        continue
    rows.append({
        "league": lg,
        "n_legs_logged": int(sub["n_legs"].sum()),
        "p_obs_baseline (weighted)": round(weighted_mean(sub, "p_obs", "n_legs"), 4),
        "p_legs_realized":          round(weighted_mean(sub, "p_legs", "n_legs"), 4),
        "selection_edge_pp":        round(weighted_mean(sub, "selection_edge_pp", "n_legs"), 2),
    })
pretty_print_table(pd.DataFrame(rows), "Selection edge by league")

# ── (c) What if we'd just always bet UNDER, ignoring the model entirely? ──
print("\n────────────────────────────────────────────────────────────────────")
print("  COUNTERFACTUAL: what if the auto-backtester just picked UNDERs?")
print("  (compute P(hit) on every observatory UNDER in (league, prop, bucket)")
print("   cells our selector ever fired in — i.e. fair apples-to-apples)")
print("────────────────────────────────────────────────────────────────────")
# Restrict observatory to cells the selector ever touched, but force side=UNDER
selector_cells = set(legs[["league", "prop", "bucket"]].drop_duplicates().itertuples(index=False, name=None))
obs["cell"] = list(zip(obs["league"], obs["prop"], obs["bucket"]))
obs_in_selector_cells = obs[obs["cell"].isin(selector_cells)]
obs_under_in_cells = obs_in_selector_cells[obs_in_selector_cells["side_u"] == "UNDER"]
obs_over_in_cells = obs_in_selector_cells[obs_in_selector_cells["side_u"] == "OVER"]
print(f"  P(hit | UNDER) in selector-visited cells: {obs_under_in_cells['y'].mean():.4f}  (n={len(obs_under_in_cells):,})")
print(f"  P(hit | OVER)  in selector-visited cells: {obs_over_in_cells['y'].mean():.4f}  (n={len(obs_over_in_cells):,})")
print(f"  P(hit | logged legs, all sides):          {legs['y'].mean():.4f}  (n={len(legs):,})")
print(f"  P(hit | logged legs, side=UNDER):         {legs[legs.side_u == 'UNDER']['y'].mean():.4f}  (n={len(legs[legs.side_u == 'UNDER']):,})")
print(f"  P(hit | logged legs, side=OVER):          {legs[legs.side_u == 'OVER']['y'].mean():.4f}  (n={len(legs[legs.side_u == 'OVER']):,})")
print()
print("  If 'always UNDER' beats our actual selection, the model is anti-helpful")
print("  on at least one side and we should restrict to UNDERs only until fixed.")

# ── (d) Side composition of logged legs vs observatory ───────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  Side composition: logged vs observatory")
print("────────────────────────────────────────────────────────────────────")
def side_share(df, name):
    n = len(df)
    s = df["side_u"].value_counts().to_dict()
    over_pct = s.get("OVER", 0) / n * 100 if n else 0
    under_pct = s.get("UNDER", 0) / n * 100 if n else 0
    print(f"  {name:>28}: OVER {over_pct:5.1f}% | UNDER {under_pct:5.1f}%  (n={n:,})")

side_share(obs, "Observatory (universe)")
side_share(legs, "Logged legs (auto-backtest)")
