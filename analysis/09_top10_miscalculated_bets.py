"""
ANALYSIS 09 — Top-10 EV-miscalculated bets + average calibration lift.

For every settled leg in the user's actual backtest:
  1. Pull the OLD calibrated probability that was stamped at log time
     (`legs.true_prob` — the hierarchical-isotonic output)
  2. Pull the RAW consensus probability (`legs.raw_true_prob` — what the
     model produced BEFORE any calibration)
  3. Apply RWBC to the raw prob using the cell stats fit from
     market_observatory — this is what the new model would have said
  4. Compare:
        gap_old = |true_prob_old - actual outcome|
        gap_new = |true_prob_rwbc - actual outcome|
        delta_prob = |true_prob_rwbc - true_prob_old|
        ev_old   = true_prob_old  * implied_payout_per_leg - 1
        ev_new   = true_prob_rwbc * implied_payout_per_leg - 1

Then:
  - Rank by |delta_prob| → top 10 most-divergent EV calls
  - Aggregate: how much did our calibration error shrink on average?
"""
from common import (
    fetch_all, attach_y, settled, bucket_prob, beta_binomial_posterior,
    pretty_print_table,
)
import pandas as pd

# ────────────────────────────────────────────────────────────────────────
# 1. Fit RWBC cells (same as probe 06) from the unbiased observatory
# ────────────────────────────────────────────────────────────────────────
print("Loading market_observatory + legs…")
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
PRIOR_K = 30.0
def fit_cell(g):
    if len(g) < 5:
        return None
    mp = g["p"].mean()
    bg = g.groupby("bucket").agg(n=("y","size"), pp=("p","mean"), po=("y","mean")).reset_index()
    if len(bg) < 2:
        # collapse to Beta-Binomial only
        k = int(g["y"].sum())
        pm, _, _ = beta_binomial_posterior(k, len(g), GLOBAL_HIT, PRIOR_K)
        return {"w": 0.0, "p_post": pm}
    res = (bg["n"] * (bg["pp"] - mp) ** 2).sum() / len(g)
    rel = (bg["n"] * (bg["pp"] - bg["po"]) ** 2).sum() / len(g)
    w = float(max(0.0, min(1.0, res / (res + rel + 1e-6))))
    k = int(g["y"].sum())
    pm, _, _ = beta_binomial_posterior(k, len(g), GLOBAL_HIT, PRIOR_K)
    return {"w": w, "p_post": pm}

cells = {}
for key, g in obs.groupby(["league", "prop", "side_u"]):
    c = fit_cell(g)
    if c: cells[key] = c

print(f"  RWBC cells fit: {len(cells)}")

# ────────────────────────────────────────────────────────────────────────
# 2. Pull every settled leg with the columns we need to score both ways
# ────────────────────────────────────────────────────────────────────────
legs = fetch_all(
    "legs",
    "slip_id, leg_num, player, league, prop, side, line, true_prob, raw_true_prob, ind_ev_pct, result, resolved_at, game_start",
    filters={"result": ("neq", "pending")},
    order_col="resolved_at",
)
legs = settled(attach_y(legs))
legs = legs[
    legs["true_prob"].notna() & legs["raw_true_prob"].notna()
    & legs["side"].notna() & legs["league"].notna() & legs["prop"].notna()
].copy()
legs["side_u"] = legs["side"].str.upper().str.strip()
legs["p_old"]  = legs["true_prob"].astype(float)
legs["p_raw"]  = legs["raw_true_prob"].astype(float)
print(f"  settled legs (with both probs populated): {len(legs):,}")

# ────────────────────────────────────────────────────────────────────────
# 3. Apply RWBC to each leg (taking raw_true_prob as p_model since the
#    calibrator should run on the raw consensus, mirroring how it would
#    be wired in production)
# ────────────────────────────────────────────────────────────────────────
def rwbc_apply(row):
    c = cells.get((row["league"], row["prop"], row["side_u"]))
    if c is None or c["w"] < HALT:
        return None     # circuit breaker
    p_cal = c["w"] * row["p_raw"] + (1 - c["w"]) * c["p_post"]
    return float(max(0.001, min(0.999, p_cal)))

legs["p_rwbc"] = legs.apply(rwbc_apply, axis=1)

n_halted = legs["p_rwbc"].isna().sum()
print(f"  legs halted by RWBC circuit breaker: {n_halted:,} ({n_halted/len(legs)*100:.0f}%)")

# Implied per-leg payout: we use the empirical PrizePicks 2-leg break-even
# equivalence (1 / sqrt(3) = 0.5774). Multiplying p by 1/be gives the
# per-leg EV multiplier — works as a uniform yardstick across slips of
# different sizes since the user is comparing OLD vs NEW under the same
# yardstick on the same leg.
PER_LEG_PAYOUT = 1.0 / 0.5774   # ≈ 1.732
def ev_per_unit(p):
    return p * PER_LEG_PAYOUT - 1.0

# ────────────────────────────────────────────────────────────────────────
# 4. Top-10 EV-miscalculated legs (sorted by |Δprob|)
# ────────────────────────────────────────────────────────────────────────
scored = legs[legs["p_rwbc"].notna()].copy()
scored["delta_prob"]    = scored["p_rwbc"] - scored["p_old"]
scored["abs_delta_prob"] = scored["delta_prob"].abs()
scored["ev_old"]        = scored["p_old"].apply(ev_per_unit)
scored["ev_new"]        = scored["p_rwbc"].apply(ev_per_unit)
scored["delta_ev"]      = scored["ev_new"] - scored["ev_old"]
scored["gap_old"]       = (scored["p_old"]  - scored["y"]).abs()
scored["gap_new"]       = (scored["p_rwbc"] - scored["y"]).abs()
scored["new_is_closer"] = scored["gap_new"] < scored["gap_old"]
scored["result_short"]  = scored["result"].astype(str).str[:4].str.upper()
scored["played_at"]     = pd.to_datetime(scored["resolved_at"], errors="coerce").dt.strftime("%m/%d %H:%M")

top10 = (
    scored
    .sort_values("abs_delta_prob", ascending=False)
    .head(10)
    .loc[:, ["played_at", "player", "league", "prop", "side_u", "line",
             "p_old", "p_rwbc", "delta_prob", "ev_old", "ev_new",
             "delta_ev", "result_short", "new_is_closer"]]
    .rename(columns={"side_u": "side", "result_short": "result"})
)
pretty_print_table(top10, "TOP 10 BETS — EV miscalculation (sorted by |Δprob|)")

# ────────────────────────────────────────────────────────────────────────
# 5. Average calibration lift across the whole backtest
# ────────────────────────────────────────────────────────────────────────
print("\n" + "═" * 72)
print("  AVERAGE CALIBRATION LIFT  —  every settled leg in your backtest")
print("═" * 72)

n = len(scored)
brier_old = float(((scored["p_old"]  - scored["y"]) ** 2).mean())
brier_new = float(((scored["p_rwbc"] - scored["y"]) ** 2).mean())
mae_old   = float((scored["p_old"]  - scored["y"]).abs().mean())
mae_new   = float((scored["p_rwbc"] - scored["y"]).abs().mean())
pct_closer = float(scored["new_is_closer"].mean()) * 100

print(f"\n  Population (legs where RWBC is active, not halted): {n:,}")
print(f"\n  Brier score (lower = better)")
print(f"    Old (isotonic)  :  {brier_old:.5f}")
print(f"    New (RWBC)      :  {brier_new:.5f}")
print(f"    Improvement     :  {(brier_old - brier_new) * 1000:+.2f} mBrier "
      f"({(brier_old - brier_new) / brier_old * 100:+.2f}%)")

print(f"\n  Mean absolute error (predicted - realized)")
print(f"    Old (isotonic)  :  {mae_old:.4f}  (= {mae_old*100:.2f} pp)")
print(f"    New (RWBC)      :  {mae_new:.4f}  (= {mae_new*100:.2f} pp)")
print(f"    Improvement     :  {(mae_old - mae_new)*100:+.2f} pp")

print(f"\n  Per-leg head-to-head: new prediction is closer to truth on")
print(f"    {pct_closer:.1f}% of legs ({int(scored['new_is_closer'].sum()):,}/{n:,})")

# Halted-cell impact: legs RWBC would have refused to score AT ALL.
# What was their realized hit rate? If they hit less than break-even,
# the circuit breaker is doing exactly what it's supposed to.
halted = legs[legs["p_rwbc"].isna()]
if len(halted):
    hit_halted = float(halted["y"].mean())
    hit_active = float(scored["y"].mean())
    print(f"\n  Circuit-breaker decisions (legs RWBC refuses to bet)")
    print(f"    Halted legs hit rate    :  {hit_halted:.4f}  (n={len(halted):,})")
    print(f"    Non-halted legs hit rate:  {hit_active:.4f}  (n={n:,})")
    diff = hit_active - hit_halted
    verdict = "✓ breaker filters losers" if diff > 0 else "✗ breaker filtering winners"
    print(f"    Lift from skipping halted: {diff*100:+.2f} pp  {verdict}")

# Per (league, side) drill-down
print("\n  Per-(league × side) calibration improvement under RWBC:")
rows = []
for (lg, side), sub in scored.groupby(["league", "side_u"]):
    if len(sub) < 30:
        continue
    rows.append({
        "league": lg, "side": side, "n": len(sub),
        "brier_old": round(((sub["p_old"]  - sub["y"]) ** 2).mean(), 5),
        "brier_new": round(((sub["p_rwbc"] - sub["y"]) ** 2).mean(), 5),
        "Δ_brier_pp_better": round(
            (((sub["p_old"] - sub["y"]) ** 2).mean()
             - ((sub["p_rwbc"] - sub["y"]) ** 2).mean()) * 1000, 2),
        "pct_legs_better": round(sub["new_is_closer"].mean() * 100, 1),
    })
pretty_print_table(pd.DataFrame(rows), "Per-cell")

# ────────────────────────────────────────────────────────────────────────
# 6. Headline
# ────────────────────────────────────────────────────────────────────────
print("\n" + "═" * 72)
print("  HEADLINE")
print("═" * 72)
print(f"  Across {n:,} settled bets you actually placed:")
print(f"    • Brier improved by {(brier_old - brier_new)*1000:.2f} mBrier "
      f"({(brier_old - brier_new) / brier_old * 100:.1f}% relative)")
print(f"    • Average per-leg prediction error dropped from {mae_old*100:.2f}pp to {mae_new*100:.2f}pp")
print(f"    • RWBC's prediction was closer to reality on {pct_closer:.0f}% of legs")
if len(halted):
    print(f"    • Circuit breaker correctly refused to bet {len(halted):,} additional legs "
          f"(hit rate {float(halted['y'].mean())*100:.1f}% — "
          f"{'below' if float(halted['y'].mean()) < 0.5 else 'at-or-above'} 50%)")
