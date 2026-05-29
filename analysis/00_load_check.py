"""
Sanity probe — verify env, pull a small slice from each table, confirm we
have settled observations with both raw_true_prob and true_prob populated.
Prints sample sizes so we know what we're working with before running the
heavier probes.
"""
from common import fetch_all, attach_y, settled, pretty_print_table
import pandas as pd

print("Pulling settled market_observatory rows for the four core leagues…")
obs = fetch_all(
    "market_observatory",
    "league, prop, side, true_prob, raw_true_prob, market_width, result, closing_prob, resolved_at, created_at",
    filters={"result": ("neq", "pending"), "league": ("in_", ["NBA", "WNBA", "MLB", "NHL"])},
    order_col="resolved_at",
)
print(f"  raw rows: {len(obs):,}")

obs = attach_y(obs)
obs_s = settled(obs)
print(f"  settled (hit/miss only, excludes push/dnp): {len(obs_s):,}")

print("\nSettled rows per league:")
print(obs_s.groupby("league").size().to_string())

print("\nMissing-data audit on settled rows:")
for col in ("raw_true_prob", "true_prob", "closing_prob", "market_width"):
    n_miss = obs_s[col].isna().sum()
    print(f"  {col:>16}  missing {n_miss:>6,}  ({n_miss/len(obs_s)*100:.1f}%)")

print("\nGlobal hit rate on settled obs (the prior we anchor everything else against):")
overall = obs_s["y"].mean()
print(f"  P(hit) = {overall:.4f}  ({obs_s['y'].sum():,} hits / {len(obs_s):,} settled)")

print("\nPer-league hit rate:")
per_lg = obs_s.groupby("league").agg(n=("y", "size"), hits=("y", "sum"), rate=("y", "mean")).reset_index()
pretty_print_table(per_lg, "League-level base rates")

print("\nSettled legs (logged backtest slips — selection-biased):")
legs = fetch_all(
    "legs",
    "league, prop, side, true_prob, raw_true_prob, result, resolved_at, line",
    filters={"result": ("neq", "pending")},
    order_col="resolved_at",
)
print(f"  raw legs rows: {len(legs):,}")
legs = attach_y(legs)
legs_s = settled(legs)
print(f"  settled legs: {len(legs_s):,}")
if len(legs_s):
    print(f"  P(hit | logged) = {legs_s['y'].mean():.4f}")
    print("\nPer-league hit rate on LOGGED legs (compare against observatory above to see selection bias):")
    per_lg_legs = legs_s.groupby("league").agg(n=("y", "size"), hits=("y", "sum"), rate=("y", "mean")).reset_index()
    pretty_print_table(per_lg_legs, "League hit rate on logged legs")
