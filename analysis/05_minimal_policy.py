"""
PROBE 5 — Bare-minimum profitable policy search.

The model's per-cell selection signal is weak or inverted on most cells.
The dominant edge is just the OVER/UNDER structural bias and avoiding
known-bad cells.

This probe enumerates simple policies and reports hit rate + 3-pick /
4-pick / 5-pick / 6-pick Power EV for each. Lets us see if ANY policy
with this model survives at any slip size.
"""
from common import fetch_all, attach_y, settled, pretty_print_table
import pandas as pd

print("Loading settled observatory…")
df = fetch_all(
    "market_observatory", "league, prop, side, true_prob, result",
    filters={"result": ("neq", "pending"), "league": ("in_", ["NBA", "WNBA", "MLB", "NHL"])},
    order_col="resolved_at",
)
df = settled(attach_y(df))
df = df[df["true_prob"].notna() & df["side"].notna()].copy()
df["side_u"] = df["side"].str.upper()
df["p"] = df["true_prob"].astype(float)
print(f"  n={len(df):,}")

POWER_PAYOUT = {2: 3.0, 3: 5.0, 4: 10.0, 5: 20.0, 6: 35.0}

def ev(hit_rate: float, n_legs: int) -> float:
    """3-pick → hit_rate^3 * 5 - 1, etc. assumes independent legs."""
    return hit_rate ** n_legs * POWER_PAYOUT[n_legs] - 1.0

def evaluate_policy(name: str, accepted: pd.DataFrame):
    n = len(accepted)
    hit = accepted["y"].mean() if n else float("nan")
    row = {"policy": name, "n_accepted": n, "hit_rate": round(hit, 4) if n else None}
    for k in (2, 3, 4, 5, 6):
        row[f"ev_{k}pick"] = round(ev(hit, k), 4) if n else None
    return row

# All policies operate on subsets of the observatory universe.
DROP_CELLS = {("NBA", "OVER"), ("NHL", "OVER"), ("NHL", "UNDER"), ("WNBA", "OVER")}
df["cell"] = list(zip(df["league"], df["side_u"]))
df["cell_dropped"] = df["cell"].isin(DROP_CELLS)

policies = []
policies.append(evaluate_policy("ALL observatory (baseline)", df))
policies.append(evaluate_policy("UNDER only", df[df.side_u == "UNDER"]))
policies.append(evaluate_policy("OVER only", df[df.side_u == "OVER"]))
policies.append(evaluate_policy("UNDER + drop bad cells", df[(df.side_u == "UNDER") & ~df.cell_dropped]))
policies.append(evaluate_policy("OVER + drop bad cells", df[(df.side_u == "OVER") & ~df.cell_dropped]))
policies.append(evaluate_policy("ANY + drop bad cells", df[~df.cell_dropped]))
policies.append(evaluate_policy("Model p ≥ 0.55", df[df.p >= 0.55]))
policies.append(evaluate_policy("Model p ≥ 0.60", df[df.p >= 0.60]))
policies.append(evaluate_policy("Model p ≥ 0.65", df[df.p >= 0.65]))
policies.append(evaluate_policy("p ≥ 0.55 + UNDER + drop bad", df[(df.p >= 0.55) & (df.side_u == "UNDER") & ~df.cell_dropped]))
policies.append(evaluate_policy("p ≥ 0.60 + UNDER + drop bad", df[(df.p >= 0.60) & (df.side_u == "UNDER") & ~df.cell_dropped]))

# Top per-league/side cells (≥ +3pp baseline edge)
HOT_CELLS = [
    ("NBA",  "UNDER"),
    ("WNBA", "UNDER"),
    ("MLB",  "UNDER"),
    ("MLB",  "OVER"),  # MLB OVER was nearly zero gap — worth testing
]
hot = df[df.cell.isin(HOT_CELLS)]
policies.append(evaluate_policy("Hot cells only", hot))
policies.append(evaluate_policy("Hot cells + p ≥ 0.55", hot[hot.p >= 0.55]))
policies.append(evaluate_policy("Hot cells + p ≥ 0.60", hot[hot.p >= 0.60]))

# League-specific
for lg in ("NBA", "MLB", "WNBA"):
    sub = df[(df.league == lg) & (df.side_u == "UNDER")]
    policies.append(evaluate_policy(f"{lg} UNDER only", sub))
    policies.append(evaluate_policy(f"{lg} UNDER + p≥0.55", sub[sub.p >= 0.55]))

print("\n────────────────────────────────────────────────────────────────────")
print("  POLICY EV TABLE")
print("  ev_Kpick = expected return per $1 stake on a K-leg Power slip")
print("  (assumes independent legs — best case; correlations make it worse)")
print("  Break-even per-leg hit rates: 2-pick 57.7%, 3-pick 58.5%, 4-pick 56.2%, 5-pick 54.9%, 6-pick 53.8%")
print("────────────────────────────────────────────────────────────────────")
result = pd.DataFrame(policies)
pretty_print_table(result, "Policy comparison")

print("\nKey notes on this table:")
print("  - hit_rate ≥ 58.5% needed for positive 3-pick EV")
print("  - hit_rate ≥ 53.8% needed for positive 6-pick EV (most forgiving)")
print("  - Any policy with all five ev_Kpick < 0 is unprofitable at every slip size")
print("  - 'n_accepted' is per individual leg — a policy must have N × slips_per_window legs")
print("    to actually let auto-backtest build that many slips")
