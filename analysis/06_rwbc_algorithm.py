"""
PROBE 6 — Reliability-Weighted Bayesian Calibration (RWBC)

Algorithm derived from:
  Walsh & Joshi (2024)   — fit on calibration (Brier), not accuracy
  Clements (2005)        — time-varying variance / regime adaptation
  Petrozziello (2022)    — realized volatility as direct calibrator input
  Goller & Heiniger (2022) — discount slip EV for cross-leg correlation

The core idea (Walsh & Joshi distilled):
  For each cell c, decompose Brier into Resolution(c) and Reliability(c).
    Resolution measures how much the model differentiates within the cell
    (good: high spread of bucket predictions).
    Reliability measures how far bucket predictions are from realized rates
    (good: low spread of bucket prediction errors).
  The cell's "trust weight" is:
       w_c = Resolution(c) / (Resolution(c) + Reliability_error(c) + ε)
  And the calibrated prediction is:
       p_cal = w_c · p_model  +  (1 - w_c) · p_cell_posterior
  where p_cell_posterior is the Beta-Binomial shrunk cell mean.

  This makes the algorithm self-equalizing: cells where the model is well-
  calibrated retain their signal; cells where it isn't collapse smoothly
  toward the empirical mean, *without* hard-dropping anything.

A "value line" is then:
       p_cal × payout > 1.0 + edge_margin
  where payout is the per-pick implied payout of the chosen slip size.
"""
from common import fetch_all, attach_y, settled, bucket_prob, beta_binomial_posterior, brier_score, pretty_print_table
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
print(f"  global hit rate (prior for Beta-Binomial): {GLOBAL_HIT:.4f}")

# ────────────────────────────────────────────────────────────────────────
# Fit per-cell Resolution and Reliability decomposition
# Cell = (league, prop, side)
# Within each cell, bucketize predictions in 5pp width.
# ────────────────────────────────────────────────────────────────────────

def fit_cell_calibration(cell_df: pd.DataFrame, prior_strength: float = 30.0):
    """Decompose Brier into Resolution and Reliability per Walsh & Joshi.

    Returns dict with:
      - n_cell, mean_pred_cell, mean_obs_cell
      - resolution (RES) = sum_b n_b * (p_b_pred - mean_pred_cell)^2 / n_cell
      - reliability_error (REL) = sum_b n_b * (p_b_pred - p_b_obs)^2 / n_cell
      - w_cell = RES / (RES + REL + eps)   in [0, 1]
      - p_cell_posterior, p_cell_post_low, p_cell_post_high
    """
    n_cell = len(cell_df)
    if n_cell < 5:
        return None
    mean_pred = cell_df["p"].mean()
    mean_obs = cell_df["y"].mean()

    # Bucket-level stats. We use the model's *prediction* as the bucket key
    # so resolution measures how predictions spread, and reliability measures
    # how predictions land relative to outcomes.
    bucket_g = (
        cell_df.groupby("bucket")
               .agg(n=("y", "size"), p_pred_b=("p", "mean"), p_obs_b=("y", "mean"))
               .reset_index()
    )
    # Resolution: variance of bucket predictions around cell mean.
    res = ((bucket_g["n"] * (bucket_g["p_pred_b"] - mean_pred) ** 2).sum()) / n_cell
    # Reliability ERROR: squared distance bucket pred → bucket observed.
    rel_err = ((bucket_g["n"] * (bucket_g["p_pred_b"] - bucket_g["p_obs_b"]) ** 2).sum()) / n_cell

    eps = 1e-6
    w_cell = res / (res + rel_err + eps)
    w_cell = float(max(0.0, min(1.0, w_cell)))

    # Beta-Binomial shrunk cell mean (prior = GLOBAL_HIT).
    k = int(cell_df["y"].sum())
    p_post, p_lo, p_hi = beta_binomial_posterior(k, n_cell, GLOBAL_HIT, prior_strength)

    return {
        "n": n_cell,
        "mean_pred": mean_pred,
        "mean_obs": mean_obs,
        "resolution": res,
        "reliability_error": rel_err,
        "w_cell": w_cell,
        "p_cell_posterior": p_post,
        "p_cell_low": p_lo,
        "p_cell_high": p_hi,
    }

print("\nFitting per-(league, prop, side) calibration cells…")
cells = {}
for key, g in df.groupby(["league", "prop", "side_u"]):
    stat = fit_cell_calibration(g)
    if stat:
        cells[key] = stat

print(f"  cells fit: {len(cells):,}")

# ────────────────────────────────────────────────────────────────────────
# Apply the calibration
# ────────────────────────────────────────────────────────────────────────

def calibrate(row, fallback_p=None):
    key = (row["league"], row["prop"], row["side_u"])
    c = cells.get(key)
    if c is None:
        return fallback_p if fallback_p is not None else row["p"]
    p_model = row["p"]
    p_cell  = c["p_cell_posterior"]
    w       = c["w_cell"]
    return w * p_model + (1 - w) * p_cell

df["p_rwbc"] = df.apply(calibrate, axis=1)

# ────────────────────────────────────────────────────────────────────────
# Compare three calibrators: current, anchored-shrinkage (probe 04 simpler),
# and RWBC (this probe)
# ────────────────────────────────────────────────────────────────────────

ANCHOR = 0.51
# Per-league α from probe 01
GLOBAL_ALPHA = 0.486
df["p_anchored"] = ANCHOR + GLOBAL_ALPHA * (df["p"] - ANCHOR)
df["p_anchored"] = df["p_anchored"].clip(0.01, 0.99)

print("\n────────────────────────────────────────────────────────────────────")
print("  BRIER COMPARISON — all three calibrators on full settled set")
print("────────────────────────────────────────────────────────────────────")
rows = []
for scope, sub in [("GLOBAL", df)] + list(df.groupby("league")):
    rows.append({
        "scope": scope,
        "n": len(sub),
        "brier_current":   round(brier_score(sub["p"], sub["y"]), 5),
        "brier_anchored":  round(brier_score(sub["p_anchored"], sub["y"]), 5),
        "brier_RWBC":      round(brier_score(sub["p_rwbc"], sub["y"]), 5),
        "RWBC vs current": round(brier_score(sub["p"], sub["y"]) - brier_score(sub["p_rwbc"], sub["y"]), 5),
    })
pretty_print_table(pd.DataFrame(rows), "Brier")

# ────────────────────────────────────────────────────────────────────────
# Reliability diagram: do bucket-level realized rates match bucket-level
# predictions under each calibrator? (smaller mean abs error = better)
# ────────────────────────────────────────────────────────────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  RELIABILITY DIAGRAM ERROR  (mean |predicted - observed| per bucket)")
print("  Smaller is better — closer to the diagonal on the calibration plot.")
print("────────────────────────────────────────────────────────────────────")
def diagram_mae(df, p_col):
    g = df.copy()
    g["b"] = g[p_col].apply(lambda p: bucket_prob(p, 0.05))
    gg = g.groupby("b").agg(n=("y", "size"), pred=(p_col, "mean"), obs=("y", "mean")).reset_index()
    gg = gg[gg["n"] >= 50]
    if not len(gg):
        return float("nan")
    return float((gg["n"] * (gg["pred"] - gg["obs"]).abs()).sum() / gg["n"].sum())

rows = []
for scope, sub in [("GLOBAL", df)] + list(df.groupby("league")):
    rows.append({
        "scope": scope,
        "n": len(sub),
        "ECE_current":  round(diagram_mae(sub, "p"),         5),
        "ECE_anchored": round(diagram_mae(sub, "p_anchored"), 5),
        "ECE_RWBC":     round(diagram_mae(sub, "p_rwbc"),    5),
    })
pretty_print_table(pd.DataFrame(rows), "Expected Calibration Error (lower=better)")

# ────────────────────────────────────────────────────────────────────────
# Value-line selection: bet iff p_cal × payout > 1.0 + edge_margin
# Compare per-slip-size what each calibrator selects and how it realizes.
# ────────────────────────────────────────────────────────────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  VALUE-LINE SELECTION  — accept iff p_cal × payout ≥ 1.0 + 0.02 edge")
print("────────────────────────────────────────────────────────────────────")
POWER = {2: 3.0, 3: 5.0, 4: 10.0, 5: 20.0, 6: 35.0}
BE = {k: 1.0 / v ** (1.0 / k) for k, v in POWER.items()}

print("\n  Break-even per-leg hit rates by slip size:")
for k, v in BE.items():
    print(f"    {k}-pick: {v:.4f}  (payout {POWER[k]:.1f}x)")

def realized_ev(legs, n_legs):
    if len(legs) < 1:
        return float("nan"), float("nan"), 0
    hit = float(legs["y"].mean())
    slip_hit = hit ** n_legs
    ev = slip_hit * POWER[n_legs] - 1.0
    return hit, ev, len(legs)

print()
for n_legs in (3, 4, 5, 6):
    be = BE[n_legs]
    thresh = be + 0.02     # 2pp safety margin
    print(f"\n  ── Slip size {n_legs}-pick (BE {be:.4f}, threshold {thresh:.4f}) ──")
    for name, col in (("current", "p"), ("anchored", "p_anchored"), ("RWBC", "p_rwbc")):
        accepted = df[df[col] >= thresh]
        hit, ev, n = realized_ev(accepted, n_legs)
        print(f"    {name:>10}: n={n:>5}  realized hit {hit:.4f}  EV/leg {ev:+.4f}")

# ────────────────────────────────────────────────────────────────────────
# Show the actual cell weights so we know what RWBC learned
# ────────────────────────────────────────────────────────────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  RWBC cell-trust weights (top 20 most-trusted, bottom 10 least-trusted)")
print("  w_cell near 1 → trust the model in this cell")
print("  w_cell near 0 → ignore model, use cell empirical mean (Beta-Binomial)")
print("────────────────────────────────────────────────────────────────────")
cell_df = pd.DataFrame([
    {"league": lg, "prop": prop, "side": sd, **stat}
    for (lg, prop, sd), stat in cells.items()
])
cell_df = cell_df[cell_df["n"] >= 50].copy()  # ignore thin cells in display
cell_df = cell_df.sort_values("w_cell", ascending=False)
top = cell_df.head(20)[["league", "prop", "side", "n", "mean_pred", "mean_obs",
                         "w_cell", "p_cell_posterior"]]
bot = cell_df.tail(10)[["league", "prop", "side", "n", "mean_pred", "mean_obs",
                         "w_cell", "p_cell_posterior"]]
pretty_print_table(top, "MOST trusted cells (RWBC respects model here)")
pretty_print_table(bot, "LEAST trusted cells (RWBC ignores model, uses Beta-Binomial)")

# ────────────────────────────────────────────────────────────────────────
# Per-cell EV scan: for each accepted leg under RWBC, what was the expected
# EV (via p_rwbc) vs realized?
# ────────────────────────────────────────────────────────────────────────
print("\n────────────────────────────────────────────────────────────────────")
print("  EXPECTED vs REALIZED HIT RATE under RWBC (bucket-by-bucket)")
print("  If RWBC is self-equalizing, predicted and realized columns track.")
print("────────────────────────────────────────────────────────────────────")
df["rwbc_bucket"] = df["p_rwbc"].apply(lambda p: bucket_prob(p, 0.025))
chk = (
    df.groupby("rwbc_bucket")
      .agg(n=("y", "size"), predicted=("p_rwbc", "mean"), realized=("y", "mean")).reset_index()
)
chk = chk[chk["n"] >= 100]
chk["gap"] = chk["realized"] - chk["predicted"]
pretty_print_table(chk, "RWBC calibration (small gap = self-equalized)")
