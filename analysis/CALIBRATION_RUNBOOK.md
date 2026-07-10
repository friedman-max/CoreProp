# Calibration Runbook — keeping true-probability honest

This is the operational process for the ONE thing that makes CoreProp's
predicted probability track the realized hit rate: the per-(league, side)
`SIDE_BIAS` correction and the `AUTO_SLIP_MIN_PROB_FLOOR` gate. There is no
automated decision-probability recalibration in the app (the only scheduled
refit is the leg-pair correlation map). Calibration is a manual, out-of-band
fit — run it on the cadence below or predicted will drift above realized.

## Why this exists (the 2026-07-09 odds audit)

An audit found the model's realized hit rate underperforms its predicted
probability for three compounding reasons:

1. **Slope error, not just offset.** The model's calibration slope is α ≈ 0.49
   (FINDINGS §2): it overstates departure from a coin flip by ~2×. `SIDE_BIAS`
   is a *constant additive* shift and cannot correct a slope. The defensible
   mitigation shipped is the high threshold (`AUTO_SLIP_MIN_PROB_FLOOR = 0.65`),
   where FINDINGS §4 shows realized ≈ 64% (near-calibrated). Do not lower it
   without a fresh fit.

2. **Stale ruler.** `SIDE_BIAS`, `CELL_DROPS`, and the 0.65 gate were all fit
   against the **plain unweighted** consensus (May FINDINGS). A FanDuel-heavy
   weighting was added in July on top of that calibration without re-fitting.
   `CONSENSUS_WEIGHTS_ENABLED` now defaults **false** so the decision prob is
   again the unweighted mean the thresholds were earned on. Re-enable the
   weighting ONLY after re-deriving the threshold + `SIDE_BIAS` on the weighted
   prob (below).

3. **Corrections decay.** A May-fit `SIDE_BIAS` table had signs REVERSE
   out-of-sample by July (WNBA under: +0.083 → −0.022). That is why 7 of 8
   cells are currently zeroed — they failed the sign-stability check, not
   because bias doesn't exist. Bias must be re-earned, never trusted stale.

## Monthly (and at each season's return): refit SIDE_BIAS

```bash
# Requires .env with SUPABASE_URL + SUPABASE_SERVICE_KEY.
python analysis/12_side_bias_refit.py --days 28
```

The probe prescribes a cell's correction ONLY when all hold:
- both split-halves of the window have ≥ 300 settled rows,
- the observed−predicted gap has the **same sign** in both halves, AND
- the pooled gap's 95% CI excludes zero.

It prints a `SIDE_BIAS = { ... }` block. **Review it, then paste into
`config.py`** — never auto-write. If a cell that is currently in `CELL_DROPS`
starts showing a stable positive selection edge for 4+ weeks, consider
re-enabling it; if an allowed cell goes negative, drop it.

## When you turn per-book weighting back on

`CONSENSUS_BOOK_WEIGHTS` (FanDuel-heavy) is a plausible but unvalidated
hypothesis. To adopt it as the live ruler:

1. Accumulate settled `market_observatory` rows logged AFTER the weighting is
   enabled (so `raw_true_prob` reflects the weighted number).
2. Re-run `12_side_bias_refit.py` on that weighted `raw_true_prob`.
3. Re-derive the profitable threshold (probe 04/05 style) on the weighted prob.
4. Confirm the top-slice edge and every `SIDE_BIAS` sign replicate across two
   disjoint windows.
5. Only then set `CONSENSUS_WEIGHTS_ENABLED=true` and commit the refit tables.

Until all five hold, keep weighting OFF: gating and bias-correcting on a ruler
you never validated is the "stale ruler" failure this runbook exists to prevent.

## What is deliberately NOT done

- **No anchored shrinkage** (`calibrated_p = 0.51 + α·(raw−0.51)`). FINDINGS E
  showed a global α-blend over-corrects and collapses the pool below the gate.
  If you ever add it, fit α on the *same weighted ruler* selection uses, and
  gate it behind an env flag defaulting OFF until validated.
- **No hierarchical calibrator.** A per-cell isotonic map now exists (below);
  a partial-pooling / hierarchical version is not built.

## Isotonic recalibration map (engine.calibration_map)

The structural fix for FINDINGS §2 (the slope error a constant `SIDE_BIAS`
can't touch). Per-(league, side) weighted isotonic regression on
`market_observatory.raw_true_prob → outcome` bends the raw consensus onto the
realized curve — offset AND slope in one pass. Fit hourly by the scheduler,
persisted to `data/calibration_map.json` (+ Supabase mirror), applied to the
DECISION prob only, and **only when `CALIBRATION_MAP_ENABLED=true` (default
OFF)**. `raw_true_prob` is never touched — one ruler, same as `SIDE_BIAS`.

A trusted per-cell fit REPLACES that cell's `SIDE_BIAS` (the curve already
carries the offset — stacking would double-correct). Cells without a trusted
fit fall back to `SIDE_BIAS`; a calibrator error falls back too.

### Before you enable it (same discipline as SIDE_BIAS — validate first)

```bash
# Inspect per-cell reliability on the live window.
python analysis/13_calibration_map_report.py --days 90
```

Enable ONLY when, for the cells you actually bet:
- the cell is marked `trusted=YES` (n ≥ MIN_CELL_OBS and ≥ MIN_BINS_SPANNED
  populated bins), AND
- its `|gap|cal` collapses to ~0 from a larger `|gap|raw` (the map is doing
  work), AND
- the improvement replicates on a disjoint earlier window (re-run with a
  different `--days` slice) — the same out-of-sample bar SIDE_BIAS clears.

Then `export CALIBRATION_MAP_ENABLED=true` and restart. Watch the **Calibration
reliability** chart + **Calibration Error (ECE)** stat on the Analytics tab:
dots should hug the diagonal and ECE should fall toward zero.

Caveat (FINDINGS §2): on cells whose raw signal is inverted (e.g. NBA UNDER,
NHL), the monotone fit correctly collapses toward the base rate — the number
becomes honest but uninformative. Those belong in `CELL_DROPS`, not in the
bet pool. Calibration makes predictions truthful; it does not create edge.

Off-season note: the map can only fit leagues with live settled volume. With
NBA/NHL dormant, only MLB/WNBA cells will reach `trusted=YES` — expected.
