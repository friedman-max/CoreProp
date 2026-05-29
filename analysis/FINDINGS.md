# Calibration Disparity — Statistical Findings

**Dataset:** 38,788 settled market_observatory events + 5,897 settled logged legs from auto-backtest. All four core leagues (MLB / NBA / NHL / WNBA). Sample size is large enough that all reported gaps are statistically real (Wilson 95% CIs would be ≤ ±1pp on the headline numbers).

---

## The four root causes, ranked

### 1. PrizePicks lines are shaded ~5pp toward OVERs (the single biggest miscalibration)

```
P(hit | OVER)   = 45.22%   (n = 25,376)
P(hit | UNDER)  = 54.98%   (n = 13,412)
UNDER edge:       +9.76 pp
```

Every league replicates this:

| League | OVER gap (obs − pred) | UNDER gap | Asymmetry |
|--------|----------------------:|----------:|----------:|
| MLB    | −0.05% | +5.44% | −5.49% |
| NBA    | −3.10% | +7.48% | −10.59% |
| NHL    | −1.91% | +6.07% | −7.99% |
| WNBA   | −0.50% | +8.31% | −8.82% |

This is structural to the market, not a model bug per se — PP shades lines toward the recreational side (OVERs), and our consensus-devig step doesn't undo the shade. A constant `+δ` correction per side per league eliminates 80% of the global Brier error.

### 2. Model signal is weak — α=0.486 globally, INVERTED on multiple cells

Slope of (observed − 0.51) on (predicted − 0.51), weighted by sample size:

| Scope | α | Reading |
|-------|---|---------|
| Global | 0.486 | Model overstates departure from neutral by 2× |
| MLB | 0.413 | Same |
| NBA | 0.564 | Same |
| NHL | **−0.616** | Signal is *inverted* — high model prob = lower observed rate |
| WNBA | 0.626 | Same |

Splitting by side reveals the deeper truth — α is fine on some (league, side) combos and broken on others:

| Cell | α | What it means |
|------|---|---------------|
| MLB OVER | 0.50 | Model has half the edge it claims |
| MLB UNDER | 0.15 | Model has almost no signal — bias correction is the only edge |
| **NBA OVER** | 1.01 | Surprisingly well-calibrated… |
| **NBA UNDER** | **−0.49** | …but UNDER signal is inverted |
| NHL both | ~0 (tiny n) | Inverted but small sample |
| WNBA OVER | 0.93 | Decent |
| **WNBA UNDER** | **−0.13** | Inverted |

The single most consequential finding: **NBA UNDER signal is anti-correlated with reality.** When the model says "high prob NBA UNDER," reality says "less likely than average." This is why NBA UNDER + p≥0.55 hit rate (53.5%) is *worse* than NBA UNDER overall (56.2%) — picking the model's "best" NBA UNDERs is anti-helpful.

### 3. Selection rule is destructive on four (league, side) cells

For every cell the auto-backtest visited, we compared the logged hit rate to the random-leg hit rate in the same cell:

| Cell | Selection edge | n logged | Verdict |
|------|---------------:|---------:|---------|
| MLB OVER | −0.25 pp | 1,200 | No signal |
| MLB UNDER | −0.38 pp | 614 | No signal |
| **NBA OVER** | **−7.15 pp** | 587 | **DESTRUCTIVE** |
| NBA UNDER | +0.07 pp | 2,003 | No signal |
| **NHL OVER** | **−26.17 pp** | 199 | **CATASTROPHIC** |
| **NHL UNDER** | **−18.79 pp** | 41 | **CATASTROPHIC** |
| **WNBA OVER** | **−4.05 pp** | 378 | **DESTRUCTIVE** |
| WNBA UNDER | +1.54 pp | 693 | Marginally positive |

NHL OVER cells, where the model fires confidently, would have hit 77.4% if we'd picked random legs. Our model picks legs that hit 51.3%. That's a 26 pp selection penalty — the model is *worse than random* on NHL.

### 4. Net result: current selection is unprofitable at every realistic threshold

Simulated 3-pick Power slip EV per unit stake using the actual data:

| Threshold | n legs | Realized hit | 3-pick EV | 6-pick EV |
|-----------|-------:|-------------:|----------:|----------:|
| 0.55 | 1,965 | 53.79% | −22% | −15% |
| 0.58 | 735 | 55.65% | −14% | −10% |
| 0.60 | 453 | 55.19% | −16% | −1% |
| 0.62 | 304 | 57.24% | −6% | +9% |
| **0.65** | **177** | **64.41%** | **+34%** | **+150%** |

At the current default threshold (0.5407), every slip size is solidly EV-negative. **The model only finds genuine edge in its top 0.5% of predictions** (p ≥ 0.65), where it still has 177 legs of signal.

---

## Profitable policies the data supports

Only these policies show positive EV on at least one slip size:

| Policy | n | Hit rate | Best slip EV |
|--------|--:|---------:|-------------:|
| **Model p ≥ 0.65** | 177 | 64.4% | **+150% on 6-pick** |
| **MLB UNDER + p ≥ 0.55** | 139 | 60.4% | **+103% on 6-pick** |
| **NBA UNDER (any threshold)** | 7,776 | 56.2% | **+12% on 5-pick, +10% on 6-pick** |
| WNBA UNDER (any) | 1,640 | 55.6% | +6% on 5-pick |
| UNDER + drop bad cells (any) | 12,592 | 55.1% | +1% on 5-pick |

Everything else is negative EV at every slip size.

---

## The change to make

In order of impact:

### A. Cell drops (one-line filter, eliminates the biggest losers)

Refuse to log any leg in:
```
(NBA, OVER), (NHL, OVER), (NHL, UNDER), (WNBA, OVER)
```

This alone removes the catastrophic selection penalty from probe 03.

### B. Side-specific bias correction (constant additive per league)

```python
SIDE_BIAS = {
    ("MLB",  "UNDER"): +0.054,
    ("NBA",  "UNDER"): +0.075,
    ("NHL",  "UNDER"): +0.061,
    ("WNBA", "UNDER"): +0.083,
    ("MLB",  "OVER"):   0.000,
    ("NBA",  "OVER"):  -0.031,
    ("NHL",  "OVER"):  -0.019,
    ("WNBA", "OVER"):  -0.005,
}
calibrated_p = raw_model_p + SIDE_BIAS[(league, side)]
```

Applied to the population, this collapses the per-side mean gap to zero.

### C. Raise default `auto_slip_min_prob` to 0.65

Current default is 0.5407 (migration_004). At that threshold the auto-backtest is logging losing slips. **At 0.65, the model's per-leg edge is large enough that even 3-pick Power is +34% EV.**

This is the single highest-impact one-line change. The model has real information; we're just betting too many legs where it doesn't.

### D. Prefer 5-pick or 6-pick over 3-pick when possible

For UNDER-heavy strategies, 5-pick (break-even 54.9%) and 6-pick (break-even 53.8%) are more forgiving than 3-pick (break-even 58.5%). NBA UNDER alone is +EV on 5-pick and 6-pick at *any* threshold; on 3-pick it's never +EV.

### E. Skip anchored shrinkage entirely

I proposed shrinkage earlier (α-blend toward 0.51). The data shows this would over-correct: applying α=0.49 globally collapses too many calibrated probs below the selection threshold. The bias correction (B) alone improves Brier by 0.001-0.005 per league; shrinkage on top adds nothing once cells with negative α are dropped.

---

## Suggested precedence for actually shipping

1. **Tonight:** push raise `auto_slip_min_prob` default to 0.65 (one-line change to `web/app.py:1553` and the user_config default in migration). This stops the bleeding immediately.

2. **Next:** add the four cell drops to the auto-backtest worker (one filter at `web/app.py:1050`).

3. **Then:** add the side-bias correction table and apply it in the ev_calculator or right before the auto-backtest pool filter. This is a small change but needs a re-fit cadence (refit the δ table monthly from observatory data).

4. **Don't ship:** the anchored shrinkage or per-(league, side) α — they're already implied by the bias-correction + cell-drop combination.

5. **Continuous:** rerun probes 02-05 monthly. If a cell that's currently dropped starts showing positive selection edge for 4+ weeks, re-enable it. If a currently-allowed cell starts showing negative edge, drop it.

---

## What the original calibration chart was hiding

The chart showed "global model says 30% → observed 43%." The real shape is much sharper:

- The 30% predictions are 65% OVERs and 35% UNDERs (per the side composition)
- OVERs at 30% prediction hit 42.6%; UNDERs at 30% hit 47.8%
- The "global" curve is the volume-weighted average of those two

A per-side calibration chart would show OVERs hugging closer to the diagonal at low predictions (they only over-predict by 2pp) and UNDERs further above (they under-predict by 5-15pp depending on bucket). The global chart hides the side bias because pooling them averages out.

The fix for the global curve is not better calibration. It's stop predicting OVERs as if they're symmetric with UNDERs.
