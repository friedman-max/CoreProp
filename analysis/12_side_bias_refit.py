"""Probe 12: SIDE_BIAS refit with split-half stability check.

Recomputes the per-(league, side) calibration gap — observed hit rate minus
mean predicted raw_true_prob — over a trailing window of settled
market_observatory rows, and prescribes a config.SIDE_BIAS table.

A cell is prescribed a nonzero correction ONLY when:
  1. both halves of the window (split by game_start) have >= MIN_N_HALF rows,
  2. the gap has the SAME SIGN in both halves (stability), and
  3. the pooled gap's 95% CI excludes zero.
The prescribed delta is the SMALLER-magnitude half's gap (conservative).

Why this exists: the original May-2026 FINDINGS table failed out-of-sample
validation on 2026-07-02 — WNBA entries had REVERSED SIGN on fresh data
(under: +0.083 fitted vs -0.022 observed) while MLB under replicated in sign
but at ~40% of the fitted magnitude. Corrections must be re-earned, not
trusted. Run monthly (or at season start for a returning league) and paste
the printed table into config.py.

Usage:  python analysis/12_side_bias_refit.py [--days 28]
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import httpx

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _env() -> dict:
    out = {}
    with open(os.path.join(REPO, ".env")) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _pull(url: str, key: str, since_iso: str) -> list[dict]:
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}
    rows, off = [], 0
    with httpx.Client(timeout=90) as c:
        while True:
            r = c.get(
                f"{url}/rest/v1/market_observatory",
                headers=headers,
                params={
                    "select": "league,side,raw_true_prob,result,game_start",
                    "result": "in.(hit,miss)",
                    "game_start": f"gte.{since_iso}",
                    "limit": 1000,
                    "offset": off,
                },
            )
            r.raise_for_status()
            batch = r.json()
            rows += batch
            if len(batch) < 1000:
                break
            off += 1000
    return rows


def _gap(cell_rows: list[tuple[float, int]]) -> tuple[int, float, float, float]:
    """(n, mean_pred, obs_rate, gap)."""
    n = len(cell_rows)
    pred = sum(p for p, _ in cell_rows) / n
    obs = sum(o for _, o in cell_rows) / n
    return n, pred, obs, obs - pred


def _gap_ci_excludes_zero(cell_rows: list[tuple[float, int]]) -> bool:
    """95% CI on (obs - pred). Treats pred as fixed; binomial SE on obs."""
    n, _, obs, gap = _gap(cell_rows)
    se = math.sqrt(max(obs * (1 - obs), 1e-9) / n)
    return abs(gap) > 1.96 * se


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=28, help="trailing window")
    ap.add_argument("--min-n-half", type=int, default=300)
    args = ap.parse_args()

    env = _env()
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    rows = _pull(env["SUPABASE_URL"].rstrip("/"), env["SUPABASE_SERVICE_KEY"],
                 since.isoformat())
    print(f"settled rows in trailing {args.days}d window: {len(rows)}")
    if not rows:
        sys.exit("no data — is result resolution running?")

    rows.sort(key=lambda x: x.get("game_start") or "")
    midpoint = rows[len(rows) // 2].get("game_start")
    print(f"split-half boundary (game_start): {midpoint}\n")

    halves: list[dict] = [defaultdict(list), defaultdict(list)]
    pooled: dict = defaultdict(list)
    for x in rows:
        try:
            p = float(x["raw_true_prob"])
        except (TypeError, ValueError):
            continue
        cell = ((x["league"] or "").upper(), (x["side"] or "").lower())
        rec = (p, 1 if x["result"] == "hit" else 0)
        idx = 0 if (x.get("game_start") or "") < midpoint else 1
        halves[idx][cell].append(rec)
        pooled[cell].append(rec)

    prescribed: dict = {}
    hdr = (f"{'cell':16s} {'n1':>5s} {'gap1':>7s} {'n2':>5s} {'gap2':>7s} "
           f"{'pooled':>7s} {'verdict'}")
    print(hdr)
    for cell in sorted(pooled):
        h1, h2 = halves[0].get(cell, []), halves[1].get(cell, [])
        if len(h1) < args.min_n_half or len(h2) < args.min_n_half:
            n, _, _, g = _gap(pooled[cell])
            print(f"{cell[0]+' '+cell[1]:16s} {len(h1):5d} {'-':>7s} "
                  f"{len(h2):5d} {'-':>7s} {g:+7.3f} thin (n<{args.min_n_half}/half)")
            continue
        _, _, _, g1 = _gap(h1)
        _, _, _, g2 = _gap(h2)
        _, _, _, gp = _gap(pooled[cell])
        stable = (g1 * g2 > 0)
        signif = _gap_ci_excludes_zero(pooled[cell])
        if stable and signif:
            delta = g1 if abs(g1) < abs(g2) else g2  # conservative half
            prescribed[cell] = round(delta, 3)
            verdict = f"APPLY {delta:+.3f}"
        else:
            verdict = "zero (unstable)" if not stable else "zero (CI spans 0)"
        print(f"{cell[0]+' '+cell[1]:16s} {len(h1):5d} {g1:+7.3f} "
              f"{len(h2):5d} {g2:+7.3f} {gp:+7.3f} {verdict}")

    print("\n# Prescribed table — paste into config.py:")
    print("SIDE_BIAS = {")
    for (lg, sd), d in sorted(prescribed.items()):
        print(f'    ("{lg}", "{sd}"): {d:+.3f},')
    print("}")


if __name__ == "__main__":
    main()
