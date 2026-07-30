"""Probe 13: isotonic recalibration-map reliability report.

Reads the same settled market_observatory rows engine.calibration_map fits on
(raw_true_prob -> outcome, per league/side), builds the isotonic map, and
prints — for every cell — the pre- vs post-calibration reliability so you can
decide whether to flip CALIBRATION_MAP_ENABLED on.

Read this BEFORE enabling the map. Per CALIBRATION_RUNBOOK, a calibrator is a
ruler you must validate before gating on it. What to look for:

  * TRUSTED cells only (n >= MIN_CELL_OBS, >= MIN_BINS_SPANNED bins). Untrusted
    cells are printed for context but the app will not apply them.
  * Post-calibration |gap| should be ~0 across the range (that's the whole
    point). If a cell's raw reliability is already flat/inverted, the map will
    correctly collapse it toward the base rate — accurate but uninformative.
    That cell belongs in CELL_DROPS, not in your bet pool.
  * Sanity of the knots: monotone non-decreasing, no wild single-bin jumps.

Out-of-sample discipline: run once on a trailing window, once on a disjoint
earlier window (--days / --end-days), and only trust cells whose corrected
curve holds on both — the same sign-stability bar SIDE_BIAS clears.

Usage:  python analysis/13_calibration_map_report.py [--days 90]
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import httpx

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from engine.calibration_map import (  # noqa: E402
    _fit_cell, _bin_index, MIN_CELL_OBS, MIN_BINS_SPANNED,
)


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
    """Pull settled observatory rows, restricted to STANDARD lines.

    MUST mirror engine.calibration_map._load_settled_rows. The observatory is
    dominated by GOBLIN (green devil) lines, which are never bettable against
    the standard payout table. Reporting on the pooled corpus makes the map
    look excellent (|gap|cal ~0.003 on every cell) because that is in-sample
    fit quality on goblins — while the same map, applied to the 2,036 settled
    STANDARD legs, doubles the calibration gap (-1.03pp -> -2.07pp) and
    inflates the 0.55-threshold pool by 78%. A report that does not filter
    here will green-light a calibrator that damages the bettable universe.
    """
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}
    rows, off = [], 0
    with httpx.Client(timeout=90) as c:
        while True:
            r = c.get(
                f"{url}/rest/v1/market_observatory",
                headers=headers,
                params={
                    "select": "league,side,raw_true_prob,true_prob,result,game_start",
                    "result": "in.(hit,miss)",
                    "game_start": f"gte.{since_iso}",
                    # NULL (pre-migration_019) is UNKNOWN, not standard — it is
                    # excluded on purpose. Expect an empty corpus until the
                    # migration lands and tagged rows accumulate.
                    "odds_type": "eq.standard",
                    "limit": 1000,
                    "offset": off,
                },
            )
            if r.status_code >= 400 and "odds_type" in r.text:
                raise SystemExit(
                    "market_observatory.odds_type does not exist — apply "
                    "migrations/migration_019.sql first.\n"
                    "Refusing to report on the pooled corpus: it is "
                    "goblin-dominated and will overstate the map's quality."
                )
            r.raise_for_status()
            batch = r.json()
            rows += batch
            if len(batch) < 1000:
                break
            off += 1000
    return rows


def _reliability(pairs: list[tuple[float, int]], apply=None) -> float:
    """Weighted mean |predicted - observed| across populated 2% bins. `apply`
    optionally remaps each raw prob first (to score the post-calibration fit).
    """
    hits: dict[int, float] = defaultdict(float)
    predsum: dict[int, float] = defaultdict(float)
    tot: dict[int, int] = defaultdict(int)
    for p, o in pairs:
        idx = _bin_index(p)
        if idx is None:
            continue
        pred = apply(p) if apply else p
        tot[idx] += 1
        hits[idx] += o
        predsum[idx] += pred
    n = sum(tot.values())
    if not n:
        return float("nan")
    err = 0.0
    for idx, cnt in tot.items():
        obs = hits[idx] / cnt
        pred = predsum[idx] / cnt
        err += cnt * abs(pred - obs)
    return err / n


def _apply_from_fit(fit: dict):
    from engine.calibration_map import _interp
    knots = fit["knots"]
    return lambda p: _interp(knots, p)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90, help="trailing window")
    args = ap.parse_args()

    env = _env()
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    rows = _pull(env["SUPABASE_URL"].rstrip("/"), env["SUPABASE_SERVICE_KEY"],
                 since.isoformat())
    print(f"settled rows in trailing {args.days}d window: {len(rows)}\n")
    if not rows:
        sys.exit("no data — is result resolution running?")

    by_cell: dict[tuple[str, str], list[tuple[float, int]]] = defaultdict(list)
    pooled: list[tuple[float, int]] = []
    for x in rows:
        raw = x.get("raw_true_prob")
        if raw is None:
            raw = x.get("true_prob")
        try:
            p = float(raw)
        except (TypeError, ValueError):
            continue
        if p <= 0 or p >= 1:
            continue
        cell = ((x["league"] or "").upper(), (x["side"] or "").lower())
        rec = (p, 1 if x["result"] == "hit" else 0)
        by_cell[cell].append(rec)
        pooled.append(rec)

    hdr = (f"{'cell':16s} {'n':>6s} {'bins':>5s} {'|gap|raw':>9s} "
           f"{'|gap|cal':>9s} {'trusted':>8s}")
    print(hdr)
    print("-" * len(hdr))

    def _report(name: str, pairs: list[tuple[float, int]]):
        fit = _fit_cell(pairs)
        if fit is None:
            print(f"{name:16s} {len(pairs):6d} {'-':>5s} {'-':>9s} "
                  f"{'-':>9s} {'no-fit':>8s}")
            return
        raw_gap = _reliability(pairs)
        cal_gap = _reliability(pairs, apply=_apply_from_fit(fit))
        print(f"{name:16s} {fit['n']:6d} {len(fit['knots']):5d} "
              f"{raw_gap:9.4f} {cal_gap:9.4f} "
              f"{'YES' if fit['trusted'] else 'no':>8s}")

    for cell in sorted(by_cell):
        _report(f"{cell[0]} {cell[1]}", by_cell[cell])
    print("-" * len(hdr))
    _report("GLOBAL", pooled)

    print(f"\nTrust gates: n >= {MIN_CELL_OBS} AND bins >= {MIN_BINS_SPANNED}.")
    print("Only cells marked trusted=YES are applied when CALIBRATION_MAP_ENABLED=true.")
    print("A cell whose |gap|cal is ~0 is well-calibrated post-map. If |gap|raw")
    print("was already tiny, the map buys little; the win is cells with a large")
    print("raw gap collapsing to a small calibrated gap.")


if __name__ == "__main__":
    main()
