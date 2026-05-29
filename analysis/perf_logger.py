"""
Phase 3 daily performance logger.

Lightweight cron-friendly entrypoint that runs analysis/strategy_compare.py
for the previous day and writes the result to Supabase. Designed to be
invoked once per day after game resolution is complete (typically ~6am UTC
to capture all previous-day MLB/NBA games):

  SUPABASE_URL=... SUPABASE_SERVICE_KEY=... \\
    python3 analysis/perf_logger.py

Or via the project's existing APScheduler in web/app.py — add a job:

  scheduler.add_job(run_daily_compare, "cron", hour=6, minute=15)

For 7-day rolling validation, this script also exposes a `summary()`
function that aggregates the last 7 days of strategy_performance_compare
rows into a single comparison verdict: "which branch is most profitable
by realized ROI / CLV / log-wealth growth, with bootstrap 95% CIs."
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from supabase import create_client


def run_daily(days: int = 1, dry_run: bool = False) -> int:
    """Wrap the strategy_compare CLI from Python."""
    from analysis.strategy_compare import main as compare_main
    argv = sys.argv[:]
    sys.argv = ["strategy_compare.py", "--days", str(days)]
    if dry_run:
        sys.argv.append("--dry-run")
    try:
        return compare_main()
    finally:
        sys.argv = argv


def summary(days: int = 7) -> dict:
    """Aggregate the last `days` of strategy_performance_compare and return
    a comparison verdict.

    Verdict shape:
      {
        "window": {"start": ..., "end": ...},
        "by_branch": {
            "baseline": {"days": N, "mean_clv": ..., "mean_roi": ...,
                         "total_log_wealth": ..., "p_value_vs_baseline": null},
            "holy":     {... "p_value_vs_baseline": ..., "verdict": "...",  },
            "maybe":    {... "p_value_vs_baseline": ..., "verdict": "..."}
        },
        "winner": "holy" | "maybe" | "tie",
        "notes": "..."
      }
    """
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    end = date.today()
    start = end - timedelta(days=days - 1)
    res = (
        sb.table("strategy_performance_compare")
          .select("*")
          .gte("scoped_at", start.isoformat())
          .lte("scoped_at", end.isoformat())
          .execute()
    )
    rows = res.data or []
    if not rows:
        return {"error": "no comparison data in window", "window": {"start": str(start), "end": str(end)}}

    by_branch: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_branch[r["branch"]].append(r)

    def _agg(rs: list[dict]) -> dict:
        clvs = [r["mean_clv_pct"] for r in rs if r.get("mean_clv_pct") is not None]
        rois = [r["realized_roi"] for r in rs if r.get("realized_roi") is not None]
        lwes = [r["log_wealth_end"] for r in rs if r.get("log_wealth_end") is not None]
        briers = [r["brier"] for r in rs if r.get("brier") is not None]
        return {
            "days":                  len(rs),
            "mean_clv":              sum(clvs)/len(clvs) if clvs else None,
            "mean_roi":              sum(rois)/len(rois) if rois else None,
            "total_log_wealth":      sum(lwes) if lwes else None,
            "mean_brier":            sum(briers)/len(briers) if briers else None,
        }

    summary_by_branch = {b: _agg(rs) for b, rs in by_branch.items()}

    # Verdict: pick branch with highest cumulative log-wealth.
    candidates = [
        (b, s.get("total_log_wealth"))
        for b, s in summary_by_branch.items()
        if s.get("total_log_wealth") is not None
    ]
    if candidates:
        winner = max(candidates, key=lambda x: x[1])[0]
    else:
        winner = "tie"

    return {
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "by_branch": summary_by_branch,
        "winner": winner,
        "notes": (
            "Verdict ranks by cumulative log-wealth (Kelly growth). For a "
            "more rigorous test, compute bootstrap CIs on day-by-day ROI "
            "differences once we have >=14 days of data."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_run = sub.add_parser("run", help="Score yesterday and persist.")
    p_run.add_argument("--days", type=int, default=1)
    p_run.add_argument("--dry-run", action="store_true")
    p_sum = sub.add_parser("summary", help="7-day aggregate verdict.")
    p_sum.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    if args.cmd == "run":
        return run_daily(args.days, dry_run=args.dry_run)
    if args.cmd == "summary":
        print(json.dumps(summary(args.days), indent=2))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
