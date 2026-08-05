"""
Repair legs that were graded DNP because ESPN was unreachable, not because the
player sat out.

Background: `site.api.espn.com` started returning 403 to everything. Every stat
lookup returned None, and the grader's `hours_since_end >= 6` fallback then
marked whole slates DNP — which pushes at 1.0x and silently erases the real P&L.
The host fix plus the fail-closed guard in results_checker.py stop it recurring;
this script fixes the rows already written.

Only rewrites a leg when the box score can now actually be found. A player who
genuinely did not appear stays DNP.

    python3 analysis/repair_false_dnp.py           # dry run, writes nothing
    python3 analysis/repair_false_dnp.py --apply   # write the corrections
"""
import argparse
import collections
import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from engine.database import get_db                              # noqa: E402
from engine.results_checker import ESPNResultsChecker, grade_leg  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    ap.add_argument("--limit", type=int, default=2000)
    args = ap.parse_args()

    db = get_db()
    if not db:
        print("no db client")
        return 1

    rows = (
        db.table("legs")
        .select("slip_id, leg_num, player, league, prop, line, side, result, stat_actual, game_start")
        .eq("result", "dnp")
        .order("game_start", desc=True)
        .limit(args.limit)
        .execute()
        .data
    ) or []
    print(f"DNP legs found: {len(rows)}")

    rc = ESPNResultsChecker()
    changes, still_dnp, unresolvable = [], 0, 0

    for r in rows:
        gs_raw = r.get("game_start")
        if not gs_raw:
            unresolvable += 1
            continue
        try:
            gs = datetime.fromisoformat(str(gs_raw).replace("Z", "+00:00"))
            if gs.tzinfo is None:
                gs = gs.replace(tzinfo=timezone.utc)
        except ValueError:
            unresolvable += 1
            continue

        league = r["league"]
        stats = rc._get_player_stats(league, gs, r["player"])
        actual = rc._compute_stat(stats, r["prop"], league) if stats is not None else None
        if actual is None:
            gl = rc._fetch_gamelog_stats(league, r["player"], gs)
            if gl is not None:
                actual = rc._compute_stat(gl, r["prop"], league)

        # Could not reach the source for this window — leave it alone rather
        # than making the same mistake in the other direction.
        if rc._window_fetch_failed(league, gs):
            unresolvable += 1
            continue

        if actual is None:
            still_dnp += 1          # genuinely absent from the box score
            continue

        try:
            line = float(r.get("line") or 0)
        except (TypeError, ValueError):
            unresolvable += 1
            continue

        new_result = grade_leg(actual, line, r.get("side", "over"))
        changes.append({**r, "new_result": new_result, "actual": actual})

    print(f"  would correct : {len(changes)}")
    print(f"  stay DNP      : {still_dnp}  (really absent from the box score)")
    print(f"  unresolvable  : {unresolvable}  (no game_start / source unreachable)")
    print()
    print("  new outcome mix:", dict(collections.Counter(c["new_result"] for c in changes)))
    print()
    for c in changes[:20]:
        print(f"    {c['league']:5} {c['player']:24} {c['prop']:22} "
              f"{c['side']:5} {c['line']:>5}  actual={c['actual']:<6} -> {c['new_result'].upper()}")
    if len(changes) > 20:
        print(f"    … and {len(changes) - 20} more")

    if not args.apply:
        print("\nDRY RUN — nothing written. Re-run with --apply to commit.")
        return 0

    print(f"\nApplying {len(changes)} corrections…")
    ok, failed = 0, []
    for i, c in enumerate(changes, 1):
        # Retry with backoff: a transient DNS/network blip previously aborted
        # the whole run partway through, which is the one thing you don't want
        # when rewriting graded results.
        for attempt in range(4):
            try:
                db.table("legs").update({
                    "result": c["new_result"],
                    "stat_actual": c["actual"],
                    "resolved_at": datetime.now(timezone.utc).isoformat(),
                }).eq("slip_id", c["slip_id"]).eq("leg_num", int(c["leg_num"])).execute()
                ok += 1
                break
            except Exception as exc:
                if attempt == 3:
                    failed.append((c["slip_id"], c["leg_num"], str(exc)[:60]))
                else:
                    time.sleep(1.5 * (attempt + 1))
        if i % 100 == 0:
            print(f"  … {i}/{len(changes)} ({ok} written)")

    print(f"Updated {ok}/{len(changes)} legs.")
    if failed:
        print(f"{len(failed)} failed — re-run to pick them up:")
        for sid, ln, err in failed[:10]:
            print(f"  {sid}/{ln}: {err}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
