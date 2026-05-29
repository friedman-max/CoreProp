#!/usr/bin/env bash
#
# Phase 3: backfill the last 7 days of strategy comparison data.
#
# Usage:
#   SUPABASE_URL=... SUPABASE_SERVICE_KEY=... bash analysis/run_phase3_backfill.sh
#
# Apply migration_013.sql first (via Supabase SQL editor or psql) to
# create the strategy_performance_compare table. Then run this to seed
# the first 7 days. After that, schedule analysis/perf_logger.py to run
# daily.
set -e

if [ -z "${SUPABASE_URL:-}" ] || [ -z "${SUPABASE_SERVICE_KEY:-}" ]; then
  echo "ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY env vars required." >&2
  exit 1
fi

cd "$(dirname "$0")/.."

echo ">>> Phase 3 backfill — last 7 days, all three branches"
python3 analysis/strategy_compare.py --days 7

echo
echo ">>> 7-day verdict:"
python3 analysis/perf_logger.py summary --days 7
