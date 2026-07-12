-- migration_016: per-book entry + closing snapshots for the CLV dataset.
--
-- ⚠️ OPTIONAL / FUTURE. The live code does NOT depend on this migration.
-- This environment has no DDL access (no psql, no direct Postgres connection
-- string, the PostgREST service key cannot run ALTER TABLE, and there is no
-- SQL-exec RPC), so the capture was implemented WITHOUT schema changes: the
-- per-book CLOSE snapshot + capture lead are stored inside the EXISTING
-- market_observatory.books jsonb under reserved "_close*" keys (see
-- engine/clv_checker.update_observatory_closing_lines). Apply this migration
-- ONLY if you later get dashboard/psql access and want to graduate the close
-- into dedicated typed columns; if you do, also update that method to write
-- the columns instead of the jsonb keys.
--
-- Goal (for reference): capture, for every priced market and every placed bet,
-- each book's de-vigged probability at ENTRY and at the last pre-tip scrape
-- (CLOSE), plus when that close was captured — the raw material for a clean,
-- same-book / same-devig closing-line-value dataset.
--
-- All statements are additive and idempotent (IF NOT EXISTS).

-- ── legs: the bets we actually place ───────────────────────────────────────
-- `books`        — per-book de-vigged prob for the bet side, at log time
--                  (entry). Shape: {"fanduel": 0.61, "pinnacle": 0.62, ...}.
-- `closing_books`— same shape, captured at the last pre-tip scrape (close).
-- `closing_lead_min` / `closing_captured_at` — capture quality (migration_012).
alter table legs add column if not exists books              jsonb       default '{}'::jsonb;
alter table legs add column if not exists closing_books       jsonb       default '{}'::jsonb;
alter table legs add column if not exists closing_lead_min    numeric;       -- minutes before game_start at capture
alter table legs add column if not exists closing_captured_at timestamptz;

-- ── market_observatory: every priced market (bet or not) ───────────────────
-- `books` already exists (migration_003) = entry per-book devigs.
-- Add the closing per-book snapshot + capture-quality columns.
alter table market_observatory add column if not exists closing_books       jsonb       default '{}'::jsonb;
alter table market_observatory add column if not exists closing_lead_min    numeric;
alter table market_observatory add column if not exists closing_captured_at timestamptz;

do $$ begin
  raise notice 'migration_016 applied: per-book entry/closing snapshots on legs + market_observatory';
end $$;
