-- migration_008: enforce hard duplicate-leg prevention at the DB level.
--
-- Application-level dedup in engine/backtest.BacktestLogger relies on
-- reading recent slips, building an in-memory key set, and skipping
-- conflicting legs at write time. This is fundamentally race-prone:
--
--   * Render runs multiple web workers; each worker has its own
--     in-process `threading.Lock`, so concurrent auto-log threads in
--     different workers can both pass the dedup gate and both write.
--   * The legacy manual add-slip endpoint in web/app.py bypasses the
--     dedup logic entirely.
--   * The 48-hour read window in _load_dedup_sets() expires for any
--     prop that sits on the board >48 h.
--
-- The only way to *guarantee* no duplicate leg ever lands again is a
-- database constraint. This migration adds a `dedup_key` column and a
-- PARTIAL UNIQUE INDEX scoped to `(user_id, dedup_key)`. Existing rows
-- have NULL dedup_keys and are grandfathered (not subject to the
-- constraint), so historical duplicates are preserved. Every new leg
-- written by app code populates `dedup_key`; the second attempt to
-- write the same canonical leg is rejected by Postgres with a unique-
-- violation error that the app catches and treats as "already logged".
--
-- Canonical fingerprint format (computed in engine/backtest.make_leg_fingerprint):
--   "<normalized_player>|<normalized_prop>|<line_4dp>|<normalized_side>|<game_sports_day>"
--
-- The fingerprint encodes everything that defines "the same market":
-- player identity (unidecode + lowercase + punctuation normalization),
-- prop type, line value (float-formatted), bet side, and the 12 h-shifted
-- "sports day" UTC date so a single game can't straddle the midnight
-- boundary.

alter table legs
  add column if not exists dedup_key text;

create unique index if not exists legs_user_dedup_key_unique
  on legs (user_id, dedup_key)
  where dedup_key is not null;
