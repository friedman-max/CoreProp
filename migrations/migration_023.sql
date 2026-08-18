-- migration_023: APNs device tokens for the native iOS app.
--
-- WHAT THIS ENABLES
--   The native iOS app (ios/) registers for remote notifications and POSTs its
--   APNs device token to /api/push/apns/register. The auto-backtest worker
--   (web/app.py) then sends that user an Apple Push ("N new +EV slips logged")
--   for the cycles it logs slips — alongside the existing Web Push path for the
--   installable PWA (migration_022).
--
--   This is the NATIVE counterpart to push_subscriptions: Web Push (VAPID) only
--   reaches the browser/PWA, so a separate table + transport is required for
--   APNs. Same shape/isolation discipline as push_subscriptions.
--
-- ISOLATION
--   Owner-scoped exactly like push_subscriptions / slips / legs / user_config:
--   RLS on, with a user_id = auth.uid() policy, so the published anon key can
--   only ever see or modify a user's OWN rows. Writes go through
--   engine/writer.py (service-role, bypasses RLS) with an explicit user_id; the
--   cross-user send path reads with the service-role client PLUS an explicit
--   .eq("user_id", …) — the same two-layer isolation used everywhere.
--
--   device_token is globally unique (one row per device); a re-login on the same
--   device re-points that token's user_id via upsert on_conflict.
--
-- ENV-GATED
--   The application never sends unless the APNS_* keys are configured
--   (engine/push.apns_is_configured), so applying this migration is inert until
--   an Apple Developer APNs key is set — the same discipline as VAPID / billing.

create table if not exists apns_tokens (
  id           uuid        primary key default gen_random_uuid(),
  user_id      uuid        not null,
  device_token text        not null unique,
  environment  text        not null default 'production',  -- 'production' | 'sandbox'
  bundle_id    text,
  created_at   timestamptz default now(),
  updated_at   timestamptz default now()
);

create index if not exists idx_apns_tokens_user on apns_tokens(user_id);

alter table apns_tokens enable row level security;

drop policy if exists "apns_tokens_owner" on apns_tokens;
create policy "apns_tokens_owner" on apns_tokens
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());
