-- migration_022: Web Push subscriptions for the installable PWA.
--
-- WHAT THIS ENABLES
--   The installable PWA (phase 1) can receive Web Push on iOS 16.4+. A signed-in
--   user who grants notification permission POSTs their PushSubscription to
--   /api/push/subscribe; the auto-backtest worker (web/app.py) then sends that
--   user a push ("N new +EV slips logged") for the cycles it logs slips.
--
-- ISOLATION
--   Owner-scoped exactly like slips/legs/user_config: RLS on, with a
--   user_id = auth.uid() policy, so the published anon key can only ever see or
--   modify a user's OWN rows. Writes go through engine/writer.py (service-role,
--   which bypasses RLS) with an explicit user_id; the cross-user send path reads
--   with the service-role client PLUS an explicit .eq("user_id", …) — the same
--   two-layer isolation the rest of the codebase uses.
--
--   endpoint is globally unique (one row per browser push endpoint); a re-login
--   on the same device re-points that endpoint's user_id via upsert on_conflict.
--
-- ENV-GATED
--   The application never sends unless VAPID keys are configured
--   (engine/push.is_configured), so applying this migration is inert until then.

create table if not exists push_subscriptions (
  id         uuid        primary key default gen_random_uuid(),
  user_id    uuid        not null,
  endpoint   text        not null unique,
  p256dh     text        not null,
  auth       text        not null,
  user_agent text,
  created_at timestamptz default now()
);

create index if not exists idx_push_subscriptions_user on push_subscriptions(user_id);

alter table push_subscriptions enable row level security;

drop policy if exists "push_subscriptions_owner" on push_subscriptions;
create policy "push_subscriptions_owner" on push_subscriptions
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());
