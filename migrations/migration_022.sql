-- migration_022: Web Push subscriptions, so a user gets a notification when
-- auto-backtest logs slips for them.
--
-- WHAT THIS ENABLES
--   The auto-backtest worker runs server-side every refresh cycle and logs
--   slips without anyone watching (web/app.py, _auto_log_bg). Until now the
--   only way to find out was to open the site. A Home Screen PWA on iOS 16.4+
--   can receive Web Push, so this table holds the push endpoints to deliver to.
--
-- ONE ROW PER BROWSER, NOT PER USER
--   A push subscription identifies a browser install, not a person: the same
--   account added to the Home Screen on a phone and an iPad is two endpoints,
--   and both should ring. `endpoint` is the primary key because that is what
--   the push service issues and what uniquely names the destination — keying
--   on user_id would silently drop every device but the last one to subscribe.
--
-- WHY SERVICE-ROLE ONLY
--   Same doctrine as migration_018/021. The browser never touches this table
--   directly; it POSTs to /api/push/subscribe, FastAPI validates the shape and
--   the service-role writer (engine/writer.py, purpose "push.subscribe")
--   inserts. RLS enabled with NO policies = deny-all for anon/authenticated.
--
--   That matters more here than for landing_events: `auth` and `p256dh` are
--   the encryption keys for that browser's push channel. Anyone who can read
--   them can send that device a notification that renders as if CoreProp sent
--   it. They are secrets, not telemetry.
--
-- EXPIRY IS NORMAL, NOT AN ERROR
--   Push services retire endpoints routinely, and iOS destroys the
--   subscription outright if the user removes the Home Screen icon. A 404 or
--   410 from the push service means "this endpoint is dead" — the sender
--   deletes the row rather than retrying, which is why there is no retry
--   bookkeeping here.

create table if not exists push_subscriptions (
  endpoint   text        primary key,
  user_id    uuid        not null,
  p256dh     text        not null,
  auth       text        not null,
  created_at timestamptz default now(),
  last_sent  timestamptz
);

-- The only read pattern: "give me every device belonging to this user" at
-- fan-out time, once per user per refresh cycle.
create index if not exists idx_push_subscriptions_user
  on push_subscriptions(user_id);

alter table push_subscriptions enable row level security;

-- NO policies on purpose: RLS-enabled + zero policies = deny-all for the
-- anon/authenticated roles. Only the service-role client touches this.

-- Defence in depth (mirrors migration_018/021): revoke the default PostgREST
-- grants so the table stays closed even if RLS is later disabled by accident.
-- Guarded on role existence so a plain Postgres (local restore, CI) without
-- Supabase's roles doesn't abort the script.
do $$
begin
  if exists (select 1 from pg_roles where rolname = 'anon') then
    revoke all on public.push_subscriptions from anon;
  end if;
  if exists (select 1 from pg_roles where rolname = 'authenticated') then
    revoke all on public.push_subscriptions from authenticated;
  end if;
end $$;

do $$ begin
  raise notice 'migration_022 applied: push_subscriptions (service-role only)';
end $$;
