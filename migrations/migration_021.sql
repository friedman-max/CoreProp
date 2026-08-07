-- migration_021: funnel telemetry for the landing-page minigame.
--
-- WHAT THIS ENABLES
--   The signed-out landing page runs a daily "beat the market" minigame
--   (GET /api/public/daily-pick and friends, web/minigame.py). The page fires
--   fire-and-forget events — game_viewed, pick_made, revealed,
--   plays_exhausted, cta_clicked — so the funnel from "saw the game" to
--   "clicked the signup CTA" can be measured with real rows instead of
--   invented conversion numbers (the same doctrine that governs every figure
--   on the landing page: state nothing you can't source).
--
-- WHY SERVICE-ROLE ONLY
--   The events arrive at an UNAUTHENTICATED endpoint, so the browser must
--   never be able to write (or read) this table directly with the published
--   anon key — FastAPI validates the event enum and is the only ingress.
--   RLS enabled with NO policies = deny-all for anon/authenticated; the
--   service-role writer (engine/writer.py, purpose "landing.event") bypasses
--   RLS. Grants are revoked too, same defence-in-depth as migration_018.
--
-- WHAT IS DELIBERATELY NOT HERE
--   No IP address, no user agent, no fingerprint. The game is anonymous and
--   the funnel questions ("how many reveals per view", "does exhausting plays
--   drive CTA clicks") don't need identity. `meta` is a small jsonb for
--   page-side context (e.g. which pick, correct/incorrect); the enum on
--   `event` is enforced in FastAPI, not here, so adding an event type is a
--   code change, not a migration.

create table if not exists landing_events (
  id        uuid        primary key default gen_random_uuid(),
  ts        timestamptz default now(),
  event     text        not null,
  day_index int,
  pick_id   text,
  meta      jsonb       default '{}'::jsonb
);

-- Funnel queries slice by day, then by event type within the day.
create index if not exists idx_landing_events_day_event
  on landing_events(day_index, event);

alter table landing_events enable row level security;

-- NO policies on purpose: RLS-enabled + zero policies = deny-all for the
-- anon/authenticated roles. Only the service-role client writes here.

-- Defence in depth (mirrors migration_018): revoke the default PostgREST
-- grants so the table stays closed even if RLS is later disabled by accident.
-- Guarded on role existence so a plain Postgres (local restore, CI) without
-- Supabase's roles doesn't abort the script.
do $$
begin
  if exists (select 1 from pg_roles where rolname = 'anon') then
    revoke all on public.landing_events from anon;
  end if;
  if exists (select 1 from pg_roles where rolname = 'authenticated') then
    revoke all on public.landing_events from authenticated;
  end if;
end $$;

do $$ begin
  raise notice 'migration_021 applied: landing_events (service-role only)';
end $$;
