-- migration_010: Stripe billing state on user_config.
--
-- Adds the columns the billing layer needs to map a Supabase user to their
-- Stripe customer + subscription, so the app can gate access behind an
-- active trial/subscription.
--
--   stripe_customer_id    — Stripe Customer id (cus_…). One per user; created
--                           lazily on first checkout. Indexed because the
--                           webhook looks rows up by this id (it only has the
--                           Stripe customer, not our user_id).
--   subscription_status   — mirrors Stripe's subscription.status, e.g.
--                           'trialing', 'active', 'past_due', 'canceled',
--                           'incomplete', 'unpaid'. NULL = never subscribed.
--   subscription_plan     — 'monthly' | 'yearly' (our own label, derived from
--                           the Stripe Price id at checkout).
--   current_period_end    — when the current paid/trial period ends. Lets the
--                           client show "renews/ends on …" and lets us treat a
--                           canceled-but-not-yet-expired sub as still active
--                           until period end.
--
-- Safe to run multiple times (IF NOT EXISTS guards).

alter table user_config
  add column if not exists stripe_customer_id  text,
  add column if not exists subscription_status text,
  add column if not exists subscription_plan   text,
  add column if not exists current_period_end  timestamptz;

-- The webhook resolves the user row from the Stripe customer id, so index it.
create index if not exists idx_user_config_stripe_customer
  on user_config (stripe_customer_id);
