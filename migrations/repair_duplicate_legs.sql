-- ============================================================================
-- Maintenance: repair slips with duplicated legs (the "12-leg slip" bug)
-- ============================================================================
-- One-off cleanup for slips corrupted by the pre-fix lost-response double
-- insert: a PostgREST batch legs-insert that committed server-side but whose
-- response was lost got retried, inserting every leg a second time. A 6-leg
-- Power slip ended up with 12 rows (two per leg_num). The write path is fixed
-- in engine/backtest.insert_legs_idempotent; this script cleans up rows written
-- before that fix.
--
-- Strategy: within each (slip_id, leg_num) group, keep exactly one row and
-- delete the rest. `ctid` (Postgres's always-present physical row id) is the
-- tiebreaker so this works whether or not the legs table has a serial `id`
-- column. Only groups with more than one row are touched, so this is a no-op
-- on a healthy database.
--
-- SAFE: idempotent (re-running finds nothing after the first pass) and scoped
-- to genuine duplicates only. Run the SELECT first to see what would change.

-- 1. Preview — how many duplicate leg rows exist, by slip.
select slip_id,
       count(*)                          as total_leg_rows,
       count(distinct leg_num)           as distinct_legs,
       count(*) - count(distinct leg_num) as duplicate_rows
from legs
group by slip_id
having count(*) <> count(distinct leg_num)
order by duplicate_rows desc;

-- 2. Repair — delete the surplus rows, keeping the earliest ctid per
--    (slip_id, leg_num). Comment out step 1 and run this block to apply.
delete from legs a
using legs b
where a.slip_id = b.slip_id
  and a.leg_num  = b.leg_num
  and a.ctid     > b.ctid;   -- keep the lowest ctid in each group

-- 3. Verify — should return zero rows after the repair.
select slip_id, count(*) - count(distinct leg_num) as duplicate_rows
from legs
group by slip_id
having count(*) <> count(distinct leg_num);
