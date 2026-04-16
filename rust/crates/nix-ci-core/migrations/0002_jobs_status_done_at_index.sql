-- Index supporting the operator-facing job-history queries:
--   GET /jobs?status=failed&since=...&cursor=...&limit=N
--   nix-ci list --failed
--
-- Filters by status, orders by done_at DESC, paginates via a
-- done_at cursor. Without this index those queries do a seq scan
-- of the jobs table, which is fine at small scale and painful as
-- terminal job rows accumulate over weeks/months.
--
-- DESC matches the pagination order so the planner can use the
-- index for ORDER BY without a separate sort step.
CREATE INDEX IF NOT EXISTS jobs_status_done_at_idx
    ON jobs (status, done_at DESC);
