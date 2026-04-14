-- Idempotent POST /jobs. When a client retries after a lost response,
-- the same external_ref should resolve to the same job_id rather than
-- minting a fresh one and orphaning the first.
--
-- Partial index (where not null) lets existing rows without an
-- external_ref coexist.
CREATE UNIQUE INDEX jobs_external_ref_uniq
    ON jobs (external_ref)
    WHERE external_ref IS NOT NULL;
