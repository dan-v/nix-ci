-- Composite index for `failed_output_hits`:
--     SELECT output_path FROM failed_outputs
--      WHERE output_path = ANY($1::text[]) AND expires_at > now()
--
-- Without this, PG falls back to a primary-key lookup per element of
-- $1 (fine at small scale) but scans expired rows sequentially once
-- the table grows into the millions. Under nixpkgs-scale load —
-- 10K-drv batches, 10–50 ingests/sec — that full-table scan became
-- a top-10% CPU consumer on the coordinator.
--
-- The index covers both filter columns so a merge-skip scan can
-- answer the hit-rate query without touching the heap.
CREATE INDEX IF NOT EXISTS failed_outputs_lookup_idx
    ON failed_outputs (output_path, expires_at);
