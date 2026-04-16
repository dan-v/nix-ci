-- Per-attempt build log archive. One row per failed (or transient-
-- retry) build attempt; successful builds do not upload a log.
--
-- Storage model: gzipped bytes inline. Compression is ~10-20× on
-- typical build stderr; with a worker-side cap of 4 MiB raw, a row's
-- log_gz is typically <500 KiB. Postgres TOAST handles >2 KiB blobs
-- out-of-line so the heap stays compact.
--
-- Lifecycle: pruned by the cleanup loop based on
-- `build_log_retention_days` (separate from job retention so we can
-- keep job metadata longer than fat log blobs).
--
-- Trust boundary: claim_id is supplied by the worker and not validated
-- against the in-memory claims map (the claim is typically gone by the
-- time the upload arrives — that's why the table is keyed on it). The
-- (job_id, claim_id) tuple is what callers reference in retrieval.
CREATE TABLE build_logs (
    claim_id        UUID PRIMARY KEY,
    job_id          UUID NOT NULL,
    drv_hash        TEXT NOT NULL,
    attempt         INT  NOT NULL,
    success         BOOLEAN NOT NULL,
    exit_code       INT,
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ NOT NULL,
    original_size   INT  NOT NULL,
    truncated       BOOLEAN NOT NULL DEFAULT FALSE,
    log_gz          BYTEA NOT NULL,
    stored_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- "all attempts for this drv on this job, newest first" is the access
-- pattern for `GET /jobs/{id}/drvs/{hash}/logs`.
CREATE INDEX build_logs_drv_idx ON build_logs (job_id, drv_hash, attempt DESC);

-- Retention cleanup scans by stored_at; index it so the DELETE is a
-- range scan and not a heap walk.
CREATE INDEX build_logs_stored_idx ON build_logs (stored_at);
