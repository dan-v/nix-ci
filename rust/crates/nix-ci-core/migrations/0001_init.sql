-- nix-ci v3 schema. Hydra-v2-style thin envelope.
--
-- Durability model: the in-memory dispatcher is the only authoritative
-- source for in-flight state. Postgres stores:
--   1. `jobs` — one row per submission. Terminal rows carry a JSONB
--      snapshot of the final JobStatusResponse so status polls work
--      post-hoc without reconstructing an in-flight graph.
--   2. `failed_outputs` — short-TTL cache of known-broken output paths,
--      used by ingest to short-circuit rebuilds of drvs that just failed
--      elsewhere.
--
-- On coordinator restart, any non-terminal job is flipped to cancelled
-- with a sentinel result; its workers' next poll returns 410 and the
-- caller (CCI) retries with a fresh job. No in-flight graph is
-- persisted or reconstructed.

CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_ref    TEXT,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','done','failed','cancelled')),
    sealed          BOOLEAN NOT NULL DEFAULT FALSE,
    result          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now(),
    done_at         TIMESTAMPTZ,
    CHECK ((status IN ('done','failed','cancelled')) = (done_at IS NOT NULL)),
    CHECK (done_at IS NULL OR result IS NOT NULL)
);

-- Idempotent POST /jobs: a retry with the same external_ref resolves to
-- the existing id rather than minting a new job.
CREATE UNIQUE INDEX jobs_external_ref_uniq
    ON jobs (external_ref)
    WHERE external_ref IS NOT NULL;

CREATE INDEX jobs_live_idx ON jobs (last_heartbeat)
    WHERE status = 'pending';

-- Short-TTL cross-job cache of failed output paths. An ingest whose
-- drv_path matches an unexpired row is pre-marked failed so we don't
-- waste a worker rebuilding a known-broken drv.
CREATE TABLE failed_outputs (
    output_path  TEXT PRIMARY KEY,
    drv_hash     TEXT NOT NULL,
    failed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL DEFAULT (now() + INTERVAL '1 hour')
);

CREATE INDEX failed_outputs_expiry_idx ON failed_outputs (expires_at);
