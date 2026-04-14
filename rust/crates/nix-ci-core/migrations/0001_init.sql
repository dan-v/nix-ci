-- nix-ci v2 schema. Single migration. Five tables.
--
-- Durability contract: Postgres is the recovery log, not the hot-path
-- scheduler. The in-memory dispatcher is authoritative at runtime; this
-- schema persists what a standby coordinator needs to rebuild the
-- dispatcher graph on promotion.

-- Jobs: one per CCI-step submission. "Job" == "submission" in the code.
CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','building','done','failed','cancelled')),
    sealed          BOOLEAN NOT NULL DEFAULT FALSE,
    eval_error      TEXT,
    external_ref    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now(),
    done_at         TIMESTAMPTZ,
    CHECK ((status IN ('done','failed','cancelled')) = (done_at IS NOT NULL))
);

CREATE INDEX jobs_live_idx ON jobs (last_heartbeat)
    WHERE status IN ('pending','building');

-- Derivations: one row per unique drv_hash across all submissions.
-- Cross-submission dedup: `Steps::create` in the dispatcher is the
-- primary dedup point; this table is the durable reflection.
CREATE TABLE derivations (
    drv_hash           TEXT PRIMARY KEY,
    drv_path           TEXT NOT NULL,
    drv_name           TEXT NOT NULL CHECK (length(drv_name) > 0),
    system             TEXT NOT NULL CHECK (length(system) > 0),
    state              TEXT NOT NULL DEFAULT 'pending'
                       CHECK (state IN ('pending','building','done','failed')),
    assigned_claim_id  UUID,
    attempt            INT NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    max_attempts       INT NOT NULL DEFAULT 2 CHECK (max_attempts >= 1),
    next_attempt_at    TIMESTAMPTZ,
    error_category     TEXT,
    error_message      TEXT CHECK (error_message IS NULL OR length(error_message) <= 4096),
    exit_code          INT,
    log_tail           TEXT CHECK (log_tail IS NULL OR length(log_tail) <= 131072),
    propagated_from    TEXT REFERENCES derivations(drv_hash) ON DELETE SET NULL,
    required_features  TEXT[] NOT NULL DEFAULT '{}',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at       TIMESTAMPTZ,
    CHECK (attempt <= max_attempts),
    CHECK ((state = 'building') = (assigned_claim_id IS NOT NULL)),
    CHECK ((state IN ('done','failed')) = (completed_at IS NOT NULL))
);

CREATE INDEX drv_pending_idx   ON derivations (system, created_at) WHERE state = 'pending';
CREATE INDEX drv_building_idx  ON derivations (assigned_claim_id)  WHERE state = 'building';
CREATE INDEX drv_failed_idx    ON derivations (completed_at)       WHERE state = 'failed';

-- Edges in the derivation DAG. drv_hash depends on dep_hash.
CREATE TABLE deps (
    drv_hash  TEXT NOT NULL REFERENCES derivations(drv_hash) ON DELETE CASCADE,
    dep_hash  TEXT NOT NULL REFERENCES derivations(drv_hash) ON DELETE CASCADE,
    PRIMARY KEY (drv_hash, dep_hash),
    CHECK (drv_hash != dep_hash)
);

CREATE INDEX deps_reverse_idx ON deps (dep_hash);

-- Roots of a job's submission: top-level drvs the CCI caller cares
-- about. Used at rehydrate time to reattach submissions to steps.
CREATE TABLE job_roots (
    job_id    UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    drv_hash  TEXT NOT NULL REFERENCES derivations(drv_hash) ON DELETE CASCADE,
    PRIMARY KEY (job_id, drv_hash)
);

-- Cross-job cached-failure cache with a short TTL. Cheap short-circuit
-- for known-broken derivations; expires so transient failures
-- self-heal.
CREATE TABLE failed_outputs (
    output_path  TEXT PRIMARY KEY,
    drv_hash     TEXT NOT NULL REFERENCES derivations(drv_hash) ON DELETE CASCADE,
    failed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL DEFAULT (now() + INTERVAL '1 hour')
);

CREATE INDEX failed_outputs_expiry_idx ON failed_outputs (expires_at);
CREATE INDEX failed_outputs_drv_idx    ON failed_outputs (drv_hash);
