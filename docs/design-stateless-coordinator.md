# Design: Stateless coordinator + shared fast store

**Status:** Exploration. Not committed.
**Branch:** `worktree-buildbuddy-shape-exploration`.
**Problem:** Today's nix-ci coordinator is a single-writer process holding the entire in-flight build graph in memory. Restarts lose that state; rollouts are disruptive. Sharding adds complexity (client-side routing, fleet scatter). We want a design that gives us HA, rolling deploys without user-visible impact, and horizontal scale — without reintroducing Postgres into the hot path.

## 1. Target architecture

```
                  Submitters + Workers
                          │
                          ▼
                    k8s Service          ← plain round-robin LB
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
          ┌───────┐   ┌───────┐   ┌───────┐
          │Coord 1│   │Coord 2│   │Coord 3│   ← stateless pods
          └───┬───┘   └───┬───┘   └───┬───┘      (Deployment, HPA)
              │           │           │
              └───────────┼───────────┘
                          │
              ┌───────────┴───────────┐
              │                       │
              ▼                       ▼
       ┌──────────────┐      ┌────────────────┐
       │  ValKey HA   │      │   Postgres     │
       │              │      │                │
       │ Sentinel     │      │ • terminal     │
       │ 1M + 2R      │      │   jobs/drvs    │
       │              │      │ • log archives │
       │ Hot-path:    │      │ • failed-cache │
       │ • claims     │      │ • external_ref │
       │ • ready Q    │      │ • outbox       │
       │ • DAG live   │      │ • analytics    │
       │ • pub/sub    │      │                │
       └──────────────┘      └────────────────┘
```

**Key principles:**

1. **Coordinators are stateless.** Any pod can serve any request. No advisory lock, no leader election for coordinators themselves. Ordinary `Deployment`, ordinary rolling updates, ordinary HPA.
2. **Hot-path state lives in ValKey.** Claim leases, ready queues, in-flight DAG, SSE pub/sub. Every atomic transition is a single `EVALSHA` against a pre-compiled Lua script. No state held in coordinator memory across requests.
3. **Postgres is the archive, not the hot path.** Terminal jobs + drvs, compressed build logs, `failed_outputs` TTL cache, `external_ref` idempotency, SQL analytics. Writes only happen on terminal transitions and on job creation (via the outbox pattern).
4. **No sharding.** We don't need it. ValKey Sentinel + HPA'd stateless coordinators scale to well beyond 10x current load.

## 2. State ownership: where does each thing live?

**ValKey (hot path):**
- Live DAG structure (steps, deps, rdeps, per-job ready queues)
- Active claim records (claim_id → job_id, drv_hash, attempt, deadline)
- Worker fleet registry (worker_id → systems, features, last_seen)
- Claim deadline ZSET for reaper sweeps
- Pub/sub channels for progress + SSE events
- Retry-backoff scheduling (per-step `next_attempt_at`)

**Postgres (durable archive):**
- Terminal job rows (`jobs` table with status, result JSON, done_at)
- Terminal drv rows (`derivations` table with final state, attempts, failure log tail)
- `job_roots` and `deps` rows — written only on terminal transition as a *snapshot*, never during in-flight scheduling
- Gzipped stderr log archives (unchanged from today)
- `failed_outputs` TTL cache (1h cache of known-broken drvs — already cold enough today)
- `external_ref` idempotency (unique constraint; days-to-weeks window)
- `outbox` table for job creation (see §5)

**Explicitly NOT in either store:**
- SSE subscriber state lives in the serving coordinator's memory for the duration of the HTTP stream. Lost on pod restart, browser reconnects.
- Long-poll waiter state (tokio::Notify equivalent). Lost on pod restart, client reconnects — same as today.

**Explicitly dropped from current design:**
- `derivations` rows written *during* in-flight scheduling. Today's code writes to PG on every claim/complete for in-memory→PG sync. This goes away — PG only sees terminal state.
- The `clear_busy` function. Meaningless when coordinator holds no state.
- The advisory lock. No leader to elect.

## 3. Deployment topology

**k8s manifests:**
- `Deployment/coordinator`: 3 replicas to start, HPA 3–10 on CPU/memory. Plain rolling update with `maxSurge: 1, maxUnavailable: 0`. No `terminationGracePeriodSeconds` drama — pods are stateless, SIGTERM just closes in-flight HTTP and exits cleanly within ~10s.
- `StatefulSet/valkey`: 3 replicas managed by `spotahome/redis-operator` in Sentinel mode. 1 master + 2 replicas. `appendfsync everysec`, RDB every 5 min. Rolling upgrades handled by the operator with automatic failover.
- `Deployment/postgres`: unchanged from today, or managed service (RDS/Cloud SQL/etc.).

**Rollout behavior:**
- Coordinator deploys: stateless pods rotate with zero user-visible impact. k8s Service drains connections; clients reconnect to new pods.
- ValKey failover: Sentinel promotes a replica in 5–15s. During the failover window, coordinator pods return 503 for any mutation (fail fast, client retries). At-most-once pub/sub means the subscriber polling fallback (NativeLink pattern — see §7.4) picks up any missed events on the next keepalive.
- Postgres failover: identical to today.

## 4. ValKey data model

### 4.1. Key naming

No hash tags (we're Sentinel, not Cluster). If we ever need Cluster, we can add `{job:<id>}` or `{drv:<hash>}` grouping prefixes to colocate related keys on one shard.

**Per-step (deduped across jobs):**
| Key | Type | Purpose |
|---|---|---|
| `step:<drv_hash>` | HASH | Identity + state: `drv_path`, `drv_name`, `system`, `required_features` (json array), `max_tries`, `runnable` (0/1), `finished` (0/1), `previous_failure` (0/1), `created` (0/1), `tries` (i32), `next_attempt_at` (i64 ms), `version` (i64, monotonic for optimistic CAS) |
| `step:<drv_hash>:deps` | SET | drv_hashes this step depends on |
| `step:<drv_hash>:rdeps` | SET | drv_hashes that depend on this step |
| `step:<drv_hash>:pending_deps` | STRING (counter) | HINCRBY-managed count of unmet deps; reaches 0 → step armable |
| `step:<drv_hash>:jobs` | SET | job_ids referencing this step (for rdep propagation + lifecycle) |

**Per-job:**
| Key | Type | Purpose |
|---|---|---|
| `job:<job_id>` | HASH | `external_ref`, `created_at` (ms), `sealed` (0/1), `terminal` (0/1), `max_drvs`, `event_capacity`, `priority` |
| `job:<job_id>:members` | SET | drv_hashes in this job (for cleanup on terminal) |
| `job:<job_id>:toplevels` | LIST | ordered list of toplevel drv_hashes |
| `job:<job_id>:ready:<system>` | LIST | FIFO queue of (drv_hash, attempt) pairs that are runnable on this system |
| `job:<job_id>:failures` | LIST | bounded ring of failure records (`max_failures_in_result` cap) |
| `job:<job_id>:progress` | HASH | live counters: `pending`, `building`, `completed`, `failed`, `transient_retries` |
| `job:<job_id>:events` | STREAM | Redis Stream for SSE fan-out (XADD on progress events; consumers use XREAD BLOCK) |

**Per-claim:**
| Key | Type | Purpose |
|---|---|---|
| `claim:<claim_id>` | HASH | `job_id`, `drv_hash`, `attempt`, `deadline` (ms), `worker_id`, `issued_at` (ms), `reconnect_token`, `reconnect_deadline` |
| `claims:deadlines` | ZSET | `claim_id` scored by deadline (for reaper) |
| `claims:by_worker:<worker_id>` | SET | claim_ids held by this worker (for disconnect recovery) |

**Per-worker:**
| Key | Type | Purpose |
|---|---|---|
| `worker:<worker_id>` | HASH | `hostname`, `systems` (json), `features` (json), `registered_at`, `last_seen` |
| `worker:<worker_id>:ttl` | STRING | auto-expiring liveness marker (EXPIRE ~30s, refreshed by heartbeat) |

**Global indexes:**
| Key | Type | Purpose |
|---|---|---|
| `jobs:active` | SET | job_ids of non-terminal jobs |
| `jobs:by_external_ref` | HASH | external_ref → job_id (for idempotency lookups; authoritative row is in PG) |
| `wake:<system>` | pub/sub channel | coordinator publishes "work available on system X", fleet-claim long-polls subscribe |

### 4.2. Size estimate

From the current-state research, at 10x scale:
- ~10⁶ unique steps × ~500 bytes each (hash with ~12 fields) = ~500 MB
- ~10⁴ concurrent jobs × ~500 bytes metadata = ~5 MB
- ~10⁵ claims in flight (unlikely sustained; peak during catastrophic reaper lag) × ~200 bytes = ~20 MB
- Dep/rdep edges: a few MB of small sets
- Events stream: bounded by MAXLEN ~10k per job

**Total: ~600 MB working set.** Fits on a single modest ValKey instance with headroom. Sentinel mode (no sharding) suffices.

### 4.3. The `version` field

Every mutable hash has a monotonic `version` field. All in-place updates use optimistic CAS via Lua: `HINCRBY key version 1` and compare against expected. This is the NativeLink pattern ([store_awaited_action_db.rs](https://github.com/TraceMachina/nativelink/blob/main/nativelink-scheduler/src/store_awaited_action_db.rs)). Version mismatch → retry with exponential backoff (`BASE_RETRY_DELAY_MS=10`, `MAX_UPDATE_RETRIES=5`). Prevents lost updates when two coordinators race on the same key.

**Heartbeat exception:** claim deadline updates (EXTEND) bypass version CAS — they're monotonic and idempotent, just `HSET deadline` + `ZADD claims:deadlines`.

## 5. Postgres schema (trimmed)

```sql
-- Terminal archive: written only when a job hits terminal state
CREATE TABLE jobs (
    id              UUID PRIMARY KEY,
    external_ref    TEXT UNIQUE,           -- idempotency
    status          TEXT NOT NULL,         -- 'completed' | 'failed' | 'cancelled'
    created_at      TIMESTAMPTZ NOT NULL,
    done_at         TIMESTAMPTZ NOT NULL,
    result          JSONB NOT NULL,        -- frozen snapshot of JobStatusResponse
    total_drvs      INT NOT NULL,
    succeeded_drvs  INT NOT NULL,
    failed_drvs     INT NOT NULL
);

-- Terminal drv snapshots (analytics + drill-down into completed job graphs)
CREATE TABLE derivations (
    drv_hash        TEXT PRIMARY KEY,
    drv_path        TEXT NOT NULL,
    drv_name        TEXT,
    system          TEXT NOT NULL,
    final_state     TEXT NOT NULL,         -- 'succeeded' | 'failed' | 'propagated'
    attempts        INT NOT NULL,
    failure_category TEXT,
    first_seen_at   TIMESTAMPTZ NOT NULL,
    last_terminal_at TIMESTAMPTZ NOT NULL
);

-- Job→drv membership for analytics queries
CREATE TABLE job_derivations (
    job_id          UUID REFERENCES jobs(id),
    drv_hash        TEXT,
    was_toplevel    BOOL,
    PRIMARY KEY (job_id, drv_hash)
);

-- Dep edges — snapshot per job on terminal (no more live scheduling reads)
CREATE TABLE deps (
    job_id          UUID REFERENCES jobs(id),
    drv_hash        TEXT,
    dep_hash        TEXT,
    PRIMARY KEY (job_id, drv_hash, dep_hash)
);

-- TTL'd cache (existing, unchanged)
CREATE TABLE failed_outputs (
    output_path     TEXT PRIMARY KEY,
    failure_reason  TEXT,
    expires_at      TIMESTAMPTZ NOT NULL
);

-- Gzipped stderr archives (existing, unchanged)
CREATE TABLE build_logs (...);

-- OUTBOX pattern for job creation (§5.1)
CREATE TABLE job_creation_outbox (
    job_id          UUID PRIMARY KEY REFERENCES jobs(id) DEFERRABLE,
    external_ref    TEXT NOT NULL,
    payload         JSONB NOT NULL,
    published_at    TIMESTAMPTZ
);
```

### 5.1. Job creation via outbox

Creating a new job must be atomic against `external_ref` idempotency. ValKey doesn't give us multi-row transactions across PG + Redis. The outbox pattern:

1. Coordinator receives `POST /jobs { external_ref, ... }`.
2. BEGIN PG transaction:
   - `INSERT INTO jobs (id, external_ref, status, created_at, ...) VALUES (..., 'pending', ...) ON CONFLICT (external_ref) DO NOTHING RETURNING id`
   - If conflict → return existing job_id from `SELECT id FROM jobs WHERE external_ref = $1` → done.
   - If new → `INSERT INTO job_creation_outbox (...)`.
3. COMMIT.
4. Publish to Redis: `HSET job:<id> ...`, `SADD jobs:active <id>`, `HSET jobs:by_external_ref <ref> <id>`.
5. `UPDATE job_creation_outbox SET published_at = NOW() WHERE job_id = $1`.

A background reconciler scans `job_creation_outbox WHERE published_at IS NULL AND created_at < NOW() - 30s` and re-publishes. Ensures every PG-inserted job reaches Redis exactly once.

### 5.2. Terminal writeback

When a job hits terminal state (sealed + all toplevels finished, OR sealed + any terminal failure):

1. Coordinator runs a Lua script that atomically: marks `terminal=1`, freezes the ready queues, collects the final counters, returns the full snapshot.
2. Coordinator writes to PG: `INSERT INTO jobs (...) ON CONFLICT (id) DO UPDATE SET status = ..., result = ..., done_at = NOW()`. Plus bulk `INSERT INTO derivations`, `job_derivations`, `deps`.
3. Coordinator removes job keys from Redis: `SREM jobs:active`, `DEL job:<id>`, `DEL job:<id>:*`, decrement `step:*:jobs` memberships, GC orphaned steps.

This is a PG→ValKey outbox in reverse, again avoiding dual-write atomicity. If step 2 succeeds and step 3 fails, a background GC reconciles (job marked terminal in PG → Redis cleanup).

## 6. Hot-path flows

### 6.1. `POST /jobs` — create

```
1. PG: INSERT INTO jobs (...) ON CONFLICT (external_ref) DO NOTHING RETURNING id
   + INSERT INTO job_creation_outbox (job_id, ...)
   (one transaction)
2. Redis: EVALSHA create_job_sha (job_id, external_ref, priority, ...)
   → HSET job:<id>, SADD jobs:active, HSET jobs:by_external_ref
3. PG: UPDATE outbox SET published_at = NOW()
```

Latency: ~5ms PG + ~1ms Redis. Not on the claim path.

### 6.2. `POST /jobs/{id}/drvs/batch` — ingest

Currently this is the most complex op. In the new design:

```
1. Redis: EVALSHA ingest_batch_sha (job_id, drv_list_json)
   (single script processes the whole batch atomically per-job)
   → for each drv in batch:
       - HSETNX step:<drv_hash> ... (create if absent)
       - SADD step:<drv_hash>:jobs <job_id>
       - SADD job:<id>:members <drv_hash>
       - wire deps: SADD step:<drv>:deps <dep>, SADD step:<dep>:rdeps <drv>
       - INCR step:<drv>:pending_deps (one per unsatisfied dep)
       - if pending_deps == 0 and required_features is subset: LPUSH job:<id>:ready:<system> (drv_hash,attempt)
   → PUBLISH wake:<system> "1" for systems that got new ready work
```

Batches are chunked client-side (e.g., 1000 drvs per EVALSHA) to keep script execution under ~50ms. ValKey single-threaded script execution means you don't want to monopolize for minutes.

### 6.3. `GET /claim` — fleet claim

```
Worker does:
  1. SUBSCRIBE wake:<system1> wake:<system2> ...
  2. loop {
       result = EVALSHA claim_any_sha (worker_id, systems[], features[], now_ms, lease_ms)
       if result.claim_id exists:
           return claim to worker
       wait on SUBSCRIBE messages OR 30s timeout
     }
```

The `claim_any` Lua script (roughly):

```lua
-- KEYS: none (iterates known jobs via SMEMBERS)
-- ARGV: worker_id, systems_csv, features_json, now_ms, lease_ms
local worker_id = ARGV[1]
local systems = cjson.decode(ARGV[2])  -- or split CSV
local features = cjson.decode(ARGV[3])
local now = tonumber(ARGV[4])
local lease = tonumber(ARGV[5])

for _, job_id in ipairs(redis.call('SMEMBERS', 'jobs:active')) do
  for _, sys in ipairs(systems) do
    local ready_key = 'job:' .. job_id .. ':ready:' .. sys
    while true do
      local entry = redis.call('LPOP', ready_key)
      if not entry then break end
      local drv_hash, attempt = parse(entry)
      local step_key = 'step:' .. drv_hash
      local step = redis.call('HMGET', step_key, 'runnable', 'finished', 'next_attempt_at', 'required_features')
      if step[2] == '1' then goto continue end  -- finished, drop
      if tonumber(step[3]) > now then
         -- retry-backoff not yet due: push to tail, move on
         redis.call('RPUSH', ready_key, entry)
         break
      end
      if not features_satisfies(features, step[4]) then
         redis.call('RPUSH', ready_key, entry)
         break
      end
      -- Atomic CAS on runnable
      if redis.call('HGET', step_key, 'runnable') == '1' then
         redis.call('HSET', step_key, 'runnable', '0')
         redis.call('HINCRBY', step_key, 'tries', 1)
         local claim_id = generate_uuid()  -- or passed in ARGV
         local deadline = now + lease
         redis.call('HSET', 'claim:' .. claim_id,
           'job_id', job_id, 'drv_hash', drv_hash, 'attempt', step[2],
           'deadline', deadline, 'worker_id', worker_id, 'issued_at', now)
         redis.call('ZADD', 'claims:deadlines', deadline, claim_id)
         redis.call('SADD', 'claims:by_worker:' .. worker_id, claim_id)
         redis.call('HINCRBY', 'job:' .. job_id .. ':progress', 'building', 1)
         return { claim_id, job_id, drv_hash, step_fields... }
      end
      ::continue::
    end
  end
end
return nil
```

Latency: single round-trip, target p50 <2ms at load.

### 6.4. `POST /claims/{id}/complete` — success path

```
EVALSHA complete_success_sha (claim_id)
  -- atomically:
  1. Read claim record, verify exists
  2. HSET step:<drv>:finished 1
  3. HDEL claim:<id>
  4. ZREM claims:deadlines <id>
  5. SREM claims:by_worker:<w> <id>
  6. For each rdep in step:<drv>:rdeps:
       new_count = HINCRBY step:<rdep>:pending_deps -1
       if new_count == 0:
          For each job in step:<rdep>:jobs:
             LPUSH job:<job>:ready:<system> (rdep_hash, 0)
             PUBLISH wake:<system> "1"
  7. For each job in step:<drv>:jobs:
       HINCRBY job:<job>:progress building -1
       HINCRBY job:<job>:progress completed 1
       XADD job:<job>:events * event DrvCompleted drv_hash <drv>
  8. Check terminal: if toplevels all finished → return "terminal" flag to caller
```

Caller (coordinator) sees terminal flag → performs §5.2 writeback + Redis GC.

### 6.5. `POST /claims/{id}/extend` — heartbeat

```
Fast path (no version CAS, monotonic):
  HSET claim:<id> deadline <new>
  ZADD claims:deadlines <new> <id>
  Return OK
```

### 6.6. Reaper

Runs on any coordinator pod on a tick (say every 2s). Single-flight via `SET reaper:lock NX EX 5`:

```
1. SET reaper:lock 1 NX EX 5 → if fail, another pod is reaping, skip
2. ZRANGEBYSCORE claims:deadlines 0 <now> LIMIT 0 100
3. For each expired claim:
     EVALSHA reap_one_sha (claim_id, now)
     -- atomically: if still expired, HSET step runnable=1, re-enqueue in ready queue, publish wake
```

No PG DB in the reaper path. No `REAPER_DB_TIMEOUT` concerns.

## 7. Rust-level abstraction

Borrow NativeLink's pattern: a `SchedulerStore` trait with memory + Redis impls.

```rust
// nix-ci-core/src/store/mod.rs

#[async_trait]
pub trait SchedulerStore: Send + Sync + 'static {
    async fn create_job(&self, req: CreateJobReq) -> Result<CreateJobResult>;
    async fn ingest_batch(&self, job_id: JobId, drvs: Vec<IngestDrv>) -> Result<()>;
    async fn claim_any(&self, req: ClaimReq) -> Result<Option<Claim>>;
    async fn complete_success(&self, claim_id: ClaimId) -> Result<CompleteResult>;
    async fn complete_failure(&self, claim_id: ClaimId, failure: Failure) -> Result<CompleteResult>;
    async fn extend_claim(&self, claim_id: ClaimId, deadline: Instant) -> Result<()>;
    async fn reap_expired(&self, now: Instant, batch: usize) -> Result<Vec<ReapedClaim>>;
    async fn subscribe_events(&self, job_id: JobId) -> Result<EventStream>;
    async fn job_status(&self, job_id: JobId) -> Result<JobStatus>;
    // ... more
}

pub struct MemoryStore { /* today's Dispatcher logic, refactored behind trait */ }
pub struct ValkeyStore { client: fred::Client, scripts: PrecompiledScripts }

impl SchedulerStore for ValkeyStore { ... }
impl SchedulerStore for MemoryStore { ... }
```

**Benefits of this shape:**

1. **Tests stay fast.** Unit tests + L1 loom + L2 deterministic sim all run against `MemoryStore`. No Redis in CI.
2. **L3 scale tests + L4 degradation tests** run against `ValkeyStore` with a real ValKey instance.
3. **Migration path** (§8) uses feature flag to pick impl at runtime.
4. **Clock injection:** both impls take `now: fn() -> Instant` so deterministic sim still works.

### 7.1. Rust crates

- `fred` for ValKey client (Sentinel-aware, automatic pipelining, `EVALSHA` script caching): https://github.com/aembke/fred.rs
- `sqlx` unchanged for PG.
- No `async_trait` needed — Rust stable supports async-fn-in-trait since 1.75; use RPIT where possible.

### 7.2. Lua script management

Scripts are embedded at compile time (`include_str!`), hashed at startup, loaded via `SCRIPT LOAD`, then called via `EVALSHA`. On `NOSCRIPT` error (server restart), auto-reload via `SCRIPT LOAD`. Fred handles this automatically.

Scripts tested via `redis-mock` crate or against a real ephemeral ValKey in CI.

### 7.3. Pub/sub reliability

ValKey pub/sub is at-most-once. Strategy (NativeLink pattern):
- Coordinator maintains an SSE stream to the client.
- Coordinator subscribes to `wake:<system>` or `events:<job>` channels.
- On pub/sub message: re-read the relevant state from ValKey (not trust the payload).
- Additionally: periodic polling fallback (every `CLIENT_KEEPALIVE_DURATION = 10s`) to catch missed events after Sentinel failover.

For SSE specifically, use Redis Streams (`XADD` / `XREAD BLOCK`) instead of pub/sub. Streams are durable, consumers have offsets, reconnect-safe. One stream per job (`job:<id>:events`), MAXLEN 10000, auto-trimmed.

## 8. Rollout / migration plan

**Phase 0 (prep).** Introduce `SchedulerStore` trait. Refactor current `Dispatcher` to implement `MemoryStore`. All handlers move to `&dyn SchedulerStore`. No behavior change. Tests stay green. **~2–4 weeks.**

**Phase 1 (ValKey in shadow).** Implement `ValkeyStore`. Add a feature flag. Run `MemoryStore` as primary, `ValkeyStore` as shadow writer (writes both, reads only from memory). Verify ValKey keeps up; diff state periodically in tests. **~3–6 weeks.**

**Phase 2 (flip reads).** Feature flag: `ValkeyStore` as primary, `MemoryStore` still shadow-written for rollback. L3 scale tests against ValKey. Deploy to staging, run shadow CCI traffic (L5 per SCALE.md). **~2–4 weeks.**

**Phase 3 (remove MemoryStore in production).** Cut `MemoryStore` out of the binary's prod path. Keep it as test-only. Remove advisory lock, remove `clear_busy`, remove per-claim PG writes. **~2 weeks.**

**Phase 4 (prune PG schema).** Drop columns and tables that are now ValKey-only during in-flight. Keep only terminal archive + outbox + caches. **~1 week.**

**Total rough estimate: 3–5 months end-to-end**, with the critical path being Phase 1's `ValkeyStore` implementation (especially the Lua scripts).

## 9. Risks, failure modes, open questions

### 9.1. ValKey is now a SPOF

Mitigation: Sentinel mode with 2 replicas, automatic failover. Measured failover time 5–15s. During failover: coordinator pods fail-fast with 503, clients retry, succeed once Sentinel elects new master.

Residual: if ValKey loses data (AOF corruption, cluster quorum failure), in-flight state is lost. Same blast radius as today's coordinator crash — submitters re-POST, workers re-claim, Nix content-addressing absorbs duplicate work. Build failures: none. Latency spike: seconds.

### 9.2. Script performance under load

Single-threaded Lua execution means one slow script blocks everyone. Mitigation:
- Keep every script bounded: claim/complete/extend touch O(1) or O(rdeps) keys.
- Batch ingests are chunked (~1000 drvs per script call).
- Reaper is batched and single-flight.
- Benchmark p99 script latency in L3 tests.

Open question: at 10⁶ steps, is `SMEMBERS jobs:active` (thousands of jobs) inside `claim_any` acceptable? Probably yes — SMEMBERS is O(n) over a set of ~10⁴ elements, microseconds. If not, add a `jobs:active:by_priority` ZSET to iterate in priority order with LIMIT.

### 9.3. Ingest write amplification

A 50k-drv job ingested chunked at 1000/call = 50 EVALSHA calls. Each call: ~5k Redis ops (HSET + SADD × deps). Total: ~250k Redis ops for one job ingest. At our p99 target (~10ms per chunk), one ingest takes ~500ms end-to-end. Acceptable. But measure.

### 9.4. State-transfer bugs

Biggest category. Today's Invariants 1–8 (SPEC.md) all need Lua-script equivalents. The most subtle:
- **Invariant 4** (created=true before runnable): must ensure ingest script only pushes to ready queue *after* full HSET of step fields. Use `HSETNX` for created flag; ready-queue enqueue happens at script tail.
- **Invariant 1** (steps dedup): SETNX semantics via `HSETNX step:<hash> drv_path ...`.
- **Invariant 5** (no await under lock): irrelevant — no Rust locks.
- **Invariant 7** (weak-registry ownership): replaced by `step:*:jobs` set membership. Orphan GC when job hits terminal.

All 8 invariants need to be translated into the Lua-script invariant set and documented in a new `SPEC.md` section.

### 9.5. Observability

In-memory state was easy to dump. Redis state needs tooling:
- `nix-ci admin dump-job <job_id>` → `HGETALL job:<id>`, `SMEMBERS job:<id>:members`, ready queue contents, etc.
- Prometheus metrics: ValKey-based gauges (active_claims, pending_jobs) via periodic scan, not real-time.
- Distributed tracing (OpenTelemetry): already there; add spans for EVALSHA calls.

### 9.6. Testing

Memory impl keeps L1/L2 tests fast. L3/L4 against real ValKey (ephemeral instance in test harness; Nix flake can provide it). L5 shadow deploy unchanged.

Critical: **parity tests** between `MemoryStore` and `ValkeyStore` during Phase 1/2. For every operation in the L2 deterministic sim, run both stores and assert identical observable state. Guards against subtle script bugs.

### 9.7. Operational complexity

New: ValKey ops (backups, upgrades, monitoring, scaling). Managed offerings (ElastiCache for ValKey, Memorystore Valkey) remove most of this. Self-hosted via `spotahome/redis-operator` is well-trodden.

Removed: advisory-lock dance, `clear_busy`, coordinator leader-election. Net: likely a wash in total complexity, with the tradeoff being "ops burden moved to a mature external system" vs "complex scheduler-internal state machine."

### 9.8. Open question: what about the worker side?

This design is purely coordinator-side. Workers still speak the existing HTTP API. No changes to the runner, the in-job CCI worker, or the fleet worker. Protocol-level evolution (e.g., workers talking ValKey directly) is explicitly out of scope.

### 9.9. Open question: do we keep fleet and per-job endpoints both?

Current API has `GET /claim` (fleet) and `GET /jobs/{id}/claim` (per-job). In this design they collapse: both invoke the same `claim_any` Lua script, filtered by an optional `job_id`. Operationally simpler. Client API can stay compatible.

## 10. Explicitly deferred / out of scope

- **Horizontal scaling of ValKey itself.** Sentinel mode caps us at a single master's working set (~tens of GB). If we ever outgrow, migrate to ValKey Cluster with hash tags. Not now.
- **Cross-region HA.** Single-region design. Multi-region later if needed.
- **ClickHouse / OLAP tier** for analytics. BuildBuddy added one; we can too, later, if PG analytics queries get slow.
- **Worker-side protocol changes.** Workers still use HTTP, coordinator translates to ValKey. Possible future: workers use Redis Streams directly as the claim channel. Not now.
- **End-to-end protocol for long builds across coordinator restarts.** The "worker continues building, reconnects, reports to any pod" enhancement is orthogonal — works under today's design, works under this one. Separate effort.

## 11. Why this design is the right answer

1. **Addresses every prior user concern:**
   - No PVCs, no stateful pods.
   - No drain gymnastics — pods are stateless, restart freely.
   - No sharding — ValKey scales to our load without client-side routing or fleet scatter.
   - No DB in hot path — PG only sees terminal events + job creation.
   - No HA complexity in app code — delegated to ValKey Sentinel.

2. **Matches a proven production shape.** BuildBuddy runs this at billions of requests/month. NativeLink's trait abstraction is directly borrowable. ValKey is BSD, LF-governed, hyperscaler-managed.

3. **Preserves the prime directive** ("never be the reason builds fail"). ValKey failover = seconds of 503s during which clients retry. Nix content-addressing absorbs any duplicate work. No build ever fails because of us.

4. **Testable.** Same L1–L5 strategy, just with `MemoryStore` as the test-only backend. Parity tests ensure no regression.

5. **Reversible.** During Phase 1/2, `MemoryStore` still works. Can roll back the feature flag.

## 12. The one thing to decide

**Is the team willing to take on ValKey as an operational dependency?**

If yes: this is the design.
If no: we're back to sharding or single-coordinator with graceful drain.

Everything else is implementation detail.
