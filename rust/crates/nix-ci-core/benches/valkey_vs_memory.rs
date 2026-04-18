//! Head-to-head benchmark: in-memory single-node dispatcher vs
//! stateless-coordinator-backed-by-Valkey prototype.
//!
//! Not a Criterion bench — we care about end-to-end wall time and
//! claim-latency tail percentiles under a concurrent-worker workload,
//! not per-op microbench. Emits a plaintext comparison table.
//!
//! # Running
//!
//! In the nix devshell (which now includes `valkey`):
//! ```
//! valkey-server --daemonize yes --save '' --appendonly no --port 6379
//! cargo bench --bench valkey_vs_memory -- 1k
//! ```
//!
//! Scenarios:
//! - `1k`   — 5 layers × 200 wide, fan-in 2; 32 workers (warmup run)
//! - `10k`  — 5 layers × 2000 wide, fan-in 2; 128 workers
//! - `50k`  — 5 layers × 10000 wide, fan-in 2; 512 workers
//!
//! # What we measure
//!
//! Both backends implement the same `SchedulerOps` trait covering the
//! hot-path: `ingest_dag`, `claim_one`, `complete_success`. Workers
//! loop claim → (simulated build, 0ms) → complete until the graph is
//! drained. We record claim + complete latency in HDR histograms and
//! report total wall time.
//!
//! # What we explicitly don't measure
//!
//! - Ingest throughput (both backends are fast enough there; not the
//!   contested bottleneck)
//! - HTTP / network overhead (benches are in-process against local
//!   Valkey; adds the Valkey wire RTT but omits HTTP)
//! - Failure propagation (add later if first numbers are interesting)
//! - Retry backoff (add later)
//! - SSE event fan-out (add later)
//!
//! The goal is a defensible apples-to-apples claim+complete number
//! across a representative DAG, good enough to make the
//! "is Valkey within ~5x of in-memory for this workload?" decision.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use fred::prelude::*;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use tokio::sync::Notify;

// ─── Shared types ───────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct IngestDrv {
    drv_hash: String,
    system: String,
    deps: Vec<String>,
}

#[derive(Clone, Debug)]
struct Claim {
    claim_id: u64,
    drv_hash: String,
}

#[derive(Debug)]
struct RunStats {
    claim_hist: Histogram<u64>,
    complete_hist: Histogram<u64>,
    total_claims: u64,
    total_completes: u64,
    wall: Duration,
}

impl RunStats {
    fn new() -> Self {
        Self {
            claim_hist: Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap(),
            complete_hist: Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap(),
            total_claims: 0,
            total_completes: 0,
            wall: Duration::ZERO,
        }
    }
}

#[async_trait]
trait SchedulerOps: Send + Sync {
    /// Insert a DAG. Leaves (no deps) are immediately runnable.
    async fn ingest_dag(&self, drvs: Vec<IngestDrv>) -> Result<(), String>;

    /// Claim one runnable drv for any of `systems`, or None if nothing
    /// is currently ready. Backends should implement this as a single
    /// atomic op (CAS / Lua).
    async fn claim_one(&self, systems: &[String]) -> Result<Option<Claim>, String>;

    /// Mark a claim as successfully completed. Propagates to rdeps:
    /// decrements their `pending_deps`, arms them as runnable if 0.
    async fn complete_success(&self, claim_id: u64) -> Result<(), String>;

    /// How many drvs are still in flight or not-yet-started. Used by
    /// the driver loop to know when the workload is drained.
    async fn remaining(&self) -> Result<u64, String>;
}

// ─── MemoryStore ────────────────────────────────────────────────────

/// Minimal in-memory dispatcher. Not using the real nix-ci-core
/// `Dispatcher` because its ownership model (Arc<Step>, Weak refs,
/// Submissions, broadcast channels) carries incidental complexity we
/// don't need for a raw-speed comparison. A HashMap + VecDeque under
/// one mutex is a generous upper bound — the real Dispatcher has
/// finer-grained locks and atomics, so if ValkeyStore comes within
/// ~5x of this, the real Dispatcher will be faster still.
struct MemoryStore {
    inner: Mutex<MemoryInner>,
    notify: Notify,
}

struct MemoryInner {
    steps: HashMap<String, MemStep>,
    ready_by_system: HashMap<String, VecDeque<String>>,
    claims: HashMap<u64, String>,
    next_claim_id: u64,
    rdeps: HashMap<String, Vec<String>>, // drv_hash -> those that depend on it
    remaining: u64,
}

struct MemStep {
    system: String,
    pending_deps: u32,
    runnable: bool,
    finished: bool,
}

impl MemoryStore {
    fn new() -> Self {
        Self {
            inner: Mutex::new(MemoryInner {
                steps: HashMap::new(),
                ready_by_system: HashMap::new(),
                claims: HashMap::new(),
                next_claim_id: 0,
                rdeps: HashMap::new(),
                remaining: 0,
            }),
            notify: Notify::new(),
        }
    }
}

#[async_trait]
impl SchedulerOps for MemoryStore {
    async fn ingest_dag(&self, drvs: Vec<IngestDrv>) -> Result<(), String> {
        let mut inner = self.inner.lock();
        for drv in drvs {
            let pending = drv.deps.len() as u32;
            inner.steps.insert(
                drv.drv_hash.clone(),
                MemStep {
                    system: drv.system.clone(),
                    pending_deps: pending,
                    runnable: pending == 0,
                    finished: false,
                },
            );
            inner.remaining += 1;
            for dep in &drv.deps {
                inner
                    .rdeps
                    .entry(dep.clone())
                    .or_default()
                    .push(drv.drv_hash.clone());
            }
            if pending == 0 {
                inner
                    .ready_by_system
                    .entry(drv.system)
                    .or_default()
                    .push_back(drv.drv_hash);
            }
        }
        drop(inner);
        self.notify.notify_waiters();
        Ok(())
    }

    async fn claim_one(&self, systems: &[String]) -> Result<Option<Claim>, String> {
        let mut inner = self.inner.lock();
        for sys in systems {
            // Borrow the queue in a tight scope, pop one candidate at a
            // time, then switch to the steps map to validate + mutate.
            loop {
                let drv_hash = match inner.ready_by_system.get_mut(sys) {
                    Some(q) => match q.pop_front() {
                        Some(h) => h,
                        None => break,
                    },
                    None => break,
                };
                let Some(step) = inner.steps.get_mut(&drv_hash) else {
                    continue;
                };
                if step.finished || !step.runnable {
                    continue;
                }
                step.runnable = false;
                inner.next_claim_id += 1;
                let claim_id = inner.next_claim_id;
                inner.claims.insert(claim_id, drv_hash.clone());
                return Ok(Some(Claim { claim_id, drv_hash }));
            }
        }
        Ok(None)
    }

    async fn complete_success(&self, claim_id: u64) -> Result<(), String> {
        let mut inner = self.inner.lock();
        let Some(drv_hash) = inner.claims.remove(&claim_id) else {
            return Ok(());
        };
        if let Some(step) = inner.steps.get_mut(&drv_hash) {
            step.finished = true;
            inner.remaining = inner.remaining.saturating_sub(1);
        }
        // Two-pass to avoid borrow conflicts: first mutate rdeps' pending_deps
        // counters, then collect newly-ready ones, then enqueue.
        let rdep_list = inner.rdeps.get(&drv_hash).cloned().unwrap_or_default();
        let mut newly_ready: Vec<(String, String)> = Vec::new();
        for rdep in &rdep_list {
            if let Some(step) = inner.steps.get_mut(rdep) {
                step.pending_deps = step.pending_deps.saturating_sub(1);
                if step.pending_deps == 0 && !step.finished && !step.runnable {
                    step.runnable = true;
                    newly_ready.push((step.system.clone(), rdep.clone()));
                }
            }
        }
        for (sys, drv) in newly_ready {
            inner.ready_by_system.entry(sys).or_default().push_back(drv);
        }
        drop(inner);
        self.notify.notify_waiters();
        Ok(())
    }

    async fn remaining(&self) -> Result<u64, String> {
        Ok(self.inner.lock().remaining)
    }
}

// ─── ValkeyStore ────────────────────────────────────────────────────

/// Valkey-backed implementation. State schema:
/// - `step:<drv_hash>` — HASH with `system`, `pending_deps`,
///   `runnable`, `finished`.
/// - `step:<drv_hash>:rdeps` — SET of rdep drv_hashes.
/// - `ready:<system>` — LIST (FIFO queue) of drv_hashes.
/// - `claim:<claim_id>` — HASH with `drv_hash`.
/// - `claim:next_id` — INCR counter.
/// - `remaining` — INCR/DECR counter.
///
/// All hot-path ops are single Lua-script round-trips.
/// Backed by a pool of connections. A single `RedisClient` serializes
/// all async ops through one TCP socket; with N in-flight workers
/// that queues fast. `RedisPool` round-robins across several clients
/// so the server sees real concurrency.
struct ValkeyStore {
    client: RedisPool,
    claim_script_sha: String,
    complete_script_sha: String,
    ingest_script_sha: String,
}

const CLAIM_SCRIPT: &str = r#"
-- ARGV: systems_csv (comma-separated)
-- Returns: nil | {claim_id, drv_hash}
local systems = {}
for s in string.gmatch(ARGV[1], "([^,]+)") do
  table.insert(systems, s)
end
for _, sys in ipairs(systems) do
  local ready = "ready:" .. sys
  while true do
    local drv = redis.call("LPOP", ready)
    if not drv then break end
    local step = "step:" .. drv
    local fields = redis.call("HMGET", step, "runnable", "finished")
    local runnable, finished = fields[1], fields[2]
    if finished == "1" then
      -- drop, continue scanning this system
    elseif runnable == "1" then
      redis.call("HSET", step, "runnable", "0")
      local cid = redis.call("INCR", "claim:next_id")
      redis.call("HSET", "claim:" .. cid, "drv_hash", drv)
      return {tostring(cid), drv}
    end
  end
end
return nil
"#;

const COMPLETE_SCRIPT: &str = r#"
-- ARGV: claim_id
-- Returns: armed_count
local cid = ARGV[1]
local claim_key = "claim:" .. cid
local drv = redis.call("HGET", claim_key, "drv_hash")
if not drv then return 0 end
redis.call("DEL", claim_key)
local step = "step:" .. drv
redis.call("HSET", step, "finished", "1", "runnable", "0")
redis.call("DECR", "remaining")

local rdeps = redis.call("SMEMBERS", step .. ":rdeps")
local armed = 0
for _, rdep in ipairs(rdeps) do
  local rkey = "step:" .. rdep
  local new_count = redis.call("HINCRBY", rkey, "pending_deps", -1)
  if new_count == 0 then
    local finished = redis.call("HGET", rkey, "finished")
    if finished ~= "1" then
      redis.call("HSET", rkey, "runnable", "1")
      local sys = redis.call("HGET", rkey, "system")
      if sys then
        redis.call("RPUSH", "ready:" .. sys, rdep)
        armed = armed + 1
      end
    end
  end
end
return armed
"#;

const INGEST_SCRIPT: &str = r#"
-- ARGV: json-encoded list of {drv_hash, system, deps: []}
-- Returns: count_ingested
local drvs = cjson.decode(ARGV[1])
local count = 0
for _, drv in ipairs(drvs) do
  local step = "step:" .. drv.drv_hash
  local pending = #drv.deps
  local runnable = (pending == 0) and "1" or "0"
  redis.call("HSET", step,
             "system", drv.system,
             "pending_deps", tostring(pending),
             "runnable", runnable,
             "finished", "0")
  for _, dep in ipairs(drv.deps) do
    redis.call("SADD", "step:" .. dep .. ":rdeps", drv.drv_hash)
  end
  if pending == 0 then
    redis.call("RPUSH", "ready:" .. drv.system, drv.drv_hash)
  end
  redis.call("INCR", "remaining")
  count = count + 1
end
return count
"#;

impl ValkeyStore {
    async fn connect(url: &str, pool_size: usize) -> Result<Self, String> {
        let config = RedisConfig::from_url(url).map_err(|e| format!("parse url: {e}"))?;
        let client = RedisPool::new(config, None, None, None, pool_size)
            .map_err(|e| format!("pool new: {e}"))?;
        client.init().await.map_err(|e| format!("init: {e}"))?;
        // Load scripts (SCRIPT LOAD is replicated to every connection
        // in the pool by fred; we only need to call on one).
        let claim_sha: String = client
            .next()
            .script_load(CLAIM_SCRIPT)
            .await
            .map_err(|e| format!("load claim: {e}"))?;
        let complete_sha: String = client
            .next()
            .script_load(COMPLETE_SCRIPT)
            .await
            .map_err(|e| format!("load complete: {e}"))?;
        let ingest_sha: String = client
            .next()
            .script_load(INGEST_SCRIPT)
            .await
            .map_err(|e| format!("load ingest: {e}"))?;
        Ok(Self {
            client,
            claim_script_sha: claim_sha,
            complete_script_sha: complete_sha,
            ingest_script_sha: ingest_sha,
        })
    }

    async fn flush(&self) -> Result<(), String> {
        let _: () = self
            .client
            .next()
            .flushall(false)
            .await
            .map_err(|e| format!("flushall: {e}"))?;
        Ok(())
    }
}

#[async_trait]
impl SchedulerOps for ValkeyStore {
    async fn ingest_dag(&self, drvs: Vec<IngestDrv>) -> Result<(), String> {
        let json = serde_json::to_string(
            &drvs
                .iter()
                .map(|d| {
                    serde_json::json!({
                        "drv_hash": d.drv_hash,
                        "system": d.system,
                        "deps": d.deps,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .map_err(|e| format!("serialize: {e}"))?;
        // Chunk to keep scripts bounded.
        let chunk_size = 500;
        for chunk in drvs.chunks(chunk_size) {
            let chunk_json = serde_json::to_string(
                &chunk
                    .iter()
                    .map(|d| {
                        serde_json::json!({
                            "drv_hash": d.drv_hash,
                            "system": d.system,
                            "deps": d.deps,
                        })
                    })
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| format!("serialize: {e}"))?;
            let _: i64 = self
                .client
                .evalsha(&self.ingest_script_sha, Vec::<String>::new(), chunk_json)
                .await
                .map_err(|e| format!("ingest evalsha: {e}"))?;
        }
        let _ = json;
        Ok(())
    }

    async fn claim_one(&self, systems: &[String]) -> Result<Option<Claim>, String> {
        let systems_csv = systems.join(",");
        let result: Option<(String, String)> = self
            .client
            .evalsha(&self.claim_script_sha, Vec::<String>::new(), systems_csv)
            .await
            .map_err(|e| format!("claim evalsha: {e}"))?;
        Ok(result.map(|(cid, drv)| Claim {
            claim_id: cid.parse().unwrap_or(0),
            drv_hash: drv,
        }))
    }

    async fn complete_success(&self, claim_id: u64) -> Result<(), String> {
        let _: i64 = self
            .client
            .evalsha(
                &self.complete_script_sha,
                Vec::<String>::new(),
                claim_id.to_string(),
            )
            .await
            .map_err(|e| format!("complete evalsha: {e}"))?;
        Ok(())
    }

    async fn remaining(&self) -> Result<u64, String> {
        let r: Option<String> = self
            .client
            .get("remaining")
            .await
            .map_err(|e| format!("get remaining: {e}"))?;
        Ok(r.and_then(|s| s.parse().ok()).unwrap_or(0))
    }
}

// ─── DAG generator ──────────────────────────────────────────────────

/// Build a layers × width DAG with fan_in edges to the previous layer.
/// Deterministic. Matches the shape of `benches/dispatcher.rs::build_dag`
/// so the two comparisons line up.
fn gen_dag(layers: usize, width: usize, fan_in: usize) -> Vec<IngestDrv> {
    let mut drvs = Vec::with_capacity(layers * width);
    for layer in 0..layers {
        for i in 0..width {
            let drv_hash = format!("l{layer:02}i{i:06}");
            let mut deps = Vec::new();
            if layer > 0 {
                for k in 0..fan_in {
                    let dep_ix = (i * 31 + k * 7 + layer) % width;
                    deps.push(format!("l{:02}i{:06}", layer - 1, dep_ix));
                }
                deps.sort();
                deps.dedup();
            }
            drvs.push(IngestDrv {
                drv_hash,
                system: "x86_64-linux".into(),
                deps,
            });
        }
    }
    drvs
}

// ─── Workload driver ────────────────────────────────────────────────

/// Drive N concurrent workers against the store until the DAG is
/// drained. Each worker loops: claim → (simulated 0-duration build) →
/// complete. Times are captured per-op into HDR histograms.
async fn run_workload(
    store: Arc<dyn SchedulerOps>,
    worker_count: usize,
    total_drvs: u64,
) -> RunStats {
    let start = Instant::now();
    let claim_hist = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap(),
    ));
    let complete_hist = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap(),
    ));
    let claims_done = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let completes_done = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let systems = vec!["x86_64-linux".to_string()];

    let mut handles = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let store = store.clone();
        let claim_hist = claim_hist.clone();
        let complete_hist = complete_hist.clone();
        let claims_done = claims_done.clone();
        let completes_done = completes_done.clone();
        let systems = systems.clone();
        handles.push(tokio::spawn(async move {
            let mut backoff = Duration::from_micros(100);
            loop {
                if completes_done.load(std::sync::atomic::Ordering::Relaxed) >= total_drvs {
                    break;
                }
                let t0 = Instant::now();
                let claim = match store.claim_one(&systems).await {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("claim err: {e}");
                        break;
                    }
                };
                let elapsed = t0.elapsed().as_micros() as u64;
                match claim {
                    Some(c) => {
                        claim_hist.lock().record(elapsed.max(1)).ok();
                        claims_done.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let t1 = Instant::now();
                        if let Err(e) = store.complete_success(c.claim_id).await {
                            eprintln!("complete err: {e}");
                            break;
                        }
                        let ce = t1.elapsed().as_micros() as u64;
                        complete_hist.lock().record(ce.max(1)).ok();
                        completes_done.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        backoff = Duration::from_micros(100);
                    }
                    None => {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_millis(10));
                    }
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let claim_hist_clone = claim_hist.lock().clone();
    let complete_hist_clone = complete_hist.lock().clone();
    RunStats {
        claim_hist: claim_hist_clone,
        complete_hist: complete_hist_clone,
        total_claims: claims_done.load(std::sync::atomic::Ordering::Relaxed),
        total_completes: completes_done.load(std::sync::atomic::Ordering::Relaxed),
        wall: start.elapsed(),
    }
}

fn print_stats(name: &str, stats: &RunStats) {
    println!("  === {name} ===");
    println!(
        "  wall       : {:>10.3} s",
        stats.wall.as_secs_f64()
    );
    println!(
        "  throughput : {:>10.0} claims/sec",
        stats.total_completes as f64 / stats.wall.as_secs_f64()
    );
    println!("  claims     : {}", stats.total_claims);
    println!("  completes  : {}", stats.total_completes);
    println!(
        "  claim p50  : {:>8} µs",
        stats.claim_hist.value_at_quantile(0.50)
    );
    println!(
        "  claim p95  : {:>8} µs",
        stats.claim_hist.value_at_quantile(0.95)
    );
    println!(
        "  claim p99  : {:>8} µs",
        stats.claim_hist.value_at_quantile(0.99)
    );
    println!(
        "  claim max  : {:>8} µs",
        stats.claim_hist.max()
    );
    println!(
        "  complete p50: {:>7} µs",
        stats.complete_hist.value_at_quantile(0.50)
    );
    println!(
        "  complete p95: {:>7} µs",
        stats.complete_hist.value_at_quantile(0.95)
    );
    println!(
        "  complete p99: {:>7} µs",
        stats.complete_hist.value_at_quantile(0.99)
    );
    println!(
        "  complete max: {:>7} µs",
        stats.complete_hist.max()
    );
}

// ─── Scenarios ──────────────────────────────────────────────────────

struct Scenario {
    name: &'static str,
    layers: usize,
    width: usize,
    fan_in: usize,
    workers: usize,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "1k",
        layers: 5,
        width: 200,
        fan_in: 2,
        workers: 32,
    },
    Scenario {
        name: "10k",
        layers: 5,
        width: 2_000,
        fan_in: 2,
        workers: 128,
    },
    Scenario {
        name: "50k",
        layers: 5,
        width: 10_000,
        fan_in: 2,
        workers: 512,
    },
];

async fn run_scenario(scenario: &Scenario, valkey_url: &str) {
    let dag = gen_dag(scenario.layers, scenario.width, scenario.fan_in);
    let total = dag.len() as u64;
    println!(
        "\n================================================================\n\
         Scenario: {} — {}×{} (fan_in={}) = {} drvs, {} workers\n\
         ================================================================",
        scenario.name, scenario.layers, scenario.width, scenario.fan_in, total, scenario.workers
    );

    // Memory run
    {
        let store = Arc::new(MemoryStore::new());
        let ingest_start = Instant::now();
        store
            .ingest_dag(dag.clone())
            .await
            .expect("memory ingest");
        println!(
            "memory ingest: {:.3} s",
            ingest_start.elapsed().as_secs_f64()
        );
        let stats = run_workload(store.clone(), scenario.workers, total).await;
        print_stats("MemoryStore", &stats);
    }

    // Valkey run. Pool sizing: single-threaded server means ~16
    // concurrent connections saturates it regardless of worker count
    // (empirically — higher pool sizes tank tail latency via
    // connection-churn overhead in fred + Docker bridge).
    let pool_size = 16usize.min(scenario.workers);
    match ValkeyStore::connect(valkey_url, pool_size).await {
        Ok(store) => {
            store.flush().await.expect("flush");
            let ingest_start = Instant::now();
            store.ingest_dag(dag.clone()).await.expect("valkey ingest");
            println!(
                "valkey ingest: {:.3} s",
                ingest_start.elapsed().as_secs_f64()
            );
            let store_arc: Arc<dyn SchedulerOps> = Arc::new(store);
            let stats = run_workload(store_arc, scenario.workers, total).await;
            print_stats("ValkeyStore", &stats);
        }
        Err(e) => {
            eprintln!(
                "\n!! ValkeyStore unavailable ({e}). Start valkey locally:\n   \
                 valkey-server --daemonize yes --save '' --appendonly no --port 6379\n   \
                 (or override with VALKEY_URL=redis://host:port)\n"
            );
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let filter: Option<&str> = args.iter().skip(1).find(|a| !a.starts_with('-')).map(|s| s.as_str());
    let valkey_url = std::env::var("VALKEY_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        for s in SCENARIOS {
            if let Some(f) = filter {
                if s.name != f {
                    continue;
                }
            }
            run_scenario(s, &valkey_url).await;
        }
    });
}
