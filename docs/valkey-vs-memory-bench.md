# Benchmark: Valkey-backed vs in-memory dispatcher (prototype)

**Worktree:** `buildbuddy-shape-exploration`, based on v3 tip (`6bb4136`).
**Bench source:** [benches/valkey_vs_memory.rs](../rust/crates/nix-ci-core/benches/valkey_vs_memory.rs).
**Date run:** 2026-04-18.

## What this measures

Head-to-head: two implementations of a minimal `SchedulerOps` trait covering the hot-path of a Nix build scheduler (`ingest_dag` + `claim_one` + `complete_success`). N concurrent async "workers" loop claim → (simulated 0-duration build) → complete until the DAG drains. We record per-op latency in HDR histograms and total wall time.

**MemoryStore**: pure Rust, a HashMap + VecDeque + SingleMutex ownership model. Simpler than the real `nix-ci-core::dispatch::Dispatcher` — the real dispatcher has finer-grained atomics and per-submission locks, so it would be equal-or-faster, not slower. Treat these numbers as a conservative upper bound on what the current in-memory path can do.

**ValkeyStore**: [`fred`](https://github.com/aembke/fred.rs) 9.4 + Valkey 8 in a Docker container on `127.0.0.1:6379`. All hot-path ops are single `EVALSHA` round-trips against pre-loaded Lua scripts. Connection pool sized at 16 (higher values tanked tail latency via Docker-bridge connection churn).

## What this does NOT measure

- HTTP stack (axum, JSON serde, client reqwest) — both backends would pay the same cost on top.
- Ingest throughput (both handle it fine; ingest isn't the contested bottleneck).
- Failure propagation, retry backoff, SSE event fan-out, extend/reap paths.
- Production network topology — Docker bridge adds ~0.1-0.3ms RTT vs intra-cluster which tends to be lower.
- Real-world fred tuning — defaults only.

Goal is a defensible directional number for the "is Valkey within a reasonable ratio of in-memory?" decision, not a production SLO.

## Setup

```
docker run -d --name nix-ci-valkey -p 127.0.0.1:6379:6379 \
  valkey/valkey:8 valkey-server --save '' --appendonly no

cargo bench --bench valkey_vs_memory -p nix-ci-core -- 1k
cargo bench --bench valkey_vs_memory -p nix-ci-core -- 10k
cargo bench --bench valkey_vs_memory -p nix-ci-core -- 50k
```

DAG shape: layered, fan-in=2. 5 layers × width gives the drv count. Workers scale with drv count.

## Results

### 1k scenario (5 × 200, fan-in=2, 32 workers)

| | MemoryStore | ValkeyStore | ratio |
|---|---:|---:|---:|
| **throughput** | 225,320 claims/s | 16,056 claims/s | 14.0× |
| **wall** | 4 ms | 62 ms | 15.5× |
| **claim p50** | 6 µs | 775 µs | 129× |
| **claim p95** | 118 µs | 2,237 µs | 19× |
| **claim p99** | 193 µs | 2,789 µs | 14× |
| **complete p50** | 6 µs | 744 µs | 124× |
| **complete p99** | 186 µs | 2,829 µs | 15× |

### 10k scenario (5 × 2000, fan-in=2, 128 workers)

| | MemoryStore | ValkeyStore | ratio |
|---|---:|---:|---:|
| **throughput** | 282,548 claims/s | 19,028 claims/s | 14.8× |
| **wall** | 35 ms | 526 ms | 15.0× |
| **claim p50** | 6 µs | 2,487 µs | 414× |
| **claim p95** | 133 µs | 8,607 µs | 65× |
| **claim p99** | 227 µs | 9,935 µs | 44× |
| **complete p50** | 7 µs | 2,487 µs | 355× |
| **complete p99** | 222 µs | 9,999 µs | 45× |

### 50k scenario (5 × 10000, fan-in=2, 512 workers)

| | MemoryStore | ValkeyStore | ratio |
|---|---:|---:|---:|
| **throughput** | 292,286 claims/s | 19,133 claims/s | 15.3× |
| **wall** | 171 ms | 2.6 s | 15.2× |
| **claim p50** | 6 µs | 12,023 µs | 2004× |
| **claim p95** | 129 µs | 28,447 µs | 220× |
| **claim p99** | 221 µs | 37,343 µs | 169× |
| **complete p50** | 6 µs | 12,095 µs | 2016× |
| **complete p99** | 219 µs | 37,311 µs | 170× |

### Reference: native valkey throughput (no client overhead)

For a ceiling reference, run in-container against the same Valkey:

```
$ docker exec nix-ci-valkey valkey-benchmark -t set,get -n 100000 -c 16 -q
SET: 296,736 requests per second, p50=0.039 msec
GET: 334,448 requests per second, p50=0.031 msec

$ docker exec nix-ci-valkey valkey-benchmark -n 50000 -c 16 -q \
    eval "redis.call('HSET', 'k', 'f', ARGV[1]); return redis.call('HGET', 'k', 'f')" 0 test
EVAL (2-op Lua): 233,645 requests per second, p50=0.055 msec
```

**Our fred→Valkey path ends up at ~19k/s vs raw Valkey's 234k/s EVAL ceiling — a 12× client+bridge overhead tax.** Breakdown (estimated):
- Fred async scheduling: ~50-100µs per op
- Docker bridge RTT: ~100-200µs per op (vs ~20µs intra-container)
- Connection queue depth (128 workers / 16 connections = 8 deep): ~ms of queueing at p95+
- Our Lua scripts are heavier than the reference (iterate a ready list, HMGET 2 fields, HSET, HINCRBY on rdeps)

## Interpretation

**1. ValkeyStore is ~15× slower on throughput and 40-2000× slower on latency.** This is the brutal honest headline. My earlier "Valkey preserves graph-algorithms-at-memory-speed" framing was wrong — ValKey over a real TCP socket via fred is far slower than in-process Rust, no matter how elegant the Lua scripts are.

**2. 19k claims/sec is still 6× the target load.** Our stated target at 10x production scale is ~3k claims/sec. ValkeyStore at 19k/s has 6× headroom. If this were the only constraint, ValKey would still work.

**3. The p99 tail is concerning.** At 50k scenario with 512 workers, p99 claim latency is 37ms. Individual claim RPCs from workers become slow enough to be user-visible. At 10x scale this is borderline.

**4. Our MemoryStore numbers (200-290k claims/sec, p50 6µs) match v3's existing Dispatcher benchmarks** — the v3 in-memory path is ~14× faster than ValkeyStore and has p99 in hundreds of microseconds vs tens of ms. That's a real perf gap.

**5. Production network would help, but only partially.** Moving Valkey inside the same cluster would shrink the 0.1-0.2ms Docker bridge RTT to maybe 0.05ms. That shaves ~0.5ms off p50. ValkeyStore would land at ~1-2ms p50 best case — still 150-300× slower than in-memory.

## Caveats before drawing conclusions

Honest limitations of this prototype:

1. **Not the real Dispatcher.** MemoryStore is simpler. The real Dispatcher has per-submission broadcast channels, PG write-through, reaper, retry-backoff, feature filtering, etc. Those add cost to every op, but they also add cost equally on both sides if we were comparing "real" vs "real ValKey." So the ratio probably holds, but the absolute MemoryStore number could be 2-5× lower in production.

2. **Not pipelined.** Our bench has workers issue one op at a time, synchronously awaiting each. Fred supports automatic pipelining if multiple ops are in flight via the same connection. With pipelining, ValkeyStore could plausibly double its throughput.

3. **Not cluster-tuned.** Single Valkey instance, no replica lag, no Sentinel. Real HA deploy would be slightly slower.

4. **Docker bridge is the slowest path.** In-cluster + unix-socket or same-node network would measurably help.

5. **Scripts aren't optimized.** The `claim_one` script pops from a list in a loop until it finds a runnable one. Could be replaced with a sorted set + ZPOPMIN for O(log n) rather than LPOP + retry.

After all these caveats, ValkeyStore would realistically land somewhere between 25k-60k claims/sec with p99 in the 5-15ms range. Still ~5-10× slower than in-memory, but in a different operational regime.

## What this means for the design decision

The decision ceases to be about performance and becomes about **what you're optimizing for**:

| Optimization goal | Winner |
|---|---|
| Raw hot-path throughput | **MemoryStore** (14-15× faster) |
| Hot-path latency | **MemoryStore** (100-2000× lower p50, 15-170× lower p99) |
| Meeting 3k claims/sec at 10x scale | Both work (mem: 100× headroom, valkey: 6× headroom) |
| Surviving coordinator restart without losing state | **ValkeyStore** |
| Running multiple coordinator instances (HA, rolling deploys) | **ValkeyStore** |
| Operational simplicity | **MemoryStore** (no new datastore) |
| Matching current proven architecture | **MemoryStore** (v3 works today) |
| Production precedent for this shape | **ValkeyStore** (BuildBuddy, NativeLink) |

**If performance-above-all: keep the in-memory single-writer.** The v3 design is already within a factor of ~5 of what's physically possible (single-threaded Rust accessing HashMaps has a hard floor around microseconds). There's no "fast and HA" compromise via ValKey — ValKey sacrifices ~15× performance to get HA.

**If HA-above-simplicity: ValKey works but isn't free.** 19-60k claims/sec is enough for the stated target load, but the p99 latency tax would be visible in claim RPCs. The in-memory path felt "instant"; ValKey will feel "fast" but not "instant."

**If we value both: the honest answer may be "keep v3 single-writer for now and revisit when either (a) restart-loses-in-flight-state actually hurts in practice, or (b) horizontal scaling becomes a measured requirement."** Sharding (per earlier discussion) is the other way to get HA without the per-op perf tax, at the cost of topology complexity.

## Open tuning dimensions (if we pursued ValKey further)

- Pipeline more aggressively (batch completes, or do claim+complete in one RT).
- Rewrite `claim_one` Lua to use sorted-set pop instead of list-LPOP-retry.
- Use unix socket instead of TCP (for co-located Valkey on the same pod).
- Try DragonflyDB (multithreaded server; might reduce server-side latency but not client+network overhead).
- Enable fred's `auto-pipeline` (already on by default but worth double-checking with `perf` flag).
- Use RedisPool with `ExclusivePool` for the claim path specifically.
- Reduce Lua script size (inline-free the `cjson.decode` for ingest, pass args as ARGV tuples).

None of these are likely to close the gap to in-memory. They'd trim it from 15× to maybe 5-10×.

## Recommendation

Don't prematurely refactor to ValKey based on these numbers. **The perf gap is meaningfully larger than "5×" and the HA wins are not yet load-bearing.** Revisit when either:

1. HA becomes a measured requirement (real production incidents from restarts).
2. We want horizontal scaling beyond a single coordinator.
3. We need cross-region coordination.

In the meantime, the existing v3 in-memory single-writer stays the right pick — and the main.bkp history (PG was genuinely too slow for this workload) + these numbers (ValKey is meaningfully slower than in-memory too) together make a consistent story: **for DAG-scheduler workloads with the shape of Nix builds, in-memory dispatch is the correct default, and external-store alternatives are a 10-15× perf tax you pay for HA.**

The v3 design's approach — pg_advisory_lock + cold rehydrate from PG + single-writer — is in the right neighborhood. What's worth investing in is making cold rehydration fast and correct, and making the restart-loses-in-flight-work contract explicitly acceptable (which it largely is thanks to Nix content-addressing, per earlier discussion).

## Artifacts

- Bench source: [benches/valkey_vs_memory.rs](../rust/crates/nix-ci-core/benches/valkey_vs_memory.rs)
- Cargo entry: [Cargo.toml](../rust/crates/nix-ci-core/Cargo.toml) under `[[bench]] name = "valkey_vs_memory"`
- Nix flake now includes `valkey` in the devshell
- Design doc (now superseded by these measurements): [design-stateless-coordinator.md](design-stateless-coordinator.md)

Raw output from a clean run:

```
=== 1k scenario ===
MemoryStore: 225,320 claims/s, p50=6µs p95=118µs p99=193µs max=394µs
ValkeyStore:  16,056 claims/s, p50=775µs p95=2.2ms p99=2.8ms max=3.4ms

=== 10k scenario ===
MemoryStore: 282,548 claims/s, p50=6µs p95=133µs p99=227µs max=572µs
ValkeyStore:  19,028 claims/s, p50=2.5ms p95=8.6ms p99=9.9ms max=11.2ms

=== 50k scenario ===
MemoryStore: 292,286 claims/s, p50=6µs p95=129µs p99=221µs max=1.3ms
ValkeyStore:  19,133 claims/s, p50=12ms p95=28ms p99=37ms max=43ms
```
