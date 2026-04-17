//! Prometheus metrics registry. One instance per coordinator.

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use std::sync::Arc;

#[derive(Clone)]
pub struct Metrics {
    pub inner: Arc<MetricsInner>,
}

pub struct MetricsInner {
    pub registry: parking_lot::Mutex<Registry>,

    // Jobs / drvs
    pub jobs_created: Counter,
    pub jobs_terminal: Family<TerminalLabels, Counter>,
    pub drvs_ingested: Counter,
    pub drvs_deduped: Counter,

    // Dispatch
    pub claims_issued: Counter,
    pub claims_in_flight: Gauge,
    pub builds_completed: Family<OutcomeLabels, Counter>,
    pub build_duration: Histogram,
    pub dispatch_wait_seconds: Histogram,

    // Failure propagation
    pub propagated_failures: Counter,

    // Reaper / retry visibility — failures that would otherwise be
    // silent "this got cleaned up eventually" behavior.
    /// Claims reaped by deadline timeout (workers that never responded).
    pub claims_expired: Counter,
    /// Jobs reaped by heartbeat timeout (worker/network lost the job).
    pub jobs_reaped: Counter,
    /// SSE broadcast events dropped because a subscriber was slow and
    /// the per-submission channel overflowed. Subscribers see a
    /// Lagged event and should re-sync; a high rate indicates a slow
    /// consumer (often a hung client).
    pub events_dropped: Counter,

    // Dispatcher snapshot gauges — refreshed on every `/metrics`
    // scrape. Useful to graph memory pressure and detect leaks (both
    // should stay bounded relative to active job count).
    pub submissions_active: Gauge,
    pub steps_registry_size: Gauge,

    // Per-endpoint HTTP latency. Wraps every handler via a Tower-style
    // axum middleware. Long-poll endpoints (`/claim`, `/jobs/{}/claim`,
    // `/jobs/{}/events`) are excluded from the histogram — their
    // latency is dominated by wait-time, not work-time, and would
    // pollute SLO buckets.
    pub http_request_duration_seconds: Family<HttpLabels, Histogram>,

    // Build log archive capacity. `bytes_total` includes TOAST + indexes
    // (i.e. true on-disk cost). `rows_total` is a planner estimate, not
    // an exact count.
    pub build_logs_bytes_total: Gauge,
    pub build_logs_rows_total: Gauge,
    /// Worker-uploaded logs that hit the per-attempt raw-size cap and
    /// were truncated. A spike here indicates pathological build
    /// output (e.g. a test that prints every line).
    pub build_logs_truncated_total: Counter,

    // Per-job ingest size. Recorded at terminal time. Buckets span 1
    // → 5M drvs to catch both trivial flakes and pathological mega-DAGs.
    pub drvs_per_job: Histogram,
    /// Soft warnings emitted when a single submission's member count
    /// crossed `submission_warn_threshold`. Counts events, not drvs.
    pub submission_warn_total: Counter,

    // H3 observability additions: metrics that matter at 3am.
    /// Histogram of how long claims live before being completed or
    /// expired. Tail = stuck workers. P99 > 10 min in a healthy
    /// nixpkgs deployment usually means a drv with runaway IO or a
    /// hung network operation.
    pub claim_age_seconds: Histogram,
    /// Histogram of per-batch ingest size (drvs per POST
    /// `/jobs/{}/drvs/batch`). Helps debug ingest latency tails.
    pub ingest_batch_drvs: Histogram,
    /// Current total size of the Postgres pool (acquired + idle).
    pub pg_pool_size: Gauge,
    /// Current idle Postgres connections.
    pub pg_pool_idle: Gauge,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, prometheus_client::encoding::EncodeLabelSet)]
pub struct TerminalLabels {
    pub status: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, prometheus_client::encoding::EncodeLabelSet)]
pub struct OutcomeLabels {
    pub outcome: String,
}

/// Labels for the per-endpoint HTTP latency histogram. Cardinality is
/// bounded by the route table — we always use the matched route
/// pattern (e.g. `/jobs/{id}`), never the rendered URL.
#[derive(Clone, Debug, Hash, Eq, PartialEq, prometheus_client::encoding::EncodeLabelSet)]
pub struct HttpLabels {
    pub method: String,
    pub route: String,
    pub status: String,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let jobs_created = Counter::default();
        registry.register(
            "nix_ci_jobs_created",
            "Jobs created since startup",
            jobs_created.clone(),
        );

        let jobs_terminal: Family<TerminalLabels, Counter> = Family::default();
        registry.register(
            "nix_ci_jobs_terminal",
            "Jobs reaching a terminal status",
            jobs_terminal.clone(),
        );

        let drvs_ingested = Counter::default();
        registry.register(
            "nix_ci_drvs_ingested",
            "Derivations ingested (new)",
            drvs_ingested.clone(),
        );

        let drvs_deduped = Counter::default();
        registry.register(
            "nix_ci_drvs_deduped",
            "Derivations deduped against existing registry entries",
            drvs_deduped.clone(),
        );

        let claims_issued = Counter::default();
        registry.register(
            "nix_ci_claims_issued",
            "Claims issued to workers",
            claims_issued.clone(),
        );

        let claims_in_flight = Gauge::default();
        registry.register(
            "nix_ci_claims_in_flight",
            "Claims currently outstanding",
            claims_in_flight.clone(),
        );

        let builds_completed: Family<OutcomeLabels, Counter> = Family::default();
        registry.register(
            "nix_ci_builds_completed",
            "Build completions by outcome",
            builds_completed.clone(),
        );

        let build_duration = Histogram::new([
            0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0,
        ]);
        registry.register(
            "nix_ci_build_duration_seconds",
            "Build wall-clock duration reported by workers",
            build_duration.clone(),
        );

        let dispatch_wait_seconds = Histogram::new([0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 30.0, 60.0]);
        registry.register(
            "nix_ci_dispatch_wait_seconds",
            "Wait time from runnable→claimed",
            dispatch_wait_seconds.clone(),
        );

        let propagated_failures = Counter::default();
        registry.register(
            "nix_ci_propagated_failures",
            "Dependents failed due to upstream failure",
            propagated_failures.clone(),
        );

        let claims_expired = Counter::default();
        registry.register(
            "nix_ci_claims_expired",
            "Claims reaped by deadline timeout",
            claims_expired.clone(),
        );

        let jobs_reaped = Counter::default();
        registry.register(
            "nix_ci_jobs_reaped",
            "Jobs reaped by heartbeat timeout",
            jobs_reaped.clone(),
        );

        let events_dropped = Counter::default();
        registry.register(
            "nix_ci_events_dropped",
            "SSE broadcast events dropped due to slow subscriber",
            events_dropped.clone(),
        );

        let submissions_active = Gauge::default();
        registry.register(
            "nix_ci_submissions_active",
            "In-memory submissions (un-terminated jobs)",
            submissions_active.clone(),
        );

        let steps_registry_size = Gauge::default();
        registry.register(
            "nix_ci_steps_registry_size",
            "Entries in the Steps registry (live + stale-weak)",
            steps_registry_size.clone(),
        );

        // Per-endpoint latency. Buckets cover the realistic range:
        // sub-ms (dedup-cached ingest, in-mem complete) through ~10s
        // (cold ingest with DB contention or terminal writeback).
        let http_request_duration_seconds: Family<HttpLabels, Histogram> =
            Family::new_with_constructor(|| {
                Histogram::new([
                    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                ])
            });
        registry.register(
            "nix_ci_http_request_duration_seconds",
            "HTTP request duration by route + status (long-poll endpoints excluded)",
            http_request_duration_seconds.clone(),
        );

        let build_logs_bytes_total = Gauge::default();
        registry.register(
            "nix_ci_build_logs_bytes_total",
            "Total on-disk bytes used by build_logs (heap + TOAST + indexes)",
            build_logs_bytes_total.clone(),
        );
        let build_logs_rows_total = Gauge::default();
        registry.register(
            "nix_ci_build_logs_rows_total",
            "Estimated row count in build_logs (planner stats)",
            build_logs_rows_total.clone(),
        );
        let build_logs_truncated_total = Counter::default();
        // Library appends `_total` per OpenMetrics — don't include it in
        // the registered name. Wire becomes `nix_ci_build_logs_truncated_total`.
        registry.register(
            "nix_ci_build_logs_truncated",
            "Worker-uploaded logs that were truncated to the per-attempt cap",
            build_logs_truncated_total.clone(),
        );

        // 1 → 5M drvs. Captures trivial single-attr jobs through
        // pathological mega-DAGs.
        let drvs_per_job = Histogram::new([
            1.0,
            10.0,
            100.0,
            1_000.0,
            10_000.0,
            100_000.0,
            500_000.0,
            1_000_000.0,
            5_000_000.0,
        ]);
        registry.register(
            "nix_ci_drvs_per_job",
            "Per-submission member count, recorded at terminal time",
            drvs_per_job.clone(),
        );
        let submission_warn_total = Counter::default();
        // Library appends `_total` per OpenMetrics — don't include it in
        // the registered name. Wire becomes `nix_ci_submission_warn_total`.
        registry.register(
            "nix_ci_submission_warn",
            "Submissions whose live member count exceeded the warning threshold",
            submission_warn_total.clone(),
        );

        // H3: new observability metrics (not registered yet).
        let claim_age_seconds = Histogram::new([
            0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 1800.0, 3600.0,
        ]);
        registry.register(
            "nix_ci_claim_age_seconds",
            "Time from claim issued to claim ended (complete or expire)",
            claim_age_seconds.clone(),
        );
        let ingest_batch_drvs = Histogram::new([
            1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1_000.0, 5_000.0, 10_000.0, 50_000.0,
        ]);
        registry.register(
            "nix_ci_ingest_batch_drvs",
            "Per-batch ingest size (drvs per POST request)",
            ingest_batch_drvs.clone(),
        );
        let pg_pool_size = Gauge::default();
        registry.register(
            "nix_ci_pg_pool_size",
            "Total Postgres connections in the coordinator pool",
            pg_pool_size.clone(),
        );
        let pg_pool_idle = Gauge::default();
        registry.register(
            "nix_ci_pg_pool_idle",
            "Idle Postgres connections in the coordinator pool",
            pg_pool_idle.clone(),
        );

        Self {
            inner: Arc::new(MetricsInner {
                registry: parking_lot::Mutex::new(registry),
                jobs_created,
                jobs_terminal,
                drvs_ingested,
                drvs_deduped,
                claims_issued,
                claims_in_flight,
                builds_completed,
                build_duration,
                dispatch_wait_seconds,
                propagated_failures,
                claims_expired,
                jobs_reaped,
                events_dropped,
                submissions_active,
                steps_registry_size,
                http_request_duration_seconds,
                build_logs_bytes_total,
                build_logs_rows_total,
                build_logs_truncated_total,
                drvs_per_job,
                submission_warn_total,
                claim_age_seconds,
                ingest_batch_drvs,
                pg_pool_size,
                pg_pool_idle,
            }),
        }
    }

    pub fn render(&self) -> String {
        let mut out = String::with_capacity(4096);
        let registry = self.inner.registry.lock();
        prometheus_client::encoding::text::encode(&mut out, &registry).unwrap_or_default();
        out
    }
}
