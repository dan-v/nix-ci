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
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, prometheus_client::encoding::EncodeLabelSet)]
pub struct TerminalLabels {
    pub status: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, prometheus_client::encoding::EncodeLabelSet)]
pub struct OutcomeLabels {
    pub outcome: String,
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
