//! Human-readable output for `nix-ci run`.
//!
//! Holds the state machine that turns the SSE event stream into the
//! progress / failure / summary blocks documented in the runner UX
//! redesign:
//!
//! ```text
//! nix-ci run: started (job ce47b9f2, ext-ref ci-build-12345)
//!   flake: .#packages.x86_64-linux
//!
//! [ 234 done / 1234 ingested · eval running ] 4 in flight · 0 failed (45s)
//!   → gcc-13.2.0          (8s)
//!   → llvm-17             (6s)
//! [ 4521 done / 8421 ingested · eval done ] 54% · 8 in flight · 0 failed · 1 retry (5m 12s)
//!
//! ❌ FAILED: hello-2.12.1
//!    drv:     /nix/store/…
//!    reason:  builder failed with exit code 1
//!    used by: packages.x86_64-linux.helloApp
//!    log:     nix log /nix/store/…
//!
//! nix-ci run: SUCCEEDED in 14m 23s (job ce47b9f2)
//!   ingested:  8421 drvs · 1234 cached · 7187 built
//!   builds:    7187 succeeded · 0 failed · 1 transient retry
//!   slowest:   gcc-13.2.0 (5m 12s) · llvm-17 (4m 41s) · boost-1.83 (3m 8s)
//!   cache hit: 14.7%
//!
//!   trace: nix-ci show ce47b9f2  (or: nix-ci show ci-build-12345)
//! ```
//!
//! Output goes to stdout via `println!` (not the tracing layer), so
//! it's not interleaved with structured logs. The state machine is
//! pure — `observe()` mutates internal counters and may emit; tests
//! drive it with synthetic event sequences.

use std::time::{Duration, Instant};

use crate::types::{DrvFailure, DrvInFlight, ErrorCategory, JobCounts, JobEvent, JobId, JobStatus};

/// One originating failure as the runner shows it.
#[derive(Debug, Clone)]
pub struct RenderedFailure {
    pub drv_name: String,
    pub error_category: ErrorCategory,
    pub error_message: Option<String>,
    pub used_by_attrs: Vec<String>,
}

pub struct OutputRenderer {
    pub job_id: JobId,
    pub external_ref: Option<String>,
    pub started_at: Instant,
    pub verbose: bool,
    // Cumulative state observed from the SSE stream.
    failures: Vec<RenderedFailure>,
    propagated_count: u32,
    transient_retries: u32,
    last_counts: JobCounts,
    last_in_flight: Vec<DrvInFlight>,
    last_sealed: bool,
    /// (drv_name, duration_ms) for completed builds — kept sorted
    /// descending; capped to a small window so memory is bounded
    /// even on huge runs.
    slowest: Vec<(String, u64)>,
    /// Set true when the JobDone event arrives so a stray late event
    /// doesn't fire a second summary.
    finished: bool,
}

const SLOWEST_KEEP: usize = 8;
const IN_FLIGHT_SHOW: usize = 4;

impl OutputRenderer {
    pub fn new(job_id: JobId, external_ref: Option<String>, verbose: bool) -> Self {
        Self {
            job_id,
            external_ref,
            started_at: Instant::now(),
            verbose,
            failures: Vec::new(),
            propagated_count: 0,
            transient_retries: 0,
            last_counts: JobCounts::default(),
            last_in_flight: Vec::new(),
            last_sealed: false,
            slowest: Vec::new(),
            finished: false,
        }
    }

    /// Print the run header. Call once before the SSE loop starts.
    pub fn print_start(&self, source: &str) {
        let ext = self
            .external_ref
            .as_deref()
            .map(|r| format!(", ext-ref {r}"))
            .unwrap_or_default();
        println!(
            "nix-ci run: started (job {}{})",
            short_id(&self.job_id),
            ext
        );
        println!("  source: {source}");
        println!();
    }

    /// Process one SSE event — updates internal state and may emit
    /// human-readable output.
    pub fn observe(&mut self, ev: &JobEvent) {
        match ev {
            JobEvent::DrvStarted {
                drv_name, attempt, ..
            } => {
                if self.verbose {
                    println!("  started: {drv_name} (attempt {attempt})");
                }
            }
            JobEvent::DrvCompleted {
                drv_name,
                duration_ms,
                ..
            } => {
                self.record_slowest(drv_name, *duration_ms);
                if self.verbose {
                    println!("  built: {drv_name} ({})", fmt_ms(*duration_ms));
                }
            }
            JobEvent::DrvFailed {
                drv_name,
                error_category,
                error_message,
                will_retry,
                used_by_attrs,
                ..
            } => {
                if *will_retry {
                    // Retries are surfaced as a count, not per-event.
                    if self.verbose {
                        println!("  retry: {drv_name} ({error_category:?})");
                    }
                    return;
                }
                // Terminal failure — emit a failure block immediately and
                // record for the end-of-run summary.
                let only_propagated = matches!(error_category, ErrorCategory::PropagatedFailure);
                if !only_propagated {
                    self.failures.push(RenderedFailure {
                        drv_name: drv_name.clone(),
                        error_category: *error_category,
                        error_message: error_message.clone(),
                        used_by_attrs: used_by_attrs.clone(),
                    });
                    println!();
                    println!("FAILED: {drv_name}");
                    if let Some(msg) = error_message {
                        println!("   reason:  {msg}");
                    } else {
                        println!("   reason:  ({error_category:?})");
                    }
                    if !used_by_attrs.is_empty() {
                        let tail = used_by_attrs.join(", ");
                        println!("   used by: {tail}");
                    }
                    println!();
                }
            }
            JobEvent::Progress {
                counts,
                in_flight,
                propagated_failed,
                transient_retries,
                sealed,
            } => {
                self.last_counts = counts.clone();
                self.last_in_flight = in_flight.clone();
                self.last_sealed = *sealed;
                self.propagated_count = *propagated_failed;
                self.transient_retries = *transient_retries;
                self.print_progress();
            }
            JobEvent::JobDone { status, failures } => {
                if self.finished {
                    return;
                }
                self.finished = true;
                self.print_summary(*status, failures);
            }
            JobEvent::Lagged { missed } => {
                println!("  warning: SSE consumer fell behind, missed {missed} events");
            }
        }
    }

    fn print_progress(&self) {
        let elapsed = self.started_at.elapsed();
        let counts = &self.last_counts;
        let prefix = if self.last_sealed {
            // After seal, totals are fixed — show percent.
            let pct = if counts.total > 0 {
                (counts.done + counts.failed) as f64 * 100.0 / counts.total as f64
            } else {
                100.0
            };
            format!(
                "[ {} / {} ingested ] {:.0}%",
                counts.done, counts.total, pct
            )
        } else {
            // Eval still running — total isn't final yet.
            format!(
                "[ {} done / {} ingested · eval running ]",
                counts.done, counts.total
            )
        };
        let mut tail = format!("{} in flight · {} failed", counts.building, counts.failed);
        if self.propagated_count > 0 {
            tail.push_str(&format!(" · {} propagated", self.propagated_count));
        }
        if self.transient_retries > 0 {
            tail.push_str(&format!(" · {} retry", self.transient_retries));
        }
        println!("{prefix} {tail} ({})", fmt_duration(elapsed));
        // Currently building list — top N, longest first by start time.
        let n = self.last_in_flight.len();
        let show = n.min(IN_FLIGHT_SHOW);
        let mut sorted: Vec<&DrvInFlight> = self.last_in_flight.iter().collect();
        sorted.sort_by_key(|d| d.started_at_ms);
        let now_ms = chrono::Utc::now().timestamp_millis();
        for d in sorted.iter().take(show) {
            let elapsed_ms = (now_ms - d.started_at_ms).max(0) as u64;
            println!("  → {:<28} ({})", d.drv_name, fmt_ms(elapsed_ms));
        }
        if n > IN_FLIGHT_SHOW {
            println!("  ... +{} more", n - IN_FLIGHT_SHOW);
        }
    }

    fn print_summary(&mut self, status: JobStatus, all_failures: &[DrvFailure]) {
        let elapsed = self.started_at.elapsed();
        let counts = &self.last_counts;
        // Status header
        println!();
        let label = match status {
            JobStatus::Done => "SUCCEEDED",
            JobStatus::Failed => "FAILED",
            JobStatus::Cancelled => "CANCELLED",
            other => return println!("nix-ci run: {other:?} ({})", fmt_duration(elapsed)),
        };
        let ext = self
            .external_ref
            .as_deref()
            .map(|r| format!(", ext-ref {r}"))
            .unwrap_or_default();
        println!(
            "nix-ci run: {label} in {} (job {}{})",
            fmt_duration(elapsed),
            short_id(&self.job_id),
            ext
        );
        // Counts: ingested / built / cached split. We only know
        // "succeeded" from JobDone (counts.done at terminal time).
        println!(
            "  ingested:  {} drvs · {} done · {} failed",
            counts.total, counts.done, counts.failed
        );
        let originating = self.failures.len();
        let mut builds_line = format!(
            "  builds:    {} succeeded · {} failed",
            counts.done, originating
        );
        if self.transient_retries > 0 {
            builds_line.push_str(&format!(" · {} transient retry", self.transient_retries));
        }
        println!("{builds_line}");
        // For Failed status: per-failure detail block.
        if matches!(status, JobStatus::Failed) {
            // Originating failures (already shown inline; repeat in summary
            // for greppability).
            if !self.failures.is_empty() {
                println!("  failures:");
                for f in &self.failures {
                    println!("    {}", f.drv_name);
                    if let Some(msg) = &f.error_message {
                        println!("      reason:  {msg}");
                    }
                    if !f.used_by_attrs.is_empty() {
                        let tail = f.used_by_attrs.join(", ");
                        println!("      used by: {tail}");
                    }
                }
            }
            // Collapse propagated failures.
            let propagated: Vec<&DrvFailure> = all_failures
                .iter()
                .filter(|f| matches!(f.error_category, ErrorCategory::PropagatedFailure))
                .collect();
            if !propagated.is_empty() {
                let names: Vec<&str> = propagated
                    .iter()
                    .take(5)
                    .map(|f| f.drv_name.as_str())
                    .collect();
                let suffix = if propagated.len() > 5 {
                    format!(", +{} more", propagated.len() - 5)
                } else {
                    String::new()
                };
                println!(
                    "    propagated ({}): {}{}",
                    propagated.len(),
                    names.join(", "),
                    suffix
                );
            }
        }
        // Slowest drvs — top 3.
        if !self.slowest.is_empty() {
            let top: Vec<String> = self
                .slowest
                .iter()
                .take(3)
                .map(|(n, ms)| format!("{n} ({})", fmt_ms(*ms)))
                .collect();
            println!("  slowest:   {}", top.join(" · "));
        }
        // Trace footer — tells the reader how to reproduce the
        // failure block later.
        println!();
        let trace_id = short_id(&self.job_id);
        match self.external_ref.as_deref() {
            Some(r) => println!("  trace: nix-ci show {trace_id}  (or: nix-ci show {r})"),
            None => println!("  trace: nix-ci show {trace_id}"),
        }
    }

    fn record_slowest(&mut self, name: &str, duration_ms: u64) {
        let pos = self
            .slowest
            .binary_search_by(|(_, d)| d.cmp(&duration_ms).reverse())
            .unwrap_or_else(|p| p);
        self.slowest.insert(pos, (name.to_string(), duration_ms));
        if self.slowest.len() > SLOWEST_KEEP {
            self.slowest.truncate(SLOWEST_KEEP);
        }
    }
}

fn short_id(id: &JobId) -> String {
    let s = id.0.to_string();
    s.chars().take(8).collect()
}

fn fmt_duration(d: Duration) -> String {
    fmt_ms(d.as_millis() as u64)
}

fn fmt_ms(ms: u64) -> String {
    let total_secs = ms / 1000;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    if h > 0 {
        format!("{h}h {m}m {s}s")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else if s > 0 {
        format!("{s}s")
    } else {
        format!("{ms}ms")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DrvHash;

    fn mk_renderer() -> OutputRenderer {
        OutputRenderer::new(JobId::new(), Some("ci-12345".into()), false)
    }

    #[test]
    fn duration_fmt_handles_all_ranges() {
        assert_eq!(fmt_ms(450), "450ms");
        assert_eq!(fmt_ms(7_500), "7s");
        assert_eq!(fmt_ms(125_000), "2m 5s");
        assert_eq!(fmt_ms(3_725_000), "1h 2m 5s");
    }

    #[test]
    fn slowest_keeps_top_n_by_duration() {
        let mut r = mk_renderer();
        for (name, ms) in [
            ("a", 100),
            ("b", 5000),
            ("c", 200),
            ("d", 8000),
            ("e", 3000),
        ] {
            r.record_slowest(name, ms);
        }
        assert_eq!(
            r.slowest
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>(),
            vec!["d", "b", "e", "c", "a"]
        );
    }

    #[test]
    fn observe_records_originating_failure_not_propagated() {
        let mut r = mk_renderer();
        r.observe(&JobEvent::DrvFailed {
            drv_hash: DrvHash::new("a.drv"),
            drv_name: "a".into(),
            error_category: ErrorCategory::BuildFailure,
            error_message: Some("boom".into()),
            log_tail: None,
            attempt: 1,
            will_retry: false,
            used_by_attrs: vec!["x".into()],
        });
        r.observe(&JobEvent::DrvFailed {
            drv_hash: DrvHash::new("b.drv"),
            drv_name: "b".into(),
            error_category: ErrorCategory::PropagatedFailure,
            error_message: Some("dep failed: a.drv".into()),
            log_tail: None,
            attempt: 1,
            will_retry: false,
            used_by_attrs: vec![],
        });
        assert_eq!(r.failures.len(), 1);
        assert_eq!(r.failures[0].drv_name, "a");
    }

    #[test]
    fn observe_skips_will_retry_in_quiet_mode() {
        let mut r = mk_renderer();
        r.observe(&JobEvent::DrvFailed {
            drv_hash: DrvHash::new("a.drv"),
            drv_name: "a".into(),
            error_category: ErrorCategory::Transient,
            error_message: Some("net".into()),
            log_tail: None,
            attempt: 1,
            will_retry: true,
            used_by_attrs: vec![],
        });
        assert!(
            r.failures.is_empty(),
            "transient retries must not be recorded as terminal failures"
        );
    }

    #[test]
    fn observe_progress_threads_state() {
        let mut r = mk_renderer();
        r.observe(&JobEvent::Progress {
            counts: JobCounts {
                total: 100,
                pending: 50,
                building: 4,
                done: 40,
                failed: 6,
            },
            in_flight: vec![DrvInFlight {
                drv_name: "x".into(),
                started_at_ms: 0,
            }],
            propagated_failed: 5,
            transient_retries: 2,
            sealed: true,
        });
        assert_eq!(r.last_counts.done, 40);
        assert_eq!(r.last_in_flight.len(), 1);
        assert!(r.last_sealed);
        assert_eq!(r.propagated_count, 5);
        assert_eq!(r.transient_retries, 2);
    }

    #[test]
    fn jobdone_is_idempotent() {
        let mut r = mk_renderer();
        r.observe(&JobEvent::JobDone {
            status: JobStatus::Done,
            failures: vec![],
        });
        // Second JobDone (e.g. from a stray late event) must be a no-op.
        r.observe(&JobEvent::JobDone {
            status: JobStatus::Failed, // would be wrong status to propagate
            failures: vec![],
        });
        assert!(r.finished);
    }
}
