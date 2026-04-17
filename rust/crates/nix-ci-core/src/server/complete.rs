//! `POST /jobs/{id}/claims/{claim_id}/complete`
//!
//! Pure in-memory state transitions on the drv graph. The only durable
//! writes are:
//!   * `failed_outputs` insert when a drv terminal-fails (TTL cache so
//!     concurrent / subsequent jobs can short-circuit a known-bad drv).
//!   * `jobs.status/done_at/result` when the submission itself reaches
//!     a terminal state.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;

use super::AppState;
use crate::dispatch::rdep::make_rdeps_runnable;
use crate::dispatch::Submission;
use crate::durable::writeback;
use crate::error::{Error, Result};
use crate::observability::metrics::{OutcomeLabels, TerminalLabels};
use crate::types::{
    ClaimId, CompleteRequest, CompleteResponse, DrvFailure, DrvHash, ErrorCategory, JobEvent,
    JobId, JobStatus, JobStatusResponse, MAX_LOG_TAIL_BYTES,
};

#[tracing::instrument(skip_all, fields(
    job_id = %job_id,
    claim_id = %claim_id,
    success = req.success,
    duration_ms = req.duration_ms,
))]
pub async fn complete(
    State(state): State<AppState>,
    Path((job_id, claim_id)): Path<(JobId, ClaimId)>,
    Json(mut req): Json<CompleteRequest>,
) -> Result<Json<CompleteResponse>> {
    let Some(claim) = state.dispatcher.claims.take(claim_id) else {
        return Ok(Json(CompleteResponse { ignored: true }));
    };
    state.metrics.inner.claims_in_flight.dec();
    // H3: claim-age histogram. `started_at` is an Instant, so this is
    // monotonic and safe to subtract.
    let claim_age = claim
        .started_at
        .elapsed()
        .as_secs_f64();
    state
        .metrics
        .inner
        .claim_age_seconds
        .observe(claim_age);
    // Mirror active_claims decrement on the submission so the fleet
    // scheduler's per-job cap accounts for the finished worker.
    // Saturating subtract guards against a prior drift (e.g., a
    // reaper + complete race that both decrement).
    if let Some(sub_for_counter) = state.dispatcher.submissions.get(claim.job_id) {
        let prev = sub_for_counter
            .active_claims
            .load(std::sync::atomic::Ordering::Acquire);
        if prev > 0 {
            sub_for_counter
                .active_claims
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
    }

    if claim.job_id != job_id {
        return Err(Error::BadRequest(
            "claim_id doesn't belong to this job".into(),
        ));
    }

    let Some(step) = state.dispatcher.steps.get(&claim.drv_hash) else {
        return Ok(Json(CompleteResponse { ignored: true }));
    };

    // Guard against the failure-propagation race: if propagation already
    // marked this step finished, skip processing so we don't double-
    // count metrics or re-fire events.
    if step.finished.load(Ordering::Acquire) {
        return Ok(Json(CompleteResponse { ignored: true }));
    }

    truncate_log(&mut req.log_tail);

    state
        .metrics
        .inner
        .build_duration
        .observe((req.duration_ms as f64) / 1000.0);

    if req.success {
        handle_success(&state, &step, req.duration_ms).await?;
    } else {
        handle_failure(&state, &claim, &step, req).await?;
    }
    Ok(Json(CompleteResponse { ignored: false }))
}

fn truncate_log(log: &mut Option<String>) {
    let Some(s) = log else { return };
    if s.len() <= MAX_LOG_TAIL_BYTES {
        return;
    }
    // Cap is a strict upper bound, so when the target byte sits inside
    // a multi-byte UTF-8 char we scan *forward* to the next boundary —
    // keeping slightly fewer bytes (≤3) rather than overshooting MAX.
    // `s.len()` is always a char boundary so the range always yields.
    let target = s.len() - MAX_LOG_TAIL_BYTES;
    let cut = (target..=s.len())
        .find(|&c| s.is_char_boundary(c))
        .expect("s.len() is always a char boundary");
    *s = s[cut..].to_string();
}

async fn handle_success(
    state: &AppState,
    step: &Arc<crate::dispatch::Step>,
    duration_ms: u64,
) -> Result<()> {
    step.finished.store(true, Ordering::Release);

    state
        .metrics
        .inner
        .builds_completed
        .get_or_create(&OutcomeLabels {
            outcome: "success".into(),
        })
        .inc();

    let subs = collect_submissions(step);
    for sub in &subs {
        sub.publish(JobEvent::DrvCompleted {
            drv_hash: step.drv_hash().clone(),
            drv_name: step.drv_name().to_string(),
            duration_ms,
        });
    }

    make_rdeps_runnable(step);

    for sub in &subs {
        check_and_publish_terminal(state, sub).await?;
    }
    state.dispatcher.wake();
    Ok(())
}

async fn handle_failure(
    state: &AppState,
    claim: &crate::dispatch::claim::ActiveClaim,
    step: &Arc<crate::dispatch::Step>,
    req: CompleteRequest,
) -> Result<()> {
    let category = req.error_category.unwrap_or(ErrorCategory::BuildFailure);
    let attempt = claim.attempt;
    let can_retry = category.is_retryable() && attempt < step.max_tries();

    if can_retry {
        return handle_flaky_retry(state, step, req, category, attempt).await;
    }

    step.previous_failure.store(true, Ordering::Release);
    step.finished.store(true, Ordering::Release);

    // Cache the output path only for deterministic, reproducible
    // failures — i.e. BuildFailure. Transient / DiskFull exhaustions
    // are builder-environment problems (network blip, worker's disk
    // was full, worker OOMed) and the drv is still potentially
    // buildable on a different worker; caching them would poison the
    // TTL cache and falsely short-circuit future ingests.
    // PropagatedFailure is likewise NOT cached: the drv itself never
    // tried to build, and if its dep is still actually bad it will be
    // caught by the dep's own failed_outputs entry.
    if matches!(category, ErrorCategory::BuildFailure) {
        // Every drv_path accepted by ingest ends in `.drv` and has at
        // least one char before the suffix (drv_hash_from_path
        // validates), so the stripped result is guaranteed non-empty
        // and distinct from the input.
        let output_path = step.drv_path().trim_end_matches(".drv").to_string();
        if let Err(e) = writeback::insert_failed_outputs(
            &state.pool,
            step.drv_hash(),
            &[output_path],
            state.cfg.failed_outputs_ttl_secs,
        )
        .await
        {
            tracing::warn!(error = %e, "failed_outputs insert failed; continuing");
        }
    }

    state
        .metrics
        .inner
        .builds_completed
        .get_or_create(&OutcomeLabels {
            outcome: "failure".into(),
        })
        .inc();

    let subs = collect_submissions(step);

    // Record the originating failure on every submission that owns this
    // step. The DrvFailure is what the terminal `jobs.result` snapshot
    // and `GET /jobs/{id}` will return for this drv.
    let failure = DrvFailure {
        drv_hash: step.drv_hash().clone(),
        drv_name: step.drv_name().to_string(),
        error_category: category,
        error_message: req.error_message.clone(),
        log_tail: req.log_tail.clone(),
        propagated_from: None,
    };
    for sub in &subs {
        sub.record_failure(failure.clone());
    }
    publish_drv_failed(&subs, step, category, &req, attempt, false);

    let propagated_count = propagate_failure_inmem(step, step.drv_hash());
    // `inc_by(0)` is a no-op, so no threshold guard needed.
    state
        .metrics
        .inner
        .propagated_failures
        .inc_by(propagated_count);

    for sub in subs {
        check_and_publish_terminal(state, &sub).await?;
    }
    state.dispatcher.wake();
    Ok(())
}

async fn handle_flaky_retry(
    state: &AppState,
    step: &Arc<crate::dispatch::Step>,
    req: CompleteRequest,
    category: ErrorCategory,
    attempt: i32,
) -> Result<()> {
    let backoff_ms = state.cfg.flaky_retry_backoff_step_ms * i64::from(attempt);
    step.next_attempt_at.store(
        chrono::Utc::now().timestamp_millis() + backoff_ms,
        Ordering::Release,
    );
    if step
        .runnable
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        crate::dispatch::rdep::enqueue_for_all_submissions(step);
    }
    let subs = collect_submissions(step);
    // Track per-submission flakiness counts so the runner can surface
    // "X transient retries" in progress + summary.
    for sub in &subs {
        sub.transient_retries.fetch_add(1, Ordering::Relaxed);
    }
    publish_drv_failed(&subs, step, category, &req, attempt, true);
    state
        .metrics
        .inner
        .builds_completed
        .get_or_create(&OutcomeLabels {
            outcome: "flaky_retry".into(),
        })
        .inc();
    state.dispatcher.wake();
    Ok(())
}

fn collect_submissions(step: &Arc<crate::dispatch::Step>) -> Vec<Arc<Submission>> {
    let st = step.state.read();
    st.submissions.iter().filter_map(|w| w.upgrade()).collect()
}

fn publish_drv_failed(
    subs: &[Arc<Submission>],
    step: &Arc<crate::dispatch::Step>,
    category: ErrorCategory,
    req: &CompleteRequest,
    attempt: i32,
    will_retry: bool,
) {
    // Per-submission attribution: walk this step's rdep closure (within
    // each submission's own membership) to find the toplevels affected,
    // then resolve their attr names from `Submission::root_attrs`. This
    // lets the runner say "FAILED gcc-13.2.0, used by:
    // packages.x86_64-linux.hello".
    for sub in subs {
        let used_by_attrs = compute_used_by_attrs(sub, step);
        sub.publish(JobEvent::DrvFailed {
            drv_hash: step.drv_hash().clone(),
            drv_name: step.drv_name().to_string(),
            error_category: category,
            error_message: req.error_message.clone(),
            log_tail: req.log_tail.clone(),
            attempt,
            will_retry,
            used_by_attrs,
        });
    }
}

/// For a failed step `s` in submission `sub`, find every toplevel of
/// `sub` that transitively depends on `s`, and look up its attr name
/// in `sub.root_attrs`. BFS over rdep weak refs scoped to this
/// submission's members so we don't bleed across jobs.
fn compute_used_by_attrs(sub: &Arc<Submission>, s: &Arc<crate::dispatch::Step>) -> Vec<String> {
    use std::collections::{HashSet, VecDeque};
    let toplevel_hashes: HashSet<crate::types::DrvHash> = sub
        .toplevels
        .read()
        .iter()
        .map(|t| t.drv_hash().clone())
        .collect();
    let members: std::collections::HashSet<crate::types::DrvHash> =
        sub.members.read().keys().cloned().collect();
    let root_attrs = sub.root_attrs.read();

    let mut visited: HashSet<crate::types::DrvHash> = HashSet::new();
    let mut frontier: VecDeque<Arc<crate::dispatch::Step>> = VecDeque::new();
    frontier.push_back(s.clone());
    let mut hits: Vec<String> = Vec::new();
    while let Some(curr) = frontier.pop_front() {
        if !visited.insert(curr.drv_hash().clone()) {
            continue;
        }
        // Drop rdeps that aren't members of THIS submission — they
        // belong to a different job's closure.
        if !members.contains(curr.drv_hash()) {
            continue;
        }
        if toplevel_hashes.contains(curr.drv_hash()) {
            // Prefer the explicit attr name; fall back to drv_name so
            // the message is still useful for older clients.
            let label = root_attrs
                .get(curr.drv_hash())
                .cloned()
                .unwrap_or_else(|| curr.drv_name().to_string());
            if !hits.contains(&label) {
                hits.push(label);
            }
        }
        for w in &curr.state.read().rdeps {
            if let Some(r) = w.upgrade() {
                frontier.push_back(r);
            }
        }
    }
    hits.sort();
    hits
}

/// Cap the terminal `failures` snapshot to keep the `jobs.result`
/// JSONB row bounded. If we truncate, we drop the tail and record a
/// synthetic marker so callers see that they're looking at a partial
/// list. SSE subscribers still get the full list (delivered in-memory
/// via `JobDone`); only the durable snapshot is capped.
pub(crate) fn cap_failures(mut all: Vec<DrvFailure>, cap: usize) -> Vec<DrvFailure> {
    if all.len() <= cap {
        return all;
    }
    let overflow = all.len() - cap;
    all.truncate(cap);
    all.push(DrvFailure {
        drv_hash: crate::types::DrvHash::new("<truncated>".to_string()),
        drv_name: "<truncated>".to_string(),
        error_category: ErrorCategory::PropagatedFailure,
        error_message: Some(format!(
            "{overflow} additional failures truncated from snapshot"
        )),
        log_tail: None,
        propagated_from: None,
    });
    all
}

/// Flip every transitive rdep of `root` to terminal-failed, recording a
/// propagated `DrvFailure` on every submission that owns the rdep.
/// Returns the count of drvs newly failed so callers can bump metrics.
///
/// Failure records are attached only to submissions that actually
/// reference each propagated drv — not to the origin submissions just
/// because they owned the root. Without that restriction a dedup-shared
/// originating failure would pollute every joined submission's failures
/// list with drvs those submissions never submitted.
fn propagate_failure_inmem(root: &Arc<crate::dispatch::Step>, origin: &DrvHash) -> u64 {
    use std::collections::VecDeque;
    let mut frontier: VecDeque<Arc<crate::dispatch::Step>> = VecDeque::new();
    {
        let st = root.state.read();
        for w in &st.rdeps {
            if let Some(r) = w.upgrade() {
                frontier.push_back(r);
            }
        }
    }
    let mut count = 0u64;
    while let Some(step) = frontier.pop_front() {
        if step.finished.load(Ordering::Acquire) {
            continue;
        }
        step.previous_failure.store(true, Ordering::Release);
        step.finished.store(true, Ordering::Release);
        count += 1;

        for sub in collect_submissions(&step) {
            sub.record_failure(DrvFailure {
                drv_hash: step.drv_hash().clone(),
                drv_name: step.drv_name().to_string(),
                error_category: ErrorCategory::PropagatedFailure,
                error_message: Some(format!("dep failed: {origin}")),
                log_tail: None,
                propagated_from: Some(origin.clone()),
            });
            // Per-submission count for the runner's progress display.
            sub.propagated_failed.fetch_add(1, Ordering::Relaxed);
        }

        let next: Vec<Arc<crate::dispatch::Step>> = {
            let st = step.state.read();
            st.rdeps.iter().filter_map(|w| w.upgrade()).collect()
        };
        frontier.extend(next);
    }
    count
}

/// Transition the submission to terminal if its toplevels are all
/// finished and it's sealed. Writes the final `JobStatusResponse`
/// snapshot to `jobs.result` and publishes `JobDone`.
pub(super) async fn check_and_publish_terminal(
    state: &AppState,
    sub: &Arc<Submission>,
) -> Result<()> {
    if !sub.is_sealed() {
        return Ok(());
    }
    if sub.terminal.load(Ordering::Acquire) {
        return Ok(());
    }
    let tops = sub.toplevels.read().clone();
    let all_finished = tops.iter().all(|t| t.finished.load(Ordering::Acquire));
    if !all_finished {
        return Ok(());
    }
    let any_failed = tops
        .iter()
        .any(|t| t.previous_failure.load(Ordering::Acquire));
    let final_status = if any_failed {
        JobStatus::Failed
    } else {
        JobStatus::Done
    };

    let counts = sub.live_counts();
    let failures_full = sub.failures.read().clone();
    let failures = cap_failures(failures_full.clone(), state.cfg.max_failures_in_result);
    let snapshot = JobStatusResponse {
        id: sub.id,
        status: final_status,
        sealed: true,
        counts,
        failures: failures.clone(),
        eval_error: None,
    };
    let snapshot_json = serde_json::to_value(&snapshot)
        .map_err(|e| Error::Internal(format!("serialize terminal result: {e}")))?;
    let _ = writeback::transition_job_terminal(
        &state.pool,
        sub.id,
        final_status.as_str(),
        &snapshot_json,
    )
    .await?;

    if !sub.mark_terminal() {
        return Ok(());
    }

    sub.publish(JobEvent::JobDone {
        status: final_status,
        failures: failures_full,
    });
    state
        .metrics
        .inner
        .jobs_terminal
        .get_or_create(&TerminalLabels {
            status: final_status.as_str().into(),
        })
        .inc();
    // Per-job size for capacity planning. Recorded at terminal time
    // (rather than on every ingest) so we get one observation per job
    // — the histogram answers "what's the p99 job size?", not "what's
    // the average ingest velocity?". A hard cap can be added later if
    // and only if the data shows it's needed.
    state
        .metrics
        .inner
        .drvs_per_job
        .observe(sub.members.read().len() as f64);

    // Drop the in-memory submission. Existing SSE subscribers keep
    // their receiver until the broadcast Sender is dropped with the
    // last Arc<Submission>. New subscribers get 404 and should poll
    // `/jobs/{id}` for the terminal result instead.
    //
    // Also evict any in-flight claims tied to this job. In the common
    // graceful path no claims should be outstanding (all toplevels
    // were finished, which means their claims already completed),
    // but in production we have observed orphan claims survive past
    // termination — defensively cleanup so the gauge stays honest.
    state.dispatcher.submissions.remove(sub.id);
    state.dispatcher.evict_claims_for(sub.id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_log_none_unchanged() {
        let mut log = None;
        truncate_log(&mut log);
        assert!(log.is_none());
    }

    #[test]
    fn truncate_log_under_cap_unchanged() {
        let mut log = Some("small payload".to_string());
        truncate_log(&mut log);
        assert_eq!(log.as_deref(), Some("small payload"));
    }

    #[test]
    fn truncate_log_exactly_cap_unchanged() {
        let s = "a".repeat(MAX_LOG_TAIL_BYTES);
        let mut log = Some(s.clone());
        truncate_log(&mut log);
        assert_eq!(log.as_deref(), Some(s.as_str()));
    }

    #[test]
    fn truncate_log_ascii_keeps_exact_tail() {
        // Pure ASCII: cut lands on a char boundary, keep exactly MAX bytes.
        let mut s = "head".to_string();
        s.push_str(&"x".repeat(MAX_LOG_TAIL_BYTES));
        let mut log = Some(s);
        truncate_log(&mut log);
        let out = log.unwrap();
        assert_eq!(out.len(), MAX_LOG_TAIL_BYTES);
        assert!(out.chars().all(|c| c == 'x'));
    }

    #[test]
    fn truncate_log_never_exceeds_cap_on_multibyte_boundary() {
        // Construct a string where `target = s.len() - MAX` lands
        // INSIDE a multi-byte char (not on a boundary). This requires
        // care — with pure 4-byte chars and MAX=65536 the target
        // position happens to always hit a boundary and the forward-
        // scan loop never runs, so a bad mutation (cut -=, cut *=)
        // would go undetected.
        //
        // Recipe: one 3-byte char (€) at the very front, then padding
        // of 1-byte chars. With `s.len() = MAX + 2`, `target = 2` lands
        // in the middle of '€' (bytes 1..=3) — guaranteed non-boundary.
        let mut s = String::from("€");
        assert_eq!(s.len(), 3);
        while s.len() < MAX_LOG_TAIL_BYTES + 2 {
            s.push('x');
        }
        // Sanity: target really is non-boundary.
        let target = s.len() - MAX_LOG_TAIL_BYTES;
        assert_eq!(target, 2);
        assert!(!s.is_char_boundary(target));
        let original_len = s.len();

        let mut log = Some(s);
        truncate_log(&mut log);
        let out = log.unwrap();
        assert!(
            out.len() <= MAX_LOG_TAIL_BYTES,
            "truncated len {} exceeds MAX {}",
            out.len(),
            MAX_LOG_TAIL_BYTES
        );
        // At most 3 bytes lost vs. strict tail (UTF-8 char width ≤ 4).
        let strict_tail = MAX_LOG_TAIL_BYTES.min(original_len);
        assert!(strict_tail - out.len() < 4);
        // The '€' prefix must be gone entirely; the tail is pure x's.
        assert!(
            out.chars().all(|c| c == 'x'),
            "tail should be all 'x', got {out:?}"
        );
    }

    #[test]
    fn truncate_log_is_tail_preserving() {
        // Tail content (the last bytes of the original) is preserved.
        let mut s = "HEAD_MARK".to_string();
        s.push_str(&"a".repeat(MAX_LOG_TAIL_BYTES));
        s.push_str("TAIL_MARK");
        let mut log = Some(s);
        truncate_log(&mut log);
        let out = log.unwrap();
        assert!(
            out.ends_with("TAIL_MARK"),
            "tail marker lost; got suffix {:?}",
            &out[out.len().saturating_sub(20)..]
        );
        assert!(!out.contains("HEAD_MARK"));
    }

    #[test]
    fn truncate_log_one_byte_over_cap() {
        // Edge case: MAX+1 ASCII bytes — cut at byte 1, keep MAX bytes.
        let mut s = String::with_capacity(MAX_LOG_TAIL_BYTES + 1);
        s.push('H');
        s.push_str(&"z".repeat(MAX_LOG_TAIL_BYTES));
        let mut log = Some(s);
        truncate_log(&mut log);
        let out = log.unwrap();
        assert_eq!(out.len(), MAX_LOG_TAIL_BYTES);
        assert!(out.chars().all(|c| c == 'z'));
    }
}
