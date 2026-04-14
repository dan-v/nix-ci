//! Consume the coordinator's SSE event stream and print structured
//! progress updates to stdout. Shutdown via `watch<bool>` so signals
//! are level-triggered (no `Notify::notified().now_or_never()` race).

use std::sync::Arc;
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures::StreamExt;
use tokio::sync::watch;

use crate::client::CoordinatorClient;
use crate::error::Result;
use crate::types::{JobEvent, JobId, JobStatus};

const MAX_RECONNECTS: u32 = 10;

/// Consume the coordinator's SSE stream. Returns when the job reaches
/// a terminal state, or when `shutdown_rx` flips to true.
///
/// On any terminal event observed over SSE, we **signal `shutdown_tx`
/// ourselves** so the rest of the orchestrator (worker, heartbeat,
/// submitter, eval kill guard) winds down promptly. Without this, an
/// external `POST /jobs/{id}/cancel` publishes JobDone{Cancelled}
/// here but the worker keeps long-polling on /claim (returning 410
/// per request) until the next claim cycle, and the submitter keeps
/// posting drvs whose ingest fails with 410 Gone. Propagating
/// shutdown from SSE closes that window.
pub async fn print_events(
    client: Arc<CoordinatorClient>,
    job_id: JobId,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<JobStatus> {
    let mut reconnect_attempt: u32 = 0;

    loop {
        if *shutdown.borrow() {
            break;
        }
        let url = client.events_url(job_id);
        let send_fut = reqwest::Client::new()
            .get(&url)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .send();
        let resp = tokio::select! {
            r = send_fut => r,
            _ = shutdown.changed() => break,
        };
        let stream = match resp {
            Ok(r) if r.status().is_success() => r.bytes_stream().eventsource(),
            Ok(r) => {
                tracing::warn!(status = %r.status(), "SSE connect: bad status");
                break;
            }
            Err(e) => {
                tracing::warn!(error = %e, "SSE connect error");
                reconnect_attempt += 1;
                if reconnect_attempt > MAX_RECONNECTS {
                    break;
                }
                tokio::select! {
                    _ = tokio::time::sleep(
                        Duration::from_secs(reconnect_attempt as u64)
                    ) => {},
                    _ = shutdown.changed() => break,
                }
                continue;
            }
        };
        reconnect_attempt = 0;
        tokio::pin!(stream);
        loop {
            tokio::select! {
                evt = stream.next() => {
                    let Some(evt) = evt else { break; };
                    match evt {
                        Ok(e) => {
                            if let Ok(parsed) = serde_json::from_str::<JobEvent>(&e.data) {
                                print_event(&parsed);
                                if let JobEvent::JobDone { status, .. } = parsed {
                                    let _ = shutdown_tx.send(true);
                                    return Ok(status);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "SSE stream error");
                            break;
                        }
                    }
                }
                _ = shutdown.changed() => return Ok(JobStatus::Pending),
            }
        }
    }
    Ok(JobStatus::Pending)
}

fn print_event(ev: &JobEvent) {
    match ev {
        JobEvent::DrvStarted {
            drv_name, attempt, ..
        } => tracing::info!(drv = %drv_name, attempt, "started"),
        JobEvent::DrvCompleted { drv_name, .. } => {
            tracing::info!(drv = %drv_name, "completed");
        }
        JobEvent::DrvFailed {
            drv_name,
            error_category,
            error_message,
            attempt,
            will_retry,
            ..
        } => {
            tracing::warn!(
                drv = %drv_name,
                cat = ?error_category,
                msg = ?error_message,
                attempt, will_retry,
                "failed"
            );
        }
        JobEvent::Progress { counts } => {
            tracing::info!(
                total = counts.total,
                pending = counts.pending,
                building = counts.building,
                done = counts.done,
                failed = counts.failed,
                "progress"
            );
        }
        JobEvent::JobDone { status, failures } => {
            tracing::info!(status = ?status, failed_drvs = failures.len(), "job done");
        }
        JobEvent::Lagged { missed } => {
            tracing::warn!(
                missed,
                "SSE consumer fell behind — some events were dropped; poll /jobs/{{id}} to re-sync"
            );
        }
    }
}
