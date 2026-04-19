//! `GET /jobs/{id}/events` — SSE event stream.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;

use super::AppState;
use crate::error::{Error, Result};
use crate::types::{JobEvent, JobId};

pub async fn events(State(state): State<AppState>, Path(id): Path<JobId>) -> Result<Response> {
    let Some(sub) = state.dispatcher.submissions.get(id) else {
        return Err(Error::NotFound(format!("job {id}")));
    };

    let rx = sub.subscribe();
    let metrics = state.metrics.clone();
    let stream = BroadcastStream::new(rx).map(move |res| match res {
        Ok(ev) => Ok::<Event, Infallible>(event_from(&ev)),
        Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(missed)) => {
            metrics.inner.events_dropped.inc_by(missed);
            Ok(event_from(&JobEvent::Lagged { missed }))
        }
    });

    // Also emit a periodic Progress event every 10s via a merged
    // stream. Built from dispatch snapshot for the submission.
    let progress_state = state.clone();
    let progress_id = id;
    let tick = Duration::from_secs(state.cfg.progress_tick_secs);
    let keepalive = Duration::from_secs(state.cfg.sse_keepalive_secs);
    let progress = async_stream::stream! {
        use std::sync::atomic::Ordering;
        let mut ticker = tokio::time::interval(tick);
        ticker.tick().await; // skip immediate fire
        loop {
            ticker.tick().await;
            let Some(sub) = progress_state.dispatcher.submissions.get(progress_id) else {
                break;
            };
            // Build the in-flight list from this job's claims. Each
            // claim has a started_at; we report wall-clock millis so
            // the runner can compute "elapsed = now_ms - started_at_ms".
            // `by_job` is O(claims for this job); `all().filter(..)` was
            // O(total claims) and at 10K+ in-flight across many jobs
            // this tick was hot.
            let now_wall = chrono::Utc::now().timestamp_millis();
            let now_mono = tokio::time::Instant::now();
            let mut in_flight: Vec<crate::types::DrvInFlight> = progress_state
                .dispatcher
                .claims
                .by_job(progress_id)
                .into_iter()
                .map(|c| {
                    let elapsed_ms = now_mono.saturating_duration_since(c.started_at).as_millis() as i64;
                    let drv_name = progress_state
                        .dispatcher
                        .steps
                        .get(&c.drv_hash)
                        .map(|s| s.drv_name().to_string())
                        .unwrap_or_else(|| c.drv_hash.as_str().to_string());
                    crate::types::DrvInFlight {
                        drv_name,
                        started_at_ms: now_wall - elapsed_ms,
                    }
                })
                .collect();
            // Stable sort: longest-running first — those are the
            // interesting ones for "what's blocking the run".
            in_flight.sort_by_key(|d| d.started_at_ms);
            yield Ok::<Event, Infallible>(event_from(&JobEvent::Progress {
                counts: sub.live_counts(),
                in_flight,
                propagated_failed: sub.propagated_failed.load(Ordering::Acquire),
                transient_retries: sub.transient_retries.load(Ordering::Acquire),
                sealed: sub.is_sealed(),
            }));
        }
    };

    let merged = stream.merge(progress);

    Ok(Sse::new(merged)
        .keep_alive(KeepAlive::new().interval(keepalive))
        .into_response())
}

fn event_from(ev: &JobEvent) -> Event {
    let payload = serde_json::to_string(ev).unwrap_or_else(|_| "{}".into());
    let kind = match ev {
        JobEvent::DrvStarted { .. } => "drv_started",
        JobEvent::DrvCompleted { .. } => "drv_completed",
        JobEvent::DrvFailed { .. } => "drv_failed",
        JobEvent::Progress { .. } => "progress",
        JobEvent::JobDone { .. } => "job_done",
        JobEvent::Lagged { .. } => "lagged",
    };
    Event::default().event(kind).data(payload)
}
