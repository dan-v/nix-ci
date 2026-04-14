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
    let stream = BroadcastStream::new(rx).map(|res| match res {
        Ok(ev) => Ok::<Event, Infallible>(event_from(&ev)),
        Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(missed)) => {
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
        let mut ticker = tokio::time::interval(tick);
        ticker.tick().await; // skip immediate fire
        loop {
            ticker.tick().await;
            if let Some(sub) = progress_state.dispatcher.submissions.get(progress_id) {
                yield Ok::<Event, Infallible>(event_from(&JobEvent::Progress {
                    counts: sub.live_counts(),
                }));
            } else {
                break;
            }
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
