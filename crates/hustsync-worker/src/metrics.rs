//! Prometheus gauges for the worker's jobs, schedule queue and concurrency.

use std::sync::Arc;

use axum::{
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use prometheus::{IntGauge, Registry, TEXT_FORMAT, TextEncoder};

use crate::server::AppState;

/// `GET /metrics` in the Prometheus text format.
pub async fn serve(State(s): State<Arc<AppState>>) -> Response {
    let jobs = s.jobs.read().await.len();
    let queued = s.schedule_queue.lock().await.len();
    let running = s.concurrent.saturating_sub(s.semaphore.available_permits());
    let gauges = [
        ("hustsync_worker_jobs", "Configured mirror jobs.", jobs),
        (
            "hustsync_worker_jobs_running",
            "Jobs holding a concurrency slot.",
            running,
        ),
        (
            "hustsync_worker_jobs_queued",
            "Jobs scheduled for a later run.",
            queued,
        ),
        (
            "hustsync_worker_concurrency",
            "Maximum concurrent jobs.",
            s.concurrent,
        ),
    ];
    match encode(gauges) {
        Ok(body) => ([(header::CONTENT_TYPE, TEXT_FORMAT)], body).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn encode(gauges: [(&str, &str, usize); 4]) -> prometheus::Result<String> {
    let reg = Registry::new();
    for (name, help, v) in gauges {
        let g = IntGauge::new(name, help)?;
        g.set(i64::try_from(v).unwrap_or(i64::MAX));
        reg.register(Box::new(g))?;
    }
    TextEncoder::new().encode_to_string(&reg.gather())
}
