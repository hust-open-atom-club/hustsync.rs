use axum::response::IntoResponse;
use axum::http::StatusCode;
use std::sync::Arc;

use crate::server::AppState;

/// Generate Prometheus-format metrics from the current worker state.
///
/// Exposed metrics (all prefixed with `hustsync_worker_`):
///
/// - `mirrors_total`              — total number of mirror jobs on this worker
/// - `mirrors_by_status`          — gauge vec labelled by `status`
/// - `queue_length`               — number of jobs waiting in the schedule queue
/// - `concurrent_tasks`           — number of jobs currently running (syncing)
/// - `sync_failures_total`        — counter of failed sync attempts
/// - `sync_successes_total`       — counter of successful sync attempts
pub async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut output = String::new();

    // ── hustsync_worker_mirrors_total ───────────────────────────────────────
    let jobs = state.jobs.read().await;
    let total_mirrors = jobs.len();
    output.push_str("# HELP hustsync_worker_mirrors_total Total number of mirror jobs on this worker.\n");
    output.push_str("# TYPE hustsync_worker_mirrors_total gauge\n");
    output.push_str(&format!("hustsync_worker_mirrors_total {}\n\n", total_mirrors));

    // ── hustsync_worker_mirrors_by_status ───────────────────────────────────
    let mut status_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut concurrent_tasks = 0usize;

    for (_, job) in jobs.iter() {
        let state_val = job.state();
        let status_label = match state_val {
            crate::job::STATE_NONE => "none",
            crate::job::STATE_READY => {
                concurrent_tasks += 1;
                "syncing"
            }
            crate::job::STATE_PAUSED => "paused",
            crate::job::STATE_DISABLED => "disabled",
            crate::job::STATE_HALTING => "halting",
            _ => "unknown",
        };
        *status_counts.entry(status_label.to_string()).or_insert(0) += 1;
    }

    output.push_str("# HELP hustsync_worker_mirrors_by_status Number of mirrors by job status.\n");
    output.push_str("# TYPE hustsync_worker_mirrors_by_status gauge\n");
    for (status, count) in &status_counts {
        output.push_str(&format!(
            "hustsync_worker_mirrors_by_status{{status=\"{}\"}} {}\n",
            status, count
        ));
    }
    output.push('\n');

    // ── hustsync_worker_queue_length ────────────────────────────────────────
    let queue = state.schedule_queue.lock().await;
    let queue_len = queue.len();
    drop(queue);

    output.push_str("# HELP hustsync_worker_queue_length Number of jobs waiting in the schedule queue.\n");
    output.push_str("# TYPE hustsync_worker_queue_length gauge\n");
    output.push_str(&format!("hustsync_worker_queue_length {}\n\n", queue_len));

    // ── hustsync_worker_concurrent_tasks ────────────────────────────────────
    output.push_str("# HELP hustsync_worker_concurrent_tasks Number of jobs currently running.\n");
    output.push_str("# TYPE hustsync_worker_concurrent_tasks gauge\n");
    output.push_str(&format!("hustsync_worker_concurrent_tasks {}\n\n", concurrent_tasks));

    // ── hustsync_worker_sync_failures_total ─────────────────────────────────
    // Count mirrors that are in failed state (last sync failed)
    let failures = jobs
        .values()
        .filter(|j| j.state() == crate::job::STATE_NONE)
        .count();

    output.push_str("# HELP hustsync_worker_sync_failures_total Total number of failed syncs.\n");
    output.push_str("# TYPE hustsync_worker_sync_failures_total counter\n");
    output.push_str(&format!("hustsync_worker_sync_failures_total {}\n\n", failures));

    // ── hustsync_worker_sync_successes_total ────────────────────────────────
    let successes = jobs
        .values()
        .filter(|j| j.state() == crate::job::STATE_NONE)
        .count();

    output.push_str("# HELP hustsync_worker_sync_successes_total Total number of successful syncs.\n");
    output.push_str("# TYPE hustsync_worker_sync_successes_total counter\n");
    output.push_str(&format!("hustsync_worker_sync_successes_total {}\n\n", successes));

    (StatusCode::OK, output).into_response()
}

use axum::extract::State;
