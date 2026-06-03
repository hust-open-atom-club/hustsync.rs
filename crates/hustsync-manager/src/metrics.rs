use axum::response::IntoResponse;
use axum::http::StatusCode;
use std::sync::Arc;

use crate::server::Manager;

/// Generate Prometheus-format metrics from the current database state.
///
/// Exposed metrics (all prefixed with `hustsync_manager_`):
///
/// - `mirrors_total`              — total number of mirror jobs
/// - `mirrors_by_status`          — gauge vec labelled by `status`
/// - `workers_online`             — number of workers seen within the last 5 min
/// - `sync_duration_seconds`      — histogram of last sync duration (successful jobs)
/// - `sync_failures_total`        — counter of failed sync attempts
/// - `sync_successes_total`       — counter of successful sync attempts
pub async fn metrics_handler(State(manager): State<Arc<Manager>>) -> impl IntoResponse {
    let Some(ref adapter) = manager.adapter else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "# Database adapter not initialized\n",
        )
            .into_response();
    };

    let mut output = String::new();

    // ── hustsync_manager_mirrors_total ──────────────────────────────────────
    let all_statuses = match adapter.list_all_mirror_status() {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("# Error listing mirror statuses: {e}\n"),
            )
                .into_response();
        }
    };

    let total_mirrors = all_statuses.len();
    output.push_str("# HELP hustsync_manager_mirrors_total Total number of mirror jobs.\n");
    output.push_str("# TYPE hustsync_manager_mirrors_total gauge\n");
    output.push_str(&format!("hustsync_manager_mirrors_total {}\n\n", total_mirrors));

    // ── hustsync_manager_mirrors_by_status ──────────────────────────────────
    let mut status_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for s in &all_statuses {
        let label = format!("{:?}", s.status).to_lowercase();
        *status_counts.entry(label).or_insert(0) += 1;
    }

    output.push_str("# HELP hustsync_manager_mirrors_by_status Number of mirrors by sync status.\n");
    output.push_str("# TYPE hustsync_manager_mirrors_by_status gauge\n");
    for (status, count) in &status_counts {
        output.push_str(&format!(
            "hustsync_manager_mirrors_by_status{{status=\"{}\"}} {}\n",
            status, count
        ));
    }
    output.push('\n');

    // ── hustsync_manager_workers_online ─────────────────────────────────────
    let workers = match adapter.list_workers() {
        Ok(w) => w,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("# Error listing workers: {e}\n"),
            )
                .into_response();
        }
    };

    let now = chrono::Utc::now();
    let online_threshold = chrono::Duration::minutes(5);
    let workers_online = workers
        .iter()
        .filter(|w| now - w.last_online < online_threshold)
        .count();

    output.push_str("# HELP hustsync_manager_workers_online Number of workers online within the last 5 minutes.\n");
    output.push_str("# TYPE hustsync_manager_workers_online gauge\n");
    output.push_str(&format!("hustsync_manager_workers_online {}\n\n", workers_online));

    // ── hustsync_manager_sync_duration_seconds ──────────────────────────────
    output.push_str("# HELP hustsync_manager_sync_duration_seconds Duration of the last successful sync in seconds.\n");
    output.push_str("# TYPE hustsync_manager_sync_duration_seconds gauge\n");
    for s in &all_statuses {
        if s.status == hustsync_internal::status::SyncStatus::Success {
            let duration = (s.last_ended - s.last_started).num_seconds().max(0);
            output.push_str(&format!(
                "hustsync_manager_sync_duration_seconds{{mirror=\"{}\"}} {}\n",
                s.name, duration
            ));
        }
    }
    output.push('\n');

    // ── hustsync_manager_sync_failures_total ────────────────────────────────
    let failures = all_statuses
        .iter()
        .filter(|s| s.status == hustsync_internal::status::SyncStatus::Failed)
        .count();

    output.push_str("# HELP hustsync_manager_sync_failures_total Total number of failed syncs.\n");
    output.push_str("# TYPE hustsync_manager_sync_failures_total counter\n");
    output.push_str(&format!("hustsync_manager_sync_failures_total {}\n\n", failures));

    // ── hustsync_manager_sync_successes_total ───────────────────────────────
    let successes = all_statuses
        .iter()
        .filter(|s| s.status == hustsync_internal::status::SyncStatus::Success)
        .count();

    output.push_str("# HELP hustsync_manager_sync_successes_total Total number of successful syncs.\n");
    output.push_str("# TYPE hustsync_manager_sync_successes_total counter\n");
    output.push_str(&format!("hustsync_manager_sync_successes_total {}\n\n", successes));

    (StatusCode::OK, output).into_response()
}

use axum::extract::State;
