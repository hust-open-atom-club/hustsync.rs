use axum::extract::{FromRequestParts, Path, State};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::{Json, response::IntoResponse};
use chrono::Utc;
use hustsync_internal::msg::{
    ClientCmd, CmdVerb, MirrorSchedules, MirrorStatus, WorkerCmd, WorkerStatus,
};
use hustsync_internal::status::SyncStatus;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::database::DbAdapterTrait;
use crate::server::{ERROR_KEY, INFO_KEY, Manager};

// Custom Extractor for the database adapter
pub struct Database(pub Arc<dyn DbAdapterTrait>);

impl FromRequestParts<Arc<Manager>> for Database {
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &Arc<Manager>,
    ) -> Result<Self, Self::Rejection> {
        let adapter = state.adapter.as_ref().ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ ERROR_KEY: "Database adapter not initialized" })),
            )
        })?;

        Ok(Database(Arc::clone(adapter)))
    }
}

pub async fn ping_handler() -> impl IntoResponse {
    let body = json!({
        INFO_KEY: "pong"
    });

    (StatusCode::OK, Json(body))
}

pub async fn register_worker(
    Database(adapter): Database,
    Json(mut worker): Json<WorkerStatus>,
) -> impl IntoResponse {
    worker.last_online = Utc::now();
    worker.last_register = Utc::now();

    match adapter.create_worker(worker) {
        Ok(new_worker) => (StatusCode::OK, Json(json!(new_worker))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ ERROR_KEY: format!("Failed to register worker: {}", e) })),
        ),
    }
}

pub async fn list_all_workers(Database(adapter): Database) -> impl IntoResponse {
    match adapter.list_workers() {
        Ok(mut workers) => {
            for w in &mut workers {
                w.token = "REDACTED".to_string();
            }
            (StatusCode::OK, Json(json!(workers)))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ ERROR_KEY: format!("Failed to list workers: {}", e) })),
        ),
    }
}

pub async fn list_all_jobs(Database(adapter): Database) -> impl IntoResponse {
    match adapter.list_all_mirror_status() {
        Ok(statuses) => {
            let web_statuses: Vec<hustsync_internal::status_web::WebMirrorStatus> =
                statuses.into_iter().map(|s| s.into()).collect();
            (StatusCode::OK, Json(json!(web_statuses)))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ ERROR_KEY: format!("Failed to list jobs: {}", e) })),
        ),
    }
}

pub async fn list_jobs_of_worker(
    Database(adapter): Database,
    Path(worker_id): Path<String>,
) -> impl IntoResponse {
    // Go `listJobsOfWorker` returns raw `[]MirrorStatus`, NOT WebMirrorStatus.
    // Workers bootstrap their schedule from this endpoint and rely on the
    // RFC3339 timestamps in MirrorStatus rather than the dual text+ts pair
    // that WebMirrorStatus adds for the dashboard.
    match adapter.list_mirror_status(&worker_id) {
        Ok(statuses) => (StatusCode::OK, Json(json!(statuses))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(
                json!({ ERROR_KEY: format!("Failed to list jobs of worker {}: {}", worker_id, e) }),
            ),
        ),
    }
}

pub async fn delete_worker(
    Database(adapter): Database,
    Path(worker_id): Path<String>,
) -> impl IntoResponse {
    match adapter.delete_worker(&worker_id) {
        Ok(_) => {
            tracing::info!("Worker <{}> deleted", worker_id);
            (StatusCode::OK, Json(json!({ INFO_KEY: "deleted" })))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ ERROR_KEY: format!("Failed to delete worker: {}", e) })),
        ),
    }
}

pub async fn flush_disabled_jobs(Database(adapter): Database) -> impl IntoResponse {
    match adapter.flush_disabled_jobs() {
        Ok(_) => (StatusCode::OK, Json(json!({ INFO_KEY: "flushed" }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ ERROR_KEY: format!("Failed to flush disabled jobs: {}", e) })),
        ),
    }
}

pub async fn update_job_of_worker(
    Database(adapter): Database,
    Path((worker_id, mirror_id)): Path<(String, String)>,
    Json(mut status): Json<MirrorStatus>,
) -> impl IntoResponse {
    if status.name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ ERROR_KEY: "mirror Name should not be empty" })),
        );
    }

    let _ = adapter.refresh_worker(&worker_id);
    let cur_status = adapter.get_mirror_status(&worker_id, &mirror_id).ok();
    let now = Utc::now();

    if status.status == SyncStatus::PreSyncing {
        if let Some(ref cur) = cur_status {
            if cur.status != SyncStatus::PreSyncing {
                status.last_started = now;
            } else {
                status.last_started = cur.last_started;
            }
        } else {
            status.last_started = now;
        }
    } else if let Some(ref cur) = cur_status {
        status.last_started = cur.last_started;
    }

    if status.status == SyncStatus::Success {
        status.last_update = now;
    } else if let Some(ref cur) = cur_status {
        status.last_update = cur.last_update;
    }

    if status.status == SyncStatus::Success || status.status == SyncStatus::Failed {
        status.last_ended = now;
    } else if let Some(ref cur) = cur_status {
        status.last_ended = cur.last_ended;
    }

    // Retain valid previous size if the new one is "unknown" or empty
    if let Some(ref cur) = cur_status
        && !cur.size.is_empty()
        && cur.size != "unknown"
        && (status.size.is_empty() || status.size == "unknown")
    {
        status.size = cur.size.clone();
    }

    match adapter.update_mirror_status(&worker_id, &mirror_id, status) {
        Ok(new_status) => (StatusCode::OK, Json(json!(new_status))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ ERROR_KEY: format!("Failed to update job: {}", e) })),
        ),
    }
}

#[derive(Deserialize)]
pub struct SizeMsg {
    pub size: String,
}

pub async fn update_mirror_size(
    Database(adapter): Database,
    Path((worker_id, mirror_id)): Path<(String, String)>,
    Json(msg): Json<SizeMsg>,
) -> impl IntoResponse {
    let _ = adapter.refresh_worker(&worker_id);

    match adapter.get_mirror_status(&worker_id, &mirror_id) {
        Ok(mut status) => {
            if !msg.size.is_empty() && msg.size != "unknown" {
                status.size = msg.size;
                match adapter.update_mirror_status(&worker_id, &mirror_id, status) {
                    Ok(new_status) => (StatusCode::OK, Json(json!(new_status))),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ ERROR_KEY: format!("Failed to save size: {}", e) })),
                    ),
                }
            } else {
                (StatusCode::OK, Json(json!(status)))
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ ERROR_KEY: format!("Mirror not found: {}", e) })),
        ),
    }
}

pub async fn update_schedules_of_worker(
    Database(adapter): Database,
    Path(worker_id): Path<String>,
    Json(schedules): Json<MirrorSchedules>,
) -> impl IntoResponse {
    let _ = adapter.refresh_worker(&worker_id);

    for s in schedules.schedules {
        if s.name.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ ERROR_KEY: "mirror Name should not be empty" })),
            );
        }

        match adapter.get_mirror_status(&worker_id, &s.name) {
            Ok(mut status) => {
                if status.next_scheduled == s.next_schedule {
                    continue;
                }
                status.next_scheduled = s.next_schedule;
                if let Err(e) = adapter.update_mirror_status(&worker_id, &s.name, status) {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(
                            json!({ ERROR_KEY: format!("failed to update job {} of worker {}: {}", s.name, worker_id, e) }),
                        ),
                    );
                }
            }
            Err(e) => {
                tracing::error!(
                    "failed to get job {} of worker {}: {}",
                    s.name,
                    worker_id,
                    e
                );
            }
        }
    }

    (StatusCode::OK, Json(json!({})))
}

pub async fn handle_cmd(
    State(manager): State<Arc<Manager>>,
    Database(adapter): Database,
    Json(client_cmd): Json<ClientCmd>,
) -> axum::response::Response {
    let worker_id = &client_cmd.worker_id;

    // Empty worker_id: Go aborts with 500 and no body (multi-worker routing
    // is a known TODO in Go too).
    if worker_id.is_empty() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let worker = match adapter.get_worker(worker_id) {
        Ok(w) => w,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    ERROR_KEY: format!("worker {} is not registered yet", worker_id)
                })),
            )
                .into_response();
        }
    };

    let worker_cmd = WorkerCmd {
        options: client_cmd.options,
        args: client_cmd.args,
        mirror_id: client_cmd.mirror_id.clone(),
        cmd: client_cmd.cmd,
    };

    // Pre-forward status bookkeeping: Disable flips the row to Disabled,
    // Stop flips a non-Disabled row to Paused. Go does the same at
    // `handleClientCmd` around line 450. These are best-effort — do not
    // gate the forward on their outcome.
    if client_cmd.cmd == CmdVerb::Disable
        && let Ok(status) = adapter.get_mirror_status(worker_id, &client_cmd.mirror_id)
    {
        let mut new_status = status;
        new_status.status = SyncStatus::Disabled;
        let _ = adapter.update_mirror_status(worker_id, &client_cmd.mirror_id, new_status);
    } else if client_cmd.cmd == CmdVerb::Stop
        && let Ok(status) = adapter.get_mirror_status(worker_id, &client_cmd.mirror_id)
        && status.status != SyncStatus::Disabled
    {
        let mut new_status = status;
        new_status.status = SyncStatus::Paused;
        let _ = adapter.update_mirror_status(worker_id, &client_cmd.mirror_id, new_status);
    }

    let Some(ref client) = manager.http_client else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ ERROR_KEY: "HTTP client not initialized" })),
        )
            .into_response();
    };

    // Workers accept commands at POST / (worker URL ends with "/").
    // Go forwards any request error (connect refused, timeout, non-2xx) as
    // 500 with a unified error JSON — no 502/504 discrimination.
    let url = worker.url.clone();
    match client.post(&url).json(&worker_cmd).send().await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({
                INFO_KEY: format!("successfully send command to worker {}", worker_id)
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                ERROR_KEY: format!(
                    "post command to worker {}({}) fail: {}",
                    worker_id, url, e
                )
            })),
        )
            .into_response(),
    }
}
