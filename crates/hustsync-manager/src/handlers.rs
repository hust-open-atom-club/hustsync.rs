use axum::Json;
use axum::extract::{FromRequestParts, Path, State};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use hustsync_internal::msg::{
    ClientCmd, CmdVerb, MirrorSchedules, MirrorStatus, WorkerCmd, WorkerStatus,
};
use hustsync_internal::status::SyncStatus;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::Path as FsPath;
use std::sync::Arc;

use crate::database::DbAdapterTrait;
use crate::server::{ERROR_KEY, INFO_KEY, Manager};

fn error_response(status: StatusCode, msg: impl std::fmt::Display) -> Response {
    (status, Json(json!({ ERROR_KEY: msg.to_string() }))).into_response()
}

fn ok_json(data: impl Serialize) -> Response {
    (StatusCode::OK, Json(json!(data))).into_response()
}

fn ok_message(msg: &str) -> Response {
    (StatusCode::OK, Json(json!({ INFO_KEY: msg }))).into_response()
}

fn write_status_file(manager: &Manager, adapter: &dyn DbAdapterTrait) -> Result<(), String> {
    let status_file = manager.config.files.status_file.trim();
    if status_file.is_empty() {
        return Ok(());
    }

    let statuses = adapter
        .list_all_mirror_status()
        .map_err(|e| format!("list all mirror status: {e}"))?;
    let web_statuses: Vec<hustsync_internal::status_web::WebMirrorStatus> =
        statuses.into_iter().map(|s| s.into()).collect();
    let data = serde_json::to_vec(&web_statuses).map_err(|e| format!("serialize status: {e}"))?;

    let expanded = hustsync_internal::util::expand_tilde(status_file);
    let path = FsPath::new(&expanded);
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("create parent directory {}: {e}", parent.display()))?;
    }
    std::fs::write(path, data).map_err(|e| format!("write {}: {e}", path.display()))
}

fn refresh_status_file(manager: &Manager, adapter: &dyn DbAdapterTrait) {
    if let Err(e) = write_status_file(manager, adapter) {
        tracing::error!("Failed to refresh status file: {}", e);
    }
}

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

pub async fn ping_handler() -> Response {
    ok_message("pong")
}

pub async fn register_worker(
    Database(adapter): Database,
    Json(mut worker): Json<WorkerStatus>,
) -> Response {
    worker.last_online = Utc::now();
    worker.last_register = Utc::now();

    match adapter.create_worker(worker) {
        Ok(new_worker) => ok_json(new_worker),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to register worker: {}", e),
        ),
    }
}

pub async fn list_all_workers(Database(adapter): Database) -> Response {
    match adapter.list_workers() {
        Ok(mut workers) => {
            for w in &mut workers {
                w.token = "REDACTED".to_string();
            }
            ok_json(workers)
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to list workers: {}", e),
        ),
    }
}

pub async fn list_all_jobs(Database(adapter): Database) -> Response {
    match adapter.list_all_mirror_status() {
        Ok(statuses) => {
            let web_statuses: Vec<hustsync_internal::status_web::WebMirrorStatus> =
                statuses.into_iter().map(|s| s.into()).collect();
            ok_json(web_statuses)
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to list jobs: {}", e),
        ),
    }
}

pub async fn list_jobs_of_worker(
    Database(adapter): Database,
    Path(worker_id): Path<String>,
) -> Response {
    // Go `listJobsOfWorker` returns raw `[]MirrorStatus`, NOT WebMirrorStatus.
    // Workers bootstrap their schedule from this endpoint and rely on the
    // RFC3339 timestamps in MirrorStatus rather than the dual text+ts pair
    // that WebMirrorStatus adds for the dashboard.
    match adapter.list_mirror_status(&worker_id) {
        Ok(statuses) => ok_json(statuses),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to list jobs of worker {}: {}", worker_id, e),
        ),
    }
}

pub async fn delete_worker(Database(adapter): Database, Path(worker_id): Path<String>) -> Response {
    match adapter.delete_worker(&worker_id) {
        Ok(_) => {
            tracing::info!("Worker <{}> deleted", worker_id);
            ok_message("deleted")
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to delete worker: {}", e),
        ),
    }
}

pub async fn flush_disabled_jobs(
    State(manager): State<Arc<Manager>>,
    Database(adapter): Database,
) -> Response {
    match adapter.flush_disabled_jobs() {
        Ok(_) => {
            refresh_status_file(&manager, adapter.as_ref());
            ok_message("flushed")
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to flush disabled jobs: {}", e),
        ),
    }
}

pub async fn update_job_of_worker(
    State(manager): State<Arc<Manager>>,
    Database(adapter): Database,
    Path((worker_id, mirror_id)): Path<(String, String)>,
    Json(mut status): Json<MirrorStatus>,
) -> Response {
    if status.name.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "mirror Name should not be empty");
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
        Ok(new_status) => {
            refresh_status_file(&manager, adapter.as_ref());
            ok_json(new_status)
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to update job: {}", e),
        ),
    }
}

#[derive(Deserialize)]
pub struct SizeMsg {
    /// Mirror name from the Go client payload; the handler uses the URL path name instead.
    /// Accepted here so Go clients that send both `name` and `size` fields are not rejected.
    #[serde(default)]
    #[allow(dead_code)]
    pub name: String,
    pub size: String,
}

pub async fn update_mirror_size(
    State(manager): State<Arc<Manager>>,
    Database(adapter): Database,
    Path((worker_id, mirror_id)): Path<(String, String)>,
    Json(msg): Json<SizeMsg>,
) -> Response {
    let _ = adapter.refresh_worker(&worker_id);

    match adapter.get_mirror_status(&worker_id, &mirror_id) {
        Ok(mut status) => {
            if !msg.size.is_empty() && msg.size != "unknown" {
                status.size = msg.size;
                match adapter.update_mirror_status(&worker_id, &mirror_id, status) {
                    Ok(new_status) => {
                        refresh_status_file(&manager, adapter.as_ref());
                        ok_json(new_status)
                    }
                    Err(e) => error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to save size: {}", e),
                    ),
                }
            } else {
                ok_json(status)
            }
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Mirror not found: {}", e),
        ),
    }
}

pub async fn update_schedules_of_worker(
    State(manager): State<Arc<Manager>>,
    Database(adapter): Database,
    Path(worker_id): Path<String>,
    Json(schedules): Json<MirrorSchedules>,
) -> Response {
    let _ = adapter.refresh_worker(&worker_id);

    for s in schedules.schedules {
        if s.name.is_empty() {
            return error_response(StatusCode::BAD_REQUEST, "mirror Name should not be empty");
        }

        match adapter.get_mirror_status(&worker_id, &s.name) {
            Ok(mut status) => {
                if status.next_scheduled == s.next_schedule {
                    continue;
                }
                status.next_scheduled = s.next_schedule;
                if let Err(e) = adapter.update_mirror_status(&worker_id, &s.name, status) {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "failed to update job {} of worker {}: {}",
                            s.name, worker_id, e
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

    refresh_status_file(&manager, adapter.as_ref());
    ok_json(json!({}))
}

pub async fn handle_cmd(
    State(manager): State<Arc<Manager>>,
    Database(adapter): Database,
    Json(client_cmd): Json<ClientCmd>,
) -> Response {
    let worker_id = &client_cmd.worker_id;

    // Empty worker_id: Go aborts with 500 and no body (multi-worker routing
    // is a known TODO in Go too).
    if worker_id.is_empty() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let worker = match adapter.get_worker(worker_id) {
        Ok(w) => w,
        Err(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("worker {} is not registered yet", worker_id),
            );
        }
    };

    let worker_cmd = WorkerCmd {
        options: client_cmd.options,
        args: client_cmd.args,
        mirror_id: client_cmd.mirror_id.clone(),
        cmd: client_cmd.cmd,
    };

    // Pre-forward status bookkeeping: Disable flips the row to Disabled,
    // Stop flips the row to Paused, including Disabled rows. Go does the same at
    // `handleClientCmd` around line 450. These are best-effort — do not
    // gate the forward on their outcome.
    if client_cmd.cmd == CmdVerb::Disable
        && let Ok(status) = adapter.get_mirror_status(worker_id, &client_cmd.mirror_id)
    {
        let mut new_status = status;
        new_status.status = SyncStatus::Disabled;
        match adapter.update_mirror_status(worker_id, &client_cmd.mirror_id, new_status) {
            Ok(_) => refresh_status_file(&manager, adapter.as_ref()),
            Err(e) => tracing::error!("Failed to pre-set disabled status: {}", e),
        }
    } else if client_cmd.cmd == CmdVerb::Stop
        && let Ok(status) = adapter.get_mirror_status(worker_id, &client_cmd.mirror_id)
    {
        let mut new_status = status;
        new_status.status = SyncStatus::Paused;
        match adapter.update_mirror_status(worker_id, &client_cmd.mirror_id, new_status) {
            Ok(_) => refresh_status_file(&manager, adapter.as_ref()),
            Err(e) => tracing::error!("Failed to pre-set paused status: {}", e),
        }
    }

    let Some(ref client) = manager.http_client else {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "HTTP client not initialized",
        );
    };

    // Workers accept commands at POST / (worker URL ends with "/").
    // Transport errors still collapse to 500 for compatibility with the old
    // manager path, but worker-side non-2xx responses must not be reported as
    // a successful command dispatch.
    let url = worker.url.clone();
    match client.post(&url).json(&worker_cmd).send().await {
        Ok(resp) if resp.status().is_success() => ok_message(&format!(
            "successfully send command to worker {}",
            worker_id
        )),
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            error_response(
                StatusCode::BAD_GATEWAY,
                format!(
                    "post command to worker {}({}) returned {}: {}",
                    worker_id, url, status, body
                ),
            )
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("post command to worker {}({}) fail: {}", worker_id, url, e),
        ),
    }
}
