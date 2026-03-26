use axum::extract::{FromRequestParts, Path};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::{Json, response::IntoResponse};
use chrono::Utc;
use hustsync_internal::msg::{MirrorStatus, WorkerStatus, ClientCmd, WorkerCmd, CmdVerb, MirrorSchedules};
use hustsync_internal::status::SyncStatus;
use serde::Deserialize;
use serde_json::json;

use crate::database::DbAdapterTrait;
use crate::server::{ERROR_KEY, INFO_KEY, get_hustsync_manager};

// Custom Extractor for the database adapter
pub struct Database(pub &'static dyn DbAdapterTrait);

impl<S> FromRequestParts<S> for Database
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request_parts(_parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let manager = get_hustsync_manager(Default::default()).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ ERROR_KEY: format!("Manager not initialized: {}", e) })),
            )
        })?;

        let adapter = manager.adapter.as_ref().ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ ERROR_KEY: "Database adapter not initialized" })),
            )
        })?;

        Ok(Database(adapter.as_ref()))
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
    }

    let is_finished = |s: SyncStatus| {
        s == SyncStatus::Success || s == SyncStatus::Failed || s == SyncStatus::Disabled
    };

    if is_finished(status.status) {
        if let Some(ref cur) = cur_status {
            if !is_finished(cur.status) {
                status.last_ended = now;
            } else {
                status.last_ended = cur.last_ended;
            }
        } else {
            status.last_ended = now;
        }
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
    pub name: String,
    pub size: String,
}

pub async fn update_mirror_size(
    Database(adapter): Database,
    Path(worker_id): Path<String>,
    Json(msg): Json<SizeMsg>,
) -> impl IntoResponse {
    let _ = adapter.refresh_worker(&worker_id);
    
    match adapter.get_mirror_status(&worker_id, &msg.name) {
        Ok(mut status) => {
            if !msg.size.is_empty() && msg.size != "unknown" {
                status.size = msg.size;
                match adapter.update_mirror_status(&worker_id, &msg.name, status) {
                    Ok(new_status) => (StatusCode::OK, Json(json!(new_status))),
                    Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ ERROR_KEY: format!("Failed to save size: {}", e) }))),
                }
            } else {
                (StatusCode::OK, Json(json!(status)))
            }
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ ERROR_KEY: format!("Mirror not found: {}", e) }))),
    }
}

pub async fn update_schedules_of_worker(
    Database(adapter): Database,
    Path(worker_id): Path<String>,
    Json(schedules): Json<MirrorSchedules>,
) -> impl IntoResponse {
    let _ = adapter.refresh_worker(&worker_id);

    for s in schedules.schedules {
        if let Ok(mut status) = adapter.get_mirror_status(&worker_id, &s.name) {
            status.next_scheduled = s.next_schedule;
            let _ = adapter.update_mirror_status(&worker_id, &s.name, status);
        }
    }

    (StatusCode::OK, Json(json!({})))
}

pub async fn handle_cmd(
    Database(adapter): Database,
    Json(client_cmd): Json<ClientCmd>,
) -> impl IntoResponse {
    let manager = match get_hustsync_manager(Default::default()) {
        Ok(m) => m,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ ERROR_KEY: "Manager not initialized" }))),
    };

    let worker_id = &client_cmd.worker_id;
    if worker_id.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({ ERROR_KEY: "workerID should not be empty" })));
    }

    let worker = match adapter.get_worker(worker_id) {
        Ok(w) => w,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(json!({ ERROR_KEY: format!("Worker {} not found: {}", worker_id, e) }))),
    };

    let worker_cmd = WorkerCmd {
        options: client_cmd.options,
        args: client_cmd.args,
        mirror_id: client_cmd.mirror_id.clone(),
        cmd: client_cmd.cmd,
    };

    if client_cmd.cmd == CmdVerb::Disable {
        if let Ok(mut status) = adapter.get_mirror_status(worker_id, &client_cmd.mirror_id) {
            status.status = SyncStatus::Disabled;
            let _ = adapter.update_mirror_status(worker_id, &client_cmd.mirror_id, status);
        }
    }

    let Some(ref client) = manager.http_client else {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ ERROR_KEY: "HTTP client not initialized" })));
    };

    let url = format!("{}/cmd", worker.url.trim_end_matches('/'));
    match client.post(&url).json(&worker_cmd).send().await {
        Ok(resp) => {
            let status = resp.status();
            match resp.json::<serde_json::Value>().await {
                Ok(json) => (status, Json(json)),
                Err(_) => (status, Json(json!({ INFO_KEY: "Forwarded" }))),
            }
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ ERROR_KEY: format!("Failed to forward command to worker: {}", e) }))),
    }
}
