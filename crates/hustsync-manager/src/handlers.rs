use axum::extract::{FromRequestParts, Path};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::{Json, response::IntoResponse};
use chrono::Utc;
use hustsync_internal::msg::{MirrorStatus, WorkerStatus};
use hustsync_internal::status::SyncStatus;
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
