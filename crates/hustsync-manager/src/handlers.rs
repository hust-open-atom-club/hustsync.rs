use axum::extract::Path;
use axum::http::StatusCode;
use axum::{Json, response::IntoResponse};
use serde_json::json;
use chrono::Utc;

use hustsync_internal::msg::WorkerStatus;
use hustsync_internal::status_web::WebMirrorStatus;
use crate::server::{INFO_KEY, ERROR_KEY, get_hustsync_manager};

pub async fn ping_handler() -> impl IntoResponse {
    let body = json!({
        INFO_KEY: "pong"
    });

    (StatusCode::OK, Json(body))
}

pub async fn list_all_jobs() -> impl IntoResponse {
    let manager = match get_hustsync_manager(None) {
        Ok(m) => m,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Manager not initialized"}))).into_response(),
    };

    let adapter = match &manager.adapter {
        Some(a) => a,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Database not initialized"}))).into_response(),
    };

    match adapter.list_all_mirror_status() {
        Ok(mirrors) => {
            let web_mirrors: Vec<WebMirrorStatus> = mirrors.into_iter().map(WebMirrorStatus::from).collect();
            (StatusCode::OK, Json(web_mirrors)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: e.to_string()}))).into_response(),
    }
}

pub async fn list_all_workers() -> impl IntoResponse {
    let manager = match get_hustsync_manager(None) {
        Ok(m) => m,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Manager not initialized"}))).into_response(),
    };

    let adapter = match &manager.adapter {
        Some(a) => a,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Database not initialized"}))).into_response(),
    };

    match adapter.list_workers() {
        Ok(workers) => {
            let redacted_workers: Vec<WorkerStatus> = workers.into_iter().map(|mut w| {
                w.token = "REDACTED".to_string();
                w
            }).collect();
            (StatusCode::OK, Json(redacted_workers)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: e.to_string()}))).into_response(),
    }
}

pub async fn register_worker(Json(mut worker): Json<WorkerStatus>) -> impl IntoResponse {
    let manager = match get_hustsync_manager(None) {
        Ok(m) => m,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Manager not initialized"}))).into_response(),
    };

    let adapter = match &manager.adapter {
        Some(a) => a,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Database not initialized"}))).into_response(),
    };

    worker.last_online = Utc::now();
    worker.last_register = Utc::now();

    match adapter.create_worker(worker) {
        Ok(new_worker) => (StatusCode::OK, Json(new_worker)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: e.to_string()}))).into_response(),
    }
}

pub async fn delete_worker(Path(id): Path<String>) -> impl IntoResponse {
    let manager = match get_hustsync_manager(None) {
        Ok(m) => m,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Manager not initialized"}))).into_response(),
    };

    let adapter = match &manager.adapter {
        Some(a) => a,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Database not initialized"}))).into_response(),
    };

    match adapter.delete_worker(&id) {
        Ok(_) => (StatusCode::OK, Json(json!({INFO_KEY: "deleted"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: e.to_string()}))).into_response(),
    }
}

pub async fn flush_disabled_jobs() -> impl IntoResponse {
    let manager = match get_hustsync_manager(None) {
        Ok(m) => m,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Manager not initialized"}))).into_response(),
    };

    let adapter = match &manager.adapter {
        Some(a) => a,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Database not initialized"}))).into_response(),
    };

    match adapter.flush_disabled_jobs() {
        Ok(_) => (StatusCode::OK, Json(json!({INFO_KEY: "flushed"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: e.to_string()}))).into_response(),
    }
}

pub async fn list_jobs_of_worker(Path(id): Path<String>) -> impl IntoResponse {
    let manager = match get_hustsync_manager(None) {
        Ok(m) => m,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Manager not initialized"}))).into_response(),
    };

    let adapter = match &manager.adapter {
        Some(a) => a,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Database not initialized"}))).into_response(),
    };

    match adapter.list_mirror_status(&id) {
        Ok(mirrors) => (StatusCode::OK, Json(mirrors)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: e.to_string()}))).into_response(),
    }
}

use hustsync_internal::msg::{MirrorStatus, MirrorSchedules, ClientCmd, WorkerCmd};

pub async fn update_job_of_worker(
    Path((worker_id, _job)): Path<(String, String)>,
    Json(mut status): Json<MirrorStatus>
) -> impl IntoResponse {
    let manager = match get_hustsync_manager(None) {
        Ok(m) => m,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Manager not initialized"}))).into_response(),
    };

    let adapter = match &manager.adapter {
        Some(a) => a,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Database not initialized"}))).into_response(),
    };

    let mirror_name = status.name.clone();
    if mirror_name.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({ERROR_KEY: "mirror Name should not be empty"}))).into_response();
    }

    let _ = adapter.refresh_worker(&worker_id);
    let cur_status = adapter.get_mirror_status(&worker_id, &mirror_name).ok();

    let cur_time = Utc::now();

    use hustsync_internal::status::SyncStatus;
    if status.status == SyncStatus::PreSyncing && cur_status.as_ref().map(|s| s.status) != Some(SyncStatus::PreSyncing) {
        status.last_started = cur_time;
    } else if let Some(cs) = &cur_status {
        status.last_started = cs.last_started;
    }

    if status.status == SyncStatus::Success {
        status.last_update = cur_time;
    } else if let Some(cs) = &cur_status {
        status.last_update = cs.last_update;
    }

    if status.status == SyncStatus::Success || status.status == SyncStatus::Failed {
        status.last_ended = cur_time;
    } else if let Some(cs) = &cur_status {
        status.last_ended = cs.last_ended;
    }

    if let Some(cs) = &cur_status {
        if !cs.size.is_empty() && cs.size != "unknown" {
            if status.size.is_empty() || status.size == "unknown" {
                status.size = cs.size.clone();
            }
        }
    }

    match adapter.update_mirror_status(&worker_id, &mirror_name, status) {
        Ok(new_status) => (StatusCode::OK, Json(new_status)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: e.to_string()}))).into_response(),
    }
}

pub async fn update_mirror_size(
    Path((worker_id, _job)): Path<(String, String)>,
    Json(msg): Json<serde_json::Value>
) -> impl IntoResponse {
    let manager = match get_hustsync_manager(None) {
        Ok(m) => m,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Manager not initialized"}))).into_response(),
    };

    let adapter = match &manager.adapter {
        Some(a) => a,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Database not initialized"}))).into_response(),
    };

    let name = msg["name"].as_str().unwrap_or("");
    let size = msg["size"].as_str().unwrap_or("");

    if name.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({ERROR_KEY: "mirror Name should not be empty"}))).into_response();
    }

    let _ = adapter.refresh_worker(&worker_id);
    match adapter.get_mirror_status(&worker_id, name) {
        Ok(mut status) => {
            if !size.is_empty() && size != "unknown" {
                status.size = size.to_string();
            }
            match adapter.update_mirror_status(&worker_id, name, status) {
                Ok(new_status) => (StatusCode::OK, Json(new_status)).into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: e.to_string()}))).into_response(),
            }
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: e.to_string()}))).into_response(),
    }
}

pub async fn update_schedules_of_worker(
    Path(worker_id): Path<String>,
    Json(schedules): Json<MirrorSchedules>
) -> impl IntoResponse {
    let manager = match get_hustsync_manager(None) {
        Ok(m) => m,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Manager not initialized"}))).into_response(),
    };

    let adapter = match &manager.adapter {
        Some(a) => a,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Database not initialized"}))).into_response(),
    };

    for schedule in schedules.schedules {
        let mirror_name = &schedule.name;
        if mirror_name.is_empty() {
            continue;
        }

        let _ = adapter.refresh_worker(&worker_id);
        if let Ok(mut cur_status) = adapter.get_mirror_status(&worker_id, mirror_name) {
            if cur_status.next_scheduled == schedule.next_schedule {
                continue;
            }
            cur_status.next_scheduled = schedule.next_schedule;
            let _ = adapter.update_mirror_status(&worker_id, mirror_name, cur_status);
        }
    }

    (StatusCode::OK, Json(json!({}))).into_response()
}

pub async fn handle_cmd(Json(client_cmd): Json<ClientCmd>) -> impl IntoResponse {
    let manager = match get_hustsync_manager(None) {
        Ok(m) => m,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Manager not initialized"}))).into_response(),
    };

    let adapter = match &manager.adapter {
        Some(a) => a,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "Database not initialized"}))).into_response(),
    };

    let worker_id = &client_cmd.worker_id;
    if worker_id.is_empty() {
         return (StatusCode::BAD_REQUEST, Json(json!({ERROR_KEY: "worker_id is required"}))).into_response();
    }

    let worker = match adapter.get_worker(worker_id) {
        Ok(w) => w,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(json!({ERROR_KEY: format!("worker {} is not registered yet: {}", worker_id, e)}))).into_response(),
    };

    let worker_url = worker.url;
    let worker_cmd = WorkerCmd {
        cmd: client_cmd.cmd,
        mirror_id: client_cmd.mirror_id.clone(),
        args: client_cmd.args.clone(),
        options: client_cmd.options.clone(),
    };

    // Update status if needed
    if let Ok(mut cur_stat) = adapter.get_mirror_status(worker_id, &client_cmd.mirror_id) {
        let mut changed = false;
        use hustsync_internal::msg::CmdVerb;
        use hustsync_internal::status::SyncStatus;
        match client_cmd.cmd {
            CmdVerb::Disable => {
                cur_stat.status = SyncStatus::Disabled;
                changed = true;
            }
            CmdVerb::Stop => {
                cur_stat.status = SyncStatus::Paused;
                changed = true;
            }
            _ => {}
        }
        if changed {
            let _ = adapter.update_mirror_status(worker_id, &client_cmd.mirror_id, cur_stat);
        }
    }

    // Post to worker
    if let Some(client) = &manager.http_client {
        match client.post(&worker_url).json(&worker_cmd).send().await {
            Ok(_) => (StatusCode::OK, Json(json!({INFO_KEY: format!("successfully send command to worker {}", worker_id)}))).into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: format!("post command to worker {} fail: {}", worker_id, e)}))).into_response(),
        }
    } else {
         (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ERROR_KEY: "HTTP client not initialized"}))).into_response()
    }
}
