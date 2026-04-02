use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use hustsync_internal::msg::{CmdVerb, WorkerCmd};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::MirrorJob;

pub struct AppState {
    pub jobs: Arc<RwLock<HashMap<String, MirrorJob>>>,
    pub schedule_queue: Arc<tokio::sync::Mutex<crate::schedule::ScheduleQueue>>,
}

pub fn make_http_server(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/cmd", post(handle_cmd))
        .with_state(state)
}

async fn handle_cmd(
    State(state): State<Arc<AppState>>,
    Json(cmd): Json<WorkerCmd>,
) -> impl IntoResponse {
    tracing::info!("Received command: {:?}", cmd);

    if cmd.mirror_id.is_empty() {
        // Worker-level commands
        match cmd.cmd {
            CmdVerb::Reload => {
                tracing::info!("Reloading worker...");
                // TODO: trigger SIGHUP or reload logic
                return (StatusCode::OK, Json(json!({"msg": "Reload triggered"})));
            }
            _ => {
                return (StatusCode::NOT_ACCEPTABLE, Json(json!({"msg": "Invalid Command"})));
            }
        }
    }

    // Job-level commands
    let jobs = state.jobs.read().await;
    let job = match jobs.get(&cmd.mirror_id) {
        Some(j) => j,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"msg": format!("Mirror ``{}'' not found", cmd.mirror_id)})),
            );
        }
    };

    // Every command flushes the schedule queue for the job
    let mut queue = state.schedule_queue.lock().await;
    queue.remove(&job.name);
    drop(queue); // Unlock early before sending control commands

    let action = match cmd.cmd {
        CmdVerb::Start => {
            if cmd.options.get("force").copied().unwrap_or(false) {
                crate::job::CtrlAction::ForceStart
            } else {
                crate::job::CtrlAction::Start
            }
        }
        CmdVerb::Restart => crate::job::CtrlAction::Restart,
        CmdVerb::Stop => {
            if job.state() == crate::job::STATE_DISABLED {
                // Ignore stop command on a disabled job
                return (StatusCode::OK, Json(json!({"msg": "Job is disabled"})));
            }
            crate::job::CtrlAction::Stop
        }
        CmdVerb::Disable => crate::job::CtrlAction::Disable,
        CmdVerb::Ping => crate::job::CtrlAction::Ping,
        _ => {
            return (StatusCode::NOT_ACCEPTABLE, Json(json!({"msg": "Invalid Command"})));
        }
    };

    if let Err(e) = job.send_ctrl(action).await {
        tracing::error!("Failed to send action to job {}: {}", cmd.mirror_id, e);
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"msg": "Internal server error"})));
    }

    (StatusCode::OK, Json(json!({"msg": "OK"})))
}
