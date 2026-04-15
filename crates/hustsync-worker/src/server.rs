use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post};
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
    Router::new().route("/", post(handle_cmd)).with_state(state)
}

#[allow(clippy::cognitive_complexity, clippy::significant_drop_tightening)]
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
                return (StatusCode::OK, Json(json!({"msg": "Invalid Command"})));
            }
        }
    }

    // Job-level commands, scoped so the read guard drops before send_ctrl.
    let action = {
        let jobs = state.jobs.read().await;
        let Some(job) = jobs.get(&cmd.mirror_id) else {
            return (
                StatusCode::OK,
                Json(
                    json!({"msg": format!("Mirror '{}' is not configured on this worker", cmd.mirror_id)}),
                ),
            );
        };

        // Flush the schedule queue for the job.
        let mut queue = state.schedule_queue.lock().await;
        queue.remove(&job.name);
        drop(queue);

        match cmd.cmd {
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
                    return (StatusCode::OK, Json(json!({"msg": "Job is disabled"})));
                }
                crate::job::CtrlAction::Stop
            }
            CmdVerb::Disable => crate::job::CtrlAction::Disable,
            CmdVerb::Ping => crate::job::CtrlAction::Ping,
            _ => {
                return (StatusCode::OK, Json(json!({"msg": "Invalid Command"})));
            }
        }
    };

    // Re-borrow jobs for the ctrl dispatch; the earlier guard is gone.
    let jobs = state.jobs.read().await;
    let Some(job) = jobs.get(&cmd.mirror_id) else {
        return (
            StatusCode::OK,
            Json(
                json!({"msg": format!("Mirror '{}' is not configured on this worker", cmd.mirror_id)}),
            ),
        );
    };

    if let Err(e) = job.send_ctrl(action).await {
        tracing::error!("Failed to send action to job {}: {}", cmd.mirror_id, e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"msg": "Internal server error"})),
        );
    }

    (StatusCode::OK, Json(json!({"msg": "OK"})))
}
