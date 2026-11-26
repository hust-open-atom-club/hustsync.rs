use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::{get, post}};
use hustsync_internal::msg::{CmdVerb, WorkerCmd};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

use crate::MirrorJob;

pub struct AppState {
    pub jobs: Arc<RwLock<HashMap<String, MirrorJob>>>,
    pub schedule_queue: Arc<tokio::sync::Mutex<crate::schedule::ScheduleQueue>>,
    pub reload_tx: Option<mpsc::Sender<()>>,
}

pub fn make_http_server(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", post(handle_cmd))
        .route("/metrics", get(crate::metrics::metrics_handler))
        .with_state(state)
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
                let Some(reload_tx) = state.reload_tx.as_ref() else {
                    tracing::error!("Reload requested but no reload handler is configured");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"msg": "Reload unavailable"})),
                    );
                };
                match reload_tx.try_send(()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        tracing::info!("Reload already pending; coalescing HTTP request");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        tracing::error!("Failed to enqueue reload request: channel closed");
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"msg": "Reload unavailable"})),
                        );
                    }
                }
                return (StatusCode::OK, Json(json!({"msg": "Reload triggered"})));
            }
            _ => {
                return (
                    StatusCode::NOT_ACCEPTABLE,
                    Json(json!({"msg": "Invalid Command"})),
                );
            }
        }
    }

    // Job-level commands, scoped so the read guard drops before send_ctrl.
    let action = {
        let jobs = state.jobs.read().await;
        let Some(job) = jobs.get(&cmd.mirror_id) else {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"msg": format!("Mirror ``{}'' not found", cmd.mirror_id)})),
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
                    return (StatusCode::OK, Json(json!({"msg": "OK"})));
                }
                crate::job::CtrlAction::Stop
            }
            CmdVerb::Disable => {
                if job.state() == crate::job::STATE_DISABLED {
                    return (StatusCode::OK, Json(json!({"msg": "OK"})));
                }
                crate::job::CtrlAction::Disable
            }
            CmdVerb::Ping => {
                return (StatusCode::OK, Json(json!({"msg": "OK"})));
            }
            _ => {
                return (
                    StatusCode::NOT_ACCEPTABLE,
                    Json(json!({"msg": "Invalid Command"})),
                );
            }
        }
    };

    // Re-borrow and clone the job for ctrl dispatch; the earlier guard is gone.
    let job = {
        let jobs = state.jobs.read().await;
        let Some(job) = jobs.get(&cmd.mirror_id) else {
            return (
                StatusCode::OK,
                Json(
                    json!({"msg": format!("Mirror '{}' is not configured on this worker", cmd.mirror_id)}),
                ),
            );
        };
        job.clone()
    };

    let disabled = if action == crate::job::CtrlAction::Disable {
        Some(Arc::clone(&job.disabled))
    } else {
        None
    };

    let disabled_notified = disabled.as_ref().map(|disabled| disabled.notified());

    if let Err(e) = job.send_ctrl(action).await {
        tracing::error!("Failed to send action to job {}: {}", cmd.mirror_id, e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"msg": "Internal server error"})),
        );
    }

    if let Some(disabled_notified) = disabled_notified {
        disabled_notified.await;
    }

    (StatusCode::OK, Json(json!({"msg": "OK"})))
}
