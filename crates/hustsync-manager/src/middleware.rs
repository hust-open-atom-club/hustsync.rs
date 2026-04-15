use std::sync::Arc;

use axum::{
    Json,
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
use reqwest::StatusCode;
use serde_json::json;
use tracing::error;

use crate::Manager;

pub async fn context_error_logger(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    let response = next.run(req).await;
    if response.status().is_client_error() || response.status().is_server_error() {
        error!(
            "in request {} {}: Status {}",
            method,
            path,
            response.status()
        );
    }
    response
}

pub async fn worker_id_validator(
    State(manager): State<Arc<Manager>>,
    req: Request,
    next: Next,
) -> Result<Response, Response> {
    
    let path = req.uri().path();
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    
    if parts.len() >= 2 && parts[0] == "workers" {
        let worker_id = parts[1];
        if let Some(adapter) = &manager.adapter {
            if let Err(e) = adapter.get_worker(worker_id) {
                let error_body = Json(json!({
                    "error": format!("invalid workerID {}", worker_id),
                    "details": e.to_string()
                }));
                return Err((StatusCode::BAD_REQUEST, error_body).into_response());
            }
        } else {
            let error_body = Json(json!({
                "error": "Database adapter not initialized"
            }));
            return Err((StatusCode::INTERNAL_SERVER_ERROR, error_body).into_response());
        }
    }

    Ok(next.run(req).await)
}
