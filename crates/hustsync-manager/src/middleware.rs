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
) -> Response {
    let parts: Vec<&str> = req
        .uri()
        .path()
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();

    if parts.len() >= 2 && parts[0] == "workers" {
        let worker_id = parts[1];
        let Some(adapter) = &manager.adapter else {
            let body = Json(json!({ "error": "Database adapter not initialized" }));
            return (StatusCode::INTERNAL_SERVER_ERROR, body).into_response();
        };
        if adapter.get_worker(worker_id).is_err() {
            let body = Json(json!({ "error": format!("invalid workerID {worker_id}") }));
            return (StatusCode::BAD_REQUEST, body).into_response();
        }
    }

    next.run(req).await
}
