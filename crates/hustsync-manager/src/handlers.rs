use axum::http::StatusCode;
use axum::{Json, response::IntoResponse};
use serde_json::{Value, json};

use crate::server::INFO_KEY;

pub async fn ping_handler() -> impl IntoResponse {
    let body = json!({
        INFO_KEY: "pong"
    });

    (StatusCode::OK, Json(body))
}
