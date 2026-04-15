#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod common;
mod config;
pub mod database;
mod handlers;
mod middleware;
mod server;

use axum::{Json, http::StatusCode, response::IntoResponse};
use thiserror::Error;

use crate::database::AdapterError;

pub use common::init_tracing;
pub use config::load_config;
pub use server::Manager;

/// Unified error type for the manager service.
///
/// All public APIs and server entry points return this type.  A single
/// `IntoResponse` impl maps it to a JSON error envelope so axum handlers
/// never need `Box<dyn Error>`.
#[derive(Debug, Error)]
pub enum ManagerError {
    #[error("adapter error: {0}")]
    Adapter(#[from] AdapterError),

    #[error("http client error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("config error: {0}")]
    Config(#[from] hustsync_config_parser::ConfigError),

    #[error("bind error: {0}")]
    Bind(String),

    #[error("tls error: {0}")]
    Tls(String),
}

impl IntoResponse for ManagerError {
    fn into_response(self) -> axum::response::Response {
        // Client-origin errors surface as 4xx; everything else is 5xx.
        let status = match &self {
            ManagerError::Config(_) => StatusCode::BAD_REQUEST,
            ManagerError::Adapter(_)
            | ManagerError::Http(_)
            | ManagerError::Bind(_)
            | ManagerError::Tls(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = Json(serde_json::json!({ "error": self.to_string() }));
        (status, body).into_response()
    }
}
