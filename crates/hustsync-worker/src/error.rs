use thiserror::Error;

use crate::provider::ProviderError;

/// Stub error type for the hook pipeline.
///
/// This variant covers only the generic case for M1. Later milestones will
/// add structured variants (pre-hook abort, post-hook failure, etc.) once
/// the hook pipeline is wired up.
#[derive(Error, Debug)]
pub enum HookError {
    #[error("{0}")]
    Generic(String),
}

/// Top-level error type for the worker process.
#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("provider error: {0}")]
    Provider(#[from] ProviderError),

    #[error("hook error: {0}")]
    Hook(HookError),

    #[error("HTTP client error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("config error: {0}")]
    Config(String),

    #[error("bind error: {0}")]
    Bind(String),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("internal error: {0}")]
    Internal(String),
}
