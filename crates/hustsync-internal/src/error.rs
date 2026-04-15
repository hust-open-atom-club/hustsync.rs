use thiserror::Error;

/// Typed error for all `hustsync-internal` operations.
///
/// Public API of this crate never exposes `Box<dyn Error>`.
/// Every helper in `util` and `logger` returns `Result<_, InternalError>`.
#[derive(Debug, Error)]
pub enum InternalError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("JSON error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("log parse error: {0}")]
    LogParse(String),

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("unexpected HTTP status: {0}")]
    HttpStatus(String),
}
