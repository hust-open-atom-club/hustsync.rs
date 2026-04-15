use thiserror::Error;

use crate::provider::ProviderError;

/// Error surfaced by a single hook invocation.
///
/// Every variant carries the hook's name so the pipeline can identify
/// which hook failed when a sync is aborted or a post-hook is logged.
#[derive(Error, Debug)]
#[error("{hook}: {kind}")]
pub struct HookError {
    pub hook: String,
    pub kind: HookErrorKind,
}

impl HookError {
    pub fn io(hook: impl Into<String>, err: std::io::Error) -> Self {
        Self {
            hook: hook.into(),
            kind: HookErrorKind::Io(err),
        }
    }

    pub fn command_exit(hook: impl Into<String>, code: i32) -> Self {
        Self {
            hook: hook.into(),
            kind: HookErrorKind::CommandExit(code),
        }
    }

    pub fn config(hook: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            hook: hook.into(),
            kind: HookErrorKind::Config(reason.into()),
        }
    }
}

#[derive(Error, Debug)]
pub enum HookErrorKind {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("command exit {0}")]
    CommandExit(i32),
    #[error("config: {0}")]
    Config(String),
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
