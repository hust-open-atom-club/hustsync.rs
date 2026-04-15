use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub mod cmd_provider;
pub mod rsync_provider;
pub mod two_stage_rsync_provider;

/// Execution context passed into every `run()` call.
///
/// `cancel` carries the operator cancellation signal — providers must select on
/// it and return `ProviderError::Terminated` promptly (within 10 s).
///
/// `attempt` is 1-based and is available for logging only: providers SHOULD log
/// it at DEBUG level on re-entry (`attempt > 1`) and MUST NOT change sync
/// behaviour based on it. The retry policy (count, backoff) is entirely the
/// job-actor's responsibility, matching Go tunasync behaviour where the retry
/// loop runs outside provider.Run().
///
/// `env` contains hook-injected variables (see `06-job-lifecycle-and-hooks.md`
/// §4). Providers layer it on top of standard env vars so hook overrides win.
#[derive(Debug, Clone, Default)]
pub struct RunContext {
    pub cancel: CancellationToken,
    pub attempt: u32,
    pub env: HashMap<String, String>,
}

#[derive(Error, Debug)]
pub enum ProviderError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("execution failed (exit code {code}): {msg}")]
    Execution { code: i32, msg: String },
    #[error("timeout after {0:?}")]
    Timeout(Duration),
    #[error("regex: {0}")]
    Regex(#[from] regex::Error),
    #[error("already running")]
    AlreadyRunning,
    #[error("terminated by operator")]
    Terminated,
    #[error("config: {0}")]
    Config(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderType {
    Command,
    Rsync,
    TwoStageRsync,
}

#[async_trait]
pub trait MirrorProvider: Send + Sync {
    /// Name of the mirror
    fn name(&self) -> &str;

    /// Upstream URL
    fn upstream(&self) -> &str;

    /// Type of the provider
    fn provider_type(&self) -> ProviderType;

    /// Interval between syncs
    fn interval(&self) -> Duration;

    /// Number of retries on failure
    fn retry(&self) -> u32;

    /// Max duration for a single sync attempt. Duration::ZERO means disabled.
    fn timeout(&self) -> Duration;

    /// The working directory for the job
    fn working_dir(&self) -> &Path;

    /// The directory where log files are written
    fn log_dir(&self) -> &Path;

    /// The path to the log file for this sync
    fn log_file(&self) -> &Path;

    /// Execute the sync process
    async fn run(&self, ctx: RunContext) -> Result<(), ProviderError>;

    /// Terminate the running process early. No-op if not running.
    async fn terminate(&self) -> Result<(), ProviderError>;

    /// Data size from last sync (if known/extracted)
    async fn data_size(&self) -> Option<String>;

    /// Is this a master mirror node?
    fn is_master(&self) -> bool;
}
