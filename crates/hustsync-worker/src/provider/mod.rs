use async_trait::async_trait;
use std::time::Duration;
use thiserror::Error;

pub mod cmd_provider;

#[derive(Error, Debug)]
pub enum ProviderError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Command execution failed: {0}")]
    Execution(String),
    #[error("Timeout")]
    Timeout,
    #[error("Regex error: {0}")]
    Regex(#[from] regex::Error),
    #[error("Already running")]
    AlreadyRunning,
    #[error("Terminated")]
    Terminated,
    #[error("Unknown provider type: {0}")]
    UnknownType(String),
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

    /// Max duration for a single sync attempt
    fn timeout(&self) -> Duration;

    /// The working directory for the job
    fn working_dir(&self) -> &str;

    /// Execute the sync process
    async fn run(&self) -> Result<(), ProviderError>;

    /// Terminate the running process early
    async fn terminate(&self) -> Result<(), ProviderError>;

    /// Data size from last sync (if known/extracted)
    fn data_size(&self) -> Option<String>;
    
    /// Is this a master mirror node?
    fn is_master(&self) -> bool;
}
