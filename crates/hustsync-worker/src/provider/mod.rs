use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use hustsync_config_parser::{MirrorConfig, WorkerConfig};
use hustsync_internal::util::format_path;

use self::cmd_provider::{CmdProvider, CmdProviderConfig};
use self::rsync_provider::{RsyncProvider, RsyncProviderConfig};
use self::two_stage_rsync_provider::{TwoStageRsyncProvider, TwoStageRsyncProviderConfig};

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

/// Construct a concrete provider from a `MirrorConfig` + the worker's
/// global config. Dispatches on the `provider` field:
///
/// - `"rsync"` (default) → `RsyncProvider`
/// - `"command"`          → `CmdProvider`
/// - `"two-stage-rsync"`  → `TwoStageRsyncProvider`
/// - anything else        → `ProviderError::Config`
///
/// All per-mirror values that inherit from globals (retry timings,
/// log/mirror dirs) are resolved here so providers receive ready-to-use
/// absolute paths and durations.
pub fn build_provider(
    name: &str,
    m_cfg: &MirrorConfig,
    g_cfg: &WorkerConfig,
) -> Result<Box<dyn MirrorProvider>, ProviderError> {
    let global = g_cfg.global.as_ref();

    let interval = m_cfg
        .retry
        .as_ref()
        .and_then(|r| r.interval)
        .or_else(|| global.and_then(|g| g.retry.as_ref().and_then(|r| r.interval)))
        .unwrap_or(120);

    let retry = m_cfg
        .retry
        .as_ref()
        .and_then(|r| r.retry)
        .or_else(|| global.and_then(|g| g.retry.as_ref().and_then(|r| r.retry)))
        .unwrap_or(2);

    let timeout = m_cfg
        .retry
        .as_ref()
        .and_then(|r| r.timeout)
        .or_else(|| global.and_then(|g| g.retry.as_ref().and_then(|r| r.timeout)))
        .unwrap_or(3600);

    let log_dir_base = m_cfg
        .log_dir
        .as_deref()
        .or_else(|| global.and_then(|g| g.log_dir.as_deref()))
        .unwrap_or("/tmp/hustsync/log/{{.Name}}");
    let log_dir = format_path(log_dir_base, name);
    let log_file = format!("{}/latest.log", log_dir.trim_end_matches('/'));

    let mirror_dir_base = m_cfg
        .mirror_dir
        .as_deref()
        .or_else(|| global.and_then(|g| g.mirror_dir.as_deref()))
        .unwrap_or("/tmp/hustsync");
    let mirror_dir = format_path(mirror_dir_base, name);

    let is_master = m_cfg.role.as_deref() != Some("slave");
    let p_type = m_cfg.provider.as_deref().unwrap_or("rsync");

    match p_type {
        "command" => {
            let cfg = CmdProviderConfig {
                name: name.to_string(),
                upstream_url: m_cfg.upstream.clone().unwrap_or_default(),
                command: m_cfg.command.clone().unwrap_or_default(),
                working_dir: mirror_dir,
                log_dir,
                log_file,
                interval: Duration::from_secs(interval as u64 * 60),
                retry,
                timeout: Duration::from_secs(timeout as u64),
                env: m_cfg.env.clone().unwrap_or_default(),
                fail_on_match: m_cfg.fail_on_match.clone(),
                size_pattern: m_cfg.size_pattern.clone(),
                is_master,
            };
            Ok(Box::new(CmdProvider::new(cfg)?))
        }
        "rsync" => {
            let cfg = RsyncProviderConfig {
                name: name.to_string(),
                command: m_cfg.command.clone().unwrap_or_else(|| "rsync".to_string()),
                upstream_url: m_cfg.upstream.clone().unwrap_or_default(),
                username: m_cfg.username.clone(),
                password: m_cfg.password.clone(),
                exclude_file: m_cfg.exclude_file.clone(),
                rsync_options: m_cfg.rsync_options.clone().unwrap_or_default(),
                global_options: global
                    .and_then(|g| g.rsync_options.clone())
                    .unwrap_or_default(),
                rsync_override: m_cfg.rsync_override.clone(),
                rsync_override_only: m_cfg.rsync_override_only.unwrap_or(false),
                rsync_no_timeout: m_cfg.rsync_no_timeout.unwrap_or(false),
                rsync_timeout: m_cfg.rsync_timeout,
                env: m_cfg.env.clone().unwrap_or_default(),
                working_dir: mirror_dir,
                log_dir,
                log_file,
                use_ipv6: m_cfg.use_ipv6.unwrap_or(false),
                use_ipv4: m_cfg.use_ipv4.unwrap_or(false),
                interval: Duration::from_secs(interval as u64 * 60),
                retry,
                timeout: Duration::from_secs(timeout as u64),
                is_master,
            };
            Ok(Box::new(RsyncProvider::new(cfg)?))
        }
        "two-stage-rsync" => {
            let cfg = TwoStageRsyncProviderConfig {
                name: name.to_string(),
                command: m_cfg.command.clone().unwrap_or_else(|| "rsync".to_string()),
                stage1_profile: m_cfg
                    .stage1_profile
                    .clone()
                    .unwrap_or_else(|| "debian".to_string()),
                upstream_url: m_cfg.upstream.clone().unwrap_or_default(),
                username: m_cfg.username.clone(),
                password: m_cfg.password.clone(),
                exclude_file: m_cfg.exclude_file.clone(),
                extra_options: m_cfg.rsync_options.clone().unwrap_or_default(),
                rsync_no_timeout: m_cfg.rsync_no_timeout.unwrap_or(false),
                rsync_timeout: m_cfg.rsync_timeout,
                env: m_cfg.env.clone().unwrap_or_default(),
                working_dir: mirror_dir,
                log_dir,
                log_file,
                use_ipv6: m_cfg.use_ipv6.unwrap_or(false),
                use_ipv4: m_cfg.use_ipv4.unwrap_or(false),
                interval: Duration::from_secs(interval as u64 * 60),
                retry,
                timeout: Duration::from_secs(timeout as u64),
                is_master,
            };
            Ok(Box::new(TwoStageRsyncProvider::new(cfg)?))
        }
        _ => Err(ProviderError::Config(format!(
            "unknown provider type `{p_type}`"
        ))),
    }
}
