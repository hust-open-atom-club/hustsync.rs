use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
use nix::sys::signal::{self, Signal};
#[cfg(unix)]
use nix::unistd::Pid;

use hustsync_config_parser::{MirrorConfig, WorkerConfig};
use hustsync_internal::util::{expand_tilde, format_path};

use self::cmd_provider::{CmdProvider, CmdProviderConfig};
use self::rsync_provider::{RsyncProvider, RsyncProviderConfig};
use self::two_stage_rsync_provider::{TwoStageRsyncProvider, TwoStageRsyncProviderConfig};

pub mod cmd_provider;
pub mod rsync_provider;
pub mod two_stage_rsync_provider;

/// Fields shared by every provider variant.
///
/// All per-mirror values that inherit from globals (retry timings, log/mirror
/// dirs) are resolved by `build_provider` before constructing this struct, so
/// providers always receive ready-to-use absolute paths and durations.
pub struct CommonProviderConfig {
    pub name: String,
    pub upstream_url: String,
    pub working_dir: String,
    pub log_dir: String,
    pub log_file: String,
    pub interval: Duration,
    pub retry: u32,
    pub timeout: Duration,
    pub env: HashMap<String, String>,
    pub is_master: bool,
}

/// Read the last `max_lines` non-empty lines from a log file.
/// Returns an empty string on any I/O error — callers use this for
/// best-effort diagnostic context, never for control flow.
pub(crate) async fn tail_log_file(path: &str, max_lines: usize) -> String {
    let content = match tokio::fs::read_to_string(path).await {
        Ok(c) => c,
        Err(_) => return String::new(),
    };
    let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
    let start = lines.len().saturating_sub(max_lines);
    lines[start..].join("\n")
}

/// Log a provider failure with an optional tail of the log file for context.
///
/// The tail is indented so log aggregators can visually separate it from the
/// primary error line.
pub(crate) async fn log_provider_failure(kind: &str, name: &str, msg: &str, log_path: &str) {
    let tail = tail_log_file(log_path, 5).await;
    if tail.is_empty() {
        tracing::error!("{kind} failed for {name}: {msg}");
    } else {
        let indented: String = tail
            .lines()
            .map(|l| format!("    {l}"))
            .collect::<Vec<_>>()
            .join("\n");
        tracing::error!("{kind} failed for {name}: {msg}\n  log tail:\n{indented}");
    }
}

/// Send SIGTERM then SIGKILL (after 2 s) to the process group identified by
/// `pgid`.
///
/// Skips the kill entirely when `raw == 0` (not running) or `raw == u32::MAX`
/// (spawning sentinel). SIGKILL on an already-exited group yields ESRCH,
/// which is logged at DEBUG and otherwise ignored.
#[cfg(unix)]
pub(crate) async fn terminate_pgid(pgid: &AtomicU32, name: &str) {
    let raw = pgid.load(Ordering::Acquire);
    if raw == 0 || raw == u32::MAX {
        return;
    }
    let pid = Pid::from_raw(-(i32::try_from(raw).unwrap_or(i32::MAX)));
    let _ = signal::kill(pid, Signal::SIGTERM);
    tracing::debug!("{}: sent SIGTERM to pgid {}", name, raw);
    tokio::time::sleep(Duration::from_secs(2)).await;
    if let Err(e) = signal::kill(pid, Signal::SIGKILL) {
        tracing::debug!(
            "{}: SIGKILL pgid {}: {} (likely already exited)",
            name,
            raw,
            e
        );
    }
}

/// Inject the standard HUSTSYNC_* environment variables into `cmd`.
///
/// Called after any provider-specific credentials (USER, RSYNC_PASSWORD) have
/// already been set, so those are not overwritten here. Hook-injected variables
/// from `ctx_env` are layered on top so they win over the config defaults.
pub(crate) fn inject_provider_env(
    cmd: &mut tokio::process::Command,
    common: &CommonProviderConfig,
    effective_log_file: &str,
    ctx_env: &HashMap<String, String>,
) {
    cmd.env("HUSTSYNC_MIRROR_NAME", &common.name)
        .env("HUSTSYNC_WORKING_DIR", &common.working_dir)
        .env("HUSTSYNC_UPSTREAM_URL", &common.upstream_url)
        .env("HUSTSYNC_LOG_DIR", &common.log_dir)
        .env("HUSTSYNC_LOG_FILE", effective_log_file);
    for (k, v) in &common.env {
        cmd.env(k, v);
    }
    for (k, v) in ctx_env {
        cmd.env(k, v);
    }
}

/// Generate the 10 boilerplate `MirrorProvider` getter methods that every
/// provider delegates to `self.config.common`.
///
/// Usage:
/// ```ignore
/// impl_provider_getters!(MyProvider, ProviderType::MyVariant);
/// ```
///
/// The macro must be invoked inside an `impl MirrorProvider for $ty` block or
/// as a standalone item — it only generates the listed methods so that each
/// provider can still implement `run()`, `terminate()`, and `data_size()`
/// manually.
macro_rules! impl_provider_getters {
    ($ty:ty, $variant:expr) => {
        fn name(&self) -> &str {
            &self.config.common.name
        }

        fn upstream(&self) -> &str {
            &self.config.common.upstream_url
        }

        fn provider_type(&self) -> $crate::provider::ProviderType {
            $variant
        }

        fn interval(&self) -> ::std::time::Duration {
            self.config.common.interval
        }

        fn retry(&self) -> u32 {
            self.config.common.retry
        }

        fn timeout(&self) -> ::std::time::Duration {
            self.config.common.timeout
        }

        fn working_dir(&self) -> &::std::path::Path {
            ::std::path::Path::new(&self.config.common.working_dir)
        }

        fn log_dir(&self) -> &::std::path::Path {
            ::std::path::Path::new(&self.config.common.log_dir)
        }

        fn log_file(&self) -> &::std::path::Path {
            ::std::path::Path::new(&self.config.common.log_file)
        }

        fn is_master(&self) -> bool {
            self.config.common.is_master
        }
    };
}

pub(crate) use impl_provider_getters;

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
/// `env` contains hook-injected variables (e.g. the rotated
/// `TUNASYNC_LOG_FILE` from the loglimit hook). Providers layer it on
/// top of their standard env vars so hook overrides win.
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

    let mirror_dir = if let Some(ref explicit) = m_cfg.mirror_dir {
        // Per-mirror mirror_dir is set — use as-is (template-expanded).
        format_path(explicit, name)
    } else {
        // No per-mirror mirror_dir: Go joins global.mirror_dir / sub_dir / name.
        let base_raw = global
            .and_then(|g| g.mirror_dir.as_deref())
            .unwrap_or("/tmp/hustsync");
        let base = expand_tilde(base_raw);
        let sub = m_cfg.mirror_subdir.as_deref().unwrap_or("");
        let mut p = std::path::PathBuf::from(base);
        if !sub.is_empty() {
            p.push(sub);
        }
        p.push(name);
        p.to_string_lossy().into_owned()
    };

    let is_master = m_cfg.role.as_deref() != Some("slave");
    let p_type = m_cfg.provider.as_deref().unwrap_or("rsync");

    let common = CommonProviderConfig {
        name: name.to_string(),
        upstream_url: m_cfg.upstream.clone().unwrap_or_default(),
        working_dir: mirror_dir,
        log_dir,
        log_file,
        interval: Duration::from_secs(interval as u64 * 60),
        retry,
        timeout: Duration::from_secs(timeout as u64),
        env: m_cfg.env.clone().unwrap_or_default(),
        is_master,
    };

    match p_type {
        "command" => {
            let cfg = CmdProviderConfig {
                command: m_cfg.command.clone().unwrap_or_default(),
                fail_on_match: m_cfg.fail_on_match.clone(),
                size_pattern: m_cfg.size_pattern.clone(),
                common,
            };
            Ok(Box::new(CmdProvider::new(cfg)?))
        }
        "rsync" => {
            let cfg = RsyncProviderConfig {
                command: m_cfg.command.clone().unwrap_or_else(|| "rsync".to_string()),
                username: m_cfg.username.clone(),
                password: m_cfg.password.clone(),
                exclude_file: m_cfg.exclude_file.as_deref().map(expand_tilde),
                rsync_options: m_cfg.rsync_options.clone().unwrap_or_default(),
                global_options: global
                    .and_then(|g| g.rsync_options.clone())
                    .unwrap_or_default(),
                rsync_override: m_cfg.rsync_override.clone(),
                rsync_override_only: m_cfg.rsync_override_only.unwrap_or(false),
                rsync_no_timeout: m_cfg.rsync_no_timeout.unwrap_or(false),
                rsync_timeout: m_cfg.rsync_timeout,
                use_ipv6: m_cfg.use_ipv6.unwrap_or(false),
                use_ipv4: m_cfg.use_ipv4.unwrap_or(false),
                common,
            };
            Ok(Box::new(RsyncProvider::new(cfg)?))
        }
        "two-stage-rsync" => {
            let cfg = TwoStageRsyncProviderConfig {
                command: m_cfg.command.clone().unwrap_or_else(|| "rsync".to_string()),
                stage1_profile: m_cfg
                    .stage1_profile
                    .clone()
                    .unwrap_or_else(|| "debian".to_string()),
                username: m_cfg.username.clone(),
                password: m_cfg.password.clone(),
                exclude_file: m_cfg.exclude_file.as_deref().map(expand_tilde),
                extra_options: m_cfg.rsync_options.clone().unwrap_or_default(),
                rsync_no_timeout: m_cfg.rsync_no_timeout.unwrap_or(false),
                rsync_timeout: m_cfg.rsync_timeout,
                use_ipv6: m_cfg.use_ipv6.unwrap_or(false),
                use_ipv4: m_cfg.use_ipv4.unwrap_or(false),
                common,
            };
            Ok(Box::new(TwoStageRsyncProvider::new(cfg)?))
        }
        _ => Err(ProviderError::Config(format!(
            "unknown provider type `{p_type}`"
        ))),
    }
}
