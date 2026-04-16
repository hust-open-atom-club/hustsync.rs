use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
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

/// Base rsync arguments shared by both the standard rsync provider (full
/// sync) and the two-stage provider's stage-2 pass.
pub(crate) const BASE_RSYNC_ARGS: &[&str] = &[
    "-aHvh",
    "--no-o",
    "--no-g",
    "--stats",
    "--filter",
    "risk .~tmp~/",
    "--exclude",
    ".~tmp~/",
    "--delete",
    "--delete-after",
    "--delay-updates",
    "--safe-links",
];

/// Stage-1 subset of `BASE_RSYNC_ARGS` — omits `--delete`, `--delete-after`,
/// and `--delay-updates` so that the first pass only fetches without removing
/// anything. The delete pass is left entirely to stage 2.
pub(crate) const BASE_RSYNC_STAGE1_ARGS: &[&str] = &[
    "-aHvh",
    "--no-o",
    "--no-g",
    "--stats",
    "--filter",
    "risk .~tmp~/",
    "--exclude",
    ".~tmp~/",
    "--safe-links",
];

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
    /// Exit codes that are treated as success in addition to 0.
    ///
    /// Merged from global and per-mirror config at construction time.
    /// Rsync-specific codes are only included for rsync/two-stage-rsync
    /// providers.
    pub success_exit_codes: Vec<i32>,
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

/// Resolve the effective log file path for a sync run.
///
/// A pre-exec hook (e.g. loglimit) may have rotated the log to a
/// timestamped path and written the new path into `ctx.env`. When the
/// key is present it takes precedence; otherwise the provider's
/// configured default is used.
pub(crate) fn resolve_log_file(ctx: &RunContext, default: &str) -> String {
    ctx.env
        .get("TUNASYNC_LOG_FILE")
        .cloned()
        .unwrap_or_else(|| default.to_string())
}

/// Store the total transfer size parsed from an rsync log file.
///
/// Extracts the size from `log_file` and writes it to `data_size` when
/// non-empty. A parse failure (malformed or absent stats block) is
/// silently treated as "no size available" — callers use this for
/// informational reporting only, never for control flow.
pub(crate) async fn store_rsync_data_size(
    data_size: &Mutex<Option<String>>,
    log_file: &str,
) {
    let size =
        hustsync_internal::util::extract_size_from_rsync_log(log_file).unwrap_or_default();
    if !size.is_empty() {
        *data_size.lock().await = Some(size);
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

/// Await a spawned child process, honoring both timeout and cancellation.
///
/// Returns `Ok(ExitStatus)` on normal exit, or `Err(ProviderError)` for
/// cancellation, timeout, or I/O error. On cancel/timeout the process group
/// is terminated via `terminate_pgid` and the child is waited to avoid
/// zombies.
///
/// Pass `Duration::ZERO` for `timeout_dur` to disable timeout (cancellation
/// still applies). Providers that manage timeout at an outer layer (e.g.
/// two-stage wrapping both stages in one budget) should pass `ZERO` here.
#[allow(clippy::cognitive_complexity)]
pub(crate) async fn run_child_with_cancellation(
    child: &mut tokio::process::Child,
    timeout_dur: Duration,
    cancel: &CancellationToken,
    pgid: &AtomicU32,
    name: &str,
) -> Result<std::process::ExitStatus, ProviderError> {
    if timeout_dur == Duration::ZERO {
        tokio::select! {
            wait_res = child.wait() => {
                wait_res.map_err(ProviderError::Io)
            }
            _ = cancel.cancelled() => {
                tracing::warn!("Provider {} cancelled", name);
                terminate_pgid(pgid, name).await;
                let _ = child.wait().await;
                Err(ProviderError::Terminated)
            }
        }
    } else {
        match tokio::time::timeout(timeout_dur, async {
            tokio::select! {
                wait_res = child.wait() => wait_res.map(Some),
                _ = cancel.cancelled() => Ok(None),
            }
        })
        .await
        {
            Ok(Ok(Some(status))) => Ok(status),
            Ok(Ok(None)) => {
                tracing::warn!("Provider {} cancelled", name);
                terminate_pgid(pgid, name).await;
                let _ = child.wait().await;
                Err(ProviderError::Terminated)
            }
            Ok(Err(e)) => Err(ProviderError::Io(e)),
            Err(_elapsed) => {
                tracing::warn!("Timeout occurred for {}", name);
                terminate_pgid(pgid, name).await;
                let _ = child.wait().await;
                Err(ProviderError::Timeout(timeout_dur))
            }
        }
    }
}

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

    // Merge success exit code allowlists.  Order mirrors Go worker/provider.go:
    // global generic → per-mirror generic → global rsync-specific →
    // per-mirror rsync-specific.  Rsync-specific codes only apply to
    // rsync/two-stage-rsync; a non-rsync mirror that sets
    // rsync_success_exit_codes gets a warning and the codes are dropped.
    let success_exit_codes = {
        let mut codes: Vec<i32> = Vec::new();

        if let Some(gc) = global.and_then(|g| g.dangerous_global_success_exit_codes.as_ref()) {
            codes.extend(gc);
        }
        if let Some(mc) = m_cfg.success_exit_codes.as_ref() {
            codes.extend(mc);
        }

        let is_rsync_provider = matches!(p_type, "rsync" | "two-stage-rsync");
        if is_rsync_provider {
            if let Some(grc) = global
                .and_then(|g| g.dangerous_global_rsync_success_exit_codes.as_ref())
            {
                codes.extend(grc);
            }
            if let Some(mrc) = m_cfg.rsync_success_exit_codes.as_ref() {
                codes.extend(mrc);
            }
        } else if m_cfg.rsync_success_exit_codes.is_some() {
            tracing::warn!(
                "mirror {}: rsync_success_exit_codes is set but provider is '{}', ignoring",
                name,
                p_type
            );
        }

        // Deduplicate while preserving insertion order.
        let mut seen = std::collections::HashSet::new();
        codes.retain(|c| seen.insert(*c));
        codes
    };

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
        success_exit_codes,
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
