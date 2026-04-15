use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::fs::{File, OpenOptions, create_dir_all};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time::timeout;

#[cfg(unix)]
use nix::sys::signal::{self, Signal};
#[cfg(unix)]
use nix::unistd::Pid;

use hustsync_internal::util::{extract_size_from_rsync_log, translate_rsync_exit_status};

use super::{MirrorProvider, ProviderError, ProviderType, RunContext};

/// Stage-1 option sets keyed by profile name.
///
/// Verbatim translation of `rsyncStage1Profiles` in Go
/// `worker/two_stage_rsync_provider.go`. Keep in sync with Go when the
/// upstream map grows.
fn stage1_profiles() -> HashMap<&'static str, Vec<&'static str>> {
    let mut map = HashMap::new();
    map.insert(
        "debian",
        vec![
            "--include=*.diff/",
            "--include=by-hash/",
            "--exclude=*.diff/Index",
            "--exclude=Contents*",
            "--exclude=Packages*",
            "--exclude=Sources*",
            "--exclude=Release*",
            "--exclude=InRelease",
            "--exclude=i18n/*",
            "--exclude=dep11/*",
            "--exclude=installer-*/current",
            "--exclude=ls-lR*",
        ],
    );
    map.insert(
        "debian-oldstyle",
        vec![
            "--exclude=Packages*",
            "--exclude=Sources*",
            "--exclude=Release*",
            "--exclude=InRelease",
            "--exclude=i18n/*",
            "--exclude=ls-lR*",
            "--exclude=dep11/*",
        ],
    );
    map
}

/// Configuration for the two-stage rsync provider.
///
/// Both stages share most fields; only `stage1_profile` governs the
/// stage-1 filter set. The stage-2 argv is the standard rsync base
/// plus `extra_options` — identical to `RsyncProvider`.
pub struct TwoStageRsyncProviderConfig {
    pub name: String,
    pub command: String,
    pub stage1_profile: String,
    pub upstream_url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub exclude_file: Option<String>,
    pub extra_options: Vec<String>,
    pub rsync_no_timeout: bool,
    pub rsync_timeout: Option<u32>,
    pub env: HashMap<String, String>,
    pub working_dir: String,
    pub log_dir: String,
    pub log_file: String,
    pub use_ipv6: bool,
    pub use_ipv4: bool,
    pub interval: Duration,
    pub retry: u32,
    pub timeout: Duration,
    pub is_master: bool,
}

/// Two-stage rsync provider.
///
/// Stage 1 pulls metadata-critical files first (diff indices, by-hash
/// trees) via a named profile. Stage 2 completes the full sync with
/// `--delete`. The two-stage approach keeps Debian-style mirrors
/// consistent: consumers always see either the old or the new Release
/// file paired with matching package files — never a half-updated state.
pub struct TwoStageRsyncProvider {
    config: TwoStageRsyncProviderConfig,
    data_size: Mutex<Option<String>>,
    run_lock: Mutex<()>,
    /// PID of the currently running rsync process group.
    /// 0 = not running, u32::MAX = spawning (transient), otherwise live pgid.
    running_pgid: AtomicU32,
}

impl TwoStageRsyncProvider {
    pub fn new(mut config: TwoStageRsyncProviderConfig) -> Result<Self, ProviderError> {
        if !config.upstream_url.ends_with('/') {
            return Err(ProviderError::Config(
                "rsync upstream URL should end with /".into(),
            ));
        }
        if config.retry == 0 {
            config.retry = 2;
        }
        if config.command.is_empty() {
            config.command = "rsync".to_string();
        }

        // Validate the profile at construction time so failures are reported
        // before any sync attempt rather than mid-run.
        let profiles = stage1_profiles();
        if !profiles.contains_key(config.stage1_profile.as_str()) {
            return Err(ProviderError::Config(format!(
                "unknown stage1_profile: {}",
                config.stage1_profile
            )));
        }

        Ok(Self {
            config,
            data_size: Mutex::new(None),
            run_lock: Mutex::new(()),
            running_pgid: AtomicU32::new(0),
        })
    }

    /// Build argv for a single stage.
    ///
    /// Stage 1 uses the named profile's filter set (no `--delete`).
    /// Stage 2 uses the standard rsync base plus `extra_options`.
    /// Both stages apply the timeout / IP / exclude-file options
    /// verbatim to match Go's `Options(stage int)` method.
    pub(crate) fn build_args_for_stage(&self, stage: u8) -> Result<Vec<String>, ProviderError> {
        let mut options: Vec<String> = match stage {
            1 => {
                // Stage-1 base — verbatim from Go newTwoStageRsyncProvider
                let mut opts = vec![
                    "-aHvh".to_string(),
                    "--no-o".to_string(),
                    "--no-g".to_string(),
                    "--stats".to_string(),
                    "--filter".to_string(),
                    "risk .~tmp~/".to_string(),
                    "--exclude".to_string(),
                    ".~tmp~/".to_string(),
                    "--safe-links".to_string(),
                ];
                // Append profile filter rules
                let profiles = stage1_profiles();
                let profile_opts = profiles
                    .get(self.config.stage1_profile.as_str())
                    .ok_or_else(|| {
                        ProviderError::Config(format!(
                            "unknown stage1_profile: {}",
                            self.config.stage1_profile
                        ))
                    })?;
                for opt in profile_opts {
                    opts.push(opt.to_string());
                }
                opts
            }
            2 => {
                // Stage-2 base — verbatim from Go newTwoStageRsyncProvider
                let mut opts = vec![
                    "-aHvh".to_string(),
                    "--no-o".to_string(),
                    "--no-g".to_string(),
                    "--stats".to_string(),
                    "--filter".to_string(),
                    "risk .~tmp~/".to_string(),
                    "--exclude".to_string(),
                    ".~tmp~/".to_string(),
                    "--delete".to_string(),
                    "--delete-after".to_string(),
                    "--delay-updates".to_string(),
                    "--safe-links".to_string(),
                ];
                // Stage 2 appends extra_options (Go's p.extraOptions)
                opts.extend(self.config.extra_options.iter().cloned());
                opts
            }
            _ => {
                return Err(ProviderError::Config(format!("invalid stage: {}", stage)));
            }
        };

        // Timeout — applied to both stages (Go: if !p.rsyncNeverTimeout)
        if !self.config.rsync_no_timeout {
            let timeo = self.config.rsync_timeout.filter(|&v| v > 0).unwrap_or(120);
            options.push(format!("--timeout={}", timeo));
        }

        // IP mode — short flags `-6` / `-4` to match Go verbatim.
        if self.config.use_ipv6 {
            options.push("-6".to_string());
        } else if self.config.use_ipv4 {
            options.push("-4".to_string());
        }

        // Exclude file — both stages
        if let Some(ref exclude_file) = self.config.exclude_file {
            options.push("--exclude-from".to_string());
            options.push(exclude_file.clone());
        }

        // Positional args
        options.push(self.config.upstream_url.clone());
        options.push(self.config.working_dir.clone());

        Ok(options)
    }

    /// Spawn one rsync stage and await its completion, honoring the shared
    /// cancellation token.  The `log_file` handle is passed in so that both
    /// stages append to the same file (Go appends; no separator written).
    #[allow(clippy::cognitive_complexity)]
    async fn run_stage(
        &self,
        stage: u8,
        log_file: &mut File,
        ctx: &RunContext,
    ) -> Result<(), ProviderError> {
        let args = self.build_args_for_stage(stage)?;

        let std_out_log = log_file.try_clone().await?.into_std().await;
        let std_err_log = log_file.try_clone().await?.into_std().await;

        let mut cmd = Command::new(&self.config.command);
        cmd.args(&args)
            .current_dir(&self.config.working_dir)
            .stdout(Stdio::from(std_out_log))
            .stderr(Stdio::from(std_err_log));

        #[cfg(unix)]
        {
            cmd.process_group(0);
        }

        if let Some(user) = &self.config.username {
            cmd.env("USER", user);
        }
        if let Some(password) = &self.config.password {
            cmd.env("RSYNC_PASSWORD", password);
        }

        cmd.env("HUSTSYNC_MIRROR_NAME", &self.config.name)
            .env("HUSTSYNC_WORKING_DIR", &self.config.working_dir)
            .env("HUSTSYNC_UPSTREAM_URL", &self.config.upstream_url)
            .env("HUSTSYNC_LOG_DIR", &self.config.log_dir)
            .env("HUSTSYNC_LOG_FILE", &self.config.log_file);

        for (k, v) in &self.config.env {
            cmd.env(k, v);
        }

        // Hook-injected env wins over everything above
        for (k, v) in &ctx.env {
            cmd.env(k, v);
        }

        tracing::info!(
            "Starting two-stage-rsync provider for {} (stage {})",
            self.config.name,
            stage
        );

        let mut spawned_child = match cmd.spawn() {
            Ok(child) => child,
            Err(err) => {
                self.running_pgid.store(0, Ordering::Release);
                return Err(ProviderError::Io(err));
            }
        };

        if let Some(pid) = spawned_child.id() {
            self.running_pgid.store(pid, Ordering::Release);
        } else {
            self.running_pgid.store(0, Ordering::Release);
            return Err(ProviderError::Execution {
                code: -1,
                msg: "failed to determine rsync process id".into(),
            });
        }

        let wait_result = tokio::select! {
            wait_res = spawned_child.wait() => {
                match wait_res {
                    Ok(status) => {
                        if status.success() {
                            Ok(())
                        } else {
                            let (code, msg) = translate_rsync_exit_status(&status);
                            let code = code.unwrap_or(-1);
                            if let Some(ref m) = msg {
                                use tokio::io::AsyncWriteExt;
                                let _ = log_file.write_all(m.as_bytes()).await;
                                let _ = log_file.write_all(b"\n").await;
                            }
                            let msg = msg.unwrap_or_else(|| {
                                format!("rsync stage {} exited with status: {}", stage, status)
                            });
                            tracing::error!(
                                "Two-stage-rsync stage {} failed for {}: {}",
                                stage,
                                self.config.name,
                                msg
                            );
                            Err(ProviderError::Execution { code, msg })
                        }
                    }
                    Err(e) => Err(ProviderError::Io(e)),
                }
            }
            _ = ctx.cancel.cancelled() => {
                tracing::warn!(
                    "Two-stage-rsync provider {} cancelled during stage {}",
                    self.config.name,
                    stage
                );
                let _ = self.terminate().await;
                let _ = spawned_child.wait().await;
                Err(ProviderError::Terminated)
            }
        };

        // Reset pgid after each stage so terminate() is a no-op between stages
        self.running_pgid.store(0, Ordering::Release);

        wait_result
    }
}

#[async_trait]
impl MirrorProvider for TwoStageRsyncProvider {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn upstream(&self) -> &str {
        &self.config.upstream_url
    }

    fn provider_type(&self) -> ProviderType {
        ProviderType::TwoStageRsync
    }

    fn interval(&self) -> Duration {
        self.config.interval
    }

    fn retry(&self) -> u32 {
        self.config.retry
    }

    fn timeout(&self) -> Duration {
        self.config.timeout
    }

    fn working_dir(&self) -> &Path {
        Path::new(&self.config.working_dir)
    }

    fn log_dir(&self) -> &Path {
        Path::new(&self.config.log_dir)
    }

    fn log_file(&self) -> &Path {
        Path::new(&self.config.log_file)
    }

    async fn run(&self, ctx: RunContext) -> Result<(), ProviderError> {
        if ctx.attempt > 1 {
            tracing::debug!(
                "Two-stage-rsync provider {} re-entering on attempt {}",
                self.config.name,
                ctx.attempt
            );
        }

        let _run_guard = self.run_lock.lock().await;

        if self.running_pgid.load(Ordering::Acquire) != 0 {
            return Err(ProviderError::AlreadyRunning);
        }
        self.running_pgid.store(u32::MAX, Ordering::Release);

        {
            let mut size_guard = self.data_size.lock().await;
            *size_guard = None;
        }

        create_dir_all(&self.config.working_dir).await?;
        create_dir_all(&self.config.log_dir).await?;

        // Pre_exec hook may rotate the log path; honor `ctx.env`
        // before opening the file handle. Fall back to config default.
        let effective_log_file = ctx
            .env
            .get("TUNASYNC_LOG_FILE")
            .cloned()
            .unwrap_or_else(|| self.config.log_file.clone());

        // Both stages append to the same log file; stage-2 output follows
        // stage-1 without a separator to match Go's `prepareLogFile`.
        let mut log_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&effective_log_file)
            .await?;

        use tokio::io::AsyncSeekExt;
        log_file.seek(std::io::SeekFrom::End(0)).await?;

        // Wrap the entire two-stage run in a single timeout budget.
        let run_body = async {
            // Stage 1: quick sync of metadata-critical files
            self.run_stage(1, &mut log_file, &ctx).await?;
            // Stage 2: full sync — only reached if stage 1 succeeded
            self.run_stage(2, &mut log_file, &ctx).await
        };

        let result = if self.config.timeout == Duration::ZERO {
            run_body.await
        } else {
            match timeout(self.config.timeout, run_body).await {
                Ok(inner) => inner,
                Err(_elapsed) => {
                    tracing::warn!("Timeout occurred for {}", self.config.name);
                    let _ = self.terminate().await;
                    Err(ProviderError::Timeout(self.config.timeout))
                }
            }
        };

        self.running_pgid.store(0, Ordering::Release);

        if result.is_ok() {
            let size = extract_size_from_rsync_log(&effective_log_file).unwrap_or_default();
            if !size.is_empty() {
                let mut size_guard = self.data_size.lock().await;
                *size_guard = Some(size);
            }
        }

        result
    }

    async fn terminate(&self) -> Result<(), ProviderError> {
        let pid = self.running_pgid.load(Ordering::Acquire);
        if pid != 0 && pid != u32::MAX {
            tracing::warn!(
                "Terminating two-stage-rsync provider for {}",
                self.config.name
            );
            #[cfg(unix)]
            {
                let pgid = Pid::from_raw(-(pid as i32));
                if let Err(e) = signal::kill(pgid, Signal::SIGKILL) {
                    tracing::debug!("Failed to send SIGKILL to pgid {}: {}", pgid, e);
                }
            }
        }
        Ok(())
    }

    async fn data_size(&self) -> Option<String> {
        self.data_size.lock().await.clone()
    }

    fn is_master(&self) -> bool {
        self.config.is_master
    }
}
