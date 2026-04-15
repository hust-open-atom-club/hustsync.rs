use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::fs::{File, create_dir_all};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time::timeout;

#[cfg(unix)]
use nix::sys::signal::{self, Signal};
#[cfg(unix)]
use nix::unistd::Pid;

use hustsync_internal::util::{extract_size_from_rsync_log, translate_rsync_exit_status};

use super::{MirrorProvider, ProviderError, ProviderType, RunContext};

pub struct RsyncProviderConfig {
    pub name: String,
    pub command: String,
    pub upstream_url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub exclude_file: Option<String>,
    pub rsync_options: Vec<String>,
    pub global_options: Vec<String>,
    pub rsync_override: Option<Vec<String>>,
    pub rsync_override_only: bool,
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

pub struct RsyncProvider {
    config: RsyncProviderConfig,
    data_size: Mutex<Option<String>>,
    run_lock: Mutex<()>,
    running_pgid: AtomicU32,
}

impl RsyncProvider {
    pub fn new(mut config: RsyncProviderConfig) -> Result<Self, ProviderError> {
        if !config.upstream_url.ends_with('/') {
            return Err(ProviderError::Config(
                "rsync upstream URL should end with /".into(),
            ));
        }
        if config.rsync_override_only && config.rsync_override.is_none() {
            return Err(ProviderError::Config(
                "rsync_override_only is set but no rsync_override provided".into(),
            ));
        }
        if config.retry == 0 {
            config.retry = 2;
        }
        if config.command.is_empty() {
            config.command = "rsync".to_string();
        }

        Ok(Self {
            config,
            data_size: Mutex::new(None),
            run_lock: Mutex::new(()),
            running_pgid: AtomicU32::new(0),
        })
    }

    pub fn build_args(&self) -> Vec<String> {
        let mut options = if let Some(overridden) = &self.config.rsync_override {
            overridden.clone()
        } else {
            // Default options
            vec![
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
            ]
        };

        if !self.config.rsync_override_only {
            if !self.config.rsync_no_timeout {
                // Go newRsyncProvider: timeo := 120; if c.rsyncTimeoutValue > 0 { timeo = ... }
                // Zero means "use default 120"; it does NOT emit --timeout=0.
                let timeo = self.config.rsync_timeout.filter(|&v| v > 0).unwrap_or(120);
                options.push(format!("--timeout={}", timeo));
            }

            if self.config.use_ipv6 {
                options.push("--ipv6".to_string());
            } else if self.config.use_ipv4 {
                options.push("--ipv4".to_string());
            }

            if let Some(exclude_file) = &self.config.exclude_file {
                options.push("--exclude-from".to_string());
                options.push(exclude_file.clone());
            }

            options.extend(self.config.global_options.clone());
            options.extend(self.config.rsync_options.clone());
        }

        let mut args = options;
        args.push(self.config.upstream_url.clone());
        args.push(self.config.working_dir.clone());

        args
    }
}

#[async_trait]
impl MirrorProvider for RsyncProvider {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn upstream(&self) -> &str {
        &self.config.upstream_url
    }

    fn provider_type(&self) -> ProviderType {
        ProviderType::Rsync
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
                "Rsync provider {} re-entering on attempt {}",
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

        let mut log_file = File::create(&self.config.log_file).await?;
        let std_out_log = log_file.try_clone().await?.into_std().await;
        let std_err_log = log_file.try_clone().await?.into_std().await;

        let mut cmd = Command::new(&self.config.command);
        cmd.args(self.build_args());

        cmd.current_dir(&self.config.working_dir)
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

        // Standard hustsync env vars
        cmd.env("HUSTSYNC_MIRROR_NAME", &self.config.name)
            .env("HUSTSYNC_WORKING_DIR", &self.config.working_dir)
            .env("HUSTSYNC_UPSTREAM_URL", &self.config.upstream_url)
            .env("HUSTSYNC_LOG_DIR", &self.config.log_dir)
            .env("HUSTSYNC_LOG_FILE", &self.config.log_file);

        // Per-mirror config env
        for (k, v) in &self.config.env {
            cmd.env(k, v);
        }

        // Hook-injected env wins over everything above
        for (k, v) in &ctx.env {
            cmd.env(k, v);
        }

        tracing::info!("Starting rsync provider for {}", self.config.name);

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

        let result = if self.config.timeout == Duration::ZERO {
            // Timeout disabled — still honour cancellation
            tokio::select! {
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
                                let msg = msg.unwrap_or_else(|| format!("rsync exited with status: {}", status));
                                tracing::error!("Rsync failed for {}: {}", self.config.name, msg);
                                Err(ProviderError::Execution { code, msg })
                            }
                        }
                        Err(e) => Err(ProviderError::Io(e)),
                    }
                }
                _ = ctx.cancel.cancelled() => {
                    tracing::warn!("Rsync provider {} cancelled", self.config.name);
                    let _ = self.terminate().await;
                    let _ = spawned_child.wait().await;
                    Err(ProviderError::Terminated)
                }
            }
        } else {
            match timeout(self.config.timeout, async {
                tokio::select! {
                    wait_res = spawned_child.wait() => wait_res.map(Some),
                    _ = ctx.cancel.cancelled() => Ok(None),
                }
            })
            .await
            {
                Ok(Ok(Some(status))) => {
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
                        let msg =
                            msg.unwrap_or_else(|| format!("rsync exited with status: {}", status));
                        tracing::error!("Rsync failed for {}: {}", self.config.name, msg);
                        Err(ProviderError::Execution { code, msg })
                    }
                }
                Ok(Ok(None)) => {
                    // cancelled
                    tracing::warn!("Rsync provider {} cancelled", self.config.name);
                    let _ = self.terminate().await;
                    let _ = spawned_child.wait().await;
                    Err(ProviderError::Terminated)
                }
                Ok(Err(e)) => Err(ProviderError::Io(e)),
                Err(_elapsed) => {
                    tracing::warn!("Timeout occurred for {}", self.config.name);
                    let _ = self.terminate().await;
                    let _ = spawned_child.wait().await;
                    Err(ProviderError::Timeout(self.config.timeout))
                }
            }
        };

        self.running_pgid.store(0, Ordering::Release);

        if result.is_ok() {
            let size = extract_size_from_rsync_log(&self.config.log_file).unwrap_or_default();
            if !size.is_empty() {
                let mut size_guard = self.data_size.lock().await;
                *size_guard = Some(size);
            }
        }

        result
    }

    async fn terminate(&self) -> Result<(), ProviderError> {
        let pid = self.running_pgid.load(Ordering::Acquire);
        if pid != 0 {
            tracing::warn!("Terminating rsync provider for {}", self.config.name);
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_rsync_args_basic() {
        let config = RsyncProviderConfig {
            name: "test".to_string(),
            command: "rsync".to_string(),
            upstream_url: "rsync://example.com/test/".to_string(),
            username: None,
            password: None,
            exclude_file: None,
            rsync_options: vec![],
            global_options: vec![],
            rsync_override: None,
            rsync_override_only: false,
            rsync_no_timeout: false,
            rsync_timeout: None,
            env: HashMap::new(),
            working_dir: "/tmp/test".to_string(),
            log_dir: "/tmp/log".to_string(),
            log_file: "/tmp/log/test.log".to_string(),
            use_ipv6: false,
            use_ipv4: false,
            interval: Duration::from_secs(60),
            retry: 2,
            timeout: Duration::from_secs(3600),
            is_master: true,
        };
        let provider = RsyncProvider::new(config).unwrap();
        let args = provider.build_args();

        assert!(args.contains(&"-aHvh".to_string()));
        assert!(args.contains(&"--delete".to_string()));
        assert!(args.contains(&"--timeout=120".to_string()));
        assert_eq!(args.last().unwrap(), "/tmp/test");
        assert_eq!(args[args.len() - 2], "rsync://example.com/test/");
    }

    #[test]
    fn test_rsync_args_override() {
        let config = RsyncProviderConfig {
            name: "test".to_string(),
            command: "rsync".to_string(),
            upstream_url: "rsync://example.com/test/".to_string(),
            username: None,
            password: None,
            exclude_file: None,
            rsync_options: vec![],
            global_options: vec![],
            rsync_override: Some(vec!["-az".to_string()]),
            rsync_override_only: true,
            rsync_no_timeout: false,
            rsync_timeout: None,
            env: HashMap::new(),
            working_dir: "/tmp/test".to_string(),
            log_dir: "/tmp/log".to_string(),
            log_file: "/tmp/log/test.log".to_string(),
            use_ipv6: false,
            use_ipv4: false,
            interval: Duration::from_secs(60),
            retry: 2,
            timeout: Duration::from_secs(3600),
            is_master: true,
        };
        let provider = RsyncProvider::new(config).unwrap();
        let args = provider.build_args();

        assert_eq!(args[0], "-az");
        assert_eq!(args[1], "rsync://example.com/test/");
        assert_eq!(args[2], "/tmp/test");
        assert!(!args.contains(&"-aHvh".to_string()));
    }

    #[test]
    fn test_rsync_args_ipv6_and_exclude() {
        let config = RsyncProviderConfig {
            name: "test".to_string(),
            command: "rsync".to_string(),
            upstream_url: "rsync://example.com/test/".to_string(),
            username: None,
            password: None,
            exclude_file: Some("/tmp/exclude.txt".to_string()),
            rsync_options: vec!["--bwlimit=1000".to_string()],
            global_options: vec!["--global".to_string()],
            rsync_override: None,
            rsync_override_only: false,
            rsync_no_timeout: true,
            rsync_timeout: None,
            env: HashMap::new(),
            working_dir: "/tmp/test".to_string(),
            log_dir: "/tmp/log".to_string(),
            log_file: "/tmp/log/test.log".to_string(),
            use_ipv6: true,
            use_ipv4: false,
            interval: Duration::from_secs(60),
            retry: 2,
            timeout: Duration::from_secs(3600),
            is_master: true,
        };
        let provider = RsyncProvider::new(config).unwrap();
        let args = provider.build_args();

        assert!(args.contains(&"--ipv6".to_string()));
        assert!(args.contains(&"--exclude-from".to_string()));
        assert!(args.contains(&"/tmp/exclude.txt".to_string()));
        assert!(args.contains(&"--bwlimit=1000".to_string()));
        assert!(args.contains(&"--global".to_string()));
        assert!(!args.iter().any(|a| a.starts_with("--timeout=")));
    }

    #[test]
    fn test_rsync_upstream_validation() {
        let config = RsyncProviderConfig {
            name: "test".to_string(),
            command: "rsync".to_string(),
            upstream_url: "rsync://example.com/test".to_string(), // No trailing slash
            username: None,
            password: None,
            exclude_file: None,
            rsync_options: vec![],
            global_options: vec![],
            rsync_override: None,
            rsync_override_only: false,
            rsync_no_timeout: false,
            rsync_timeout: None,
            env: HashMap::new(),
            working_dir: "/tmp/test".to_string(),
            log_dir: "/tmp/log".to_string(),
            log_file: "/tmp/log/test.log".to_string(),
            use_ipv6: false,
            use_ipv4: false,
            interval: Duration::from_secs(60),
            retry: 2,
            timeout: Duration::from_secs(3600),
            is_master: true,
        };
        let res = RsyncProvider::new(config);
        assert!(res.is_err());
    }

    #[test]
    fn test_rsync_defaults_are_applied_in_provider() {
        let config = RsyncProviderConfig {
            name: "test".to_string(),
            command: "".to_string(),
            upstream_url: "rsync://example.com/test/".to_string(),
            username: None,
            password: None,
            exclude_file: None,
            rsync_options: vec![],
            global_options: vec![],
            rsync_override: None,
            rsync_override_only: false,
            rsync_no_timeout: false,
            rsync_timeout: None,
            env: HashMap::new(),
            working_dir: "/tmp/test".to_string(),
            log_dir: "/tmp/log".to_string(),
            log_file: "/tmp/log/test.log".to_string(),
            use_ipv6: false,
            use_ipv4: false,
            interval: Duration::from_secs(60),
            retry: 0,
            timeout: Duration::from_secs(3600),
            is_master: true,
        };

        let provider = RsyncProvider::new(config).unwrap();
        assert_eq!(provider.config.command, "rsync");
        assert_eq!(provider.retry(), 2);
    }

    #[test]
    fn test_rsync_override_only_requires_override() {
        let config = RsyncProviderConfig {
            name: "test".to_string(),
            command: "rsync".to_string(),
            upstream_url: "rsync://example.com/test/".to_string(),
            username: None,
            password: None,
            exclude_file: None,
            rsync_options: vec![],
            global_options: vec![],
            rsync_override: None,
            rsync_override_only: true,
            rsync_no_timeout: false,
            rsync_timeout: None,
            env: HashMap::new(),
            working_dir: "/tmp/test".to_string(),
            log_dir: "/tmp/log".to_string(),
            log_file: "/tmp/log/test.log".to_string(),
            use_ipv6: false,
            use_ipv4: false,
            interval: Duration::from_secs(60),
            retry: 2,
            timeout: Duration::from_secs(3600),
            is_master: true,
        };

        let res = RsyncProvider::new(config);
        match res {
            Ok(_) => panic!("expected config validation error"),
            Err(err) => assert!(
                err.to_string()
                    .contains("rsync_override_only is set but no rsync_override provided")
            ),
        }
    }
}
