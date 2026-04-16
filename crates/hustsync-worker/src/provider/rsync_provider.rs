use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use tokio::fs::{File, create_dir_all};
use tokio::process::Command;
use tokio::sync::Mutex;

use hustsync_internal::util::translate_rsync_exit_status;

use super::{
    BASE_RSYNC_ARGS, CommonProviderConfig, MirrorProvider, ProviderError, ProviderType,
    RunContext, impl_provider_getters, inject_provider_env, log_provider_failure,
    resolve_log_file, run_child_with_cancellation, store_rsync_data_size,
};

pub struct RsyncProviderConfig {
    pub common: CommonProviderConfig,
    pub command: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub exclude_file: Option<String>,
    pub rsync_options: Vec<String>,
    pub global_options: Vec<String>,
    pub rsync_override: Option<Vec<String>>,
    pub rsync_override_only: bool,
    pub rsync_no_timeout: bool,
    pub rsync_timeout: Option<u32>,
    pub use_ipv6: bool,
    pub use_ipv4: bool,
}

pub struct RsyncProvider {
    config: RsyncProviderConfig,
    data_size: Mutex<Option<String>>,
    run_lock: Mutex<()>,
    running_pgid: AtomicU32,
}

impl RsyncProvider {
    pub fn new(mut config: RsyncProviderConfig) -> Result<Self, ProviderError> {
        if !config.common.upstream_url.ends_with('/') {
            return Err(ProviderError::Config(
                "rsync upstream URL should end with /".into(),
            ));
        }
        if config.rsync_override_only && config.rsync_override.is_none() {
            return Err(ProviderError::Config(
                "rsync_override_only is set but no rsync_override provided".into(),
            ));
        }
        if config.common.retry == 0 {
            config.common.retry = 2;
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
            BASE_RSYNC_ARGS.iter().map(|s| s.to_string()).collect()
        };

        if !self.config.rsync_override_only {
            if !self.config.rsync_no_timeout {
                // Go newRsyncProvider: timeo := 120; if c.rsyncTimeoutValue > 0 { timeo = ... }
                // Zero means "use default 120"; it does NOT emit --timeout=0.
                let timeo = self.config.rsync_timeout.filter(|&v| v > 0).unwrap_or(120);
                options.push(format!("--timeout={}", timeo));
            }

            if self.config.use_ipv6 {
                options.push("-6".to_string());
            } else if self.config.use_ipv4 {
                options.push("-4".to_string());
            }

            if let Some(exclude_file) = &self.config.exclude_file {
                options.push("--exclude-from".to_string());
                options.push(exclude_file.clone());
            }

            options.extend(self.config.global_options.clone());
            options.extend(self.config.rsync_options.clone());
        }

        let mut args = options;
        args.push(self.config.common.upstream_url.clone());
        args.push(self.config.common.working_dir.clone());

        args
    }
}

#[async_trait]
impl MirrorProvider for RsyncProvider {
    impl_provider_getters!(RsyncProvider, ProviderType::Rsync);

    async fn run(&self, ctx: RunContext) -> Result<(), ProviderError> {
        if ctx.attempt > 1 {
            tracing::debug!(
                "Rsync provider {} re-entering on attempt {}",
                self.config.common.name,
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

        create_dir_all(&self.config.common.working_dir).await?;
        create_dir_all(&self.config.common.log_dir).await?;

        // Loglimit (or any pre_exec hook) may have rotated the log path;
        // honor it before opening the file handle.
        let effective_log_file = resolve_log_file(&ctx, &self.config.common.log_file);

        let mut log_file = File::create(&effective_log_file).await?;
        let std_out_log = log_file.try_clone().await?.into_std().await;
        let std_err_log = log_file.try_clone().await?.into_std().await;

        let mut cmd = Command::new(&self.config.command);
        cmd.args(self.build_args());

        cmd.current_dir(&self.config.common.working_dir)
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

        inject_provider_env(&mut cmd, &self.config.common, &effective_log_file, &ctx.env);

        tracing::info!("Starting rsync provider for {}", self.config.common.name);

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

        let result = match run_child_with_cancellation(
            &mut spawned_child,
            self.config.common.timeout,
            &ctx.cancel,
            &self.running_pgid,
            &self.config.common.name,
        )
        .await
        {
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
                    let msg =
                        msg.unwrap_or_else(|| format!("rsync exited with status: {}", status));
                    log_provider_failure(
                        "Rsync",
                        &self.config.common.name,
                        &msg,
                        &effective_log_file,
                    )
                    .await;
                    Err(ProviderError::Execution { code, msg })
                }
            }
            Err(e) => Err(e),
        };

        self.running_pgid.store(0, Ordering::Release);

        if result.is_ok() {
            store_rsync_data_size(&self.data_size, &effective_log_file).await;
        }

        result
    }

    async fn terminate(&self) -> Result<(), ProviderError> {
        #[cfg(unix)]
        {
            if self.running_pgid.load(Ordering::Acquire) != 0 {
                tracing::warn!(
                    "Terminating rsync provider for {}",
                    self.config.common.name
                );
            }
            super::terminate_pgid(&self.running_pgid, &self.config.common.name).await;
        }
        Ok(())
    }

    async fn data_size(&self) -> Option<String> {
        self.data_size.lock().await.clone()
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments
)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    fn make_config(
        upstream_url: &str,
        rsync_override: Option<Vec<String>>,
        rsync_override_only: bool,
        rsync_no_timeout: bool,
        rsync_timeout: Option<u32>,
        use_ipv6: bool,
        use_ipv4: bool,
        rsync_options: Vec<String>,
        global_options: Vec<String>,
        exclude_file: Option<String>,
        command: &str,
        retry: u32,
    ) -> RsyncProviderConfig {
        RsyncProviderConfig {
            common: CommonProviderConfig {
                name: "test".to_string(),
                upstream_url: upstream_url.to_string(),
                working_dir: "/tmp/test".to_string(),
                log_dir: "/tmp/log".to_string(),
                log_file: "/tmp/log/test.log".to_string(),
                interval: Duration::from_secs(60),
                retry,
                timeout: Duration::from_secs(3600),
                env: HashMap::new(),
                is_master: true,
            },
            command: command.to_string(),
            username: None,
            password: None,
            exclude_file,
            rsync_options,
            global_options,
            rsync_override,
            rsync_override_only,
            rsync_no_timeout,
            rsync_timeout,
            use_ipv6,
            use_ipv4,
        }
    }

    #[test]
    fn test_rsync_args_basic() {
        let config = make_config(
            "rsync://example.com/test/",
            None,
            false,
            false,
            None,
            false,
            false,
            vec![],
            vec![],
            None,
            "rsync",
            2,
        );
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
        let config = make_config(
            "rsync://example.com/test/",
            Some(vec!["-az".to_string()]),
            true,
            false,
            None,
            false,
            false,
            vec![],
            vec![],
            None,
            "rsync",
            2,
        );
        let provider = RsyncProvider::new(config).unwrap();
        let args = provider.build_args();

        assert_eq!(args[0], "-az");
        assert_eq!(args[1], "rsync://example.com/test/");
        assert_eq!(args[2], "/tmp/test");
        assert!(!args.contains(&"-aHvh".to_string()));
    }

    #[test]
    fn test_rsync_args_ipv6_and_exclude() {
        let config = make_config(
            "rsync://example.com/test/",
            None,
            false,
            true,
            None,
            true,
            false,
            vec!["--bwlimit=1000".to_string()],
            vec!["--global".to_string()],
            Some("/tmp/exclude.txt".to_string()),
            "rsync",
            2,
        );
        let provider = RsyncProvider::new(config).unwrap();
        let args = provider.build_args();

        assert!(args.contains(&"-6".to_string()));
        assert!(args.contains(&"--exclude-from".to_string()));
        assert!(args.contains(&"/tmp/exclude.txt".to_string()));
        assert!(args.contains(&"--bwlimit=1000".to_string()));
        assert!(args.contains(&"--global".to_string()));
        assert!(!args.iter().any(|a| a.starts_with("--timeout=")));
    }

    #[test]
    fn test_rsync_upstream_validation() {
        let config = make_config(
            "rsync://example.com/test", // No trailing slash
            None,
            false,
            false,
            None,
            false,
            false,
            vec![],
            vec![],
            None,
            "rsync",
            2,
        );
        let res = RsyncProvider::new(config);
        assert!(res.is_err());
    }

    #[test]
    fn test_rsync_defaults_are_applied_in_provider() {
        let config = make_config(
            "rsync://example.com/test/",
            None,
            false,
            false,
            None,
            false,
            false,
            vec![],
            vec![],
            None,
            "", // empty command — should default to "rsync"
            0,  // retry=0 — should default to 2
        );
        let provider = RsyncProvider::new(config).unwrap();
        assert_eq!(provider.config.command, "rsync");
        assert_eq!(provider.retry(), 2);
    }

    #[test]
    fn test_rsync_override_only_requires_override() {
        let config = make_config(
            "rsync://example.com/test/",
            None, // no override provided
            true, // but override_only=true
            false,
            None,
            false,
            false,
            vec![],
            vec![],
            None,
            "rsync",
            2,
        );

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
