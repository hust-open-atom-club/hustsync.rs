use std::process::Stdio;

use async_trait::async_trait;
use regex::Regex;
use tokio::fs::{File, create_dir_all};
use tokio::process::Command;
use tokio::sync::Mutex;

use std::sync::atomic::{AtomicU32, Ordering};

use super::{
    CommonProviderConfig, MirrorProvider, ProviderError, ProviderType, RunContext,
    impl_provider_getters, inject_provider_env, log_provider_failure, resolve_log_file,
    run_child_with_cancellation,
};

pub struct CmdProviderConfig {
    pub common: CommonProviderConfig,
    pub command: String,
    pub fail_on_match: Option<String>,
    pub size_pattern: Option<String>,
}

pub struct CmdProvider {
    config: CmdProviderConfig,
    cmd_args: Vec<String>,
    fail_on_match: Option<Regex>,
    size_pattern: Option<Regex>,
    data_size: Mutex<Option<String>>,
    // We store the running process PGID here so terminate() can kill it
    // without needing to acquire a Mutex that run() might be holding.
    running_pgid: AtomicU32,
}

impl CmdProvider {
    pub fn new(mut config: CmdProviderConfig) -> Result<Self, ProviderError> {
        if config.common.retry == 0 {
            config.common.retry = 2;
        }

        let cmd_args = shlex::split(&config.command)
            .ok_or_else(|| ProviderError::Config("Failed to parse command with shlex".into()))?;

        if cmd_args.is_empty() {
            return Err(ProviderError::Config("Command is empty".into()));
        }

        let fail_on_match = match &config.fail_on_match {
            Some(pattern) if !pattern.is_empty() => Some(Regex::new(pattern)?),
            _ => None,
        };

        let size_pattern = match &config.size_pattern {
            Some(pattern) if !pattern.is_empty() => Some(Regex::new(pattern)?),
            _ => None,
        };

        Ok(Self {
            config,
            cmd_args,
            fail_on_match,
            size_pattern,
            data_size: Mutex::new(None),
            running_pgid: AtomicU32::new(0),
        })
    }
}

#[async_trait]
impl MirrorProvider for CmdProvider {
    impl_provider_getters!(CmdProvider, ProviderType::Command);

    async fn run(&self, ctx: RunContext) -> Result<(), ProviderError> {
        if ctx.attempt > 1 {
            tracing::debug!(
                "Cmd provider {} re-entering on attempt {}",
                self.config.common.name,
                ctx.attempt
            );
        }

        // Prevent concurrent runs of the same provider instance
        if self.running_pgid.load(Ordering::Acquire) != 0 {
            return Err(ProviderError::AlreadyRunning);
        }

        {
            let mut size_guard = self.data_size.lock().await;
            *size_guard = None;
        }

        // Ensure directories exist
        create_dir_all(&self.config.common.working_dir).await?;
        create_dir_all(&self.config.common.log_dir).await?;

        // Loglimit hook (or any other pre_exec hook) may redirect the log
        // file to a rotated timestamped path; honor it via ctx.env before
        // opening the file handle. Falls back to the config default when
        // no hook has set it.
        let effective_log_file = resolve_log_file(&ctx, &self.config.common.log_file);

        // Setup log file
        let log_file = File::create(&effective_log_file).await?;
        let std_out_log = log_file.into_std().await;
        let std_err_log = std_out_log.try_clone()?;

        let mut cmd = Command::new(&self.cmd_args[0]);
        if self.cmd_args.len() > 1 {
            cmd.args(&self.cmd_args[1..]);
        }

        cmd.current_dir(&self.config.common.working_dir)
            .stdout(Stdio::from(std_out_log))
            .stderr(Stdio::from(std_err_log));

        #[cfg(unix)]
        {
            cmd.process_group(0);
        }

        // Inject both TUNASYNC_* (Go parity for existing mirror scripts) and
        // HUSTSYNC_* (Rust-port canonical names).  Both sets must stay in sync.
        cmd.env("TUNASYNC_MIRROR_NAME", &self.config.common.name)
            .env("TUNASYNC_WORKING_DIR", &self.config.common.working_dir)
            .env("TUNASYNC_UPSTREAM_URL", &self.config.common.upstream_url)
            .env("TUNASYNC_LOG_DIR", &self.config.common.log_dir)
            .env("TUNASYNC_LOG_FILE", &effective_log_file);

        inject_provider_env(&mut cmd, &self.config.common, &effective_log_file, &ctx.env);

        tracing::info!("Starting command provider for {}", self.config.common.name);

        let mut spawned_child = cmd.spawn()?;

        // Record the PID so terminate() can kill it without locks
        if let Some(pid) = spawned_child.id() {
            self.running_pgid.store(pid, Ordering::Release);
        }

        // Wait for the process to complete, respecting both timeout and cancellation.
        // Terminated/Timeout/Io variants propagate via `?` at the call site.
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
                    let code = status.code().unwrap_or(-1);
                    let msg = format!("Command exited with status: {}", status);
                    log_provider_failure("Cmd", &self.config.common.name, &msg, &effective_log_file)
                        .await;
                    Err(ProviderError::Execution { code, msg })
                }
            }
            Err(e) => Err(e),
        };

        // Clear the PID
        self.running_pgid.store(0, Ordering::Release);

        // If execution succeeded, check logs for patterns
        if result.is_ok() {
            let log_content = tokio::fs::read_to_string(&effective_log_file)
                .await
                .unwrap_or_default();

            if let Some(fail_regex) = &self.fail_on_match {
                let match_count = fail_regex.find_iter(&log_content).count();
                if match_count > 0 {
                    return Err(ProviderError::Execution {
                        code: -1,
                        msg: format!("Fail-on-match regexp found {} matches", match_count),
                    });
                }
            }

            // Size pattern: take the LAST match, not the first. Long-running
            // syncs emit multiple size-like lines; only the final total is
            // authoritative. Matches Go `internal.ExtractSizeFromLog`.
            if let Some(size_regex) = &self.size_pattern
                && let Some(m) = size_regex
                    .captures_iter(&log_content)
                    .last()
                    .and_then(|c| c.get(1))
            {
                let mut size_guard = self.data_size.lock().await;
                *size_guard = Some(m.as_str().to_string());
            }
        }

        result
    }

    async fn terminate(&self) -> Result<(), ProviderError> {
        #[cfg(unix)]
        {
            if self.running_pgid.load(Ordering::Acquire) != 0 {
                tracing::warn!(
                    "Terminating command provider for {}",
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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;
    use tempfile::tempdir;

    fn setup_provider(
        name: &str,
        command: &str,
        timeout_secs: u64,
    ) -> (CmdProvider, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let log_file = dir.path().join("test.log");
        let config = CmdProviderConfig {
            common: CommonProviderConfig {
                name: name.to_string(),
                upstream_url: "http://example.com".to_string(),
                working_dir: dir.path().to_str().unwrap().to_string(),
                log_dir: dir.path().to_str().unwrap().to_string(),
                log_file: log_file.to_str().unwrap().to_string(),
                interval: Duration::from_secs(60),
                retry: 1,
                timeout: Duration::from_secs(timeout_secs),
                env: HashMap::new(),
                is_master: true,
            },
            command: command.to_string(),
            fail_on_match: None,
            size_pattern: None,
        };
        (CmdProvider::new(config).unwrap(), dir)
    }

    #[tokio::test]
    async fn test_cmd_basic_execution() {
        let (provider, _dir) = setup_provider("test_echo", "echo hello_world", 5);
        let res = provider.run(RunContext::default()).await;
        assert!(res.is_ok());

        let log: String = tokio::fs::read_to_string(&provider.config.common.log_file)
            .await
            .unwrap();
        assert!(log.contains("hello_world"));
    }

    #[tokio::test]
    async fn test_cmd_env_vars() {
        let (mut provider, _dir) = setup_provider("test_env", "sh -c 'echo $TEST_VAR'", 5);
        provider
            .config
            .common
            .env
            .insert("TEST_VAR".to_string(), "env_works".to_string());
        let _ = provider.run(RunContext::default()).await;

        let log: String = tokio::fs::read_to_string(&provider.config.common.log_file)
            .await
            .unwrap();
        assert!(log.contains("env_works"));
    }

    #[tokio::test]
    async fn test_cmd_size_extraction() {
        let (mut provider, _dir) = setup_provider("test_size", "echo 'Total size: 1.23G'", 5);
        provider.size_pattern = Some(Regex::new(r"Total size: ([0-9\.]+[KMG])").unwrap());
        let _ = provider.run(RunContext::default()).await;

        assert_eq!(provider.data_size().await.unwrap(), "1.23G");
    }

    #[tokio::test]
    async fn test_cmd_timeout() {
        // Sleep for 10s but timeout is 1s
        let (provider, _dir) = setup_provider("test_timeout", "sleep 10", 1);
        let res = provider.run(RunContext::default()).await;

        match res {
            Err(ProviderError::Timeout(_)) => (),
            _ => panic!("Expected timeout error, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cmd_fail_on_match() {
        let (mut provider, _dir) = setup_provider("test_fail", "echo 'ERROR: disk full'", 5);
        #[allow(clippy::trivial_regex)]
        let re = Regex::new("ERROR").unwrap();
        provider.fail_on_match = Some(re);
        let res = provider.run(RunContext::default()).await;

        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Fail-on-match"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_process_group_kill() {
        // Run a shell script that spawns a long-running grandchild
        let (provider, _dir) = setup_provider(
            "test_pgid",
            "sh -c 'sleep 100 & sleep 100'",
            1, // timeout quickly
        );

        let res = provider.run(RunContext::default()).await;
        assert!(matches!(res, Err(ProviderError::Timeout(_))));

        // At this point, thanks to process_group(0) and kill(-pid),
        // there should be no "sleep 100" processes left related to this test.
    }
}
