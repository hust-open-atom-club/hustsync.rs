// Contract tests for the cmd provider (env, fail_on_match, size, terminate).
//
// All six contract points are exercised against real process forks so that
// the env-injection, fail_on_match, size_pattern, terminate, and timeout
// logic is verified end-to-end without a running daemon.

#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc
)]
#[cfg(test)]
mod cmd_provider_contract {
    use std::collections::HashMap;
    use std::time::Duration;

    use hustsync_worker::provider::cmd_provider::{CmdProvider, CmdProviderConfig};
    use hustsync_worker::provider::{
        CommonProviderConfig, MirrorProvider, ProviderError, RunContext,
    };
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    /// Build a `CmdProvider` whose working dir and log dir live inside `dir`.
    fn make_provider(
        dir: &tempfile::TempDir,
        name: &str,
        command: &str,
        timeout: Duration,
        fail_on_match: Option<String>,
        size_pattern: Option<String>,
    ) -> CmdProvider {
        make_provider_with_env(
            dir,
            name,
            command,
            timeout,
            fail_on_match,
            size_pattern,
            HashMap::new(),
        )
    }

    fn make_provider_with_env(
        dir: &tempfile::TempDir,
        name: &str,
        command: &str,
        timeout: Duration,
        fail_on_match: Option<String>,
        size_pattern: Option<String>,
        env: HashMap<String, String>,
    ) -> CmdProvider {
        let log_file = dir.path().join("run.log");
        let cfg = CmdProviderConfig {
            common: CommonProviderConfig {
                name: name.to_string(),
                upstream_url: "https://upstream.example/".to_string(),
                working_dir: dir.path().to_str().unwrap().to_string(),
                log_dir: dir.path().to_str().unwrap().to_string(),
                log_file: log_file.to_str().unwrap().to_string(),
                interval: Duration::from_secs(3600),
                retry: 1,
                timeout,
                env,
                is_master: true,
                success_exit_codes: vec![],
            },
            command: command.to_string(),
            fail_on_match,
            size_pattern,
        };
        CmdProvider::new(cfg).unwrap()
    }

    // ── 1. HUSTSYNC_* env injection ──────────────────────────────────────────

    /// All five canonical HUSTSYNC_* variables must be present in the child env.
    /// TUNASYNC_* remains a legacy alias, but HUSTSYNC_* is the primary
    /// contract new scripts should depend on.
    #[tokio::test]
    async fn hustsync_env_vars_are_injected() {
        let dir = tempdir().unwrap();
        let provider = make_provider(
            &dir,
            "test-mirror",
            "sh -c 'env | grep HUSTSYNC_'",
            Duration::ZERO,
            None,
            None,
        );

        provider.run(RunContext::default()).await.unwrap();

        let log = tokio::fs::read_to_string(provider.log_file())
            .await
            .unwrap();
        let hustsync_count = log.lines().filter(|l| l.starts_with("HUSTSYNC_")).count();

        assert!(
            hustsync_count >= 5,
            "expected ≥5 HUSTSYNC_* lines, got {hustsync_count}; log:\n{log}"
        );

        for var in &[
            "HUSTSYNC_MIRROR_NAME",
            "HUSTSYNC_WORKING_DIR",
            "HUSTSYNC_UPSTREAM_URL",
            "HUSTSYNC_LOG_DIR",
            "HUSTSYNC_LOG_FILE",
        ] {
            assert!(log.contains(var), "missing env var {var} in log:\n{log}");
        }
    }

    // ── 2. legacy alias sync / conflict handling ─────────────────────────────

    /// Legacy TUNASYNC_* aliases are still exported, but they mirror the
    /// canonical HUSTSYNC_* value so scripts do not see conflicting data.
    #[tokio::test]
    async fn tunasync_legacy_aliases_mirror_canonical_values() {
        let dir = tempdir().unwrap();
        let provider = make_provider(
            &dir,
            "test-mirror",
            "sh -c 'printf \"%s\\n\" \"$HUSTSYNC_MIRROR_NAME|$TUNASYNC_MIRROR_NAME\" \"$HUSTSYNC_WORKING_DIR|$TUNASYNC_WORKING_DIR\" \"$HUSTSYNC_UPSTREAM_URL|$TUNASYNC_UPSTREAM_URL\" \"$HUSTSYNC_LOG_DIR|$TUNASYNC_LOG_DIR\" \"$HUSTSYNC_LOG_FILE|$TUNASYNC_LOG_FILE\"'",
            Duration::ZERO,
            None,
            None,
        );

        provider.run(RunContext::default()).await.unwrap();

        let log = tokio::fs::read_to_string(provider.log_file())
            .await
            .unwrap();

        for line in log.lines() {
            let (canonical, legacy) = line.split_once('|').unwrap();
            assert_eq!(
                canonical, legacy,
                "legacy alias must mirror canonical: {line}"
            );
        }
    }

    /// If both prefixes are configured for the same semantic field, HUSTSYNC_*
    /// wins and both exported names are normalised to that value.
    #[tokio::test]
    async fn hustsync_env_wins_when_prefixes_conflict() {
        let dir = tempdir().unwrap();
        let mut env = HashMap::new();
        env.insert("HUSTSYNC_UPSTREAM_URL".into(), "canonical".into());
        env.insert("TUNASYNC_UPSTREAM_URL".into(), "legacy".into());
        let provider = make_provider_with_env(
            &dir,
            "test-mirror",
            "sh -c 'printf \"%s\\n\" \"$HUSTSYNC_UPSTREAM_URL\" \"$TUNASYNC_UPSTREAM_URL\"'",
            Duration::ZERO,
            None,
            None,
            env,
        );

        provider.run(RunContext::default()).await.unwrap();

        let log = tokio::fs::read_to_string(provider.log_file())
            .await
            .unwrap();
        let lines: Vec<_> = log.lines().collect();
        assert_eq!(lines, vec!["canonical", "canonical"]);
    }

    /// Runtime hook env is layered after config env. A HUSTSYNC_LOG_FILE /
    /// TUNASYNC_LOG_FILE conflict must choose HUSTSYNC_LOG_FILE for both the
    /// actual log path and the child process env.
    #[tokio::test]
    async fn hustsync_log_file_wins_over_legacy_log_file_conflict() {
        let dir = tempdir().unwrap();
        let canonical_log = dir.path().join("canonical.log");
        let legacy_log = dir.path().join("legacy.log");
        let provider = make_provider(
            &dir,
            "test-mirror",
            "sh -c 'printf \"%s\\n\" \"$HUSTSYNC_LOG_FILE\" \"$TUNASYNC_LOG_FILE\"'",
            Duration::ZERO,
            None,
            None,
        );
        let mut ctx = RunContext::default();
        ctx.env.insert(
            "HUSTSYNC_LOG_FILE".into(),
            canonical_log.to_string_lossy().into_owned(),
        );
        ctx.env.insert(
            "TUNASYNC_LOG_FILE".into(),
            legacy_log.to_string_lossy().into_owned(),
        );

        provider.run(ctx).await.unwrap();

        let log = tokio::fs::read_to_string(&canonical_log).await.unwrap();
        let lines: Vec<_> = log.lines().collect();
        let canonical = canonical_log.to_string_lossy();
        assert_eq!(lines, vec![canonical.as_ref(), canonical.as_ref()]);
        assert!(
            !legacy_log.exists(),
            "legacy log path must not be used when canonical path is present"
        );
    }

    // ── 3. fail_on_match ─────────────────────────────────────────────────────

    /// When the log contains a line matching `fail_on_match`, `run()`
    /// must return `ProviderError::Execution` even if the exit code was 0.
    #[tokio::test]
    async fn fail_on_match_turns_success_into_execution_error() {
        let dir = tempdir().unwrap();
        let provider = make_provider(
            &dir,
            "fail-mirror",
            "echo 'ERROR: disk full'",
            Duration::ZERO,
            Some("ERROR:".to_string()),
            None,
        );

        let result = provider.run(RunContext::default()).await;

        assert!(
            matches!(result, Err(ProviderError::Execution { .. })),
            "expected Execution error, got {:?}",
            result
        );
    }

    // ── 4. size_pattern ──────────────────────────────────────────────────────

    /// After a successful run, capture group 1 of `size_pattern` is
    /// returned by `data_size()`. A single-match log verifies the extraction
    /// without depending on first-vs-last ordering.
    #[tokio::test]
    async fn size_pattern_captures_match() {
        let dir = tempdir().unwrap();
        let provider = make_provider(
            &dir,
            "size-mirror",
            "echo 'Total: 1.23G'",
            Duration::ZERO,
            None,
            // Capture group 1 must be the size token.
            Some(r"Total: ([\d.]+G)".to_string()),
        );

        provider.run(RunContext::default()).await.unwrap();

        let size = provider.data_size().await;
        assert_eq!(
            size.as_deref(),
            Some("1.23G"),
            "expected '1.23G', got {size:?}"
        );
    }

    // ── 5. terminate ─────────────────────────────────────────────────────────

    /// Cancelling via `ctx.cancel` while `sleep 99` is running must
    /// cause `run()` to return `ProviderError::Terminated` within 10 s.
    /// The `CancellationToken` is the spec-mandated cancellation channel;
    /// callers (the job actor) cancel the token to stop a running provider.
    #[cfg(unix)]
    #[tokio::test]
    async fn terminate_via_cancel_token_stops_running_provider() {
        let dir = tempdir().unwrap();
        let provider = std::sync::Arc::new(make_provider(
            &dir,
            "long-mirror",
            "sleep 99",
            Duration::ZERO,
            None,
            None,
        ));

        let cancel = CancellationToken::new();
        let ctx = RunContext {
            cancel: cancel.clone(),
            attempt: 1,
            env: HashMap::new(),
        };

        let p = std::sync::Arc::clone(&provider);
        let run_handle = tokio::spawn(async move { p.run(ctx).await });

        // Give the child process time to start before firing the token.
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        let result = tokio::time::timeout(Duration::from_secs(10), run_handle)
            .await
            .expect("run() did not return within 10 s after cancellation")
            .expect("task panicked");

        assert!(
            matches!(result, Err(ProviderError::Terminated)),
            "expected Terminated, got {:?}",
            result
        );
    }

    // ── 6. zero-timeout = disabled ───────────────────────────────────────────

    /// `Duration::ZERO` means "no timeout". A fast
    /// command (`echo hello`) must complete successfully — no spurious
    /// `Timeout` error is allowed.
    #[tokio::test]
    async fn zero_timeout_does_not_time_out_fast_command() {
        let dir = tempdir().unwrap();
        let provider = make_provider(
            &dir,
            "fast-mirror",
            "echo hello",
            Duration::ZERO, // explicitly disabled
            None,
            None,
        );

        // Use a cancellation token that is never fired.
        let ctx = RunContext {
            cancel: CancellationToken::new(),
            attempt: 1,
            env: HashMap::new(),
        };

        let result = provider.run(ctx).await;
        assert!(result.is_ok(), "expected Ok(()), got {:?}", result);
    }
}
