// Contract tests for the cmd provider — spec §5 of 05-provider-contract.md.
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
    use hustsync_worker::provider::{MirrorProvider, ProviderError, RunContext};
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
        let log_file = dir.path().join("run.log");
        let cfg = CmdProviderConfig {
            name: name.to_string(),
            upstream_url: "https://upstream.example/".to_string(),
            command: command.to_string(),
            working_dir: dir.path().to_str().unwrap().to_string(),
            log_dir: dir.path().to_str().unwrap().to_string(),
            log_file: log_file.to_str().unwrap().to_string(),
            interval: Duration::from_secs(3600),
            retry: 1,
            timeout,
            env: HashMap::new(),
            fail_on_match,
            size_pattern,
            is_master: true,
        };
        CmdProvider::new(cfg).unwrap()
    }

    // ── 1. TUNASYNC_* env injection ──────────────────────────────────────────

    /// §5.1: All five TUNASYNC_* variables must be present in the child's env.
    /// A `sh -c 'env | grep TUNASYNC_'` command writes them to the log file;
    /// we count distinct TUNASYNC_-prefixed lines.
    #[tokio::test]
    async fn tunasync_env_vars_are_injected() {
        let dir = tempdir().unwrap();
        let provider = make_provider(
            &dir,
            "test-mirror",
            "sh -c 'env | grep TUNASYNC_'",
            Duration::ZERO,
            None,
            None,
        );

        provider.run(RunContext::default()).await.unwrap();

        let log = tokio::fs::read_to_string(provider.log_file())
            .await
            .unwrap();
        let tunasync_count = log.lines().filter(|l| l.starts_with("TUNASYNC_")).count();

        assert!(
            tunasync_count >= 5,
            "expected ≥5 TUNASYNC_* lines, got {tunasync_count}; log:\n{log}"
        );

        // Verify the exact variable names mandated by §5.1.
        for var in &[
            "TUNASYNC_MIRROR_NAME",
            "TUNASYNC_WORKING_DIR",
            "TUNASYNC_UPSTREAM_URL",
            "TUNASYNC_LOG_DIR",
            "TUNASYNC_LOG_FILE",
        ] {
            assert!(log.contains(var), "missing env var {var} in log:\n{log}");
        }
    }

    // ── 2. HUSTSYNC_* dual-prefix injection ──────────────────────────────────

    /// §5.1 (Rust-port extension): The five HUSTSYNC_* synonyms must also be
    /// set so that scripts written for hustsync.rs do not need a TUNASYNC_ shim.
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

    // ── 3. fail_on_match ─────────────────────────────────────────────────────

    /// §5.2: When the log contains a line matching `fail_on_match`, `run()`
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

    /// §5.3: After a successful run, capture group 1 of `size_pattern` is
    /// returned by `data_size()`.  A single-match log verifies the extraction
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

    /// §2.6: Cancelling via `ctx.cancel` while `sleep 99` is running must
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

    /// §1 (timeout() contract): `Duration::ZERO` means "no timeout". A fast
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
