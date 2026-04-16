//! Contract tests for the two-stage rsync provider — stage sequencing + terminate.
//!
//! A shell script at `tests/fixtures/bin/fake_rsync.sh` stands in for rsync.
//! It detects the stage by argv (`--delete` → stage 2) and exits with a
//! code picked from env vars (`FAKE_STAGE1_EXIT`, `FAKE_STAGE2_EXIT`).
//! `FAKE_SLEEP` makes it sleep before exiting so cancellation tests can
//! intercept a running stage.
//!
//! Scenarios:
//! 1. stage-1 exit 23 → whole run fails, stage 2 does not start
//! 2. stage-1 exit 24 → whole run fails, stage 2 does not start
//! 3. stage-1 success + stage-2 success → run succeeds
//! 4. stage-1 success + stage-2 failure → run fails
//! 5. cancel mid-stage-1 via `ctx.cancel` → `ProviderError::Terminated`
//! 6. unknown `stage1_profile` → rejected at `new()`

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use hustsync_worker::provider::{
    CommonProviderConfig, MirrorProvider, ProviderError, RunContext,
    two_stage_rsync_provider::{TwoStageRsyncProvider, TwoStageRsyncProviderConfig},
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn fake_rsync_path() -> String {
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
        .join("tests/fixtures/bin/fake_rsync.sh")
        .to_string_lossy()
        .into_owned()
}

fn make_config(
    name: &str,
    work: &TempDir,
    env: HashMap<String, String>,
) -> TwoStageRsyncProviderConfig {
    let root = work.path().to_path_buf();
    let log_dir = root.join("log");
    std::fs::create_dir_all(&log_dir).unwrap();
    TwoStageRsyncProviderConfig {
        common: CommonProviderConfig {
            name: name.to_string(),
            upstream_url: "rsync://upstream.test/mirror/".to_string(),
            working_dir: root.to_string_lossy().into_owned(),
            log_dir: log_dir.to_string_lossy().into_owned(),
            log_file: log_dir.join("latest.log").to_string_lossy().into_owned(),
            interval: Duration::from_secs(60),
            retry: 1,
            timeout: Duration::from_secs(5),
            env,
            is_master: true,
            success_exit_codes: vec![],
        },
        command: fake_rsync_path(),
        stage1_profile: "debian".to_string(),
        username: None,
        password: None,
        exclude_file: None,
        extra_options: vec![],
        rsync_no_timeout: true,
        rsync_timeout: None,
        use_ipv6: false,
        use_ipv4: false,
    }
}

async fn run(provider: &TwoStageRsyncProvider) -> Result<(), ProviderError> {
    provider.run(RunContext::default()).await
}

// ---------------------------------------------------------------------------
// Stage-1 failure cases (rsync exit 23 and 24 both abort stage 2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stage1_exit_23_fails_run_and_skips_stage2() {
    let dir = TempDir::new().unwrap();
    let mut env = HashMap::new();
    env.insert("FAKE_STAGE1_EXIT".to_string(), "23".to_string());
    // Also make stage 2 pass to prove that if the provider incorrectly
    // continued, the run would succeed — we want to see it fail instead.
    env.insert("FAKE_STAGE2_EXIT".to_string(), "0".to_string());

    let provider = TwoStageRsyncProvider::new(make_config("m-exit23", &dir, env)).unwrap();
    let err = run(&provider).await.expect_err("stage-1 exit 23 must fail");
    assert!(
        matches!(err, ProviderError::Execution { code: 23, .. }),
        "expected Execution{{code:23}}, got: {err:?}"
    );
}

#[tokio::test]
async fn stage1_exit_24_fails_run_and_skips_stage2() {
    let dir = TempDir::new().unwrap();
    let mut env = HashMap::new();
    env.insert("FAKE_STAGE1_EXIT".to_string(), "24".to_string());
    env.insert("FAKE_STAGE2_EXIT".to_string(), "0".to_string());

    let provider = TwoStageRsyncProvider::new(make_config("m-exit24", &dir, env)).unwrap();
    let err = run(&provider).await.expect_err("stage-1 exit 24 must fail");
    assert!(
        matches!(err, ProviderError::Execution { code: 24, .. }),
        "expected Execution{{code:24}}, got: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Stage-1 success → stage-2 runs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stage1_success_leads_to_stage2_success() {
    let dir = TempDir::new().unwrap();
    let mut env = HashMap::new();
    env.insert("FAKE_STAGE1_EXIT".to_string(), "0".to_string());
    env.insert("FAKE_STAGE2_EXIT".to_string(), "0".to_string());

    let provider = TwoStageRsyncProvider::new(make_config("m-ok", &dir, env)).unwrap();
    run(&provider).await.expect("both stages must succeed");
}

#[tokio::test]
async fn stage2_failure_fails_run() {
    let dir = TempDir::new().unwrap();
    let mut env = HashMap::new();
    env.insert("FAKE_STAGE1_EXIT".to_string(), "0".to_string());
    env.insert("FAKE_STAGE2_EXIT".to_string(), "23".to_string());

    let provider = TwoStageRsyncProvider::new(make_config("m-s2fail", &dir, env)).unwrap();
    let err = run(&provider).await.expect_err("stage-2 failure must fail");
    assert!(
        matches!(err, ProviderError::Execution { code: 23, .. }),
        "expected Execution{{code:23}}, got: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Cancel mid-stage-1 → ProviderError::Terminated
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_mid_stage1_returns_terminated() {
    let dir = TempDir::new().unwrap();
    let mut env = HashMap::new();
    // Make stage 1 sleep so we have time to cancel it.
    env.insert("FAKE_SLEEP".to_string(), "10".to_string());
    env.insert("FAKE_STAGE1_EXIT".to_string(), "0".to_string());

    let provider = TwoStageRsyncProvider::new(make_config("m-cancel", &dir, env)).unwrap();

    let cancel = CancellationToken::new();
    let ctx = RunContext {
        cancel: cancel.clone(),
        attempt: 1,
        env: HashMap::new(),
    };

    let run_fut = tokio::spawn(async move { provider.run(ctx).await });
    // Let stage 1 spawn and start sleeping before cancelling.
    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel.cancel();

    let res = tokio::time::timeout(Duration::from_secs(5), run_fut)
        .await
        .expect("run must observe cancellation within the timeout")
        .expect("task join ok");

    assert!(
        matches!(res, Err(ProviderError::Terminated)),
        "expected Terminated, got: {res:?}"
    );
}

// ---------------------------------------------------------------------------
// Profile validation at construction
// ---------------------------------------------------------------------------

#[test]
fn unknown_stage1_profile_rejected_at_new() {
    let dir = TempDir::new().unwrap();
    let mut cfg = make_config("m-bad", &dir, HashMap::new());
    cfg.stage1_profile = "not-a-profile".to_string();
    let res = TwoStageRsyncProvider::new(cfg);
    match res {
        Err(ProviderError::Config(_)) => {}
        Err(other) => panic!("expected Config error, got: {other:?}"),
        Ok(_) => panic!("unknown profile must fail at new()"),
    }
}
