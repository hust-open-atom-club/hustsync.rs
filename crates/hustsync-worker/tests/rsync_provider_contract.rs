// Contract tests for RsyncProvider argv, log parsing, and lifecycle;
// verified against Go `worker/rsync_provider.go`.
//
// argv tests load JSON fixtures from tests/fixtures/rsync-argv/ to keep the
// expected argument sequences in a single canonical place rather than
// scattered across test bodies.
//
// size-parsing is already covered by rsync_log_parity.rs — skipped
// here to avoid duplication as directed by the plan.
//
// timeout and terminate tests drive a real shell stub (`sh -c "sleep 99"`)
// instead of a live rsync daemon so no network access is needed and the tests
// are deterministic.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use hustsync_worker::provider::rsync_provider::{RsyncProvider, RsyncProviderConfig};
use hustsync_worker::provider::{CommonProviderConfig, MirrorProvider, ProviderError, RunContext};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn fixture_argv_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/fixtures/rsync-argv");
    p.push(name);
    p
}

/// Parsed representation of an rsync-argv fixture file.
#[derive(Debug, Deserialize)]
struct ArgvFixture {
    config: FixtureConfig,
    expected_argv: Vec<String>,
    /// Optional env map (present in auth.json; not consumed by build_args but
    /// recorded here to keep the fixture self-documenting).
    #[serde(default)]
    env: HashMap<String, String>,
}

/// The subset of config fields represented in the argv fixtures.
#[derive(Debug, Deserialize)]
struct FixtureConfig {
    name: String,
    upstream: String,
    working_dir: String,
    // provider field is ignored — always rsync in these fixtures
    #[serde(default)]
    rsync_override_only: bool,
    #[serde(default)]
    rsync_override: Option<Vec<String>>,
    #[serde(default)]
    use_ipv6: bool,
    #[serde(default)]
    use_ipv4: bool,
    #[serde(default)]
    exclude_file: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

/// Build a minimal `RsyncProviderConfig` from a fixture config, using safe
/// dummy values for fields not covered by the fixture (log paths, interval,
/// etc.). The `env` map is passed in directly so the auth fixture's env
/// assertions can be exercised separately.
fn config_from_fixture(f: &ArgvFixture) -> RsyncProviderConfig {
    RsyncProviderConfig {
        common: CommonProviderConfig {
            name: f.config.name.clone(),
            upstream_url: f.config.upstream.clone(),
            working_dir: f.config.working_dir.clone(),
            log_dir: "/tmp/hustsync-test-logs".to_string(),
            log_file: "/tmp/hustsync-test-logs/test.log".to_string(),
            interval: Duration::from_secs(3600),
            retry: 2,
            timeout: Duration::from_secs(7200),
            env: f.env.clone(),
            is_master: true,
        },
        command: "rsync".to_string(),
        username: f.config.username.clone(),
        password: f.config.password.clone(),
        exclude_file: f.config.exclude_file.clone(),
        rsync_options: vec![],
        global_options: vec![],
        rsync_override: f.config.rsync_override.clone(),
        rsync_override_only: f.config.rsync_override_only,
        rsync_no_timeout: false,
        rsync_timeout: None,
        use_ipv6: f.config.use_ipv6,
        use_ipv4: f.config.use_ipv4,
    }
}

// ---------------------------------------------------------------------------
// argv derivation — one test per fixture file
//
// IPv flag: Go rsync_provider.go emits `-6`/`-4` (short form). Spec has
// been corrected to match; the Rust implementation follows suit. The
// `ipv6-exclude.json` fixture asserts the short flag verbatim.
// ---------------------------------------------------------------------------

fn load_fixture(name: &str) -> ArgvFixture {
    let bytes = std::fs::read(fixture_argv_path(name))
        .unwrap_or_else(|e| panic!("failed to read fixture {name}: {e}"));
    serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("failed to parse fixture {name}: {e}"))
}

/// Assert that `build_args()` returns exactly the expected argv for a fixture.
///
/// The first element of the returned argv is the first *option* flag, not the
/// binary name — `build_args()` omits the binary itself. The fixture's
/// `expected_argv` starts with `"rsync"` (the binary), so we compare from
/// index 1 onward.
fn assert_argv(fixture_name: &str, expected: &[String], actual: &[String]) {
    assert_eq!(
        actual, expected,
        "argv mismatch for fixture '{fixture_name}':\n expected: {expected:?}\n actual: {actual:?}"
    );
}

#[test]
fn argv_basic_matches_fixture() {
    let fixture = load_fixture("basic.json");
    let provider = RsyncProvider::new(config_from_fixture(&fixture)).unwrap();
    let args = provider.build_args();

    // expected_argv[0] is "rsync" (binary); build_args() omits it.
    let expected = &fixture.expected_argv[1..];
    assert_argv("basic.json", expected, &args);
}

#[test]
fn argv_override_only_matches_fixture() {
    let fixture = load_fixture("override-only.json");
    let provider = RsyncProvider::new(config_from_fixture(&fixture)).unwrap();
    let args = provider.build_args();

    let expected = &fixture.expected_argv[1..];
    assert_argv("override-only.json", expected, &args);
}

#[test]
fn argv_ipv6_exclude_matches_fixture() {
    let fixture = load_fixture("ipv6-exclude.json");
    let provider = RsyncProvider::new(config_from_fixture(&fixture)).unwrap();
    let args = provider.build_args();

    let expected = &fixture.expected_argv[1..];
    assert_argv("ipv6-exclude.json", expected, &args);
}

#[test]
fn argv_auth_matches_fixture() {
    // Auth fixture: username + password are set; argv itself is identical to
    // the basic case because credentials go into env (USER / RSYNC_PASSWORD),
    // not into the argv.
    let fixture = load_fixture("auth.json");
    let provider = RsyncProvider::new(config_from_fixture(&fixture)).unwrap();
    let args = provider.build_args();

    let expected = &fixture.expected_argv[1..];
    assert_argv("auth.json", expected, &args);
}

// ---------------------------------------------------------------------------
// zero-timeout = disabled — must NOT inject --timeout=0 into argv
// ---------------------------------------------------------------------------

#[test]
fn zero_rsync_timeout_falls_back_to_default_not_zero() {
    // rsync_timeout = Some(0) must be treated identically to None (default
    // 120 s). The Rust implementation uses `.filter(|&v| v > 0).unwrap_or(120)`.
    let config = RsyncProviderConfig {
        common: CommonProviderConfig {
            name: "zero-timeout".to_string(),
            upstream_url: "rsync://example.com/repo/".to_string(),
            working_dir: "/tmp/zero-timeout-test".to_string(),
            log_dir: "/tmp/zero-timeout-test-logs".to_string(),
            log_file: "/tmp/zero-timeout-test-logs/test.log".to_string(),
            interval: Duration::from_secs(3600),
            retry: 2,
            timeout: Duration::from_secs(7200),
            env: HashMap::new(),
            is_master: true,
        },
        command: "rsync".to_string(),
        username: None,
        password: None,
        exclude_file: None,
        rsync_options: vec![],
        global_options: vec![],
        rsync_override: None,
        rsync_override_only: false,
        rsync_no_timeout: false,
        rsync_timeout: Some(0),
        use_ipv6: false,
        use_ipv4: false,
    };
    let provider = RsyncProvider::new(config).unwrap();
    let args = provider.build_args();

    assert!(
        !args.contains(&"--timeout=0".to_string()),
        "`rsync_timeout = 0` must not emit --timeout=0; got: {args:?}"
    );
    assert!(
        args.contains(&"--timeout=120".to_string()),
        "`rsync_timeout = 0` must fall back to --timeout=120; got: {args:?}"
    );
}

// ---------------------------------------------------------------------------
// timeout enforcement — provider.run() returns ProviderError::Timeout
//
// We use `sh -c "sleep 99"` as the command (rsync_override_only with a
// shell stub) so no rsync daemon is required and the test is deterministic.
// The provider-level timeout is set to 1 s; we expect the error within ~2 s.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_returns_timeout_error_when_deadline_exceeded() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let working_dir = tmp.path().to_str().unwrap().to_string();
    let log_dir = working_dir.clone();
    let log_file = format!("{}/run.log", working_dir);

    let config = RsyncProviderConfig {
        common: CommonProviderConfig {
            name: "timeout-test".to_string(),
            upstream_url: "rsync://example.com/repo/".to_string(),
            working_dir,
            log_dir,
            log_file,
            interval: Duration::from_secs(3600),
            retry: 2,
            // 1-second provider-level timeout — the job-actor wraps run() here.
            timeout: Duration::from_secs(1),
            env: HashMap::new(),
            is_master: true,
        },
        // Use `sh` as the executable with the sleep command as an override so
        // the binary validation in RsyncProvider::new does not reject it.
        command: "sh".to_string(),
        username: None,
        password: None,
        exclude_file: None,
        rsync_options: vec![],
        global_options: vec![],
        // Override completely: run `sh -c "sleep 99"` instead of rsync.
        rsync_override: Some(vec!["-c".to_string(), "sleep 99".to_string()]),
        rsync_override_only: true,
        rsync_no_timeout: false,
        rsync_timeout: None,
        use_ipv6: false,
        use_ipv4: false,
    };

    let provider = RsyncProvider::new(config).unwrap();
    let ctx = RunContext {
        cancel: CancellationToken::new(),
        attempt: 1,
        env: HashMap::new(),
    };

    let result = tokio::time::timeout(Duration::from_secs(5), provider.run(ctx))
        .await
        .expect("test itself must not time out within 5 s");

    match result {
        Err(ProviderError::Timeout(_)) => {} // expected
        other => panic!("expected ProviderError::Timeout, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// terminate mid-run — concurrent cancel → ProviderError::Terminated
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_returns_terminated_on_cancel() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let working_dir = tmp.path().to_str().unwrap().to_string();
    let log_dir = working_dir.clone();
    let log_file = format!("{}/run.log", working_dir);

    let config = RsyncProviderConfig {
        common: CommonProviderConfig {
            name: "terminate-test".to_string(),
            upstream_url: "rsync://example.com/repo/".to_string(),
            working_dir,
            log_dir,
            log_file,
            interval: Duration::from_secs(3600),
            retry: 2,
            // Disable provider-level timeout so only cancellation fires.
            timeout: Duration::ZERO,
            env: HashMap::new(),
            is_master: true,
        },
        command: "sh".to_string(),
        username: None,
        password: None,
        exclude_file: None,
        rsync_options: vec![],
        global_options: vec![],
        rsync_override: Some(vec!["-c".to_string(), "sleep 99".to_string()]),
        rsync_override_only: true,
        rsync_no_timeout: false,
        rsync_timeout: None,
        use_ipv6: false,
        use_ipv4: false,
    };

    use std::sync::Arc;
    let provider = Arc::new(RsyncProvider::new(config).unwrap());
    let token = CancellationToken::new();

    let ctx = RunContext {
        cancel: token.clone(),
        attempt: 1,
        env: HashMap::new(),
    };

    // Spawn run() as a separate task so we can cancel it from this task.
    let run_handle = {
        let p = Arc::clone(&provider);
        tokio::spawn(async move { p.run(ctx).await })
    };

    // Give the child process a moment to start before cancelling.
    tokio::time::sleep(Duration::from_millis(200)).await;
    token.cancel();

    let result = tokio::time::timeout(Duration::from_secs(5), run_handle)
        .await
        .expect("test must not hang for 5 s")
        .expect("task must not panic");

    match result {
        Err(ProviderError::Terminated) => {} // expected
        other => panic!("expected ProviderError::Terminated, got: {other:?}"),
    }
}
