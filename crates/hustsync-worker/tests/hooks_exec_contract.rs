//! Contract tests for `ExecPostHook` — `06 §4.1`.
//!
//! Each test spawns the hook with a shell command that writes a marker
//! file; assertions inspect the marker to verify the command ran with
//! the expected env. `sh -c` is used so the env vars propagate into the
//! spawned process without needing a compiled test binary.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::path::PathBuf;

use hustsync_worker::hooks::{ExecPostHook, HookCtx, JobHook};
use tempfile::TempDir;

fn make_ctx(dir: &TempDir, mirror: &str) -> HookCtx {
    let working = dir.path().join("work");
    let log_dir = dir.path().join("log");
    std::fs::create_dir_all(&working).unwrap();
    std::fs::create_dir_all(&log_dir).unwrap();
    let log_file = log_dir.join("latest.log");
    std::fs::File::create(&log_file).unwrap();
    HookCtx {
        mirror_name: mirror.into(),
        working_dir: working,
        upstream_url: "rsync://up.test/m/".into(),
        log_dir,
        log_file,
        attempt: 1,
        env: HashMap::new(),
    }
}

fn marker(dir: &TempDir, name: &str) -> PathBuf {
    dir.path().join(name)
}

#[tokio::test]
async fn post_success_runs_each_command_with_tunasync_env() {
    let tmp = TempDir::new().unwrap();
    let marker_a = marker(&tmp, "a.marker");
    let marker_b = marker(&tmp, "b.marker");
    let hook = ExecPostHook::new(
        vec![
            format!(
                "echo \"$TUNASYNC_MIRROR_NAME|$TUNASYNC_JOB_EXIT_STATUS\" > {}",
                marker_a.display()
            ),
            format!("echo \"$HUSTSYNC_UPSTREAM_URL\" > {}", marker_b.display()),
        ],
        vec![],
    );

    let mut ctx = make_ctx(&tmp, "archlinux");
    hook.post_success(&mut ctx).await.unwrap();

    let a = std::fs::read_to_string(&marker_a).unwrap();
    assert_eq!(a.trim(), "archlinux|success");
    let b = std::fs::read_to_string(&marker_b).unwrap();
    assert_eq!(b.trim(), "rsync://up.test/m/");
}

#[tokio::test]
async fn post_fail_runs_failure_list_not_success_list() {
    let tmp = TempDir::new().unwrap();
    let ok_marker = marker(&tmp, "ok.marker");
    let fail_marker = marker(&tmp, "fail.marker");
    let hook = ExecPostHook::new(
        vec![format!("touch {}", ok_marker.display())],
        vec![format!("touch {}", fail_marker.display())],
    );

    let mut ctx = make_ctx(&tmp, "m1");
    hook.post_fail(&mut ctx).await.unwrap();

    assert!(!ok_marker.exists(), "on_success must not fire on post_fail");
    assert!(fail_marker.exists(), "on_failure must fire on post_fail");
}

#[tokio::test]
async fn command_failure_is_logged_and_next_still_runs() {
    let tmp = TempDir::new().unwrap();
    let second = marker(&tmp, "second.marker");
    let hook = ExecPostHook::new(
        vec!["exit 7".to_string(), format!("touch {}", second.display())],
        vec![],
    );

    let mut ctx = make_ctx(&tmp, "m1");
    hook.post_success(&mut ctx).await.unwrap();

    // First command exited non-zero but was logged; the second still ran.
    assert!(second.exists());
}

#[tokio::test]
async fn commands_execute_in_config_order() {
    let tmp = TempDir::new().unwrap();
    let out = marker(&tmp, "order.txt");
    let hook = ExecPostHook::new(
        vec![
            format!("echo 1 >> {}", out.display()),
            format!("echo 2 >> {}", out.display()),
            format!("echo 3 >> {}", out.display()),
        ],
        vec![],
    );

    let mut ctx = make_ctx(&tmp, "m1");
    hook.post_success(&mut ctx).await.unwrap();

    let content = std::fs::read_to_string(&out).unwrap();
    assert_eq!(content, "1\n2\n3\n");
}
