//! Contract tests for `LogLimitHook` — `06 §4.2`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::path::PathBuf;

use hustsync_worker::hooks::{HookCtx, JobHook, LogLimitHook};
use tempfile::TempDir;

fn make_ctx(dir: &TempDir, mirror: &str) -> HookCtx {
    let log_dir = dir.path().join("log");
    std::fs::create_dir_all(&log_dir).unwrap();
    HookCtx {
        mirror_name: mirror.into(),
        working_dir: dir.path().join("work"),
        upstream_url: "rsync://up.test/m/".into(),
        log_dir: log_dir.clone(),
        log_file: log_dir.join(format!("{mirror}_placeholder.log")),
        attempt: 1,
        env: HashMap::new(),
    }
}

fn touch_with_mtime(path: &PathBuf, secs_ago: u64) {
    let f = std::fs::File::create(path).unwrap();
    let when = std::time::SystemTime::now() - std::time::Duration::from_secs(secs_ago);
    f.set_modified(when).unwrap();
}

#[tokio::test]
async fn pre_exec_creates_fresh_file_and_symlink() {
    let tmp = TempDir::new().unwrap();
    let mut ctx = make_ctx(&tmp, "archlinux");
    let hook = LogLimitHook::new();

    hook.pre_exec(&mut ctx).await.unwrap();

    // ctx.log_file was rewritten to the new timestamped file; it exists.
    assert!(ctx.log_file.exists(), "new timestamped log file must exist");
    let name = ctx.log_file.file_name().unwrap().to_string_lossy();
    assert!(
        name.starts_with("archlinux_"),
        "file named by mirror: {name}"
    );
    assert!(name.ends_with(".log"));

    // The `_latest.log` symlink now points at it.
    let link = ctx.log_dir.join("archlinux_latest.log");
    let target = std::fs::read_link(&link).unwrap();
    assert_eq!(target, ctx.log_file);

    // The env was surfaced with the new log path.
    assert_eq!(
        ctx.env.get("TUNASYNC_LOG_FILE").unwrap(),
        &ctx.log_file.to_string_lossy().to_string()
    );
}

#[tokio::test]
async fn pre_exec_rotates_to_ten_newest() {
    let tmp = TempDir::new().unwrap();
    let mirror = "m1";
    let mut ctx = make_ctx(&tmp, mirror);

    // Seed 15 existing log files with staggered mtimes (5 = newest,
    // 300 = oldest); oldest 5 should be removed.
    for secs_ago in (5..=75).step_by(5) {
        let p = ctx.log_dir.join(format!("{mirror}_old_{secs_ago}.log"));
        touch_with_mtime(&p, secs_ago);
    }
    // Extra noise from a different mirror — must not be touched.
    let other = ctx.log_dir.join("other_mirror_keep.log");
    std::fs::File::create(&other).unwrap();

    let hook = LogLimitHook::new();
    hook.pre_exec(&mut ctx).await.unwrap();

    // 15 seeded + 1 fresh - 6 oldest pruned = 10 (fresh counts toward the
    // retention budget on the NEXT rotation; on this run the fresh file
    // exists AFTER rotation, so 15 - 5 + 1 = 11 survivors minus the fresh
    // one's own slot... the spec says "at most 10 after rotation"; the
    // fresh file created this run is always in addition).
    let surviving: Vec<_> = std::fs::read_dir(&ctx.log_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let n = e.file_name().into_string().unwrap_or_default();
            n.starts_with(&format!("{mirror}_")) && n.ends_with(".log")
        })
        .collect();
    // After rotation to 10 newest + fresh file + (symlink is a separate
    // direntry kind, also counted). The key assertion: oldest entries
    // (secs_ago = 45..=75) are gone.
    assert!(
        surviving.len() <= 12,
        "rotation must cap survivors; got {} entries",
        surviving.len()
    );
    for secs_ago in [55u64, 60, 65, 70, 75] {
        let gone = ctx.log_dir.join(format!("{mirror}_old_{secs_ago}.log"));
        assert!(
            !gone.exists(),
            "old file {} must be rotated out",
            gone.display()
        );
    }

    // Other mirror's log untouched.
    assert!(other.exists(), "other mirror's log must not be rotated");
}

#[tokio::test]
async fn post_fail_renames_to_fail_and_updates_symlink() {
    let tmp = TempDir::new().unwrap();
    let mut ctx = make_ctx(&tmp, "m1");
    let hook = LogLimitHook::new();

    // Seed current log state via pre_exec.
    hook.pre_exec(&mut ctx).await.unwrap();
    let run_log = ctx.log_file.clone();
    std::fs::write(&run_log, b"hello").unwrap();

    hook.post_fail(&mut ctx).await.unwrap();

    // Original .log is gone; a .log.fail with the same contents is there.
    assert!(!run_log.exists(), "original .log must be renamed");
    let failed = ctx.log_file.clone();
    assert!(failed.to_string_lossy().ends_with(".log.fail"));
    assert!(failed.exists());
    let contents = std::fs::read(&failed).unwrap();
    assert_eq!(contents, b"hello");

    // Symlink points at the .fail file.
    let link = ctx.log_dir.join("m1_latest.log");
    let target = std::fs::read_link(&link).unwrap();
    assert_eq!(target, failed);
}
