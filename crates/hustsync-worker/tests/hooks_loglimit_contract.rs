//! Contract tests for `LogLimitHook`: rotation, symlink, fail rename, gzip.

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

    // Go writes a bare `latest` symlink sibling in the log dir.
    let link = ctx.log_dir.join("latest");
    let target = std::fs::read_link(&link).unwrap();
    assert_eq!(target, ctx.log_file);

    // The env was surfaced with the new log path. HUSTSYNC_LOG_FILE is
    // canonical; TUNASYNC_LOG_FILE remains a legacy alias with the same value.
    assert_eq!(
        ctx.env.get("HUSTSYNC_LOG_FILE").unwrap(),
        &ctx.log_file.to_string_lossy().to_string()
    );
    assert_eq!(
        ctx.env.get("TUNASYNC_LOG_FILE").unwrap(),
        &ctx.log_file.to_string_lossy().to_string()
    );
}

#[tokio::test]
async fn pre_exec_rotates_retaining_go_fencepost() {
    let tmp = TempDir::new().unwrap();
    let mirror = "m1";
    let mut ctx = make_ctx(&tmp, mirror);

    // Seed 15 staggered files. Go's fencepost: keep 9 oldest-before-
    // create + create 1 new = 10 on disk after pre_exec.
    for secs_ago in (5..=75).step_by(5) {
        let p = ctx.log_dir.join(format!("{mirror}_old_{secs_ago}.log"));
        touch_with_mtime(&p, secs_ago);
    }
    // Extra noise from a different mirror — must not be touched.
    let other = ctx.log_dir.join("other_mirror_keep.log");
    std::fs::File::create(&other).unwrap();

    let hook = LogLimitHook::new();
    hook.pre_exec(&mut ctx).await.unwrap();

    let surviving: Vec<_> = std::fs::read_dir(&ctx.log_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let n = e.file_name().into_string().unwrap_or_default();
            n.starts_with(&format!("{mirror}_")) && n.ends_with(".log")
        })
        .collect();

    // 9 retained + 1 freshly created = 10 total (the symlink `latest`
    // is a separate direntry and does not match the `<mirror>_*.log`
    // predicate above).
    assert_eq!(
        surviving.len(),
        10,
        "rotation must retain 9 oldest-before-create + 1 fresh = 10; got {} entries",
        surviving.len()
    );

    // Oldest 6 (45..=75 secs_ago) must have been rotated out.
    for secs_ago in [50u64, 55, 60, 65, 70, 75] {
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
async fn retention_is_configurable() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let mut ctx = make_ctx(&tmp, "arch");
    std::fs::create_dir_all(&ctx.log_dir)?;
    for secs in [10, 20, 30, 40, 50] {
        touch_with_mtime(&ctx.log_dir.join(format!("arch_old_{secs}.log")), secs);
    }

    LogLimitHook::retaining(3).pre_exec(&mut ctx).await?;

    let mut left: Vec<_> = std::fs::read_dir(&ctx.log_dir)?
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.starts_with("arch_"))
        .collect();
    let fresh = ctx.log_file.file_name().ok_or("no log file")?;
    let mut want = vec![
        "arch_old_10.log".to_owned(),
        "arch_old_20.log".to_owned(),
        fresh.to_string_lossy().into_owned(),
    ];
    left.sort();
    want.sort();
    assert_eq!(left, want);
    Ok(())
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
    let link = ctx.log_dir.join("latest");
    let target = std::fs::read_link(&link).unwrap();
    assert_eq!(target, failed);
}

fn names(dir: &std::path::Path) -> std::io::Result<Vec<String>> {
    let mut v: Vec<String> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n != "latest")
        .collect();
    v.sort();
    Ok(v)
}

#[tokio::test]
async fn gzip_keeps_older_logs_compressed_in_order() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Read;

    let tmp = TempDir::new()?;
    let mut ctx = make_ctx(&tmp, "m1");
    let now = std::time::SystemTime::now();
    let ago = |s| now - std::time::Duration::from_secs(s);
    for (name, secs) in [("a", 10), ("b", 20), ("c", 30), ("d", 40)] {
        let p = ctx.log_dir.join(format!("m1_{name}.log"));
        std::fs::write(&p, name)?;
        std::fs::File::options()
            .write(true)
            .open(&p)?
            .set_modified(ago(secs))?;
    }
    let hook = LogLimitHook::retaining(3).gzip(true);

    hook.pre_exec(&mut ctx).await?;
    let fresh = ctx
        .log_file
        .file_name()
        .ok_or("no log file")?
        .to_string_lossy()
        .into_owned();
    let mut want = vec![
        "m1_a.log.gz".to_owned(),
        "m1_b.log.gz".into(),
        fresh.clone(),
    ];
    want.sort();
    assert_eq!(names(&ctx.log_dir)?, want);

    let gz = ctx.log_dir.join("m1_a.log.gz");
    let mut body = String::new();
    flate2::read::GzDecoder::new(std::fs::File::open(&gz)?).read_to_string(&mut body)?;
    assert_eq!(body, "a");
    assert_eq!(std::fs::metadata(&gz)?.modified()?, ago(10));

    hook.pre_exec(&mut ctx).await?;
    let mut want = vec![format!("{fresh}.gz"), "m1_a.log.gz".into(), fresh];
    want.sort();
    assert_eq!(names(&ctx.log_dir)?, want, "oldest .gz must go first");
    Ok(())
}
