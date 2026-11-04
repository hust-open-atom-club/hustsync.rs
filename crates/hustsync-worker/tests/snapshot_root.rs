//! The ZFS hook on a real dataset: `HUSTSYNC_ZFS=<dir> sudo -E <test binary> --ignored --test-threads=1`.

#![cfg(target_os = "linux")]

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use hustsync_worker::hooks::{HookCtx, JobHook, ZfsHook};

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn ctx(name: &str, dir: &Path) -> HookCtx {
    HookCtx {
        mirror_name: name.into(),
        working_dir: dir.into(),
        upstream_url: String::new(),
        log_dir: PathBuf::new(),
        log_file: PathBuf::new(),
        attempt: 0,
        env: HashMap::new(),
    }
}

fn scratch(var: &str) -> Option<PathBuf> {
    let dir = std::env::var_os(var).map(PathBuf::from);
    if dir.is_none() {
        eprintln!("skipped: {var} is not set");
    }
    dir
}

#[tokio::test]
#[ignore = "needs a ZFS dataset mounted at HUSTSYNC_ZFS"]
async fn zfs_accepts_a_dataset_and_refuses_a_directory_inside_it() -> Res {
    let Some(dataset) = scratch("HUSTSYNC_ZFS") else {
        return Ok(());
    };
    let hook = ZfsHook::new("hstest");
    hook.pre_job(&mut ctx("debian", &dataset)).await?;

    let inner = dataset.join("inner");
    fs::create_dir_all(&inner)?;
    let err = hook
        .pre_job(&mut ctx("inner", &inner))
        .await
        .err()
        .ok_or("a directory inside a dataset is not a dataset")?;
    assert!(err.to_string().contains("is not a mount point"), "{err}");
    fs::remove_dir(&inner)?;
    Ok(())
}
