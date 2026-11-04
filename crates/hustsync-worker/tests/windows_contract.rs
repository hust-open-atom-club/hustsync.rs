//! Windows-only behaviour: hooks run through `cmd`, stopped jobs end their whole process tree, Job Objects cap memory, rsync takes drive paths.

#![cfg(windows)]

use std::{collections::HashMap, path::Path, process::Command, sync::Arc, time::Duration};

use hustsync_config_parser::{MemBytes, MirrorConfig, WorkerCgroupConfig, WorkerConfig};
use hustsync_worker::{
    cgroup::Cgroups,
    hooks::{ExecPostHook, HookCtx, JobHook},
    provider::{
        CommonProviderConfig, MirrorProvider, ProviderError, RunContext, Runner, build_provider,
        cmd_provider::{CmdProvider, CmdProviderConfig},
    },
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// Pings no other process on the machine is likely to run.
const MARK: &str = "127.0.0.79";
const JOB_MARK: &str = "127.0.0.78";

fn provider(tmp: &TempDir, command: &str, runner: Runner) -> Res<CmdProvider> {
    let path = |p: &str| tmp.path().join(p).to_string_lossy().into_owned();
    Ok(CmdProvider::new(CmdProviderConfig {
        common: CommonProviderConfig {
            name: "tree".into(),
            upstream_url: String::new(),
            working_dir: path("data"),
            log_dir: path("log"),
            log_file: path("log/latest.log"),
            interval: Duration::from_secs(3600),
            retry: 1,
            timeout: Duration::ZERO,
            env: HashMap::new(),
            is_master: true,
            success_exit_codes: vec![],
            runner,
        },
        command: command.into(),
        fail_on_match: None,
        size_pattern: None,
    })?)
}

fn job(mem: Option<MemBytes>) -> Res<Runner> {
    let cg = Cgroups::init(&WorkerCgroupConfig::default())?;
    Ok(Runner::Cgroup(Arc::new(cg), mem))
}

/// Starts `p`, cancels it once a ping marked `mark` is up, and checks the ping went with it.
async fn cancel_tree(p: CmdProvider, mark: &str) -> Res {
    let cancel = CancellationToken::new();
    let ctx = RunContext {
        cancel: cancel.clone(),
        ..RunContext::default()
    };
    let run = tokio::spawn(async move { p.run(ctx).await });
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(pings_left(mark)?, "ping never started");
    cancel.cancel();

    let res = tokio::time::timeout(Duration::from_secs(10), run).await??;
    assert!(matches!(res, Err(ProviderError::Terminated)), "{res:?}");
    assert!(!pings_left(mark)?, "ping outlived its job");
    Ok(())
}

fn pings_left(mark: &str) -> Res<bool> {
    let query = format!(
        "@(Get-CimInstance Win32_Process -Filter \"Name='PING.EXE'\" | Where-Object CommandLine -match '{mark}').Count"
    );
    let out = Command::new("powershell")
        .args(["-NoProfile", "-Command", &query])
        .output()?;
    Ok(String::from_utf8_lossy(&out.stdout).trim() != "0")
}

#[tokio::test]
async fn exec_hook_runs_through_cmd() -> Res {
    let tmp = TempDir::new()?;
    let out = tmp.path().join("out.txt");
    let mut ctx = HookCtx {
        mirror_name: "m".into(),
        working_dir: tmp.path().to_path_buf(),
        upstream_url: String::new(),
        log_dir: tmp.path().to_path_buf(),
        log_file: tmp.path().join("latest.log"),
        attempt: 1,
        env: HashMap::new(),
    };
    let hook = ExecPostHook::new(
        vec![format!(
            "echo %HUSTSYNC_MIRROR_NAME%> \"{}\"",
            out.display()
        )],
        vec![],
    );
    hook.post_success(&mut ctx).await?;
    assert_eq!(std::fs::read_to_string(&out)?.trim(), "m");
    Ok(())
}

#[tokio::test]
async fn cancel_ends_the_process_tree() -> Res {
    let tmp = TempDir::new()?;
    let p = provider(&tmp, &format!("cmd /C ping -n 60 {MARK}"), Runner::Direct)?;
    cancel_tree(p, MARK).await
}

#[tokio::test]
async fn job_cancel_ends_the_process_tree() -> Res {
    let tmp = TempDir::new()?;
    let p = provider(&tmp, &format!("cmd /C ping -n 60 {JOB_MARK}"), job(None)?)?;
    cancel_tree(p, JOB_MARK).await
}

#[tokio::test]
async fn job_memory_limit_reports_oom() -> Res {
    let tmp = TempDir::new()?;
    let cmd = "powershell -NoProfile -Command \"$b = New-Object byte[] 1073741824; $b[0] = 1\"";
    let p = provider(&tmp, cmd, job(Some(MemBytes(256 << 20)))?)?;
    let res = p.run(RunContext::default()).await;
    assert!(matches!(res, Err(ProviderError::OutOfMemory)), "{res:?}");
    Ok(())
}

/// MSYS2's rsync, the one the Windows docs recommend.
const MSYS_RSYNC: &str = r"C:\msys64\usr\bin\rsync.exe";

#[tokio::test]
async fn rsync_mirrors_into_a_drive_path() -> Res {
    if !Path::new(MSYS_RSYNC).exists() {
        eprintln!("skipped: {MSYS_RSYNC} not installed");
        return Ok(());
    }
    let tmp = TempDir::new()?;
    let src = tmp.path().join("src");
    std::fs::create_dir_all(src.join("pool"))?;
    std::fs::write(src.join("pool/a.deb"), "deb")?;
    let dir = |p: &str| tmp.path().join(p).to_string_lossy().into_owned();
    let m = MirrorConfig {
        name: Some("local".into()),
        provider: Some("rsync".into()),
        command: Some(MSYS_RSYNC.into()),
        upstream: Some(format!("{}/", dir("src").replace('\\', "/"))),
        mirror_dir: Some(dir("dst")),
        log_dir: Some(dir("log")),
        ..MirrorConfig::default()
    };
    let p = build_provider("local", &m, &WorkerConfig::default(), None)?;
    p.run(RunContext::default()).await?;
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("dst/pool/a.deb"))?,
        "deb"
    );
    Ok(())
}
