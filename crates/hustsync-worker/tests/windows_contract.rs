//! Windows-only behaviour: hooks run through `cmd`, stopped jobs end their whole process tree.

#![cfg(windows)]

use std::{collections::HashMap, process::Command, time::Duration};

use hustsync_worker::{
    hooks::{ExecPostHook, HookCtx, JobHook},
    provider::{
        CommonProviderConfig, MirrorProvider, ProviderError, RunContext, Runner,
        cmd_provider::{CmdProvider, CmdProviderConfig},
    },
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// Pings no other process on the machine is likely to run.
const MARK: &str = "127.0.0.79";

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
