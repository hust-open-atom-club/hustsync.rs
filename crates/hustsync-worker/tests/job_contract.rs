//! Status messages a job reports while syncing.

#![cfg(unix)]

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use hustsync_internal::status::SyncStatus;
use hustsync_worker::{
    JobMessage, MirrorJob,
    hooks::{HookCtx, HookError, JobHook},
    job::{CtrlAction, JobActor},
    provider::{
        CommonProviderConfig, ProviderError,
        cmd_provider::{CmdProvider, CmdProviderConfig},
    },
};
use tempfile::TempDir;
use tokio::sync::{Semaphore, mpsc};

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;
type Actor = (MirrorJob, mpsc::Receiver<JobMessage>, Arc<Semaphore>);

/// A job actor whose provider `make` builds, sharing a semaphore with `permits` slots.
fn spawn_with(
    dir: &TempDir,
    retry: u32,
    hooks: Vec<Arc<dyn JobHook>>,
    permits: usize,
    make: impl FnOnce(CmdProviderConfig) -> Result<CmdProvider, ProviderError>,
) -> Res<Actor> {
    let path = |p: &str| dir.path().join(p).to_string_lossy().into_owned();
    let provider = make(CmdProviderConfig {
        common: CommonProviderConfig {
            name: "m".into(),
            upstream_url: String::new(),
            working_dir: path("data"),
            log_dir: path("log"),
            log_file: path("log/latest.log"),
            interval: Duration::from_secs(3600),
            retry,
            timeout: Duration::from_secs(60),
            env: HashMap::new(),
            is_master: true,
            success_exit_codes: vec![],
            runner: Default::default(),
        },
        command: String::new(),
        fail_on_match: None,
        size_pattern: None,
    })?;
    let (tx, rx) = mpsc::channel::<JobMessage>(32);
    let sem = Arc::new(Semaphore::new(permits));
    let (job, actor) = JobActor::new("m".into(), tx, Arc::clone(&sem), Box::new(provider), hooks);
    tokio::spawn(actor.run());
    Ok((job, rx, sem))
}

/// A job actor running `command`.
fn spawn(
    dir: &TempDir,
    command: &str,
    retry: u32,
    hooks: Vec<Arc<dyn JobHook>>,
    permits: usize,
) -> Res<Actor> {
    spawn_with(dir, retry, hooks, permits, |mut cfg| {
        cfg.command = command.into();
        CmdProvider::new(cfg)
    })
}

async fn next(rx: &mut mpsc::Receiver<JobMessage>, secs: u64) -> Option<JobMessage> {
    tokio::time::timeout(Duration::from_secs(secs), rx.recv())
        .await
        .ok()
        .flatten()
}

async fn status(rx: &mut mpsc::Receiver<JobMessage>, secs: u64) -> Res<SyncStatus> {
    Ok(next(rx, secs).await.ok_or("no report")?.status)
}

struct Broken;

#[async_trait]
impl JobHook for Broken {
    fn name(&self) -> &str {
        "broken"
    }

    async fn pre_job(&self, _ctx: &mut HookCtx) -> Result<(), HookError> {
        Err(HookError::config("broken", "no disk"))
    }
}

#[tokio::test]
async fn failed_pre_job_hook_reports_and_reschedules() -> Res {
    let dir = TempDir::new()?;
    let (job, mut rx, _sem) = spawn(&dir, "true", 2, vec![Arc::new(Broken)], 1)?;
    job.send_ctrl(CtrlAction::Start).await?;
    assert_eq!(status(&mut rx, 10).await?, SyncStatus::PreSyncing);
    let failed = next(&mut rx, 10).await.ok_or("no report")?;
    assert_eq!((failed.status, failed.schedule), (SyncStatus::Failed, true));
    assert!(
        failed.msg.starts_with("error exec hook pre_job: "),
        "{}",
        failed.msg
    );
    Ok(())
}
