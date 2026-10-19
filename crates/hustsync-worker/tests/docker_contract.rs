//! Runs the command provider inside real containers; skipped without a Docker daemon.

use std::{collections::HashMap, process::Command, time::Duration};

use hustsync_worker::provider::{
    CommonProviderConfig, Docker, MirrorProvider, ProviderError, RunContext, Runner,
    cmd_provider::{CmdProvider, CmdProviderConfig},
    runner::container,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const IMAGE: &str = "alpine:3.23";

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn docker_ok() -> bool {
    let ok = Command::new("docker")
        .args(["image", "inspect", IMAGE])
        .output()
        .is_ok_and(|o| o.status.success());
    if !ok {
        eprintln!("skipped: docker or {IMAGE} unavailable");
    }
    ok
}

fn provider(dir: &TempDir, name: &str, command: &str) -> Res<CmdProvider> {
    let path = |p: &str| dir.path().join(p).to_string_lossy().into_owned();
    Ok(CmdProvider::new(CmdProviderConfig {
        common: CommonProviderConfig {
            name: name.into(),
            upstream_url: "https://upstream.example/".into(),
            working_dir: path("data"),
            log_dir: path("log"),
            log_file: path("log/latest.log"),
            interval: Duration::from_secs(3600),
            retry: 1,
            timeout: Duration::ZERO,
            env: HashMap::from([("TOKEN".into(), "s3cret".into())]),
            is_master: true,
            success_exit_codes: vec![],
            runner: Runner::Docker(Docker {
                image: IMAGE.into(),
                volumes: vec![],
                options: vec![],
                memory: None,
            }),
        },
        command: command.into(),
        fail_on_match: None,
        size_pattern: None,
    })?)
}

fn container_exists(name: &str) -> bool {
    Command::new("docker")
        .args(["container", "inspect", &container(name)])
        .output()
        .is_ok_and(|o| o.status.success())
}

#[tokio::test]
async fn runs_in_container_with_env_and_mounts() -> Res {
    if !docker_ok() {
        return Ok(());
    }
    let dir = TempDir::new()?;
    let p = provider(
        &dir,
        "dk-env",
        "sh -c 'echo $HUSTSYNC_MIRROR_NAME $TOKEN; cat /etc/alpine-release; pwd'",
    )?;
    p.run(RunContext::default()).await?;

    let log = std::fs::read_to_string(dir.path().join("log/latest.log"))?;
    let data = dir.path().join("data");
    assert!(log.contains("dk-env s3cret"), "{log}");
    assert!(log.contains("3.23"), "{log}");
    assert!(log.contains(&*data.to_string_lossy()), "{log}");
    assert!(!container_exists("dk-env"));
    Ok(())
}

#[tokio::test]
async fn cancel_stops_and_removes_container() -> Res {
    if !docker_ok() {
        return Ok(());
    }
    let dir = TempDir::new()?;
    let p = provider(&dir, "dk-stop", "sleep 60")?;
    let cancel = CancellationToken::new();
    let ctx = RunContext {
        cancel: cancel.clone(),
        ..RunContext::default()
    };

    let run = tokio::spawn(async move { p.run(ctx).await });
    for _ in 0..150 {
        if container_exists("dk-stop") || run.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    if run.is_finished() {
        let log = std::fs::read_to_string(dir.path().join("log/latest.log")).unwrap_or_default();
        return Err(format!(
            "run ended before the container started: {:?}\n{log}",
            run.await?
        )
        .into());
    }
    assert!(
        container_exists("dk-stop"),
        "container did not start within 30 s"
    );
    cancel.cancel();

    let res = tokio::time::timeout(Duration::from_secs(20), run).await??;
    assert!(matches!(res, Err(ProviderError::Terminated)), "{res:?}");
    assert!(!container_exists("dk-stop"));
    Ok(())
}
