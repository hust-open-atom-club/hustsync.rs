//! fpsync mirrors against the real tool; skipped when it is missing.

#![cfg(unix)]

use std::path::Path;

use hustsync_config_parser::{MirrorConfig, WorkerConfig};
use hustsync_worker::provider::{MirrorProvider, RunContext, build_provider};
use tempfile::TempDir;

fn has(bin: &str) -> bool {
    let ok = std::process::Command::new("sh")
        .args(["-c", &format!("command -v {bin}")])
        .output()
        .is_ok_and(|o| o.status.success());
    if !ok {
        eprintln!("skipped: {bin} not installed");
    }
    ok
}

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn mirror(tmp: &TempDir, provider: &str, opts: &[&str]) -> Res<Box<dyn MirrorProvider>> {
    let dir = |p: &str| tmp.path().join(p).to_string_lossy().into_owned();
    let opts = Some(opts.iter().map(|s| (*s).to_owned()).collect());
    let m = MirrorConfig {
        name: Some("m".into()),
        provider: Some(provider.into()),
        upstream: Some(dir("src")),
        mirror_dir: Some(dir("dst")),
        log_dir: Some(dir("log")),
        fpsync_options: opts,
        ..MirrorConfig::default()
    };
    Ok(build_provider("m", &m, &WorkerConfig::default(), None)?)
}

fn seed(src: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(src.join("pool/main"))?;
    (0..20)
        .try_for_each(|i| std::fs::write(src.join(format!("pool/main/f{i}")), format!("data {i}")))
}

#[tokio::test]
async fn fpsync_pulls_a_local_tree() -> Res {
    if !has("fpsync") {
        return Ok(());
    }
    let tmp = TempDir::new()?;
    seed(&tmp.path().join("src"))?;
    let p = mirror(&tmp, "fpsync", &["-n", "2", "-f", "5"])?;
    p.run(RunContext::default()).await?;
    let got = std::fs::read_to_string(tmp.path().join("dst/pool/main/f7"))?;
    assert_eq!(got, "data 7");
    Ok(())
}
