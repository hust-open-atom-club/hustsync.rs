//! `exec_on_success` / `exec_on_failure` hook.
//!
//! Runs each configured shell command via `sh -c` with a standardised
//! env block matching Go `worker/exec_post_hook.go` verbatim
//! (`TUNASYNC_*`) plus the `HUSTSYNC_*` synonyms. Non-zero exit from
//! one command is logged but does not abort the list; the next command
//! still runs.
//!
//! Invariant: `TUNASYNC_JOB_EXIT_STATUS` is the literal string
//! `"success"` on the success hook and `"failure"` on the fail hook —
//! do NOT change these spellings, operator scripts depend on them.

use std::time::Duration;

use async_trait::async_trait;
use tokio::process::Command;
use tokio::time::timeout;

use super::{HookCtx, HookError, JobHook};

/// Hard-coded 30s per-command budget. Operator-tunable timeouts and
/// per-status overrides are future extensions.
const EXEC_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ExecPostHook {
    pub on_success: Vec<String>,
    pub on_failure: Vec<String>,
}

impl ExecPostHook {
    pub fn new(on_success: Vec<String>, on_failure: Vec<String>) -> Self {
        Self {
            on_success,
            on_failure,
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn run_commands(&self, phase: &str, cmds: &[String], ctx: &HookCtx) {
        if cmds.is_empty() {
            return;
        }
        let env = build_env(ctx, phase);
        for cmd in cmds {
            let res = timeout(EXEC_TIMEOUT, spawn_shell(cmd, &env)).await;
            match res {
                Ok(Ok(status)) if status.success() => {}
                Ok(Ok(status)) => {
                    tracing::warn!(
                        "{} hook command `{}` exited with {} — continuing",
                        self.name(),
                        cmd,
                        status
                    );
                }
                Ok(Err(e)) => {
                    tracing::warn!(
                        "{} hook command `{}` failed to spawn: {} — continuing",
                        self.name(),
                        cmd,
                        e
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        "{} hook command `{}` exceeded {:?} — continuing",
                        self.name(),
                        cmd,
                        EXEC_TIMEOUT
                    );
                }
            }
        }
    }
}

async fn spawn_shell(
    cmd: &str,
    env: &[(String, String)],
) -> std::io::Result<std::process::ExitStatus> {
    let mut child = Command::new("sh");
    child.arg("-c").arg(cmd);
    for (k, v) in env {
        child.env(k, v);
    }
    child.spawn()?.wait().await
}

fn build_env(ctx: &HookCtx, phase: &str) -> Vec<(String, String)> {
    let status = match phase {
        "post_success" => "success",
        "post_fail" => "failure",
        _ => "",
    };
    let pairs = [
        ("MIRROR_NAME", ctx.mirror_name.clone()),
        (
            "WORKING_DIR",
            ctx.working_dir.to_string_lossy().into_owned(),
        ),
        ("UPSTREAM_URL", ctx.upstream_url.clone()),
        ("LOG_DIR", ctx.log_dir.to_string_lossy().into_owned()),
        ("LOG_FILE", ctx.log_file.to_string_lossy().into_owned()),
    ];
    let mut env = Vec::with_capacity(pairs.len() * 2 + 2);
    for (key, val) in &pairs {
        env.push((format!("TUNASYNC_{key}"), val.clone()));
        env.push((format!("HUSTSYNC_{key}"), val.clone()));
    }
    if !status.is_empty() {
        env.push(("TUNASYNC_JOB_EXIT_STATUS".to_string(), status.to_string()));
        env.push(("HUSTSYNC_JOB_EXIT_STATUS".to_string(), status.to_string()));
    }
    // Propagate any hook-injected env from upstream (e.g. loglimit's
    // updated log_file). Later entries override earlier.
    for (k, v) in &ctx.env {
        env.push((k.clone(), v.clone()));
    }
    env
}

#[async_trait]
impl JobHook for ExecPostHook {
    fn name(&self) -> &str {
        "exec_post"
    }

    async fn post_success(&self, ctx: &mut HookCtx) -> Result<(), HookError> {
        self.run_commands("post_success", &self.on_success, ctx)
            .await;
        Ok(())
    }

    async fn post_fail(&self, ctx: &mut HookCtx) -> Result<(), HookError> {
        self.run_commands("post_fail", &self.on_failure, ctx).await;
        Ok(())
    }
}
