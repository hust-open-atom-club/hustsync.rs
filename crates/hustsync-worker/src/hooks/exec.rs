//! `exec_on_success` / `exec_on_failure` hook.
//!
//! Runs each configured shell command via `sh -c` with a standardised
//! env block using canonical `HUSTSYNC_*` variables plus legacy
//! `TUNASYNC_*` aliases with the same values. Non-zero exit from one
//! command is logged but does not abort the list; the next command still runs.
//!
//! Invariant: `HUSTSYNC_JOB_EXIT_STATUS` and its legacy
//! `TUNASYNC_JOB_EXIT_STATUS` alias are the literal strings `"success"` on the
//! success hook and `"failure"` on the fail hook. Do NOT change these spellings.

use std::time::Duration;

use async_trait::async_trait;
use std::collections::HashMap;
use tokio::process::Command;
use tokio::time::timeout;

use super::{HookCtx, HookError, JobHook};

/// Hard-coded 30s per-command budget. Operator-tunable timeouts and
/// per-status overrides are future extensions.
const EXEC_TIMEOUT: Duration = Duration::from_secs(30);

const HOOK_ENV_SUFFIXES: &[&str] = &[
    "MIRROR_NAME",
    "WORKING_DIR",
    "UPSTREAM_URL",
    "LOG_DIR",
    "LOG_FILE",
    "JOB_EXIT_STATUS",
];

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
    let mut env = HashMap::new();
    for (key, val) in pairs {
        set_sync_env_pair(&mut env, key, val);
    }
    if !status.is_empty() {
        set_sync_env_pair(&mut env, "JOB_EXIT_STATUS", status.to_string());
    }
    // Propagate any hook-injected env from upstream (e.g. loglimit's
    // updated log_file). Later entries override earlier; HUSTSYNC_* wins if a
    // legacy TUNASYNC_* alias conflicts with it.
    apply_env_layer(&mut env, &ctx.env);
    env.into_iter().collect()
}

fn sync_env_keys(suffix: &str) -> (String, String) {
    (format!("HUSTSYNC_{suffix}"), format!("TUNASYNC_{suffix}"))
}

fn set_sync_env_pair(env: &mut HashMap<String, String>, suffix: &str, value: String) {
    let (canonical, legacy) = sync_env_keys(suffix);
    env.insert(canonical, value.clone());
    env.insert(legacy, value);
}

fn known_sync_suffix(key: &str) -> Option<&'static str> {
    let suffix = key
        .strip_prefix("HUSTSYNC_")
        .or_else(|| key.strip_prefix("TUNASYNC_"))?;
    HOOK_ENV_SUFFIXES
        .iter()
        .copied()
        .find(|candidate| *candidate == suffix)
}

fn apply_env_layer(env: &mut HashMap<String, String>, layer: &HashMap<String, String>) {
    for (key, value) in layer {
        if known_sync_suffix(key).is_none() {
            env.insert(key.clone(), value.clone());
        }
    }

    for suffix in HOOK_ENV_SUFFIXES {
        let (canonical, legacy) = sync_env_keys(suffix);
        match (layer.get(&canonical), layer.get(&legacy)) {
            (Some(canonical_value), Some(legacy_value)) => {
                if canonical_value != legacy_value {
                    tracing::warn!("conflicting {canonical}/{legacy}; using canonical {canonical}");
                }
                set_sync_env_pair(env, suffix, canonical_value.clone());
            }
            (Some(value), None) | (None, Some(value)) => {
                set_sync_env_pair(env, suffix, value.clone());
            }
            (None, None) => {}
        }
    }
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
