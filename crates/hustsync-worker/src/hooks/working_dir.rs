//! Always-on built-in hook that ensures `working_dir` exists with mode
//! `0o755` before the sync runs. Never creates parents recursively — a
//! missing parent is an operator mistake that should surface loudly.

use async_trait::async_trait;
use tokio::fs;

use super::{HookCtx, HookError, JobHook};

const HOOK_NAME: &str = "working_dir";

pub struct WorkingDirHook;

impl WorkingDirHook {
    pub fn new() -> Self {
        Self
    }
}

impl Default for WorkingDirHook {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl JobHook for WorkingDirHook {
    fn name(&self) -> &str {
        HOOK_NAME
    }

    async fn pre_job(&self, ctx: &mut HookCtx) -> Result<(), HookError> {
        let dir = &ctx.working_dir;
        if fs::metadata(dir).await.map(|m| m.is_dir()).unwrap_or(false) {
            return Ok(());
        }
        // Parent MUST exist — refuse to create it recursively.
        if let Some(parent) = dir.parent()
            && fs::metadata(parent).await.is_err()
        {
            return Err(HookError::config(
                HOOK_NAME,
                format!(
                    "parent directory of working_dir does not exist: {}",
                    parent.display()
                ),
            ));
        }
        fs::create_dir(dir)
            .await
            .map_err(|e| HookError::io(HOOK_NAME, e))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perm = std::fs::Permissions::from_mode(0o755);
            let _ = fs::set_permissions(dir, perm).await;
        }
        Ok(())
    }
}
