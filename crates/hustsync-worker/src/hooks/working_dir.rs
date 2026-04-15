//! Always-on built-in hook that ensures `working_dir` exists with mode
//! `0o755` before the sync runs. Never creates parents recursively —
//! a missing parent is an operator mistake that should surface loudly.

use async_trait::async_trait;

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
        if dir.is_dir() {
            return Ok(());
        }
        // Refuse to create parents — Spec §4.3 says parent must exist.
        if let Some(parent) = dir.parent()
            && !parent.exists()
        {
            return Err(HookError::config(
                HOOK_NAME,
                format!(
                    "parent directory of working_dir does not exist: {}",
                    parent.display()
                ),
            ));
        }
        std::fs::create_dir(dir).map_err(|e| HookError::io(HOOK_NAME, e))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perm = std::fs::Permissions::from_mode(0o755);
            let _ = std::fs::set_permissions(dir, perm);
        }
        Ok(())
    }
}
