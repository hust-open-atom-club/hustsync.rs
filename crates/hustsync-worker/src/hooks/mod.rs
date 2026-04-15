//! Job hook pipeline.
//!
//! Hooks fire at the five phases of `JobActor::run_sync_loop`:
//! `pre_job` → `pre_exec` → (provider runs) → `post_exec` → either
//! `post_success` or `post_fail`. Config order runs for the `pre_*`
//! phases; the `post_*` phases run in reverse (LIFO) so teardown
//! unwinds in the opposite order of setup.
//!
//! A hook returning `Err(_)` from `pre_job` / `pre_exec` aborts the
//! sync without running the provider; `post_fail` still runs on
//! already-executed hooks so operators see a complete trail.
//!
//! A hook returning `Err(_)` from any `post_*` phase is logged but
//! does not change the recorded sync outcome.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;

pub use crate::error::{HookError, HookErrorKind};

pub mod exec;
pub mod loglimit;
pub mod working_dir;

pub use exec::ExecPostHook;
pub use loglimit::LogLimitHook;
pub use working_dir::WorkingDirHook;

/// Context passed through the hook pipeline.
///
/// `log_file` may be rewritten by a hook (e.g. `LogLimitHook` on
/// `pre_exec` rotates in a fresh timestamped file and updates the
/// context so subsequent hooks + the provider see the new path).
pub struct HookCtx {
    pub mirror_name: String,
    pub working_dir: PathBuf,
    pub upstream_url: String,
    pub log_dir: PathBuf,
    pub log_file: PathBuf,
    pub attempt: u32,
    pub env: HashMap<String, String>,
}

impl HookCtx {
    pub fn log_file_path(&self) -> &Path {
        &self.log_file
    }
}

#[async_trait]
pub trait JobHook: Send + Sync {
    fn name(&self) -> &str;

    async fn pre_job(&self, _ctx: &mut HookCtx) -> Result<(), HookError> {
        Ok(())
    }

    async fn pre_exec(&self, _ctx: &mut HookCtx) -> Result<(), HookError> {
        Ok(())
    }

    async fn post_exec(&self, _ctx: &mut HookCtx) -> Result<(), HookError> {
        Ok(())
    }

    async fn post_success(&self, _ctx: &mut HookCtx) -> Result<(), HookError> {
        Ok(())
    }

    async fn post_fail(&self, _ctx: &mut HookCtx) -> Result<(), HookError> {
        Ok(())
    }
}
