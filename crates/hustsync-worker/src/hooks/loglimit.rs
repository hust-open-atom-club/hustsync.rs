//! Log-file layout matching Go `worker/loglimit_hook.go`.
//!
//! On `pre_exec`:
//!   - Rotate out oldest `<mirror>_*.log*` files beyond the retention budget.
//!   - Create a fresh timestamped log file
//!     `<log_dir>/<mirror>_<YYYY-MM-DD_HH_MM>.log` and update the
//!     `HookCtx.log_file` so downstream hooks + the provider write to it.
//!   - (Re)point `<log_dir>/latest` at the new file.
//!
//! On `post_fail`: rename the current file to `<...>.log.fail` and
//! re-point the `latest` symlink.
//!
//! Retention is hard-coded at 9 kept-before-create + 1 fresh = 10 total
//! on disk after each rotation pass. Operator-tunable retention and
//! compressed `.log.gz` rotation are future extensions.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use async_trait::async_trait;
use chrono::{DateTime, Local};
use tokio::fs;

use super::{HookCtx, HookError, JobHook};

/// Matches Go's `LogDirTimeFormat = "2006-01-02_15_04"` — minute
/// precision, no seconds; hour and minute joined by `_`, not `-`.
const TIMESTAMP_FMT: &str = "%Y-%m-%d_%H_%M";

/// Go keeps 9 old files and creates the new one on top, so after
/// `pre_exec` there are 10 on disk. Matching this fencepost keeps the
/// Go test suite's `len(files) == 10` assertion happy.
const RETENTION: usize = 9;

const HOOK_NAME: &str = "loglimit";
const LATEST_LINK: &str = "latest";

pub struct LogLimitHook;

impl LogLimitHook {
    pub fn new() -> Self {
        Self
    }

    fn latest_symlink(&self, ctx: &HookCtx) -> PathBuf {
        // Go writes a bare `latest` sibling in the per-mirror log dir;
        // dashboards and operator `tail -f` commands key off this
        // exact filename.
        ctx.log_dir.join(LATEST_LINK)
    }

    fn prefix(&self, ctx: &HookCtx) -> String {
        format!("{}_", ctx.mirror_name)
    }

    /// Return all files in `log_dir` whose name starts with
    /// `<mirror>_` and contains `.log` (including `.log.fail` and
    /// `.log.gz`). Excludes the `latest` symlink itself.
    async fn mirror_logs(&self, ctx: &HookCtx) -> Result<Vec<(PathBuf, SystemTime)>, HookError> {
        let mut out = Vec::new();
        let prefix = self.prefix(ctx);
        let mut iter = match fs::read_dir(&ctx.log_dir).await {
            Ok(i) => i,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(HookError::io(HOOK_NAME, e)),
        };
        while let Some(entry) = iter
            .next_entry()
            .await
            .map_err(|e| HookError::io(HOOK_NAME, e))?
        {
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            if !name.starts_with(&prefix) || !name.contains(".log") {
                continue;
            }
            if name == LATEST_LINK {
                continue;
            }
            let Ok(meta) = entry.metadata().await else {
                continue;
            };
            let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            out.push((entry.path(), mtime));
        }
        // Newest first.
        out.sort_by_key(|(_, t)| std::cmp::Reverse(*t));
        Ok(out)
    }

    async fn rotate(&self, ctx: &HookCtx) -> Result<(), HookError> {
        let files = self.mirror_logs(ctx).await?;
        for (path, _) in files.into_iter().skip(RETENTION) {
            if let Err(e) = fs::remove_file(&path).await {
                // Don't fail the sync on a rotation glitch; logged and
                // continue so the fresh log file still gets created.
                tracing::warn!(
                    "{HOOK_NAME}: failed to remove {}: {} — continuing",
                    path.display(),
                    e
                );
            }
        }
        Ok(())
    }

    async fn point_symlink(&self, target: &Path, link: &Path) -> Result<(), HookError> {
        let _ = fs::remove_file(link).await;
        #[cfg(unix)]
        {
            fs::symlink(target, link)
                .await
                .map_err(|e| HookError::io(HOOK_NAME, e))?;
        }
        #[cfg(not(unix))]
        {
            // No symlink support — write a plain file containing the
            // target path so dashboards at least find it.
            fs::write(link, target.to_string_lossy().as_bytes())
                .await
                .map_err(|e| HookError::io(HOOK_NAME, e))?;
        }
        Ok(())
    }

    /// Format a log-file name from a given instant. Test helper that
    /// keeps the timestamp pattern in one place.
    pub fn format_name(mirror: &str, when: DateTime<Local>) -> String {
        format!("{}_{}.log", mirror, when.format(TIMESTAMP_FMT))
    }
}

impl Default for LogLimitHook {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl JobHook for LogLimitHook {
    fn name(&self) -> &str {
        HOOK_NAME
    }

    async fn pre_exec(&self, ctx: &mut HookCtx) -> Result<(), HookError> {
        fs::create_dir_all(&ctx.log_dir)
            .await
            .map_err(|e| HookError::io(HOOK_NAME, e))?;

        self.rotate(ctx).await?;

        let name = Self::format_name(&ctx.mirror_name, Local::now());
        let new_file = ctx.log_dir.join(&name);
        // `touch` the new file so it exists and mtime is fresh.
        fs::File::create(&new_file)
            .await
            .map_err(|e| HookError::io(HOOK_NAME, e))?;

        ctx.log_file = new_file.clone();
        self.point_symlink(&new_file, &self.latest_symlink(ctx))
            .await?;
        // Surface the log_file to subsequent hooks / provider env.
        let as_str = new_file.to_string_lossy().into_owned();
        ctx.env.insert("TUNASYNC_LOG_FILE".into(), as_str.clone());
        ctx.env.insert("HUSTSYNC_LOG_FILE".into(), as_str);

        Ok(())
    }

    async fn post_fail(&self, ctx: &mut HookCtx) -> Result<(), HookError> {
        if fs::metadata(&ctx.log_file).await.is_err() {
            return Ok(());
        }
        let failed = ctx.log_file.with_extension("log.fail");
        if let Err(e) = fs::rename(&ctx.log_file, &failed).await {
            tracing::warn!(
                "{HOOK_NAME}: failed to rename {} → {}: {} — leaving original in place",
                ctx.log_file.display(),
                failed.display(),
                e
            );
            return Ok(());
        }
        ctx.log_file = failed.clone();
        self.point_symlink(&failed, &self.latest_symlink(ctx))
            .await?;
        Ok(())
    }
}
