//! `loglimit` hook — log-file layout matching Go `worker/loglimit_hook.go`.
//!
//! On `pre_exec`:
//!   - Delete oldest `<mirror>_*.log*` files until at most 10 remain.
//!   - Create a fresh timestamped log file
//!     `<log_dir>/<mirror>_<YYYY-MM-DD_HH-MM-SS>.log` and update the
//!     `HookCtx.log_file` so downstream hooks + the provider write to it.
//!   - (Re)point `<log_dir>/<mirror>_latest.log` at the new file.
//!
//! On `post_fail`: rename the current file to `<...>.log.fail` and
//! re-point the `_latest` symlink.
//!
//! Retention is hard-coded at 10 per Spec §4.2 — operator-tunable is
//! track B.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use async_trait::async_trait;
use chrono::{DateTime, Local, SecondsFormat};

use super::{HookCtx, HookError, JobHook};

/// Matches Go's layout `2006-01-02_15-04-05` at runtime timezone.
const TIMESTAMP_FMT: &str = "%Y-%m-%d_%H-%M-%S";

/// Files matching `<mirror>_*.log*` beyond this count are purged on
/// `pre_exec`. Compressed `.log.gz` files count too (future track B).
const RETENTION: usize = 10;

const HOOK_NAME: &str = "loglimit";

pub struct LogLimitHook;

impl LogLimitHook {
    pub fn new() -> Self {
        Self
    }

    fn latest_symlink(&self, ctx: &HookCtx) -> PathBuf {
        ctx.log_dir.join(format!("{}_latest.log", ctx.mirror_name))
    }

    fn prefix(&self, ctx: &HookCtx) -> String {
        format!("{}_", ctx.mirror_name)
    }

    /// Return all files in `log_dir` whose name starts with
    /// `<mirror>_` and contains `.log` (incl. `.log.fail` / `.log.gz`).
    fn mirror_logs(&self, ctx: &HookCtx) -> Result<Vec<(PathBuf, SystemTime)>, HookError> {
        let mut out = Vec::new();
        let prefix = self.prefix(ctx);
        let iter = match std::fs::read_dir(&ctx.log_dir) {
            Ok(i) => i,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(HookError::io(HOOK_NAME, e)),
        };
        for entry in iter.flatten() {
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            if !name.starts_with(&prefix) || !name.contains(".log") {
                continue;
            }
            // Never list the symlink itself.
            if name == format!("{}_latest.log", ctx.mirror_name) {
                continue;
            }
            let Ok(meta) = entry.metadata() else { continue };
            let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            out.push((entry.path(), mtime));
        }
        // Newest first.
        out.sort_by_key(|(_, t)| std::cmp::Reverse(*t));
        Ok(out)
    }

    fn rotate(&self, ctx: &HookCtx) -> Result<(), HookError> {
        let files = self.mirror_logs(ctx)?;
        for (path, _) in files.into_iter().skip(RETENTION) {
            if let Err(e) = std::fs::remove_file(&path) {
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

    fn point_symlink(&self, target: &Path, link: &Path) -> Result<(), HookError> {
        let _ = std::fs::remove_file(link);
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(target, link).map_err(|e| HookError::io(HOOK_NAME, e))?;
        }
        #[cfg(not(unix))]
        {
            // No symlink support — write a plain file pointing at the
            // target path so dashboards at least find it.
            std::fs::write(link, target.to_string_lossy().as_bytes())
                .map_err(|e| HookError::io(HOOK_NAME, e))?;
        }
        Ok(())
    }

    /// Public helper for tests — format a log-file name from a given
    /// instant. Keeps the timestamp parsing strictly in one place.
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
        // Ensure the log directory exists before we create files in it.
        if let Err(e) = std::fs::create_dir_all(&ctx.log_dir) {
            return Err(HookError::io(HOOK_NAME, e));
        }

        self.rotate(ctx)?;

        let name = Self::format_name(&ctx.mirror_name, Local::now());
        let new_file = ctx.log_dir.join(&name);
        // `touch` the new file so it exists and mtime is fresh.
        std::fs::File::create(&new_file).map_err(|e| HookError::io(HOOK_NAME, e))?;

        ctx.log_file = new_file.clone();
        self.point_symlink(&new_file, &self.latest_symlink(ctx))?;
        // Surface the log_file to subsequent hooks / provider env.
        ctx.env.insert(
            "TUNASYNC_LOG_FILE".into(),
            new_file.to_string_lossy().into_owned(),
        );
        ctx.env.insert(
            "HUSTSYNC_LOG_FILE".into(),
            new_file.to_string_lossy().into_owned(),
        );

        // Silence the unused-import warning for chrono's SecondsFormat
        // in this module — it's kept for future timestamp comparison.
        let _ = SecondsFormat::Secs;

        Ok(())
    }

    async fn post_fail(&self, ctx: &mut HookCtx) -> Result<(), HookError> {
        if !ctx.log_file.exists() {
            return Ok(());
        }
        let failed = ctx.log_file.with_extension("log.fail");
        if let Err(e) = std::fs::rename(&ctx.log_file, &failed) {
            tracing::warn!(
                "{HOOK_NAME}: failed to rename {} → {}: {} — leaving original in place",
                ctx.log_file.display(),
                failed.display(),
                e
            );
            return Ok(());
        }
        ctx.log_file = failed.clone();
        self.point_symlink(&failed, &self.latest_symlink(ctx))?;
        Ok(())
    }
}
