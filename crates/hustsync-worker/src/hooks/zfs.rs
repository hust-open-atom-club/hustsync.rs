//! `[zfs]`: refuses to sync into a directory that is not a ZFS dataset of its own,
//! so a missing dataset cannot fill the parent with one mirror's data.

use std::{io, os::unix::fs::MetadataExt, path::Path};

use async_trait::async_trait;
use nix::sys::statfs::{FsType, statfs};

use super::{HookCtx, HookError, JobHook};

const HOOK_NAME: &str = "zfs";

/// `ZFS_SUPER_MAGIC`; nix names only the filesystems in the kernel tree.
const ZFS: FsType = FsType(0x2fc1_2fc1);

pub struct ZfsHook {
    pool: String,
}

impl ZfsHook {
    pub fn new(pool: impl Into<String>) -> Self {
        Self { pool: pool.into() }
    }

    fn help(&self, ctx: &HookCtx) -> String {
        let dataset = format!("{}/{}", self.pool, ctx.mirror_name).to_lowercase();
        let dir = ctx.working_dir.display();
        let uid = nix::unistd::geteuid();
        let own = if uid.is_root() {
            String::new()
        } else {
            format!(" && chown {uid} '{dir}'")
        };
        format!(
            "create it with: zfs create '{dataset}' && zfs set mountpoint='{dir}' '{dataset}'{own}"
        )
    }
}

#[async_trait]
impl JobHook for ZfsHook {
    fn name(&self) -> &str {
        HOOK_NAME
    }

    async fn pre_job(&self, ctx: &mut HookCtx) -> Result<(), HookError> {
        let dir = ctx.working_dir.clone();
        let found = tokio::task::spawn_blocking(move || refusal(&dir))
            .await
            .map_err(|e| HookError::io(HOOK_NAME, io::Error::other(e)))?;
        let why = match found {
            Ok(None) => return Ok(()),
            Ok(Some(why)) => why,
            Err(e) if e.kind() == io::ErrorKind::NotFound => "does not exist",
            Err(e) => return Err(HookError::io(HOOK_NAME, e)),
        };
        Err(HookError::config(
            HOOK_NAME,
            format!("{} {why}; {}", ctx.working_dir.display(), self.help(ctx)),
        ))
    }
}

/// Why `dir` is no dataset of its own, or `None` when it is one.
fn refusal(dir: &Path) -> io::Result<Option<&'static str>> {
    if statfs(dir)?.filesystem_type() != ZFS {
        return Ok(Some("is not on ZFS"));
    }
    let own = std::fs::metadata(dir)?.dev() != std::fs::metadata(dir.join(".."))?.dev();
    Ok((!own).then_some("is not a mount point"))
}
