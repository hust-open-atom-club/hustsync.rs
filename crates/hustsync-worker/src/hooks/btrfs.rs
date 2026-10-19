//! `[btrfs_snapshot]`: syncs into a Btrfs subvolume and, after each success, swaps
//! in a fresh snapshot of it, so whoever serves the snapshot never sees a sync half done.

use std::{
    ffi::CString,
    fs::File,
    io,
    os::{fd::AsRawFd, unix::ffi::OsStrExt, unix::fs::MetadataExt},
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use nix::{
    libc,
    sys::statfs::{BTRFS_SUPER_MAGIC, statfs},
};

use super::{HookCtx, HookError, JobHook};

const HOOK_NAME: &str = "btrfs_snapshot";

pub struct BtrfsSnapshotHook {
    snapshot: PathBuf,
}

impl BtrfsSnapshotHook {
    pub fn new(snapshot: impl Into<PathBuf>) -> Self {
        Self {
            snapshot: snapshot.into(),
        }
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, HookError> + Send + 'static,
) -> Result<T, HookError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| HookError::io(HOOK_NAME, io::Error::other(e)))?
}

fn io_err(e: impl Into<io::Error>) -> HookError {
    HookError::io(HOOK_NAME, e.into())
}

#[async_trait]
impl JobHook for BtrfsSnapshotHook {
    fn name(&self) -> &str {
        HOOK_NAME
    }

    async fn pre_job(&self, ctx: &mut HookCtx) -> Result<(), HookError> {
        let dir = ctx.working_dir.clone();
        blocking(move || match subvol::is_subvolume(&dir) {
            Ok(true) => Ok(()),
            Ok(false) => Err(HookError::config(
                HOOK_NAME,
                format!("{} exists but is not a Btrfs subvolume", dir.display()),
            )),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                subvol::create(&dir).map_err(io_err)?;
                tracing::info!("created Btrfs subvolume {}", dir.display());
                Ok(())
            }
            Err(e) => Err(io_err(e)),
        })
        .await
    }

    async fn post_success(&self, ctx: &mut HookCtx) -> Result<(), HookError> {
        let (dir, snap) = (ctx.working_dir.clone(), self.snapshot.clone());
        blocking(move || publish(&dir, &snap)).await
    }
}

/// Snapshots `dir` beside `snap`, then exchanges the two in one rename and drops the old one.
fn publish(dir: &Path, snap: &Path) -> Result<(), HookError> {
    let fresh = sibling(snap, "new")?;
    drop_if_subvolume(&fresh)?;
    subvol::snapshot(dir, &fresh).map_err(io_err)?;
    match subvol::is_subvolume(snap) {
        Ok(true) => {
            subvol::exchange(&fresh, snap).map_err(io_err)?;
            subvol::delete(&fresh).map_err(io_err)?;
        }
        Ok(false) => {
            subvol::delete(&fresh).map_err(io_err)?;
            return Err(HookError::config(
                HOOK_NAME,
                format!("{} exists but is not a Btrfs snapshot", snap.display()),
            ));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            std::fs::rename(&fresh, snap).map_err(io_err)?;
        }
        Err(e) => return Err(io_err(e)),
    }
    tracing::info!("published Btrfs snapshot {}", snap.display());
    Ok(())
}

/// A hidden name next to `path`, on the same filesystem so a rename can swap them.
fn sibling(path: &Path, tag: &str) -> Result<PathBuf, HookError> {
    let name = path.file_name().ok_or_else(|| {
        HookError::config(HOOK_NAME, format!("{} names no snapshot", path.display()))
    })?;
    Ok(path.with_file_name(format!(".{}.{tag}", name.to_string_lossy())))
}

/// Clears a snapshot an interrupted run left behind.
fn drop_if_subvolume(path: &Path) -> Result<(), HookError> {
    match subvol::is_subvolume(path) {
        Ok(true) => subvol::delete(path).map_err(io_err),
        Ok(false) => Err(HookError::config(
            HOOK_NAME,
            format!("{} is in the way of the next snapshot", path.display()),
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_err(e)),
    }
}

/// Subvolume calls through the kernel's Btrfs ioctls, so `btrfs-progs` need not be installed.
pub mod subvol {
    use super::*;

    /// `BTRFS_PATH_NAME_MAX + 1`, which makes `btrfs_ioctl_vol_args` 4096 bytes.
    const NAME_LEN: usize = 4088;

    #[repr(C)]
    pub(crate) struct VolArgs {
        fd: i64,
        name: [u8; NAME_LEN],
    }

    const _: () = assert!(size_of::<VolArgs>() == 4096);

    nix::ioctl_write_ptr!(snap_create, 0x94, 1, VolArgs);
    nix::ioctl_write_ptr!(subvol_create, 0x94, 14, VolArgs);
    nix::ioctl_write_ptr!(snap_destroy, 0x94, 15, VolArgs);

    pub(crate) fn args(name: &[u8], fd: i64) -> io::Result<VolArgs> {
        if name.is_empty() || name.len() >= NAME_LEN || name.contains(&b'/') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("bad subvolume name {:?}", String::from_utf8_lossy(name)),
            ));
        }
        let mut a = VolArgs {
            fd,
            name: [0; NAME_LEN],
        };
        a.name[..name.len()].copy_from_slice(name);
        Ok(a)
    }

    /// The directory holding `path`, opened, and the args naming `path` in it.
    fn at(path: &Path, fd: i64) -> io::Result<(File, VolArgs)> {
        let name = path.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} has no name", path.display()),
            )
        })?;
        let parent = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        Ok((File::open(parent)?, args(name.as_bytes(), fd)?))
    }

    pub fn is_subvolume(path: &Path) -> io::Result<bool> {
        const FIRST_FREE_OBJECTID: u64 = 256;
        Ok(statfs(path)?.filesystem_type() == BTRFS_SUPER_MAGIC
            && std::fs::metadata(path)?.ino() == FIRST_FREE_OBJECTID)
    }

    pub fn create(path: &Path) -> io::Result<()> {
        let (dir, a) = at(path, 0)?;
        // SAFETY: `a` is a live, initialised `btrfs_ioctl_vol_args`; the kernel only reads it.
        unsafe { subvol_create(dir.as_raw_fd(), &a) }?;
        Ok(())
    }

    /// Writable, so it can later be deleted without clearing a read-only flag first.
    pub fn snapshot(src: &Path, dst: &Path) -> io::Result<()> {
        let from = File::open(src)?;
        let (dir, a) = at(dst, i64::from(from.as_raw_fd()))?;
        // SAFETY: as in `create`; `from` stays open until the call returns.
        unsafe { snap_create(dir.as_raw_fd(), &a) }?;
        Ok(())
    }

    /// Swaps two paths in one step; through the raw syscall, as musl has no wrapper.
    pub fn exchange(a: &Path, b: &Path) -> io::Result<()> {
        let a = CString::new(a.as_os_str().as_bytes())?;
        let b = CString::new(b.as_os_str().as_bytes())?;
        // SAFETY: both paths are NUL-terminated and outlive the call.
        let rc = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                a.as_ptr(),
                libc::AT_FDCWD,
                b.as_ptr(),
                libc::RENAME_EXCHANGE,
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn delete(path: &Path) -> io::Result<()> {
        let (dir, a) = at(path, 0)?;
        // SAFETY: as in `create`.
        unsafe { snap_destroy(dir.as_raw_fd(), &a) }?;
        Ok(())
    }
}
