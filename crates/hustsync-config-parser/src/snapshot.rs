//! `[zfs]` and `[btrfs_snapshot]`, with the same keys as tunasync.

use std::path::PathBuf;

use serde::Deserialize;

use crate::{ConfigError, MirrorConfig, WorkerConfig};

/// `[zfs]`: every mirror's working directory must be a ZFS dataset of its own.
#[derive(Debug, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct WorkerZfsConfig {
    pub enable: Option<bool>,
    /// The pool or parent dataset the mirrors' datasets live under.
    pub zpool: Option<String>,
}

/// `[btrfs_snapshot]`: working directories are Btrfs subvolumes, snapshotted after each successful sync.
#[derive(Debug, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct WorkerBtrfsSnapshotConfig {
    pub enable: Option<bool>,
    /// The directory holding one snapshot per mirror, named after it, unless the mirror sets `snapshot_path`.
    pub snapshot_path: Option<String>,
}

impl WorkerConfig {
    /// The pool named by an enabled `[zfs]`.
    pub fn zpool(&self) -> Option<&str> {
        self.zfs
            .as_ref()
            .filter(|z| z.enable == Some(true))
            .and_then(|z| z.zpool.as_deref())
    }

    /// Where an enabled `[btrfs_snapshot]` puts this mirror's snapshot.
    pub fn snapshot_path(&self, mirror: &MirrorConfig) -> Option<PathBuf> {
        let btrfs = self
            .btrfs_snapshot
            .as_ref()
            .filter(|b| b.enable == Some(true))?;
        mirror
            .snapshot_path
            .as_deref()
            .filter(|p| !p.is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                let dir = btrfs.snapshot_path.as_deref().filter(|p| !p.is_empty())?;
                Some(PathBuf::from(dir).join(mirror.name.as_deref()?))
            })
    }
}

pub(crate) fn validate(cfg: &WorkerConfig) -> Result<(), ConfigError> {
    if cfg.zfs.as_ref().is_some_and(|z| z.enable == Some(true))
        && cfg.zpool().is_none_or(str::is_empty)
    {
        return Err(ConfigError::InvalidValue {
            field: "zfs.zpool".into(),
            reason: "name the pool or parent dataset the mirrors' datasets go under".into(),
        });
    }
    if cfg
        .btrfs_snapshot
        .as_ref()
        .is_some_and(|b| b.enable == Some(true))
    {
        for (idx, m) in cfg.mirrors.iter().flatten().enumerate() {
            if cfg.snapshot_path(m).is_none() {
                let label = m.name.clone().unwrap_or_else(|| format!("mirrors[{idx}]"));
                return Err(ConfigError::InvalidValue {
                    field: format!("mirrors.{label}.snapshot_path"),
                    reason: "set it here or under `[btrfs_snapshot]`".into(),
                });
            }
        }
    }
    Ok(())
}
