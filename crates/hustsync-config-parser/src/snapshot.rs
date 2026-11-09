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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::{WorkerConfig, flatten_mirrors, validate_worker_config};

    type Res = Result<(), Box<dyn std::error::Error>>;

    fn parse(text: &str) -> Result<WorkerConfig, toml::de::Error> {
        let mut cfg: WorkerConfig = toml::from_str(text)?;
        cfg.mirrors = cfg.mirrors.map(flatten_mirrors);
        Ok(cfg)
    }

    const BTRFS: &str = r#"
[btrfs_snapshot]
enable = true
snapshot_path = "/srv/publish"

[[mirrors]]
name = "debian"

[[mirrors]]
name = "elvish"
snapshot_path = "/data/publish/elvish"
"#;

    #[test]
    fn snapshots_go_under_the_global_path_unless_a_mirror_names_its_own() -> Res {
        let cfg = parse(BTRFS)?;
        validate_worker_config(&cfg)?;
        let m = cfg.mirrors.as_deref().ok_or("no mirrors")?;
        assert_eq!(
            cfg.snapshot_path(&m[0]).as_deref(),
            Some(Path::new("/srv/publish/debian"))
        );
        assert_eq!(
            cfg.snapshot_path(&m[1]).as_deref(),
            Some(Path::new("/data/publish/elvish"))
        );
        Ok(())
    }

    #[test]
    fn disabled_means_no_snapshot() -> Res {
        let cfg = parse(&BTRFS.replace("enable = true", "enable = false"))?;
        let m = cfg.mirrors.as_deref().ok_or("no mirrors")?;
        assert_eq!(cfg.snapshot_path(&m[1]), None);
        Ok(())
    }

    #[test]
    fn every_mirror_needs_somewhere_to_snapshot() -> Res {
        let cfg = parse(&BTRFS.replace("snapshot_path = \"/srv/publish\"\n", ""))?;
        let err = validate_worker_config(&cfg).err().ok_or("must fail")?;
        assert!(
            err.to_string().contains("mirrors.debian.snapshot_path"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn children_do_not_share_a_snapshot() -> Res {
        let cfg = parse(
            r#"
[btrfs_snapshot]
enable = true
snapshot_path = "/srv/publish"

[[mirrors]]
name = "pypi"
snapshot_path = "/srv/pypi"

[[mirrors.mirrors]]
name = "pypi-simple"
"#,
        )?;
        let child = &cfg.mirrors.as_deref().ok_or("no mirrors")?[0];
        assert_eq!(
            cfg.snapshot_path(child).as_deref(),
            Some(Path::new("/srv/publish/pypi-simple"))
        );
        Ok(())
    }

    #[test]
    fn zfs_needs_a_pool() -> Res {
        let cfg: WorkerConfig = toml::from_str("[zfs]\nenable = true\n")?;
        let err = validate_worker_config(&cfg).err().ok_or("must fail")?;
        assert!(err.to_string().contains("zfs.zpool"), "{err}");

        let cfg: WorkerConfig = toml::from_str("[zfs]\nenable = true\nzpool = \"tank/m\"\n")?;
        validate_worker_config(&cfg)?;
        assert_eq!(cfg.zpool(), Some("tank/m"));

        let cfg: WorkerConfig = toml::from_str("[zfs]\nzpool = \"tank\"\n")?;
        assert_eq!(cfg.zpool(), None);
        Ok(())
    }
}
