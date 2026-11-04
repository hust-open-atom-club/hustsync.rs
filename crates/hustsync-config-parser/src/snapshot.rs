//! `[zfs]`, with the same keys as tunasync.

use serde::Deserialize;

use crate::{ConfigError, WorkerConfig};

/// `[zfs]`: every mirror's working directory must be a ZFS dataset of its own.
#[derive(Debug, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct WorkerZfsConfig {
    pub enable: Option<bool>,
    /// The pool or parent dataset the mirrors' datasets live under.
    pub zpool: Option<String>,
}

impl WorkerConfig {
    /// The pool named by an enabled `[zfs]`.
    pub fn zpool(&self) -> Option<&str> {
        self.zfs
            .as_ref()
            .filter(|z| z.enable == Some(true))
            .and_then(|z| z.zpool.as_deref())
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{WorkerConfig, validate_worker_config};

    type Res = Result<(), Box<dyn std::error::Error>>;

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
