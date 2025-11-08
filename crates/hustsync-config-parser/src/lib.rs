use serde::{Deserialize, de::DeserializeOwned};
use std::{error::Error, fs, path::Path};

#[derive(Debug, Deserialize, PartialEq)]
pub struct ManagerConfig {
    pub server: Option<ManagerServerConfig>,
    pub files: Option<ManagerFileConfig>,
    pub debug: Option<bool>,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        ManagerConfig {
            server: Some(ManagerServerConfig::default()),
            files: Some(ManagerFileConfig::default()),
            debug: Some(false),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ManagerServerConfig {
    pub addr: Option<String>,
    pub port: Option<u16>,
    pub ssl_cert: Option<String>,
    pub ssl_key: Option<String>,
}

impl Default for ManagerServerConfig {
    fn default() -> Self {
        ManagerServerConfig {
            addr: Some("127.0.0.1".into()),
            port: Some(12345),
            ssl_cert: Some("".into()),
            ssl_key: Some("".into()),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ManagerFileConfig {
    pub db_type: Option<String>,
    pub db_file: Option<String>,
    pub ca_cert: Option<String>,
}

impl Default for ManagerFileConfig {
    fn default() -> Self {
        ManagerFileConfig {
            db_type: Some("bolt".into()),
            db_file: Some("/tmp/tunasync/manager.db".into()),
            ca_cert: Some("".into()),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct WorkerConfig {
    pub global: Option<WorkerGlobalConfig>,
    pub manager: Option<WorkerManagerConfig>,
    pub cgroup: Option<WorkerCgroupConfig>,
    pub server: Option<WorkerServerConfig>,
    pub mirrors: Option<Vec<MirrorConfig>>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        WorkerConfig {
            global: Some(WorkerGlobalConfig::default()),
            manager: Some(WorkerManagerConfig::default()),
            cgroup: Some(WorkerCgroupConfig::default()),
            server: Some(WorkerServerConfig::default()),
            mirrors: Some(vec![MirrorConfig::default()]),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct WorkerGlobalConfig {
    pub name: Option<String>,
    pub log_dir: Option<String>,
    pub mirror_dir: Option<String>,
    pub concurrent: Option<u32>,
    pub interval: Option<u32>,
}

impl Default for WorkerGlobalConfig {
    fn default() -> Self {
        WorkerGlobalConfig {
            name: Some("test_worker".into()),
            log_dir: Some("/tmp/tunasync/log/tunasync/{{.Name}}".into()),
            mirror_dir: Some("/tmp/tunasync".into()),
            concurrent: Some(10),
            interval: Some(120),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct WorkerManagerConfig {
    pub api_base: Option<String>,
    pub token: Option<String>,
    pub ca_cert: Option<String>,
}

impl Default for WorkerManagerConfig {
    fn default() -> Self {
        WorkerManagerConfig {
            api_base: Some("http://localhost:12345".into()),
            token: Some("".into()),
            ca_cert: Some("".into()),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct WorkerCgroupConfig {
    pub enable: Option<bool>,
    pub base_path: Option<String>,
    pub group: Option<String>,
}

impl Default for WorkerCgroupConfig {
    fn default() -> Self {
        WorkerCgroupConfig {
            enable: Some(false),
            base_path: Some("/sys/fs/cgroup".into()),
            group: Some("tunasync".into()),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct WorkerServerConfig {
    pub hostname: Option<String>,
    pub listen_addr: Option<String>,
    pub listen_port: Option<u16>,
    pub ssl_cert: Option<String>,
    pub ssl_key: Option<String>,
}

impl Default for WorkerServerConfig {
    fn default() -> Self {
        WorkerServerConfig {
            hostname: Some("localhost".into()),
            listen_addr: Some("127.0.0.1".into()),
            listen_port: Some(6000),
            ssl_cert: Some("".into()),
            ssl_key: Some("".into()),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct MirrorConfig {
    pub name: Option<String>,
    pub provider: Option<String>,
    pub upstream: Option<String>,
    pub use_ipv6: Option<bool>,
}

impl Default for MirrorConfig {
    fn default() -> Self {
        MirrorConfig {
            name: Some("elvish".into()),
            provider: Some("rsync".into()),
            upstream: Some("rsync://rsync.elv.sh/elvish/".into()),
            use_ipv6: Some(false),
        }
    }
}

pub fn parse_config<P, T>(path: P) -> Result<T, Box<dyn Error>>
where
    P: AsRef<Path>,
    T: DeserializeOwned,
{
    let rel_path = fs::canonicalize(path.as_ref())?;
    let config_content = fs::read_to_string(rel_path)?;
    let config: T = toml::from_str(&config_content)?;
    Ok(config)
}
