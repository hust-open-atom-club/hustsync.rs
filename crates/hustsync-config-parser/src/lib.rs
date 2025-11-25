use serde::{Deserialize, de::DeserializeOwned};
use std::{collections::HashMap, error::Error, fs, path::Path};

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct WorkerGlobalConfig {
    #[serde(flatten)]
    pub retry: Option<RetryStrategy>,
    #[serde(flatten)]
    pub exec_on_status: Option<ExecOnStatus>,
    pub name: Option<String>,
    pub log_dir: Option<String>,
    pub mirror_dir: Option<String>,
    pub concurrent: Option<u32>,
    pub rsync_options: Option<Vec<String>>,
    pub dangerous_global_success_exit_codes: Option<Vec<i32>>,
}

impl Default for WorkerGlobalConfig {
    fn default() -> Self {
        WorkerGlobalConfig {
            retry: Some(RetryStrategy {
                retry: None,
                timeout: None,
                interval: Some(120),
            }),
            exec_on_status: Some(ExecOnStatus {
                exec_on_success: None,
                exec_on_failure: None,
            }),
            name: Some("test_worker".into()),
            log_dir: Some("/tmp/tunasync/log/tunasync/{{.Name}}".into()),
            mirror_dir: Some("/tmp/tunasync".into()),
            concurrent: Some(10),
            rsync_options: None,
            dangerous_global_success_exit_codes: None,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct MirrorConfig {
    #[serde(flatten)]
    pub retry: Option<RetryStrategy>,
    #[serde(flatten)]
    pub exec_on_status: Option<ExecOnStatus>,
    #[serde(flatten)]
    pub exec_on_status_extra: Option<ExecOnStatusExtra>,
    pub name: Option<String>,
    pub provider: Option<String>,
    pub upstream: Option<String>,
    pub use_ipv6: Option<bool>,
    pub mirror_dir: Option<String>,
    pub mirror_type: Option<String>,
    pub log_dir: Option<String>,
    pub env: Option<HashMap<String, String>>,
    pub role: Option<String>,
    pub command: Option<String>,
    pub fail_on_match: Option<String>,
    pub size_pattern: Option<String>,
    pub rsync_options: Option<Vec<String>>,
    pub stage1_profile: Option<String>,
    pub memory_limit: Option<String>,
}

impl Default for MirrorConfig {
    fn default() -> Self {
        MirrorConfig {
            retry: Some(RetryStrategy {
                retry: None,
                timeout: None,
                interval: None,
            }),
            exec_on_status: Some(ExecOnStatus {
                exec_on_success: None,
                exec_on_failure: None,
            }),
            exec_on_status_extra: Some(ExecOnStatusExtra {
                exec_on_success_extra: None,
                exec_on_failure_extra: None,
            }),
            name: Some("elvish".into()),
            provider: Some("rsync".into()),
            upstream: Some("rsync://rsync.elv.sh/elvish/".into()),
            use_ipv6: Some(false),
            mirror_dir: None,
            mirror_type: None,
            log_dir: None,
            env: None,
            role: None,
            command: None,
            fail_on_match: None,
            size_pattern: None,
            rsync_options: None,
            stage1_profile: None,
            memory_limit: None,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct RetryStrategy {
    pub retry: Option<u32>,
    pub timeout: Option<u32>,
    pub interval: Option<u32>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ExecOnStatus {
    pub exec_on_success: Option<Vec<String>>,
    pub exec_on_failure: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ExecOnStatusExtra {
    pub exec_on_success_extra: Option<Vec<String>>,
    pub exec_on_failure_extra: Option<Vec<String>>,
}

pub fn parse_config<T>(path: impl AsRef<Path>) -> Result<T, Box<dyn Error>>
where
    T: DeserializeOwned,
{
    let rel_path = fs::canonicalize(path.as_ref())?;
    let config_content = fs::read_to_string(rel_path)?;
    let config: T = toml::from_str(&config_content)?;
    Ok(config)
}
