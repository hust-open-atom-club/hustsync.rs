use serde::{Deserialize, de::DeserializeOwned};
use std::{collections::HashMap, error::Error, fs, path::Path};

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ManagerConfig {
    pub server: ManagerServerConfig,
    pub files: ManagerFileConfig,
    pub debug: bool,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        ManagerConfig {
            server: ManagerServerConfig::default(),
            files: ManagerFileConfig::default(),
            debug: false,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ManagerServerConfig {
    pub addr: String,
    pub port: u16,
    pub ssl_cert: String,
    pub ssl_key: String,
}

impl Default for ManagerServerConfig {
    fn default() -> Self {
        ManagerServerConfig {
            addr: "127.0.0.1".into(),
            port: 12345,
            ssl_cert: "".into(),
            ssl_key: "".into(),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ManagerFileConfig {
    #[serde(default)]
    pub status_file: String,
    pub db_type: String,
    pub db_file: String,
    pub ca_cert: String,
}

impl Default for ManagerFileConfig {
    fn default() -> Self {
        ManagerFileConfig {
            status_file: "".into(),
            db_type: "redb".into(),
            db_file: "/tmp/hustsync/manager.db".into(),
            ca_cert: "".into(),
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
            log_dir: Some("/tmp/hustsync/log/hustsync/{{.Name}}".into()),
            mirror_dir: Some("/tmp/hustsync".into()),
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
            group: Some("hustsync".into()),
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
    pub use_ipv4: Option<bool>,
    pub mirror_dir: Option<String>,
    pub mirror_subdir: Option<String>,
    pub mirror_type: Option<String>,
    pub log_dir: Option<String>,
    pub env: Option<HashMap<String, String>>,
    pub role: Option<String>,
    pub command: Option<String>,
    pub fail_on_match: Option<String>,
    pub size_pattern: Option<String>,
    pub exclude_file: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub rsync_no_timeout: Option<bool>,
    pub rsync_timeout: Option<u32>,
    pub rsync_options: Option<Vec<String>>,
    pub rsync_override: Option<Vec<String>>,
    pub rsync_override_only: Option<bool>,
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
            use_ipv4: None,
            mirror_dir: None,
            mirror_subdir: None,
            mirror_type: None,
            log_dir: None,
            env: None,
            role: None,
            command: None,
            fail_on_match: None,
            size_pattern: None,
            exclude_file: None,
            username: None,
            password: None,
            rsync_no_timeout: None,
            rsync_timeout: None,
            rsync_options: None,
            rsync_override: None,
            rsync_override_only: None,
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
