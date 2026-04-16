#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

use serde::{Deserialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};
use thiserror::Error;

/// Typed error returned by every public entry-point in this crate.
///
/// Variants carry enough context for callers to produce actionable
/// diagnostics without inspecting the inner message string.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Low-level I/O failure (missing file, permission denied, …).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// TOML deserialisation error (syntax / schema mismatch).
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),

    /// A field present in Track-B (Go tunasynctl) but not yet supported.
    /// `tracked_in` names the issue / spec section where support is planned.
    #[error("unsupported field `{field}` (tracked in: {tracked_in})")]
    UnsupportedField {
        field: String,
        tracked_in: &'static str,
    },

    /// A field was present but its value is semantically invalid.
    #[error("invalid value for `{field}`: {reason}")]
    InvalidValue { field: String, reason: String },

    /// A required field was absent from the config file.
    #[error("missing required field: {0}")]
    MissingRequired(String),

    /// An I/O error tied to a specific path (used by the semantic-validation
    /// pass when it must stat or open a referenced path).
    #[error("could not read path `{path}`: {source}")]
    PathRead {
        path: PathBuf,
        source: std::io::Error,
    },
}

// ─── Structs ──────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ManagerConfig {
    pub server: ManagerServerConfig,
    pub files: ManagerFileConfig,
    pub debug: bool,
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
            db_file: "/var/lib/hustsync/manager.db".into(),
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

// ─── Public API ───────────────────────────────────────────────────────────────

/// Read and deserialise a TOML config file from `path`.
///
/// Errors are typed — callers should match on `ConfigError` variants rather
/// than formatting the message for logic decisions.
pub fn parse_config<T>(path: impl AsRef<Path>) -> Result<T, ConfigError>
where
    T: DeserializeOwned,
{
    let canonical = fs::canonicalize(path.as_ref())?;
    let content = fs::read_to_string(canonical)?;
    let config: T = toml::from_str(&content)?;
    Ok(config)
}

// ---------------------------------------------------------------------------
// Provider semantic validation
// ---------------------------------------------------------------------------

const VALID_STAGE1_PROFILES: &[&str] = &["debian", "debian-oldstyle"];

/// True when `upstream` begins `scheme://<host>...` and `<host>` contains a
/// colon without enclosing brackets (bare IPv6 literal).
fn contains_unbracketed_ipv6(upstream: &str) -> bool {
    let Some(pos) = upstream.find("://") else {
        return false;
    };
    let after_scheme = &upstream[pos + 3..];
    if after_scheme.starts_with('[') {
        return false;
    }
    let host_part = after_scheme.split('/').next().unwrap_or("");
    host_part.contains(':')
}

/// Run the semantic validation pass over every mirror declared in `cfg`.
///
/// Rules enforced (from the spec):
///
/// 1. `rsync`/`two-stage-rsync` upstream must end with `/`.
/// 2. `rsync_override_only = true` requires a non-empty `rsync_override`.
/// 3. Bare IPv6 literals in upstream must be bracketed (`[…]`).
/// 4. `size_pattern`, when set, must compile and have exactly one capture group.
/// 5. `stage1_profile`, when set, must be one of the known profiles.
pub fn validate_worker_config(cfg: &WorkerConfig) -> Result<(), ConfigError> {
    let Some(mirrors) = cfg.mirrors.as_deref() else {
        return Ok(());
    };
    for (idx, mirror) in mirrors.iter().enumerate() {
        let label = mirror
            .name
            .clone()
            .unwrap_or_else(|| format!("mirrors[{idx}]"));
        validate_mirror(mirror, &label)?;
    }
    Ok(())
}

fn validate_mirror(mirror: &MirrorConfig, label: &str) -> Result<(), ConfigError> {
    let provider = mirror.provider.as_deref().unwrap_or("rsync");
    let is_rsync_family = matches!(provider, "rsync" | "two-stage-rsync");

    if is_rsync_family {
        if let Some(upstream) = mirror.upstream.as_deref() {
            if !upstream.ends_with('/') {
                return Err(ConfigError::InvalidValue {
                    field: format!("mirrors.{label}.upstream"),
                    reason: format!("rsync upstream must end with `/`, e.g. `{upstream}/`"),
                });
            }
            if contains_unbracketed_ipv6(upstream) {
                return Err(ConfigError::InvalidValue {
                    field: format!("mirrors.{label}.upstream"),
                    reason: format!(
                        "IPv6 literal in upstream must be bracketed, \
                         e.g. `rsync://[fe80::1]/path/` (got `{upstream}`)"
                    ),
                });
            }
        }

        if mirror.rsync_override_only == Some(true) {
            let override_empty = mirror
                .rsync_override
                .as_ref()
                .map(|v| v.is_empty())
                .unwrap_or(true);
            if override_empty {
                return Err(ConfigError::InvalidValue {
                    field: format!("mirrors.{label}.rsync_override_only"),
                    reason: "`rsync_override_only = true` requires `rsync_override` to be \
                             a non-empty list of rsync arguments"
                        .into(),
                });
            }
        }

        if let Some(profile) = mirror.stage1_profile.as_deref()
            && !VALID_STAGE1_PROFILES.contains(&profile)
        {
            return Err(ConfigError::InvalidValue {
                field: format!("mirrors.{label}.stage1_profile"),
                reason: format!(
                    "unknown stage1_profile `{profile}`; valid values are: {}",
                    VALID_STAGE1_PROFILES.join(", ")
                ),
            });
        }
    }

    if let Some(pattern) = mirror.size_pattern.as_deref() {
        match regex::Regex::new(pattern) {
            Err(e) => {
                return Err(ConfigError::InvalidValue {
                    field: format!("mirrors.{label}.size_pattern"),
                    reason: format!("regex does not compile: {e}"),
                });
            }
            Ok(re) => {
                // captures_len() = 1 (whole match) + explicit groups; exactly
                // one capture group means captures_len() == 2.
                let groups = re.captures_len();
                if groups != 2 {
                    return Err(ConfigError::InvalidValue {
                        field: format!("mirrors.{label}.size_pattern"),
                        reason: format!(
                            "`size_pattern` must have exactly 1 capture group, \
                             but found {} (e.g. `([0-9]+[KMGTP]?)`)",
                            groups.saturating_sub(1)
                        ),
                    });
                }
            }
        }
    }

    Ok(())
}
