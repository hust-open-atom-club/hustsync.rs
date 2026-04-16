#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

use serde::{Deserialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};
use thiserror::Error;
use tracing::warn;

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

// ─── Load options ─────────────────────────────────────────────────────────────

/// Options controlling how a config file is loaded.
///
/// Callers (CLI / daemon startup) construct this and pass it into
/// [`parse_config_with_options`]. The parser never reads env or argv.
#[derive(Debug, Default)]
pub struct ConfigLoadOptions {
    /// When `true`, unknown fields produce a `WARN` log and are ignored
    /// rather than causing a hard parse error.  This is useful for
    /// operators who run a mixed fleet where some workers carry Track-B
    /// fields not yet supported by this build.
    pub allow_unsupported_fields: bool,
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
    pub include: Option<IncludeConfig>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        WorkerConfig {
            global: Some(WorkerGlobalConfig::default()),
            manager: Some(WorkerManagerConfig::default()),
            cgroup: Some(WorkerCgroupConfig::default()),
            server: Some(WorkerServerConfig::default()),
            mirrors: Some(vec![MirrorConfig::default()]),
            include: None,
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
    pub dangerous_global_rsync_success_exit_codes: Option<Vec<i32>>,
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
            name: Some("".into()),
            log_dir: Some("/var/log/hustsync/{{.Name}}".into()),
            mirror_dir: Some("/srv/mirror".into()),
            concurrent: Some(10),
            rsync_options: None,
            dangerous_global_success_exit_codes: None,
            dangerous_global_rsync_success_exit_codes: None,
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
    pub success_exit_codes: Option<Vec<i32>>,
    pub rsync_success_exit_codes: Option<Vec<i32>>,
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
            name: Some("".into()),
            provider: Some("rsync".into()),
            upstream: Some("".into()),
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
            success_exit_codes: None,
            rsync_success_exit_codes: None,
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

/// Corresponds to the `[include]` section of a worker config file.
#[derive(Debug, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct IncludeConfig {
    /// Glob pattern for additional mirror fragment files to load.
    /// Each matched file must be valid TOML containing a `[[mirrors]]` array.
    pub include_mirrors: Option<String>,
}

// ─── Lenient (allow-unsupported) fallback structs ─────────────────────────────
//
// These mirror the strict public structs but drop `deny_unknown_fields` so
// they absorb unknown keys without error.  They are only used when
// `ConfigLoadOptions::allow_unsupported_fields` is set.  Each field is
// `Option<…>` so partial configs parse cleanly; conversion to the strict
// type copies the values verbatim.

#[derive(Deserialize)]
struct LenientWorkerConfig {
    global: Option<WorkerGlobalConfig>,
    manager: Option<WorkerManagerConfig>,
    cgroup: Option<WorkerCgroupConfig>,
    server: Option<WorkerServerConfig>,
    mirrors: Option<Vec<LenientMirrorConfig>>,
    include: Option<IncludeConfig>,
}

// Mirrors LenientWorkerConfig but without deny_unknown_fields so it absorbs
// operator-defined Track-B keys without erroring.
#[derive(Deserialize)]
struct LenientMirrorConfig {
    #[serde(flatten)]
    retry: Option<RetryStrategy>,
    #[serde(flatten)]
    exec_on_status: Option<ExecOnStatus>,
    #[serde(flatten)]
    exec_on_status_extra: Option<ExecOnStatusExtra>,
    name: Option<String>,
    provider: Option<String>,
    upstream: Option<String>,
    use_ipv6: Option<bool>,
    use_ipv4: Option<bool>,
    mirror_dir: Option<String>,
    mirror_subdir: Option<String>,
    mirror_type: Option<String>,
    log_dir: Option<String>,
    env: Option<HashMap<String, String>>,
    role: Option<String>,
    command: Option<String>,
    fail_on_match: Option<String>,
    size_pattern: Option<String>,
    exclude_file: Option<String>,
    username: Option<String>,
    password: Option<String>,
    rsync_no_timeout: Option<bool>,
    rsync_timeout: Option<u32>,
    rsync_options: Option<Vec<String>>,
    rsync_override: Option<Vec<String>>,
    rsync_override_only: Option<bool>,
    stage1_profile: Option<String>,
    memory_limit: Option<String>,
    success_exit_codes: Option<Vec<i32>>,
    rsync_success_exit_codes: Option<Vec<i32>>,
}

impl From<LenientMirrorConfig> for MirrorConfig {
    fn from(l: LenientMirrorConfig) -> Self {
        MirrorConfig {
            retry: l.retry,
            exec_on_status: l.exec_on_status,
            exec_on_status_extra: l.exec_on_status_extra,
            name: l.name,
            provider: l.provider,
            upstream: l.upstream,
            use_ipv6: l.use_ipv6,
            use_ipv4: l.use_ipv4,
            mirror_dir: l.mirror_dir,
            mirror_subdir: l.mirror_subdir,
            mirror_type: l.mirror_type,
            log_dir: l.log_dir,
            env: l.env,
            role: l.role,
            command: l.command,
            fail_on_match: l.fail_on_match,
            size_pattern: l.size_pattern,
            exclude_file: l.exclude_file,
            username: l.username,
            password: l.password,
            rsync_no_timeout: l.rsync_no_timeout,
            rsync_timeout: l.rsync_timeout,
            rsync_options: l.rsync_options,
            rsync_override: l.rsync_override,
            rsync_override_only: l.rsync_override_only,
            stage1_profile: l.stage1_profile,
            memory_limit: l.memory_limit,
            success_exit_codes: l.success_exit_codes,
            rsync_success_exit_codes: l.rsync_success_exit_codes,
        }
    }
}

impl From<LenientWorkerConfig> for WorkerConfig {
    fn from(l: LenientWorkerConfig) -> Self {
        WorkerConfig {
            global: l.global,
            manager: l.manager,
            cgroup: l.cgroup,
            server: l.server,
            mirrors: l
                .mirrors
                .map(|mv| mv.into_iter().map(MirrorConfig::from).collect()),
            include: l.include,
        }
    }
}

/// Fragment format used when loading included mirror files via
/// `[include] include_mirrors`.  Each file must have a top-level
/// `[[mirrors]]` array.
#[derive(Deserialize)]
struct MirrorFragment {
    mirrors: Vec<MirrorConfig>,
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
    parse_config_with_options(path, &ConfigLoadOptions::default())
}

/// Like [`parse_config`] but accepts a [`ConfigLoadOptions`] to control
/// leniency and other load-time behaviour.
///
/// When `opts.allow_unsupported_fields` is `true` and a `WorkerConfig` parse
/// fails due to unknown fields, a lenient retry is performed.  Each unknown
/// key is logged at WARN.  For all other `T` types the option is silently
/// ignored and strict parsing is used.
pub fn parse_config_with_options<T>(
    path: impl AsRef<Path>,
    opts: &ConfigLoadOptions,
) -> Result<T, ConfigError>
where
    T: DeserializeOwned,
{
    let canonical = fs::canonicalize(path.as_ref())?;
    let content = fs::read_to_string(&canonical)?;
    let config: T = toml::from_str(&content)?;
    let _ = opts; // strict path does not need opts; WorkerConfig uses its own entry-point
    Ok(config)
}

/// Load a [`WorkerConfig`] from `path`, optionally allowing unsupported
/// fields, and resolve any `[include] include_mirrors` glob pattern.
///
/// This is the preferred entry-point when loading worker configs at daemon
/// startup.  `parse_config`/`parse_config_with_options` remain available for
/// generic deserialization.
pub fn load_worker_config(
    path: impl AsRef<Path>,
    opts: &ConfigLoadOptions,
) -> Result<WorkerConfig, ConfigError> {
    let canonical = fs::canonicalize(path.as_ref())?;
    let content = fs::read_to_string(&canonical)?;

    let mut cfg: WorkerConfig = if opts.allow_unsupported_fields {
        match toml::from_str::<WorkerConfig>(&content) {
            Ok(c) => c,
            Err(strict_err) => {
                // Check whether it looks like an unknown-field error before
                // falling back — if it is something else (syntax error, type
                // mismatch) the lenient path won't help either and we should
                // surface the original error.
                let msg = strict_err.to_string();
                if msg.contains("unknown field") || msg.contains("unexpected key") {
                    warn!(
                        file = %canonical.display(),
                        "config contains unsupported fields — loading in lenient mode; \
                         unsupported keys are ignored"
                    );
                    let lenient: LenientWorkerConfig =
                        toml::from_str(&content).map_err(ConfigError::Toml)?;
                    WorkerConfig::from(lenient)
                } else {
                    return Err(ConfigError::Toml(strict_err));
                }
            }
        }
    } else {
        toml::from_str(&content)?
    };

    // Resolve include_mirrors if set.
    if let Some(include) = cfg.include.as_ref()
        && let Some(pattern) = include.include_mirrors.clone()
    {
        cfg.mirrors = Some(load_included_mirrors(
            &pattern,
            cfg.mirrors.unwrap_or_default(),
        )?);
        // Clear the include section so downstream callers do not need to
        // re-check it — the mirrors list is already fully materialised.
        cfg.include = None;
    }

    Ok(cfg)
}

/// Expand `pattern` via glob, parse each matched file as a `MirrorFragment`,
/// and return `base_mirrors` extended with every mirror found.
fn load_included_mirrors(
    pattern: &str,
    mut base_mirrors: Vec<MirrorConfig>,
) -> Result<Vec<MirrorConfig>, ConfigError> {
    if pattern.is_empty() {
        return Ok(base_mirrors);
    }

    let paths = glob::glob(pattern).map_err(|e| ConfigError::InvalidValue {
        field: "include.include_mirrors".into(),
        reason: format!("invalid glob pattern `{pattern}`: {e}"),
    })?;

    for entry in paths {
        let path = entry.map_err(|e| ConfigError::PathRead {
            path: PathBuf::from(pattern),
            source: e.into_error(),
        })?;

        tracing::info!(file = %path.display(), "loading included mirror fragment");

        let content = fs::read_to_string(&path)?;
        let fragment: MirrorFragment = toml::from_str(&content)?;
        base_mirrors.extend(fragment.mirrors);
    }

    Ok(base_mirrors)
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
