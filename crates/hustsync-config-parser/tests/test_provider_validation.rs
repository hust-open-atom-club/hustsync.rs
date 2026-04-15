// Semantic-validation tests for provider config shapes.
//
// Each of the five validation rules (Spec 05 §3.5, §4.1, §5.4) has at
// least two cases: one legal input that must be accepted and at least one
// illegal input that must produce `ConfigError::InvalidValue`.
#![allow(clippy::unwrap_used)]
#![allow(clippy::panic)]
use hustsync_config_parser::{ConfigError, MirrorConfig, WorkerConfig, validate_worker_config};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn worker_with_single_mirror(mirror: MirrorConfig) -> WorkerConfig {
    WorkerConfig {
        global: None,
        manager: None,
        cgroup: None,
        server: None,
        mirrors: Some(vec![mirror]),
    }
}

fn named_rsync_mirror(name: &str, upstream: &str) -> MirrorConfig {
    MirrorConfig {
        name: Some(name.into()),
        provider: Some("rsync".into()),
        upstream: Some(upstream.into()),
        ..MirrorConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Rule 1 — rsync upstream must end with `/`
// ---------------------------------------------------------------------------

#[test]
fn accept_rsync_upstream_with_trailing_slash() {
    let cfg = worker_with_single_mirror(named_rsync_mirror(
        "arch",
        "rsync://mirror.example.org/archlinux/",
    ));
    assert!(validate_worker_config(&cfg).is_ok());
}

#[test]
fn reject_rsync_upstream_missing_trailing_slash() {
    let cfg = worker_with_single_mirror(named_rsync_mirror(
        "arch",
        "rsync://mirror.example.org/archlinux",
    ));
    let err = validate_worker_config(&cfg).unwrap_err();
    assert!(
        matches!(err, ConfigError::InvalidValue { .. }),
        "expected InvalidValue, got {err:?}"
    );
}

#[test]
fn reject_rsync_upstream_bare_host_no_slash() {
    // A URL that is syntactically unusual but still lacks the trailing `/`
    let cfg = worker_with_single_mirror(named_rsync_mirror("foo", "rsync://cdn.example.com"));
    let err = validate_worker_config(&cfg).unwrap_err();
    assert!(matches!(err, ConfigError::InvalidValue { .. }));
}

// ---------------------------------------------------------------------------
// Rule 2 — rsync_override_only = true requires non-empty rsync_override
// ---------------------------------------------------------------------------

#[test]
fn accept_rsync_override_only_with_non_empty_override() {
    let mirror = MirrorConfig {
        name: Some("debian".into()),
        provider: Some("rsync".into()),
        upstream: Some("rsync://ftp.debian.org/debian/".into()),
        rsync_override_only: Some(true),
        rsync_override: Some(vec!["--bwlimit=10000".into()]),
        ..MirrorConfig::default()
    };
    let cfg = worker_with_single_mirror(mirror);
    assert!(validate_worker_config(&cfg).is_ok());
}

#[test]
fn reject_rsync_override_only_true_with_empty_override() {
    let mirror = MirrorConfig {
        name: Some("debian".into()),
        provider: Some("rsync".into()),
        upstream: Some("rsync://ftp.debian.org/debian/".into()),
        rsync_override_only: Some(true),
        rsync_override: Some(vec![]), // empty list
        ..MirrorConfig::default()
    };
    let cfg = worker_with_single_mirror(mirror);
    let err = validate_worker_config(&cfg).unwrap_err();
    assert!(matches!(err, ConfigError::InvalidValue { .. }));
}

#[test]
fn reject_rsync_override_only_true_with_absent_override() {
    let mirror = MirrorConfig {
        name: Some("debian".into()),
        provider: Some("rsync".into()),
        upstream: Some("rsync://ftp.debian.org/debian/".into()),
        rsync_override_only: Some(true),
        rsync_override: None, // entirely absent
        ..MirrorConfig::default()
    };
    let cfg = worker_with_single_mirror(mirror);
    let err = validate_worker_config(&cfg).unwrap_err();
    assert!(matches!(err, ConfigError::InvalidValue { .. }));
}

// ---------------------------------------------------------------------------
// Rule 3 — IPv6 literal must use `[…]` brackets
// ---------------------------------------------------------------------------

#[test]
fn accept_ipv6_upstream_with_brackets() {
    let cfg = worker_with_single_mirror(named_rsync_mirror("ipv6ok", "rsync://[fe80::1]/mirror/"));
    assert!(validate_worker_config(&cfg).is_ok());
}

#[test]
fn reject_ipv6_upstream_without_brackets() {
    let cfg = worker_with_single_mirror(named_rsync_mirror("ipv6bad", "rsync://fe80::1/mirror/"));
    let err = validate_worker_config(&cfg).unwrap_err();
    assert!(matches!(err, ConfigError::InvalidValue { .. }));
}

#[test]
fn reject_ipv6_full_address_without_brackets() {
    let cfg = worker_with_single_mirror(named_rsync_mirror(
        "ipv6full",
        "rsync://2001:db8::1/debian/",
    ));
    let err = validate_worker_config(&cfg).unwrap_err();
    assert!(matches!(err, ConfigError::InvalidValue { .. }));
}

// ---------------------------------------------------------------------------
// Rule 4 — size_pattern must have exactly one capture group
// ---------------------------------------------------------------------------

#[test]
fn accept_size_pattern_with_exactly_one_group() {
    let mirror = MirrorConfig {
        name: Some("cmd-mirror".into()),
        provider: Some("command".into()),
        upstream: Some("https://example.org/".into()),
        size_pattern: Some(r"Total: ([0-9]+[KMGTP]?)".into()),
        ..MirrorConfig::default()
    };
    let cfg = worker_with_single_mirror(mirror);
    assert!(validate_worker_config(&cfg).is_ok());
}

#[test]
fn reject_size_pattern_with_zero_groups() {
    let mirror = MirrorConfig {
        name: Some("cmd-mirror".into()),
        provider: Some("command".into()),
        upstream: Some("https://example.org/".into()),
        size_pattern: Some(r"Total: [0-9]+[KMGTP]?".into()), // no group
        ..MirrorConfig::default()
    };
    let cfg = worker_with_single_mirror(mirror);
    let err = validate_worker_config(&cfg).unwrap_err();
    assert!(matches!(err, ConfigError::InvalidValue { .. }));
}

#[test]
fn reject_size_pattern_with_two_groups() {
    let mirror = MirrorConfig {
        name: Some("cmd-mirror".into()),
        provider: Some("command".into()),
        upstream: Some("https://example.org/".into()),
        size_pattern: Some(r"(Total): ([0-9]+)".into()), // two groups
        ..MirrorConfig::default()
    };
    let cfg = worker_with_single_mirror(mirror);
    let err = validate_worker_config(&cfg).unwrap_err();
    assert!(matches!(err, ConfigError::InvalidValue { .. }));
}

// ---------------------------------------------------------------------------
// Rule 5 — stage1_profile must be a known value
// ---------------------------------------------------------------------------

#[test]
fn accept_stage1_profile_debian() {
    let mirror = MirrorConfig {
        name: Some("deb".into()),
        provider: Some("two-stage-rsync".into()),
        upstream: Some("rsync://ftp.debian.org/debian/".into()),
        stage1_profile: Some("debian".into()),
        ..MirrorConfig::default()
    };
    let cfg = worker_with_single_mirror(mirror);
    assert!(validate_worker_config(&cfg).is_ok());
}

#[test]
fn accept_stage1_profile_debian_oldstyle() {
    let mirror = MirrorConfig {
        name: Some("deb-old".into()),
        provider: Some("two-stage-rsync".into()),
        upstream: Some("rsync://ftp.debian.org/debian/".into()),
        stage1_profile: Some("debian-oldstyle".into()),
        ..MirrorConfig::default()
    };
    let cfg = worker_with_single_mirror(mirror);
    assert!(validate_worker_config(&cfg).is_ok());
}

#[test]
fn reject_stage1_profile_unknown_value() {
    let mirror = MirrorConfig {
        name: Some("deb-sec".into()),
        provider: Some("two-stage-rsync".into()),
        upstream: Some("rsync://security.debian.org/debian-security/".into()),
        stage1_profile: Some("debian-security".into()),
        ..MirrorConfig::default()
    };
    let cfg = worker_with_single_mirror(mirror);
    let err = validate_worker_config(&cfg).unwrap_err();
    assert!(matches!(err, ConfigError::InvalidValue { .. }));
}

#[test]
fn reject_stage1_profile_debian_oldstable() {
    // Spec §4.1 explicitly calls out that `debian-oldstable` does not exist.
    let mirror = MirrorConfig {
        name: Some("deb-oldstable".into()),
        provider: Some("two-stage-rsync".into()),
        upstream: Some("rsync://ftp.debian.org/debian/".into()),
        stage1_profile: Some("debian-oldstable".into()),
        ..MirrorConfig::default()
    };
    let cfg = worker_with_single_mirror(mirror);
    let err = validate_worker_config(&cfg).unwrap_err();
    assert!(matches!(err, ConfigError::InvalidValue { .. }));
}

// ---------------------------------------------------------------------------
// Edge: non-rsync provider is not checked for rsync rules
// ---------------------------------------------------------------------------

#[test]
fn cmd_provider_upstream_no_trailing_slash_is_ok() {
    // The trailing-slash requirement is only for rsync-family providers.
    let mirror = MirrorConfig {
        name: Some("cmd".into()),
        provider: Some("command".into()),
        upstream: Some("https://example.org/no-slash".into()),
        size_pattern: Some(r"([0-9]+)".into()),
        ..MirrorConfig::default()
    };
    let cfg = worker_with_single_mirror(mirror);
    assert!(validate_worker_config(&cfg).is_ok());
}

// ---------------------------------------------------------------------------
// Error message quality: reason must be actionable
// ---------------------------------------------------------------------------

#[test]
fn reject_reason_mentions_trailing_slash() {
    let cfg = worker_with_single_mirror(named_rsync_mirror(
        "arch",
        "rsync://mirror.example.org/archlinux",
    ));
    let err = validate_worker_config(&cfg).unwrap_err();
    if let ConfigError::InvalidValue { reason, .. } = err {
        assert!(
            reason.contains('/'),
            "reason should suggest adding a trailing slash, got: {reason}"
        );
    } else {
        panic!("expected InvalidValue");
    }
}
