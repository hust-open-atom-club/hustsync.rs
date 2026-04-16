#![allow(clippy::unwrap_used)]
#![allow(clippy::panic)]

#[cfg(test)]
mod tests {
    use hustsync_config_parser::{ConfigLoadOptions, ManagerConfig, WorkerConfig};
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::{NamedTempFile, TempDir};

    #[test]
    fn test_parse_manager_config_with_defaults() {
        let mut manager_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        manager_path.pop();
        manager_path.pop();
        manager_path.push("docs/example/manager.conf");

        let manager_config: ManagerConfig =
            hustsync_config_parser::parse_config(&manager_path).unwrap();
        let default_manager_config = ManagerConfig::default();
        assert_eq!(manager_config, default_manager_config);
    }

    #[test]
    fn test_parse_worker_config_with_defaults() {
        let mut worker_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        worker_path.pop();
        worker_path.pop();
        worker_path.push("docs/example/worker.conf");

        let worker_config: hustsync_config_parser::WorkerConfig =
            hustsync_config_parser::parse_config(&worker_path).unwrap();
        let default_worker_config = hustsync_config_parser::WorkerConfig::default();
        assert_eq!(worker_config, default_worker_config);
    }

    /// 测试华科镜像实际使用的配置文件能否被正确解析
    /// 预期解析所有字段成功，且不报错
    #[test]
    fn test_parse_dot_local_manager_config() {
        let mut manager_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        manager_path.pop();
        manager_path.pop();
        manager_path.push(".local/manager.toml");
        if !manager_path.exists() {
            return;
        }
        hustsync_config_parser::parse_config::<ManagerConfig>(&manager_path).unwrap();
    }

    /// 测试华科镜像实际使用的配置文件能否被正确解析
    /// 预期解析所有字段成功，且不报错
    #[test]
    fn test_parse_dot_local_worker_config() {
        let mut worker_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        worker_path.pop();
        worker_path.pop();
        worker_path.push(".local/worker.toml");
        if !worker_path.exists() {
            return;
        }
        hustsync_config_parser::parse_config::<WorkerConfig>(&worker_path).unwrap();
    }

    /// `WorkerGlobalConfig` production-safe defaults.
    #[test]
    fn default_worker_global_config_values() {
        let cfg = hustsync_config_parser::WorkerGlobalConfig::default();
        assert_eq!(cfg.name.as_deref(), Some(""));
        assert_eq!(
            cfg.log_dir.as_deref(),
            Some("/var/log/hustsync/{{.Name}}")
        );
        assert_eq!(cfg.mirror_dir.as_deref(), Some("/srv/mirror"));
    }

    /// `MirrorConfig` name and upstream default to empty strings so no
    /// example host leaks into production configuration.
    #[test]
    fn default_mirror_config_name_and_upstream_are_empty() {
        let default_mirror = hustsync_config_parser::MirrorConfig::default();
        assert_eq!(default_mirror.name.as_deref(), Some(""));
        assert_eq!(default_mirror.upstream.as_deref(), Some(""));
    }

    /// `use_ipv4` defaults to `None` when absent from config.
    #[test]
    fn default_mirror_use_ipv4_is_none() {
        let default_mirror = hustsync_config_parser::MirrorConfig::default();
        assert_eq!(default_mirror.use_ipv4, None);
    }

    /// `use_ipv4 = true` round-trips through TOML correctly.
    #[test]
    fn parse_mirror_use_ipv4_true() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"
[[mirrors]]
name = "archlinux"
use_ipv4 = true
"#
        )
        .unwrap();

        let cfg: hustsync_config_parser::WorkerConfig =
            hustsync_config_parser::parse_config(f.path()).unwrap();
        let mirror = &cfg.mirrors.unwrap()[0];
        assert_eq!(mirror.use_ipv4, Some(true));
    }

    /// track-B hooks (cgroup/docker/zfs/btrfs) must
    /// error at parse time, never silently accepted. `deny_unknown_fields`
    /// on `MirrorConfig` is what enforces this; the test pins the
    /// rejection so a future struct change that relaxes the guard
    /// breaks loudly.
    #[test]
    fn parse_rejects_track_b_unknown_field() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"
[[mirrors]]
name = "m1"
provider = "rsync"
upstream = "rsync://example.test/m1/"
docker_image = "ghcr.io/example/rsync:latest"
"#
        )
        .unwrap();

        let res = hustsync_config_parser::parse_config::<WorkerConfig>(f.path());
        assert!(
            res.is_err(),
            "unknown track-B field must be rejected at parse, got Ok"
        );
    }

    // ── Feature 1: exit code allowlist fields ──────────────────────────────

    /// `success_exit_codes` defaults to `None` when absent.
    #[test]
    fn default_mirror_success_exit_codes_is_none() {
        let m = hustsync_config_parser::MirrorConfig::default();
        assert_eq!(m.success_exit_codes, None);
        assert_eq!(m.rsync_success_exit_codes, None);
    }

    /// `dangerous_global_rsync_success_exit_codes` defaults to `None`.
    #[test]
    fn default_global_rsync_success_exit_codes_is_none() {
        let g = hustsync_config_parser::WorkerGlobalConfig::default();
        assert_eq!(g.dangerous_global_rsync_success_exit_codes, None);
    }

    /// `success_exit_codes` round-trips through TOML.
    #[test]
    fn parse_mirror_success_exit_codes() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"
[[mirrors]]
name = "archlinux"
success_exit_codes = [0, 23, 24]
rsync_success_exit_codes = [0, 6]
"#
        )
        .unwrap();

        let cfg: WorkerConfig = hustsync_config_parser::parse_config(f.path()).unwrap();
        let mirror = &cfg.mirrors.unwrap()[0];
        assert_eq!(mirror.success_exit_codes, Some(vec![0, 23, 24]));
        assert_eq!(mirror.rsync_success_exit_codes, Some(vec![0, 6]));
    }

    /// `dangerous_global_rsync_success_exit_codes` round-trips through TOML.
    #[test]
    fn parse_global_rsync_success_exit_codes() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"
[global]
dangerous_global_rsync_success_exit_codes = [0, 1, 23]
"#
        )
        .unwrap();

        let cfg: WorkerConfig = hustsync_config_parser::parse_config(f.path()).unwrap();
        let global = cfg.global.unwrap();
        assert_eq!(
            global.dangerous_global_rsync_success_exit_codes,
            Some(vec![0, 1, 23])
        );
    }

    // ── Feature 2: --allow-unsupported-fields ──────────────────────────────

    /// Without the flag, a Track-B field in `[[mirrors]]` causes a hard error.
    #[test]
    fn strict_mode_rejects_unknown_mirror_field() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"
[[mirrors]]
name = "archlinux"
docker_image = "ghcr.io/example/rsync:latest"
"#
        )
        .unwrap();

        let res = hustsync_config_parser::load_worker_config(
            f.path(),
            &ConfigLoadOptions { allow_unsupported_fields: false },
        );
        assert!(res.is_err(), "strict mode must reject unknown mirror fields");
    }

    /// With the flag, the same Track-B mirror field is accepted with a WARN
    /// and the known fields parse correctly.
    #[test]
    fn lenient_mode_accepts_unknown_mirror_field_and_continues() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"
[[mirrors]]
name = "archlinux"
docker_image = "ghcr.io/example/rsync:latest"
"#
        )
        .unwrap();

        let res = hustsync_config_parser::load_worker_config(
            f.path(),
            &ConfigLoadOptions { allow_unsupported_fields: true },
        );
        assert!(
            res.is_ok(),
            "lenient mode must accept unknown mirror fields, got: {:?}",
            res.err()
        );
        let cfg = res.unwrap();
        assert_eq!(
            cfg.mirrors.unwrap()[0].name.as_deref(),
            Some("archlinux"),
            "known fields must be preserved in lenient mode"
        );
    }

    // ── Feature 3: include_mirrors ─────────────────────────────────────────

    /// `include.include_mirrors` defaults to `None` when absent.
    #[test]
    fn default_worker_include_is_none() {
        let cfg = WorkerConfig::default();
        assert_eq!(cfg.include, None);
    }

    /// `[include] include_mirrors` glob expands and appends mirrors from
    /// matched fragment files.
    #[test]
    fn include_mirrors_loads_fragment_files() {
        let dir = TempDir::new().unwrap();

        // Write a mirror fragment file.
        let frag_path = dir.path().join("extra.conf");
        std::fs::write(
            &frag_path,
            r#"
[[mirrors]]
name = "debian"
provider = "rsync"
upstream = "rsync://ftp.debian.org/debian/"
"#,
        )
        .unwrap();

        // Write the main config that references the fragment via glob.
        let pattern = format!("{}/*.conf", dir.path().display());
        let main_conf = NamedTempFile::new().unwrap();
        writeln!(main_conf.as_file(), "[include]\ninclude_mirrors = {pattern:?}").unwrap();

        let cfg = hustsync_config_parser::load_worker_config(
            main_conf.path(),
            &ConfigLoadOptions::default(),
        )
        .unwrap();

        let mirrors = cfg.mirrors.unwrap();
        assert_eq!(mirrors.len(), 1, "expected 1 mirror from fragment");
        assert_eq!(mirrors[0].name.as_deref(), Some("debian"));
        // include section is cleared after expansion.
        assert_eq!(cfg.include, None);
    }

    /// When the glob matches no files the mirrors list is unchanged (no error).
    #[test]
    fn include_mirrors_no_matches_is_noop() {
        let dir = TempDir::new().unwrap();
        let pattern = format!("{}/*.conf", dir.path().display());
        let main_conf = NamedTempFile::new().unwrap();
        writeln!(
            main_conf.as_file(),
            "[include]\ninclude_mirrors = {pattern:?}"
        )
        .unwrap();

        let cfg = hustsync_config_parser::load_worker_config(
            main_conf.path(),
            &ConfigLoadOptions::default(),
        )
        .unwrap();

        // No mirrors were added; mirrors key should be absent (None) because
        // the main config had no [[mirrors]] section.
        assert!(
            cfg.mirrors.is_none() || cfg.mirrors.as_ref().map(Vec::is_empty).unwrap_or(false),
            "expected empty or absent mirrors"
        );
    }

    /// An invalid TOML fragment produces an error.
    #[test]
    fn include_mirrors_invalid_toml_in_fragment_is_error() {
        let dir = TempDir::new().unwrap();

        let frag_path = dir.path().join("bad.conf");
        std::fs::write(&frag_path, "this is not valid toml [[[").unwrap();

        let pattern = format!("{}/*.conf", dir.path().display());
        let main_conf = NamedTempFile::new().unwrap();
        writeln!(
            main_conf.as_file(),
            "[include]\ninclude_mirrors = {pattern:?}"
        )
        .unwrap();

        let res = hustsync_config_parser::load_worker_config(
            main_conf.path(),
            &ConfigLoadOptions::default(),
        );
        assert!(res.is_err(), "invalid fragment TOML must produce error");
    }
}
