#![allow(clippy::unwrap_used)]

#[cfg(test)]
mod tests {
    use hustsync_config_parser::{ManagerConfig, WorkerConfig};
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;

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
}
