#[cfg(test)]
mod tests {
    use hustsync_config_parser::ManagerConfig;
    use std::path::PathBuf;

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

}
