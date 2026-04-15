use std::path::Path;

use hustsync_config_parser::ManagerConfig;

use crate::ManagerError;

pub fn load_config(cfg_file: impl AsRef<Path>) -> Result<ManagerConfig, ManagerError> {
    hustsync_config_parser::parse_config::<ManagerConfig>(cfg_file)
        .map_err(|e| ManagerError::Config(e.to_string()))
}
