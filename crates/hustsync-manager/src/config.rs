use std::path::Path;

use hustsync_config_parser::ManagerConfig;

use crate::ManagerError;

pub fn load_config(cfg_file: impl AsRef<Path>) -> Result<ManagerConfig, ManagerError> {
    if cfg_file.as_ref().as_os_str().is_empty() {
        return Ok(ManagerConfig::default());
    }

    Ok(hustsync_config_parser::parse_config::<ManagerConfig>(
        cfg_file,
    )?)
}
