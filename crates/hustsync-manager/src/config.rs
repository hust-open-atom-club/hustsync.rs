use std::error::Error;
use std::path::Path;

use hustsync_config_parser;
use hustsync_config_parser::ManagerConfig;

pub fn load_config(cfg_file: impl AsRef<Path>) -> Result<ManagerConfig, Box<dyn Error>> {
    hustsync_config_parser::parse_config::<ManagerConfig>(cfg_file)
}
