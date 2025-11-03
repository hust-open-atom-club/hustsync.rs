use serde::{Deserialize, de::DeserializeOwned};
use std::{error::Error, fs, path::Path};

#[derive(Debug, Deserialize, PartialEq)]
pub struct ManagerConfig {
    pub server: Option<ManagerServerConfig>,
    pub files: Option<ManagerFileConfig>,
    pub debug: Option<bool>,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        ManagerConfig {
            server: Some(ManagerServerConfig::default()),
            files: Some(ManagerFileConfig::default()),
            debug: Some(false),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ManagerServerConfig {
    pub addr: Option<String>,
    pub port: Option<u16>,
    pub ssl_cert: Option<String>,
    pub ssl_key: Option<String>,
}

impl Default for ManagerServerConfig {
    fn default() -> Self {
        ManagerServerConfig {
            addr: Some("127.0.0.1".into()),
            port: Some(12345),
            ssl_cert: Some("".into()),
            ssl_key: Some("".into()),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ManagerFileConfig {
    pub db_type: Option<String>,
    pub db_file: Option<String>,
    pub ca_cert: Option<String>,
}

impl Default for ManagerFileConfig {
    fn default() -> Self {
        ManagerFileConfig {
            db_type: Some("bolt".into()),
            db_file: Some("/tmp/tunasync/manager.db".into()),
            ca_cert: Some("".into()),
        }
    }
}

pub fn parse_config<P, T>(path: P) -> Result<T, Box<dyn Error>>
where
    P: AsRef<Path>,
    T: DeserializeOwned,
{
    let rel_path = fs::canonicalize(path.as_ref())?;
    let config_content = fs::read_to_string(rel_path)?;
    let config: T = toml::from_str(&config_content)?;
    Ok(config)
}
