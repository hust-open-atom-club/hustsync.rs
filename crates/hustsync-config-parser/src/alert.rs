//! `[alert]` of the manager: when a mirror's failures turn into an alert.

use serde::Deserialize;

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AlertConfig {
    /// Failed runs in a row that raise `mirror failing`; 0 turns it off.
    #[serde(default = "AlertConfig::failures")]
    pub failures: u32,
}

impl AlertConfig {
    const fn failures() -> u32 {
        3
    }
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            failures: Self::failures(),
        }
    }
}
