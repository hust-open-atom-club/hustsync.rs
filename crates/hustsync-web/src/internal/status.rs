use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SyncStatus {
    None,
    Failed,
    Success,
    Syncing,
    PreSyncing,
    Paused,
    Disabled,
    #[serde(other)]
    Unknown,
}
