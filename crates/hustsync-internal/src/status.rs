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

#[cfg(test)]
mod tests {
    use super::SyncStatus;
    use serde_json;
    #[test]
    fn test_sync_status_json_ser_de() {
        let b = serde_json::to_vec(&SyncStatus::PreSyncing).unwrap();
        assert_eq!(b, b"\"pre-syncing\"");

        let s: SyncStatus = serde_json::from_slice(b"\"failed\"").unwrap();
        assert_eq!(s, SyncStatus::Failed);
    }
}
