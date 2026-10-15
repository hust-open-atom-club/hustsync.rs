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
}

impl SyncStatus {
    pub const ALL: [Self; 7] = [
        Self::None,
        Self::Failed,
        Self::Success,
        Self::Syncing,
        Self::PreSyncing,
        Self::Paused,
        Self::Disabled,
    ];

    /// The status as written in JSON.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Failed => "failed",
            Self::Success => "success",
            Self::Syncing => "syncing",
            Self::PreSyncing => "pre-syncing",
            Self::Paused => "paused",
            Self::Disabled => "disabled",
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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

    #[test]
    fn test_sync_status_unknown_string_is_error() {
        // Go tunasync rejects unknown status strings; there is no silent Unknown fallback.
        let result = serde_json::from_str::<SyncStatus>("\"bogus\"");
        assert!(result.is_err(), "unknown status string must deserialize as Err");

        let result2 = serde_json::from_str::<SyncStatus>("\"unknown\"");
        assert!(result2.is_err(), "literal 'unknown' string must deserialize as Err");
    }
}
