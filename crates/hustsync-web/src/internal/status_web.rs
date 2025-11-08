use chrono::DateTime;
use serde::Deserialize;
use serde::Serialize;

use crate::internal::msg::MirrorStatus;
use crate::internal::status::SyncStatus;

type TextTime = DateTime<chrono::Utc>;
type StampTime = DateTime<chrono::Utc>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct WebMirrorStatus {
    name: String,
    upstream: String,
    size: String,
    laste_update: TextTime,
    last_update_ts: StampTime,
    last_started: TextTime,
    last_started_ts: StampTime,
    last_ended: TextTime,
    last_ended_ts: StampTime,
    next_scheduled: TextTime,
    next_scheduled_ts: StampTime,
    status: SyncStatus,
    is_master: bool,
}

impl From<MirrorStatus> for WebMirrorStatus {
    fn from(ms: MirrorStatus) -> Self {
        WebMirrorStatus {
            name: ms.name,
            upstream: ms.upstream,
            size: ms.size,
            laste_update: ms.last_update,
            last_update_ts: ms.last_update,
            last_started: ms.last_started,
            last_started_ts: ms.last_started,
            last_ended: ms.lastt_ended,
            last_ended_ts: ms.lastt_ended,
            next_scheduled: ms.next_scheduled,
            next_scheduled_ts: ms.next_scheduled,
            status: ms.status,
            is_master: ms.is_master,
        }
    }
}
