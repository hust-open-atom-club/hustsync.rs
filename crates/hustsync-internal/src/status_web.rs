use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};

use crate::msg::MirrorStatus;
use crate::status::SyncStatus;

pub mod web_time_format {
    use super::*;

    pub fn serialize_text<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = date.format("%Y-%m-%d %H:%M:%S %z").to_string();
        serializer.serialize_str(&s)
    }

    pub fn serialize_ts<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(date.timestamp())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WebMirrorStatus {
    pub name: String,
    pub upstream: String,
    pub size: String,
    
    #[serde(serialize_with = "web_time_format::serialize_text")]
    pub last_update: DateTime<Utc>,
    #[serde(serialize_with = "web_time_format::serialize_ts")]
    pub last_update_ts: DateTime<Utc>,
    
    #[serde(serialize_with = "web_time_format::serialize_text")]
    pub last_started: DateTime<Utc>,
    #[serde(serialize_with = "web_time_format::serialize_ts")]
    pub last_started_ts: DateTime<Utc>,
    
    #[serde(serialize_with = "web_time_format::serialize_text")]
    pub last_ended: DateTime<Utc>,
    #[serde(serialize_with = "web_time_format::serialize_ts")]
    pub last_ended_ts: DateTime<Utc>,
    
    #[serde(rename = "next_schedule")]
    #[serde(serialize_with = "web_time_format::serialize_text")]
    pub next_schedule: DateTime<Utc>,
    #[serde(rename = "next_schedule_ts")]
    #[serde(serialize_with = "web_time_format::serialize_ts")]
    pub next_schedule_ts: DateTime<Utc>,
    
    pub status: SyncStatus,
    pub is_master: bool,
}

impl From<MirrorStatus> for WebMirrorStatus {
    fn from(ms: MirrorStatus) -> Self {
        WebMirrorStatus {
            name: ms.name,
            upstream: ms.upstream,
            size: ms.size,
            last_update: ms.last_update,
            last_update_ts: ms.last_update,
            last_started: ms.last_started,
            last_started_ts: ms.last_started,
            last_ended: ms.last_ended,
            last_ended_ts: ms.last_ended,
            next_schedule: ms.next_scheduled,
            next_schedule_ts: ms.next_scheduled,
            status: ms.status,
            is_master: ms.is_master,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};
    use serde_json;

    #[test]
    fn status_json_serialization_format() {
        let t = Utc.with_ymd_and_hms(2016, 4, 16, 23, 8, 10).unwrap();

        let m = WebMirrorStatus {
            name: "hustlinux".to_string(),
            upstream: "rsync://mirrors.hust.edu.cn/hustlinux/".to_string(),
            size: "5GB".to_string(),
            last_update: t,
            last_update_ts: t,
            last_started: t,
            last_started_ts: t,
            last_ended: t,
            last_ended_ts: t,
            next_schedule: t,
            next_schedule_ts: t,
            status: SyncStatus::Success,
            is_master: false,
        };

        let b = serde_json::to_value(&m).expect("serialize should succeed");
        
        // Check text format: "YYYY-MM-DD HH:MM:SS ±ZZZZ"
        // Note: Utc in chrono formats +0000
        assert_eq!(b["last_update"], "2016-04-16 23:08:10 +0000");
        
        // Check timestamp format: integer
        assert_eq!(b["last_update_ts"], t.timestamp());
        
        assert_eq!(b["next_schedule"], "2016-04-16 23:08:10 +0000");
        assert_eq!(b["next_schedule_ts"], t.timestamp());
    }

    #[test]
    fn build_web_mirror_status_should_work() {
        let now = Utc::now();
        let m = MirrorStatus {
            name: "arch-sync3".to_string(),
            worker: "testWorker".to_string(),
            is_master: true,
            status: SyncStatus::Failed,
            last_update: now - Duration::minutes(30),
            last_started: now - Duration::minutes(1),
            last_ended: now,
            next_scheduled: now + Duration::minutes(5),
            upstream: "mirrors.tuna.tsinghua.edu.cn".to_string(),
            size: "4GB".to_string(),
            error_msg: "Network error".to_string(),
        };

        let m2: WebMirrorStatus = WebMirrorStatus::from(m);

        assert_eq!(m2.name, "arch-sync3");
        assert_eq!(m2.status, SyncStatus::Failed);
        assert_eq!(m2.last_update, m2.last_update_ts);
    }
}
