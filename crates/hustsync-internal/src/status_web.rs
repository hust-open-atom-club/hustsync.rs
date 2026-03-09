use chrono::{DateTime, Utc, TimeZone};
use serde::{Deserialize, Serialize, Serializer, Deserializer};

use crate::msg::MirrorStatus;
use crate::status::SyncStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextTime(pub DateTime<Utc>);

impl Serialize for TextTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = self.0.format("%Y-%m-%d %H:%M:%S %z").to_string();
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for TextTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let dt = DateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S %z")
            .map_err(serde::de::Error::custom)?;
        Ok(TextTime(dt.with_timezone(&Utc)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StampTime(pub DateTime<Utc>);

impl Serialize for StampTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(self.0.timestamp())
    }
}

impl<'de> Deserialize<'de> for StampTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ts = i64::deserialize(deserializer)?;
        Ok(StampTime(Utc.timestamp_opt(ts, 0).unwrap()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WebMirrorStatus {
    pub name: String,
    pub upstream: String,
    pub size: String,
    pub last_update: TextTime,
    pub last_update_ts: StampTime,
    pub last_started: TextTime,
    pub last_started_ts: StampTime,
    pub last_ended: TextTime,
    pub last_ended_ts: StampTime,
    #[serde(rename = "next_schedule")]
    pub next_scheduled: TextTime,
    #[serde(rename = "next_schedule_ts")]
    pub next_scheduled_ts: StampTime,
    pub status: SyncStatus,
    pub is_master: bool,
}


impl From<MirrorStatus> for WebMirrorStatus {
    fn from(ms: MirrorStatus) -> Self {
        WebMirrorStatus {
            name: ms.name,
            upstream: ms.upstream,
            size: ms.size,
            last_update: TextTime(ms.last_update),
            last_update_ts: StampTime(ms.last_update),
            last_started: TextTime(ms.last_started),
            last_started_ts: StampTime(ms.last_started),
            last_ended: TextTime(ms.last_ended),
            last_ended_ts: StampTime(ms.last_ended),
            next_scheduled: TextTime(ms.next_scheduled),
            next_scheduled_ts: StampTime(ms.next_scheduled),
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
    fn status_json_ser_de_should_work() {
        let t = Utc.with_ymd_and_hms(2016, 4, 16, 23, 8, 10).unwrap();

        let m = WebMirrorStatus {
            name: "hustlinux".to_string(),
            upstream: "rsync://mirrors.hust.edu.cn/hustlinux/".to_string(),
            size: "5GB".to_string(),
            last_update: TextTime(t),
            last_update_ts: StampTime(t),
            last_started: TextTime(t),
            last_started_ts: StampTime(t),
            last_ended: TextTime(t),
            last_ended_ts: StampTime(t),
            next_scheduled: TextTime(t),
            next_scheduled_ts: StampTime(t),
            status: SyncStatus::Success,
            is_master: false,
        };

        let b = serde_json::to_string(&m).expect("serialize should succeed");
        // Verify format
        assert!(b.contains("\"last_update\":\"2016-04-16 23:08:10 +0000\""));
        assert!(b.contains("\"last_update_ts\":1460848090"));
        
        let m2: WebMirrorStatus = serde_json::from_str(&b).expect("deserialize should succeed");

        assert_eq!(m2.name, m.name);
        assert_eq!(m2.last_update.0.timestamp(), m.last_update.0.timestamp());
        assert_eq!(m2.last_update_ts.0.timestamp(), m.last_update_ts.0.timestamp());
    }
}
