use chrono::DateTime;
use serde::Deserialize;
use serde::Serialize;

use crate::msg::MirrorStatus;
use crate::status::SyncStatus;

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
            last_ended: ms.last_ended,
            last_ended_ts: ms.last_ended,
            next_scheduled: ms.next_scheduled,
            next_scheduled_ts: ms.next_scheduled,
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
            laste_update: t,
            last_update_ts: t,
            last_started: t,
            last_started_ts: t,
            last_ended: t,
            last_ended_ts: t,
            next_scheduled: t,
            next_scheduled_ts: t,
            status: SyncStatus::Success,
            is_master: false,
        };

        let b = serde_json::to_string(&m).expect("serialize should succeed");
        let m2: WebMirrorStatus = serde_json::from_str(&b).expect("deserialize should succeed");

        assert_eq!(m2.name, m.name);
        assert_eq!(m2.upstream, m.upstream);
        assert_eq!(m2.size, m.size);

        assert_eq!(m2.laste_update.timestamp(), m.laste_update.timestamp());
        assert_eq!(m2.last_update_ts.timestamp(), m.last_update_ts.timestamp());
        assert_eq!(
            m2.laste_update.timestamp_nanos_opt(),
            m.laste_update.timestamp_nanos_opt()
        );
        assert_eq!(
            m2.last_update_ts.timestamp_nanos_opt(),
            m.last_update_ts.timestamp_nanos_opt()
        );

        assert_eq!(m2.last_started.timestamp(), m.last_started.timestamp());
        assert_eq!(
            m2.last_started_ts.timestamp(),
            m.last_started_ts.timestamp()
        );
        assert_eq!(
            m2.last_started.timestamp_nanos_opt(),
            m.last_started.timestamp_nanos_opt()
        );
        assert_eq!(
            m2.last_started_ts.timestamp_nanos_opt(),
            m.last_started_ts.timestamp_nanos_opt()
        );

        assert_eq!(m2.last_ended.timestamp(), m.last_ended.timestamp());
        assert_eq!(m2.last_ended_ts.timestamp(), m.last_ended_ts.timestamp());
        assert_eq!(
            m2.last_ended.timestamp_nanos_opt(),
            m.last_ended.timestamp_nanos_opt()
        );
        assert_eq!(
            m2.last_ended_ts.timestamp_nanos_opt(),
            m.last_ended_ts.timestamp_nanos_opt()
        );

        assert_eq!(m2.next_scheduled.timestamp(), m.next_scheduled.timestamp());
        assert_eq!(
            m2.next_scheduled_ts.timestamp(),
            m.next_scheduled_ts.timestamp()
        );
        assert_eq!(
            m2.next_scheduled.timestamp_nanos_opt(),
            m.next_scheduled.timestamp_nanos_opt()
        );
        assert_eq!(
            m2.next_scheduled_ts.timestamp_nanos_opt(),
            m.next_scheduled_ts.timestamp_nanos_opt()
        );

        assert_eq!(m2.is_master, m.is_master);
        assert_eq!(m2.status, m.status);
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
        assert_eq!(m2.upstream, "mirrors.tuna.tsinghua.edu.cn");
        assert_eq!(m2.size, "4GB");
        assert_eq!(m2.status, SyncStatus::Failed);

        let lu = m2.laste_update.timestamp();
        let lu_ts = m2.last_update_ts.timestamp();
        assert_eq!(lu, lu_ts);
        let ls = m2.last_started.timestamp();
        let ls_ts = m2.last_started_ts.timestamp();
        assert_eq!(ls, ls_ts);
        let le = m2.last_ended.timestamp();
        let le_ts = m2.last_ended_ts.timestamp();
        assert_eq!(le, le_ts);
        let ns = m2.next_scheduled.timestamp();
        let ns_ts = m2.next_scheduled_ts.timestamp();
        assert_eq!(ns, ns_ts);
    }
}
