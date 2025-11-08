use std::collections::HashMap;

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;

use crate::internal::status::SyncStatus;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MirrorStatus {
    pub(super) name: String,
    pub(super) worker: String,
    pub(super) upstream: String,
    pub(super) size: String,
    // TODO 错误不一定存在，考虑改成 Option<String>
    pub(super) error_msg: String,
    pub(super) last_update: DateTime<Utc>,
    pub(super) last_started: DateTime<Utc>,
    pub(super) lastt_ended: DateTime<Utc>,
    pub(super) next_scheduled: DateTime<Utc>,
    pub(super) status: SyncStatus,
    pub(super) is_master: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct WorkerStatus {
    id: String,
    url: String,
    token: String,
    last_online: DateTime<Utc>,
    last_register: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct MirrorSchedules {
    schedules: Vec<MirrorSchedule>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct MirrorSchedule {
    name: String,
    next_schedule: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CmdVerb {
    CmdStart,
    CmdStop,
    CmdDisable,
    CmdRestart,
    CmdPing,
    CmdReload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct WorkerCmd {
    options: HashMap<String, bool>,
    args: Vec<String>,
    mirror_id: String,
    cmd: CmdVerb,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct ClientCmd {
    options: HashMap<String, bool>,
    args: Vec<String>,
    mirror_id: String,
    worker_id: String,
    cmd: CmdVerb,
}
