use std::collections::HashMap;

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;

use crate::status::SyncStatus;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MirrorStatus {
    pub name: String,
    pub worker: String,
    pub upstream: String,
    pub size: String,
    // TODO 错误不一定存在，考虑改成 Option<String>
    pub error_msg: String,
    pub last_update: DateTime<Utc>,
    pub last_started: DateTime<Utc>,
    pub last_ended: DateTime<Utc>,
    pub next_scheduled: DateTime<Utc>,
    pub status: SyncStatus,
    pub is_master: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WorkerStatus {
    pub id: String,
    pub url: String,
    pub token: String,
    pub last_online: DateTime<Utc>,
    pub last_register: DateTime<Utc>,
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
    Start,
    Stop,
    Disable,
    Restart,
    Ping,
    Reload,
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
