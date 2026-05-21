use std::collections::HashMap;

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;

use crate::status::SyncStatus;

fn null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MirrorStatus {
    pub name: String,
    pub worker: String,
    pub upstream: String,
    pub size: String,
    pub error_msg: String,
    pub last_update: DateTime<Utc>,
    pub last_started: DateTime<Utc>,
    pub last_ended: DateTime<Utc>,
    #[serde(rename = "next_schedule")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MirrorSchedules {
    pub schedules: Vec<MirrorSchedule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MirrorSchedule {
    pub name: String,
    pub next_schedule: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CmdVerb {
    Start,
    Stop,
    Disable,
    Restart,
    Ping,
    Reload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WorkerCmd {
    #[serde(default, deserialize_with = "null_as_default")]
    pub options: HashMap<String, bool>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub args: Vec<String>,
    pub mirror_id: String,
    pub cmd: CmdVerb,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ClientCmd {
    #[serde(default, deserialize_with = "null_as_default")]
    pub options: HashMap<String, bool>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub args: Vec<String>,
    pub mirror_id: String,
    pub worker_id: String,
    pub cmd: CmdVerb,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn worker_cmd_accepts_go_null_args_and_options() {
        let cmd: WorkerCmd = serde_json::from_str(
            r#"{"options":null,"args":null,"mirror_id":"archlinux","cmd":"ping"}"#,
        )
        .unwrap();

        assert!(cmd.options.is_empty());
        assert!(cmd.args.is_empty());
        assert_eq!(cmd.mirror_id, "archlinux");
        assert_eq!(cmd.cmd, CmdVerb::Ping);
    }

    #[test]
    fn client_cmd_accepts_go_null_args_and_options() {
        let cmd: ClientCmd = serde_json::from_str(
            r#"{"options":null,"args":null,"worker_id":"w1","mirror_id":"archlinux","cmd":"restart"}"#,
        )
        .unwrap();

        assert!(cmd.options.is_empty());
        assert!(cmd.args.is_empty());
        assert_eq!(cmd.worker_id, "w1");
        assert_eq!(cmd.mirror_id, "archlinux");
        assert_eq!(cmd.cmd, CmdVerb::Restart);
    }
}
